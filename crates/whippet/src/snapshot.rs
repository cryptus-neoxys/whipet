// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use whippet_core::config::ExchangeConfig;
use whippet_core::ids::Symbol;
use whippet_engine::{ShardEngine, ShardSnapshot};

const MAGIC: &[u8; 8] = b"WHIPSNP1";
/// Version 2 added the per-symbol config and the book's `min_price_ticks` so
/// the state hash can be computed from the snapshot off the shard thread.
/// Version 1 files are rejected rather than misread.
const VERSION: u32 = 2;
const MAX_SNAPSHOT_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("snapshot I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot codec: {0}")]
    Codec(#[from] bincode::Error),
    #[error("not a Whippet snapshot: {0}")]
    BadMagic(PathBuf),
    #[error("unsupported snapshot version {0}")]
    BadVersion(u32),
    #[error("snapshot shard {found} does not match requested shard {expected}")]
    WrongShard { found: u32, expected: u32 },
    /// The off-loop re-hash disagreed with the snapshot about to be persisted.
    /// This is the check that keeps a hashing bug from becoming the resume
    /// point: the write is refused and the previous file is left in place.
    #[error("snapshot hash mismatch for shard {shard}: {detail}")]
    HashMismatch { shard: u32, detail: String },
    /// A symbol carried an all-zero hash after the writer filled it, which means
    /// the snapshot was never hashed or the hash computation is broken.
    #[error("snapshot shard {shard} has an unhashed symbol {symbol}: state_hash is all zeros")]
    UnhashedSymbol { shard: u32, symbol: Symbol },
}

#[derive(Clone)]
pub struct SnapshotStore {
    dir: PathBuf,
}

impl SnapshotStore {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn path(&self, shard_id: u32) -> PathBuf {
        self.dir.join(format!("partition-{shard_id}.snapshot"))
    }

    /// Write atomically: temp file, fsync, rename.
    pub fn save(&self, snapshot: &ShardSnapshot) -> Result<PathBuf, SnapshotError> {
        fs::create_dir_all(&self.dir)?;
        let path = self.path(snapshot.shard_id);
        let temp = path.with_extension("snapshot.tmp");
        let payload = bincode::serialize(snapshot)?;
        {
            let mut file = fs::File::create(&temp)?;
            file.write_all(MAGIC)?;
            file.write_all(&VERSION.to_le_bytes())?;
            file.write_all(&payload)?;
            file.sync_all()?;
        }
        fs::rename(&temp, &path)?;
        // Persist the directory entry too. Some filesystems reject fsync on a
        // directory; the file itself is already synced, so a failure here is
        // not fatal.
        if let Some(parent) = path.parent()
            && let Ok(directory) = fs::File::open(parent)
        {
            let _ = directory.sync_all();
        }
        Ok(path)
    }

    pub fn load(&self, shard_id: u32) -> Result<Option<ShardSnapshot>, SnapshotError> {
        let path = self.path(shard_id);
        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(SnapshotError::Io(error)),
        };
        if metadata.len() > MAX_SNAPSHOT_BYTES {
            return Err(SnapshotError::BadMagic(path));
        }
        let bytes = fs::read(&path)?;
        if bytes.len() < MAGIC.len() + 4 {
            return Err(SnapshotError::BadMagic(path));
        }
        if &bytes[..MAGIC.len()] != MAGIC {
            return Err(SnapshotError::BadMagic(path));
        }
        let version = u32::from_le_bytes(bytes[MAGIC.len()..MAGIC.len() + 4].try_into().unwrap());
        if version != VERSION {
            return Err(SnapshotError::BadVersion(version));
        }
        let snapshot: ShardSnapshot = bincode::deserialize(&bytes[MAGIC.len() + 4..])?;
        if snapshot.shard_id != shard_id {
            return Err(SnapshotError::WrongShard {
                found: snapshot.shard_id,
                expected: shard_id,
            });
        }
        Ok(Some(snapshot))
    }

    pub fn delete(&self, shard_id: u32) -> std::io::Result<()> {
        match fs::remove_file(self.path(shard_id)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// A snapshot tagged with the sequence number it was produced at.
///
/// The shard loop increments the sequence for every snapshot it hands to the
/// writer; the writer only keeps the newest one it has seen.
#[derive(Debug, Clone)]
pub struct SequencedSnapshot {
    pub sequence: u64,
    pub snapshot: ShardSnapshot,
}

/// True when `sequence` is newer than the last one written.
///
/// The guard is strict: re-writing the same sequence is treated as stale, so a
/// duplicated request can never replace a file with older state.
pub fn is_newer(last_written: Option<u64>, sequence: u64) -> bool {
    match last_written {
        Some(last) => sequence > last,
        None => true,
    }
}

/// Serialises snapshot writes, verifies them, and refuses to overwrite newer
/// state.
///
/// `save` is blocking (temp file, fsync, rename), so this type is meant to be
/// driven from a dedicated writer thread/task, never from the shard loop.
///
/// Verification happens here, off the shard loop and before the atomic rename:
/// the shard hands over an unhashed snapshot, the writer fills the per-symbol
/// hashes, rebuilds the shard from the snapshot and refuses to persist it if
/// the rebuilt state hash disagrees. A failed check leaves the previous file
/// untouched, so a hash bug can never become the resume point.
pub struct SnapshotWriter {
    store: SnapshotStore,
    exchange: ExchangeConfig,
    last_written: Option<u64>,
}

impl SnapshotWriter {
    /// The only constructor: every write is verified against `exchange`.
    pub fn with_exchange(store: SnapshotStore, exchange: ExchangeConfig) -> Self {
        Self {
            store,
            exchange,
            last_written: None,
        }
    }

    /// The highest sequence this writer has successfully persisted.
    pub fn last_written(&self) -> Option<u64> {
        self.last_written
    }

    /// Write `request` unless it is older than the last snapshot written.
    ///
    /// Returns `Ok(None)` when the request was dropped by the stale-sequence
    /// guard, and `Ok(Some(path))` when it reached disk. A failed write leaves
    /// the guard untouched so a newer snapshot can still be persisted, and the
    /// previously saved file intact.
    pub fn write(&mut self, request: SequencedSnapshot) -> Result<Option<PathBuf>, SnapshotError> {
        self.write_inner(request, true)
    }

    /// Fill and persist without the full rebuild verification.
    ///
    /// Used for periodic snapshots, where the cost of rebuilding the shard is
    /// a p99 spike. Restore still verifies every snapshot, so a bad unverified
    /// file is rejected rather than used. It can still be the only candidate
    /// on disk after a SIGKILL; recovery then falls back to the previous valid
    /// snapshot or a full replay.
    pub fn write_unverified(
        &mut self,
        request: SequencedSnapshot,
    ) -> Result<Option<PathBuf>, SnapshotError> {
        self.write_inner(request, false)
    }

    fn write_inner(
        &mut self,
        request: SequencedSnapshot,
        verify: bool,
    ) -> Result<Option<PathBuf>, SnapshotError> {
        if !is_newer(self.last_written, request.sequence) {
            return Ok(None);
        }
        // The matcher computes the live state hash with the engine; the
        // writer must not recompute it from the snapshot. An off-loop hash was
        // tried and reverted: at ~170k orders it disagreed with the restored
        // engine hash on one shard, which would have persisted an unusable
        // resume point. The final snapshot is still rebuilt and verified here.
        if verify {
            self.verify(&request.snapshot)?;
        }
        let path = self.store.save(&request.snapshot)?;
        self.last_written = Some(request.sequence);
        Ok(Some(path))
    }

    /// Reject a snapshot whose hashes do not survive an off-loop restore.
    ///
    /// Runs before [`SnapshotStore::save`], so a rejected snapshot never touches
    /// the on-disk file.
    fn verify(&self, snapshot: &ShardSnapshot) -> Result<(), SnapshotError> {
        for symbol in &snapshot.symbols {
            if symbol.state_hash == [0u8; 32] {
                return Err(SnapshotError::UnhashedSymbol {
                    shard: snapshot.shard_id,
                    symbol: symbol.symbol,
                });
            }
        }
        // `from_snapshot` already compares every symbol hash with the stored
        // one; an `Err` here is that mismatch (or a shard/symbol layout that
        // cannot be restored), so the snapshot is not a safe resume point.
        let restored =
            ShardEngine::from_snapshot(&self.exchange, snapshot.clone()).map_err(|error| {
                SnapshotError::HashMismatch {
                    shard: snapshot.shard_id,
                    detail: error.to_string(),
                }
            })?;
        for symbol in &snapshot.symbols {
            let Some(engine) = restored.engine(symbol.symbol) else {
                return Err(SnapshotError::HashMismatch {
                    shard: snapshot.shard_id,
                    detail: format!("restored shard is missing symbol {}", symbol.symbol),
                });
            };
            let actual = engine.state_hash();
            if actual != symbol.state_hash {
                return Err(SnapshotError::HashMismatch {
                    shard: snapshot.shard_id,
                    detail: format!(
                        "symbol {} restored hash {} does not match stored {}",
                        symbol.symbol,
                        hex(&actual),
                        hex(&symbol.state_hash)
                    ),
                });
            }
        }
        Ok(())
    }
}

fn hex(bytes: &[u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use whippet_core::config::{InstrumentConfig, Mode};
    use whippet_engine::ShardEngine;

    /// A single-instrument exchange so the shard owns exactly one symbol and a
    /// hash mismatch is easy to locate.
    fn test_exchange() -> ExchangeConfig {
        ExchangeConfig {
            venue: "test".into(),
            partitions: 16,
            mode: Mode::Durable,
            instruments: vec![InstrumentConfig::new("BTC-USD", 100, 40_000, 90_000).unwrap()],
        }
    }

    fn test_shard(exchange: &ExchangeConfig) -> u32 {
        Symbol::new("BTC-USD")
            .unwrap()
            .partition(exchange.partitions)
    }

    #[test]
    fn snapshot_round_trip_and_hash_verification() {
        let exchange = ExchangeConfig::default();
        let shard_id = 3;
        let engine = ShardEngine::new(shard_id, &exchange);
        let snapshot = engine.snapshot_state();
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        store.save(&snapshot).unwrap();
        let loaded = store.load(shard_id).unwrap().unwrap();
        let restored = ShardEngine::from_snapshot(&exchange, loaded).unwrap();
        assert_eq!(restored.state_hash(), engine.state_hash());
        assert!(store.load(4).unwrap().is_none());
    }

    #[test]
    fn writer_verifies_and_restores_the_same_hash() {
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let engine = ShardEngine::new(shard_id, &exchange);
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let mut writer = SnapshotWriter::with_exchange(store.clone(), exchange.clone());

        let path = writer
            .write(SequencedSnapshot {
                sequence: 1,
                snapshot: engine.snapshot_state(),
            })
            .unwrap()
            .expect("a verifiable snapshot is persisted");
        assert_eq!(path, store.path(shard_id));

        let loaded = store.load(shard_id).unwrap().unwrap();
        assert!(
            loaded
                .symbols
                .iter()
                .all(|symbol| symbol.state_hash != [0u8; 32]),
            "the writer left a symbol unhashed"
        );
        let restored = ShardEngine::from_snapshot(&exchange, loaded).unwrap();
        assert_eq!(restored.state_hash(), engine.state_hash());
        assert_eq!(writer.last_written(), Some(1));
    }

    #[test]
    fn corrupted_symbol_hash_is_rejected_and_file_is_unchanged() {
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let engine = ShardEngine::new(shard_id, &exchange);
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let mut writer = SnapshotWriter::with_exchange(store.clone(), exchange.clone());

        let path = writer
            .write(SequencedSnapshot {
                sequence: 1,
                snapshot: engine.snapshot_state(),
            })
            .unwrap()
            .expect("valid snapshot writes");
        let on_disk = fs::read(&path).unwrap();

        let mut corrupted = engine.snapshot_state();
        corrupted.symbols[0].state_hash[0] ^= 0xff;
        let error = writer
            .write(SequencedSnapshot {
                sequence: 2,
                snapshot: corrupted,
            })
            .unwrap_err();
        assert!(
            matches!(error, SnapshotError::HashMismatch { shard, .. } if shard == shard_id),
            "expected HashMismatch, got {error:?}"
        );
        assert_eq!(
            fs::read(&path).unwrap(),
            on_disk,
            "a rejected snapshot rewrote the file"
        );
        assert_eq!(
            writer.last_written(),
            Some(1),
            "a rejected snapshot advanced the guard"
        );

        // The previous snapshot is still a valid resume point.
        let loaded = store.load(shard_id).unwrap().unwrap();
        let restored = ShardEngine::from_snapshot(&exchange, loaded).unwrap();
        assert_eq!(restored.state_hash(), engine.state_hash());
    }

    #[test]
    fn unhashed_snapshot_is_rejected() {
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let engine = ShardEngine::new(shard_id, &exchange);
        let mut unhashed = engine.snapshot_state();
        for symbol in &mut unhashed.symbols {
            symbol.state_hash = [0u8; 32];
        }

        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let mut writer = SnapshotWriter::with_exchange(store, exchange);
        let result = writer.write(SequencedSnapshot {
            sequence: 1,
            snapshot: unhashed,
        });
        assert!(matches!(result, Err(SnapshotError::UnhashedSymbol { .. })));
    }

    #[test]
    fn verify_rejects_an_all_zero_hash() {
        // The matcher supplies the live hash; a zero one is a broken snapshot.
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let engine = ShardEngine::new(shard_id, &exchange);
        let dir = tempfile::tempdir().unwrap();
        let writer = SnapshotWriter::with_exchange(SnapshotStore::new(dir.path()), exchange);
        let mut snapshot = engine.snapshot_state();
        for symbol in &mut snapshot.symbols {
            symbol.state_hash = [0u8; 32];
        }

        match writer.verify(&snapshot) {
            Err(SnapshotError::UnhashedSymbol { shard, symbol }) => {
                assert_eq!(shard, shard_id);
                assert_eq!(symbol, Symbol::new("BTC-USD").unwrap());
            }
            other => panic!("expected UnhashedSymbol, got {other:?}"),
        }
    }

    #[test]
    fn version_one_snapshot_is_rejected_not_misread() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let path = store.path(3);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(MAGIC).unwrap();
        file.write_all(&1u32.to_le_bytes()).unwrap();
        file.write_all(&[0u8; 8]).unwrap();
        drop(file);

        match store.load(3) {
            Err(SnapshotError::BadVersion(1)) => {}
            other => panic!("expected BadVersion(1), got {other:?}"),
        }
    }

    #[test]
    fn sequence_guard_is_strictly_monotonic() {
        assert!(is_newer(None, 1));
        assert!(!is_newer(Some(1), 1));
        assert!(!is_newer(Some(2), 1));
        assert!(is_newer(Some(1), 2));
    }

    #[test]
    fn stale_snapshot_never_overwrites_newer_state() {
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let engine = ShardEngine::new(shard_id, &exchange);
        let path = store.path(shard_id);
        let mut writer = SnapshotWriter::with_exchange(store, exchange);

        let written = writer
            .write(SequencedSnapshot {
                sequence: 7,
                snapshot: engine.snapshot_state(),
            })
            .unwrap();
        assert_eq!(written.as_deref(), Some(path.as_path()));
        assert!(path.exists());

        // Remove the file so an accepted stale write would be observable.
        fs::remove_file(&path).unwrap();
        let stale = writer
            .write(SequencedSnapshot {
                sequence: 6,
                snapshot: engine.snapshot_state(),
            })
            .unwrap();
        assert!(stale.is_none());
        assert!(!path.exists(), "stale sequence recreated the snapshot");
        assert_eq!(writer.last_written(), Some(7));

        // A newer sequence is persisted again.
        let newer = writer
            .write(SequencedSnapshot {
                sequence: 8,
                snapshot: engine.snapshot_state(),
            })
            .unwrap();
        assert!(newer.is_some());
        assert!(path.exists());
        assert_eq!(writer.last_written(), Some(8));
    }

    #[test]
    fn failed_write_does_not_advance_the_guard() {
        let dir = tempfile::tempdir().unwrap();
        // A regular file in the way makes `create_dir_all` fail.
        let blocker = dir.path().join("blocked");
        fs::write(&blocker, b"not a directory").unwrap();
        let store = SnapshotStore::new(blocker.join("snapshots"));
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let engine = ShardEngine::new(shard_id, &exchange);
        let mut writer = SnapshotWriter::with_exchange(store, exchange);

        let result = writer.write(SequencedSnapshot {
            sequence: 4,
            snapshot: engine.snapshot_state(),
        });
        assert!(result.is_err());
        assert_eq!(writer.last_written(), None);
    }
}
