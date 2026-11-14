// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use whippet_engine::ShardSnapshot;

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

/// Serialises snapshot writes and refuses to overwrite newer state.
///
/// `save` is blocking (temp file, fsync, rename), so this type is meant to be
/// driven from a dedicated writer thread/task, never from the shard loop.
pub struct SnapshotWriter {
    store: SnapshotStore,
    last_written: Option<u64>,
}

impl SnapshotWriter {
    pub fn new(store: SnapshotStore) -> Self {
        Self {
            store,
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
    /// the guard untouched so a newer snapshot can still be persisted.
    pub fn write(&mut self, request: SequencedSnapshot) -> Result<Option<PathBuf>, SnapshotError> {
        if !is_newer(self.last_written, request.sequence) {
            return Ok(None);
        }
        let path = self.store.save(&request.snapshot)?;
        self.last_written = Some(request.sequence);
        Ok(Some(path))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use whippet_core::config::ExchangeConfig;
    use whippet_engine::ShardEngine;

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
    fn sequence_guard_is_strictly_monotonic() {
        assert!(is_newer(None, 1));
        assert!(!is_newer(Some(1), 1));
        assert!(!is_newer(Some(2), 1));
        assert!(is_newer(Some(1), 2));
    }

    #[test]
    fn stale_snapshot_never_overwrites_newer_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let engine = ShardEngine::new(3, &ExchangeConfig::default());
        let path = store.path(3);
        let mut writer = SnapshotWriter::new(store);

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
        let engine = ShardEngine::new(3, &ExchangeConfig::default());
        let mut writer = SnapshotWriter::new(store);

        let result = writer.write(SequencedSnapshot {
            sequence: 4,
            snapshot: engine.snapshot_state(),
        });
        assert!(result.is_err());
        assert_eq!(writer.last_written(), None);
    }
}
