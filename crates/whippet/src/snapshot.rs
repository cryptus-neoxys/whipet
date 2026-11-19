// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use bincode::Options;
use whippet_core::config::ExchangeConfig;
use whippet_core::ids::Symbol;
use whippet_engine::{ShardEngine, ShardSnapshot, SymbolSnapshot, fill_symbol_hashes};

const MAGIC: &[u8; 8] = b"WHIPSNP1";
/// Version 6 added the per-symbol `risk` table, so a restored engine enforces
/// the same limits and resumes the same exposure without replaying `SetRisk`.
/// Version 5 added the per-symbol `session` field (`SessionState`), so a
/// restored engine knows whether new orders are admitted without replaying the
/// session command again. Version 4 stores the dedup terminal-eviction queue as
/// `u32` indices into the insertion-order walk rather than `OrderId`s, which
/// cuts that payload from 40 to 4 bytes per terminal id (~3.4 MB at the 100k
/// dedup window) without changing the state hash. Version 3 added the terminal
/// order; version 2 added the per-symbol config and the book's
/// `min_price_ticks`. Older versions are rejected rather than misread: every
/// layout change is a hard cut, not a compatibility shim.
const VERSION: u32 = 6;
const MAX_SNAPSHOT_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("snapshot I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot codec: {0}")]
    Codec(#[from] bincode::Error),
    #[error("not a Whippet snapshot: {0}")]
    BadMagic(PathBuf),
    /// The file, or a length prefix inside it, asks for more memory than a
    /// snapshot can legitimately need. Refused before the allocation so a
    /// truncated or crafted file cannot turn into an OOM.
    #[error("snapshot {path} is too large to decode: {detail}")]
    TooLarge { path: PathBuf, detail: String },
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
    /// A shard snapshot with no symbols is not a checkpoint: restoring it would
    /// fabricate an engine layout the exchange never configured.
    #[error("snapshot shard {shard} has no symbols")]
    EmptySnapshot { shard: u32 },
    /// `symbols` must be strictly sorted, which also rules out duplicates. The
    /// order is the canonical order the state hash and the verify rebuild use.
    #[error("snapshot shard {shard} symbols are not strictly sorted: {detail}")]
    UnsortedSymbols { shard: u32, detail: String },
    /// A per-symbol structural invariant needed to restore at all. The writer's
    /// rebuild verification is the full check; this is the cheap subset the
    /// file must already satisfy.
    #[error("snapshot shard {shard} symbol {symbol} is invalid: {detail}")]
    InvalidSymbol {
        shard: u32,
        symbol: Symbol,
        detail: String,
    },
    /// Every symbol applied zero commands, so the snapshot carries no offset to
    /// resume after. The matcher already gates final snapshots on this; the
    /// writer refuses one anyway rather than persist a file that resumes at or
    /// before the log origin.
    #[error("snapshot shard {shard} has no resume point: every symbol has zero commands")]
    NoResumePoint { shard: u32 },
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
    ///
    /// The structural invariants ([`SnapshotInvariants::check_invariants`]) are
    /// checked *before* the temp file is opened, so a bad snapshot never exists
    /// on disk, not even transiently. A failure at any later stage removes the
    /// temp file and leaves the previous snapshot in place: the rename is the
    /// only step that can replace a resume point.
    ///
    /// The payload is streamed through a `BufWriter` rather than built as one
    /// `Vec` first. A snapshot at the dedup window is >10 MB, so buffering it
    /// whole bought nothing but a second full-size allocation and copy on the
    /// writer thread; the bytes on disk are identical either way.
    pub fn save(&self, snapshot: &ShardSnapshot) -> Result<PathBuf, SnapshotError> {
        snapshot.check_invariants()?;
        fs::create_dir_all(&self.dir)?;
        let path = self.path(snapshot.shard_id);
        let temp = temp_path(&path);
        if let Err(error) = write_temp(&temp, snapshot) {
            remove_temp(&temp);
            return Err(error);
        }
        if let Err(error) = fs::rename(&temp, &path) {
            remove_temp(&temp);
            return Err(SnapshotError::Io(error));
        }
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
            return Err(SnapshotError::TooLarge {
                path,
                detail: format!(
                    "{} bytes exceeds the {MAX_SNAPSHOT_BYTES}-byte cap",
                    metadata.len()
                ),
            });
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
        let snapshot = decode_snapshot(&path, &bytes[MAGIC.len() + 4..])?;
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

/// The sibling path a snapshot is written to before the atomic rename.
fn temp_path(path: &Path) -> PathBuf {
    path.with_extension("snapshot.tmp")
}

/// Serialize `snapshot` into `temp`, syncing it before the caller renames it.
fn write_temp(temp: &Path, snapshot: &ShardSnapshot) -> Result<(), SnapshotError> {
    use std::io::BufWriter;

    let mut file = fs::File::create(temp)?;
    file.write_all(MAGIC)?;
    file.write_all(&VERSION.to_le_bytes())?;
    {
        let mut buffered = BufWriter::with_capacity(1 << 20, &mut file);
        bincode::serialize_into(&mut buffered, snapshot)?;
        buffered.flush()?;
    }
    file.sync_all()?;
    Ok(())
}

/// Best-effort removal of a temp file after a failed write or rename.
///
/// The previous snapshot is the resume point, so leaking a partial temp file is
/// only a disk-space bug; failing to remove it must not mask the write error.
fn remove_temp(temp: &Path) {
    let _ = fs::remove_file(temp);
}

/// Decode the bincode payload that follows the magic and version.
///
/// The options are the byte-compatible superset of `bincode::deserialize`:
/// fixint encoding (the format the writer emits), a hard limit so a crafted
/// length cannot drive a giant allocation, and trailing-byte rejection so a
/// file with junk after a valid payload is not silently accepted.
fn decode_snapshot(path: &Path, payload: &[u8]) -> Result<ShardSnapshot, SnapshotError> {
    // The payload starts with `shard_id: u32` then the `symbols: Vec<..>`
    // length prefix (a fixed 8-byte little-endian integer under fixint
    // encoding). Every element costs more than one byte, so a count larger
    // than the remaining payload cannot be a valid snapshot: reject it before
    // serde can use it to size a container. This layout is part of the same
    // versioned format as the struct itself.
    if let Some(length_prefix) = payload.get(4..12) {
        let declared = u64::from_le_bytes(length_prefix.try_into().expect("8-byte slice"));
        if declared > payload.len() as u64 {
            return Err(SnapshotError::TooLarge {
                path: path.to_path_buf(),
                detail: format!(
                    "declared symbol count {declared} cannot fit in {} payload bytes",
                    payload.len()
                ),
            });
        }
    }

    let options = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_SNAPSHOT_BYTES)
        .reject_trailing_bytes();
    match options.deserialize(payload) {
        Ok(snapshot) => Ok(snapshot),
        Err(error) if matches!(&*error, bincode::ErrorKind::SizeLimit) => {
            Err(SnapshotError::TooLarge {
                path: path.to_path_buf(),
                detail: format!("decoding exceeded the {MAX_SNAPSHOT_BYTES}-byte limit"),
            })
        }
        Err(error) => Err(SnapshotError::Codec(error)),
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

/// Structural invariants a [`ShardSnapshot`] must satisfy before it may become
/// a file.
///
/// This is deliberately a subset of what the writer's rebuild verification
/// proves: it is cheap, needs no exchange config, and catches the mistakes that
/// would otherwise persist an unusable resume point. Symbol-to-shard routing is
/// still owned by that verification, which knows the exchange; the snapshot
/// carries `shard_id` explicitly and `load` rejects a mismatch.
///
/// The rule is fail closed: if a check cannot prove the snapshot is a valid
/// resume point, the write is refused and the previous file stays in place.
pub trait SnapshotInvariants {
    /// Return the first structural violation.
    fn check_invariants(&self) -> Result<(), SnapshotError>;

    /// True when every symbol carries a filled `state_hash`.
    ///
    /// An all-zero hash is the "not hashed yet" sentinel produced by
    /// [`ShardEngine::snapshot_state_without_hash`]. Only
    /// [`SnapshotWriter::fill_hashes`] leaves that phase. An empty symbol list
    /// is vacuously hashed; [`SnapshotInvariants::check_invariants`] rejects it
    /// separately.
    fn is_hashed(&self) -> bool;
}

impl SnapshotInvariants for ShardSnapshot {
    fn check_invariants(&self) -> Result<(), SnapshotError> {
        let shard = self.shard_id;
        if self.symbols.is_empty() {
            return Err(SnapshotError::EmptySnapshot { shard });
        }
        // Strict ordering covers duplicates too: a repeated symbol is not
        // "strictly before" itself.
        for pair in self.symbols.windows(2) {
            if pair[0].symbol >= pair[1].symbol {
                return Err(SnapshotError::UnsortedSymbols {
                    shard,
                    detail: format!("{} is not before {}", pair[0].symbol, pair[1].symbol),
                });
            }
        }
        if !self.symbols.iter().any(|symbol| symbol.commands > 0) {
            return Err(SnapshotError::NoResumePoint { shard });
        }
        for symbol in &self.symbols {
            check_symbol(shard, symbol)?;
        }
        Ok(())
    }

    fn is_hashed(&self) -> bool {
        self.symbols
            .iter()
            .all(|symbol| symbol.state_hash != [0u8; 32])
    }
}

/// The per-symbol half of [`SnapshotInvariants::check_invariants`].
fn check_symbol(shard: u32, symbol: &SymbolSnapshot) -> Result<(), SnapshotError> {
    let invalid = |detail: String| SnapshotError::InvalidSymbol {
        shard,
        symbol: symbol.symbol,
        detail,
    };

    if symbol.book.symbol != symbol.symbol {
        return Err(invalid(format!(
            "book carries symbol {}",
            symbol.book.symbol
        )));
    }
    // Both sequences are handed out starting at 1 and only increase.
    if symbol.exec_seq.0 == 0 || symbol.md_seq.0 == 0 {
        return Err(invalid(format!(
            "sequences start at 1, got exec {} and md {}",
            symbol.exec_seq.0, symbol.md_seq.0
        )));
    }
    // A symbol that never applied a command has no offset, timestamp, or dedup
    // record. Anything else means the snapshot mixes two states.
    if symbol.commands == 0 {
        if symbol.last_offset != 0 {
            return Err(invalid(format!(
                "zero commands but last_offset {}",
                symbol.last_offset
            )));
        }
        if !symbol.dedup.is_empty() {
            return Err(invalid(format!(
                "zero commands but {} dedup entries",
                symbol.dedup.len()
            )));
        }
        if !symbol.dedup_terminal_idx.is_empty() {
            return Err(invalid(format!(
                "zero commands but {} terminal indices",
                symbol.dedup_terminal_idx.len()
            )));
        }
    }
    // `last_offset` is the latest log offset and `last_ts_ns` the running max
    // timestamp, so no recorded entry can be beyond either.
    for (order_id, entry) in &symbol.dedup {
        if entry.input_offset > symbol.last_offset {
            return Err(invalid(format!(
                "dedup entry {order_id} records offset {} beyond last_offset {}",
                entry.input_offset, symbol.last_offset
            )));
        }
        if entry.timestamp_ns > symbol.last_ts_ns {
            return Err(invalid(format!(
                "dedup entry {order_id} records ts {} beyond last_ts_ns {}",
                entry.timestamp_ns, symbol.last_ts_ns
            )));
        }
    }
    check_terminal_indices(shard, symbol)?;
    symbol
        .risk
        .check_invariants()
        .map_err(|detail| invalid(format!("risk table: {detail}")))?;
    symbol
        .risk
        .check_against_orders(&symbol.book.orders, symbol.config.tick_size_cents)
        .map_err(|detail| invalid(format!("risk exposure: {detail}")))?;
    if symbol.state_hash == [0u8; 32] {
        return Err(SnapshotError::UnhashedSymbol {
            shard,
            symbol: symbol.symbol,
        });
    }
    Ok(())
}

/// A cheap structural check of the compact terminal queue.
///
/// Every index must be in range, point at a terminal entry, and appear once;
/// the queue must cover every terminal id, because a restore rebuilds eviction
/// order from it. [`whippet_engine::DedupTable::from_entries`] repeats these on
/// restore, and the writer's rebuild is the full check; this one keeps an
/// unusable queue from ever reaching a file.
fn check_terminal_indices(shard: u32, symbol: &SymbolSnapshot) -> Result<(), SnapshotError> {
    let invalid = |detail: String| SnapshotError::InvalidSymbol {
        shard,
        symbol: symbol.symbol,
        detail,
    };

    let mut seen = HashSet::with_capacity(symbol.dedup_terminal_idx.len());
    for index in &symbol.dedup_terminal_idx {
        let Some((_, entry)) = symbol.dedup.get(*index as usize) else {
            return Err(invalid(format!("terminal index {index} is out of range")));
        };
        if !entry.status.is_terminal() {
            return Err(invalid(format!(
                "terminal index {index} points at a non-terminal entry"
            )));
        }
        if !seen.insert(*index) {
            return Err(invalid(format!("terminal index {index} is repeated")));
        }
    }
    let terminal = symbol
        .dedup
        .iter()
        .filter(|(_, entry)| entry.status.is_terminal())
        .count();
    if seen.len() != terminal {
        return Err(invalid(format!(
            "terminal queue covers {} of {terminal} terminal ids",
            seen.len()
        )));
    }
    Ok(())
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
    ///
    /// "Unverified" skips only the rebuild; the structural invariants still
    /// apply, so a periodic snapshot must also carry filled hashes.
    pub fn write_unverified(
        &mut self,
        request: SequencedSnapshot,
    ) -> Result<Option<PathBuf>, SnapshotError> {
        self.write_inner(request, false)
    }

    /// Fill each symbol's `state_hash` off the shard thread.
    ///
    /// The matcher hands over a snapshot built by
    /// `ShardEngine::snapshot_state_without_hash`; this recomputes the hashes
    /// with [`hash_symbol_snapshot`] and refuses a snapshot whose supplied
    /// (non-zero) hash disagrees with the recomputed one. A zero hash is the
    /// expected "not hashed yet" sentinel.
    ///
    /// Recomputing unconditionally would silently repair a corrupted stored
    /// hash, so a mismatch is an error, not a quiet overwrite.
    pub fn fill_hashes(&self, snapshot: &mut ShardSnapshot) -> Result<(), SnapshotError> {
        let provided: Vec<[u8; 32]> = snapshot
            .symbols
            .iter()
            .map(|symbol| symbol.state_hash)
            .collect();
        fill_symbol_hashes(snapshot);
        for (symbol, provided) in snapshot.symbols.iter().zip(&provided) {
            if *provided != [0u8; 32] && *provided != symbol.state_hash {
                return Err(SnapshotError::HashMismatch {
                    shard: snapshot.shard_id,
                    detail: format!(
                        "symbol {} stored hash {} does not match recomputed {}",
                        symbol.symbol,
                        hex(provided),
                        hex(&symbol.state_hash)
                    ),
                });
            }
        }
        Ok(())
    }

    fn write_inner(
        &mut self,
        request: SequencedSnapshot,
        verify: bool,
    ) -> Result<Option<PathBuf>, SnapshotError> {
        if !is_newer(self.last_written, request.sequence) {
            return Ok(None);
        }
        // The matcher only clones the state; the writer fills the hashes with
        // `fill_hashes` before reaching this point, so the match loop never
        // pays the hash. A snapshot that arrives here unhashed, empty, unsorted,
        // or without a resume offset is a bug. This runs for the periodic path
        // too: skipping the rebuild must not skip the contract.
        request.snapshot.check_invariants()?;
        if verify {
            self.verify(&request.snapshot)?;
        }
        // `save` repeats the structural check so a direct caller cannot persist
        // an unusable file either; the sequence guard only advances after the
        // rename has succeeded.
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
    use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Seq};
    use whippet_core::protocol::{Command, OrderType, Side};
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

    /// The smallest snapshot that is a real resume point: one symbol with one
    /// resting order at offset 0.
    fn engine_with_one_order(exchange: &ExchangeConfig) -> ShardEngine {
        let mut engine = ShardEngine::new(test_shard(exchange), exchange);
        let mut out = whippet_engine::EngineOutput::with_capacity(8, 8);
        let order = Command::new_order(
            Symbol::new("BTC-USD").unwrap(),
            Side::Buy,
            OrderType::Limit,
            Price(40_000),
            Qty(1),
            OrderId(1),
            ParticipantId(1),
            1,
        );
        engine.process(&order, 0, &mut out).unwrap();
        engine
    }

    fn temp_snapshot_path(store: &SnapshotStore, shard_id: u32) -> PathBuf {
        temp_path(&store.path(shard_id))
    }

    fn clear_hashes(snapshot: &mut ShardSnapshot) {
        for symbol in &mut snapshot.symbols {
            symbol.state_hash = [0u8; 32];
        }
    }

    /// Two orders, both cancelled, so the compact terminal queue has a real
    /// index vector to corrupt.
    fn engine_with_two_cancelled_orders(exchange: &ExchangeConfig) -> ShardEngine {
        let mut engine = engine_with_one_order(exchange);
        let symbol = Symbol::new("BTC-USD").unwrap();
        let mut out = whippet_engine::EngineOutput::with_capacity(8, 8);
        let second = Command::new_order(
            symbol,
            Side::Buy,
            OrderType::Limit,
            Price(40_001),
            Qty(1),
            OrderId(2),
            ParticipantId(1),
            2,
        );
        engine.process(&second, 1, &mut out).unwrap();
        for (offset, order_id) in [(2u64, OrderId(1)), (3, OrderId(2))] {
            out.clear();
            let cancel = Command::cancel(symbol, order_id, ParticipantId(1), offset + 1);
            engine.process(&cancel, offset, &mut out).unwrap();
        }
        engine
    }

    #[test]
    fn snapshot_round_trip_and_hash_verification() {
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let engine = engine_with_one_order(&exchange);
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
    fn restore_rejects_a_rehashed_risk_table_that_disagrees_with_the_book() {
        let exchange = test_exchange();
        let mut snapshot = engine_with_one_order(&exchange).snapshot_state();
        assert!(
            !snapshot.symbols[0].risk.is_empty(),
            "the resting order must be tracked as exposure"
        );

        // Drop the resting buy from the risk table while leaving the book
        // untouched, then recompute the hash. The stored hash can no longer
        // catch the tamper, so only the structural risk/book comparison can.
        snapshot.symbols[0]
            .risk
            .on_cancel(ParticipantId(1), Side::Buy, 40_000 * 100, 1);
        assert!(snapshot.symbols[0].risk.is_empty());
        whippet_engine::fill_symbol_hashes(&mut snapshot);

        match ShardEngine::from_snapshot(&exchange, snapshot) {
            Ok(_) => panic!("a risk table that disagrees with the book was restored"),
            Err(error) => {
                let message = error.to_string();
                assert!(message.contains("risk"), "{message}");
                assert!(message.contains("disagrees with the book"), "{message}");
            }
        }
    }

    #[test]
    fn snapshot_round_trip_preserves_the_session() {
        use whippet_core::protocol::SessionState;

        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let symbol = Symbol::new("BTC-USD").unwrap();
        let mut engine = ShardEngine::new(shard_id, &exchange);
        let mut out = whippet_engine::EngineOutput::with_capacity(8, 8);
        let order = Command::new_order(
            symbol,
            Side::Buy,
            OrderType::Limit,
            Price(40_000),
            Qty(1),
            OrderId(1),
            ParticipantId(1),
            1,
        );
        engine.process(&order, 0, &mut out).unwrap();
        out.clear();
        let pre_open = Command::set_session(symbol, SessionState::PreOpen, ParticipantId(9), 2);
        engine.process(&pre_open, 1, &mut out).unwrap();
        out.clear();
        let live = engine.state_hash();
        assert_eq!(
            engine.engine(symbol).unwrap().session(),
            SessionState::PreOpen
        );

        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        store.save(&engine.snapshot_state()).unwrap();
        let loaded = store.load(shard_id).unwrap().unwrap();
        assert_eq!(loaded.symbols[0].session, SessionState::PreOpen);

        let mut restored = ShardEngine::from_snapshot(&exchange, loaded).unwrap();
        assert_eq!(restored.state_hash(), live);
        assert_eq!(
            restored.engine(symbol).unwrap().session(),
            SessionState::PreOpen
        );
        // v5 restores the gate, not only the flag.
        let rejected = Command::new_order(
            symbol,
            Side::Buy,
            OrderType::Limit,
            Price(40_000),
            Qty(1),
            OrderId(2),
            ParticipantId(1),
            3,
        );
        engine.process(&rejected, 2, &mut out).unwrap();
        assert_eq!(
            out.executions[0].reason,
            whippet_core::protocol::RejectReason::SessionNotOpen
        );
        out.clear();
        restored.process(&rejected, 2, &mut out).unwrap();
        assert_eq!(
            out.executions[0].reason,
            whippet_core::protocol::RejectReason::SessionNotOpen
        );
    }

    #[test]
    fn writer_verifies_and_restores_the_same_hash() {
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let engine = engine_with_one_order(&exchange);
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
        let engine = engine_with_one_order(&exchange);
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
    fn unhashed_snapshot_is_rejected_without_an_off_loop_fill() {
        let exchange = test_exchange();
        let engine = engine_with_one_order(&exchange);
        let mut unhashed = engine.snapshot_state();
        clear_hashes(&mut unhashed);

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
    fn fill_hashes_makes_an_unhashed_snapshot_writable() {
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let mut engine = ShardEngine::new(shard_id, &exchange);
        let mut out = whippet_engine::EngineOutput::with_capacity(8, 8);
        let symbol = Symbol::new("BTC-USD").unwrap();
        let mut id = 0u128;
        for offset in 0..2_000u64 {
            id += 1;
            let command = whippet_core::protocol::Command::new_order(
                symbol,
                whippet_core::protocol::Side::Buy,
                whippet_core::protocol::OrderType::Limit,
                whippet_core::ids::Price(40_000 + (offset as i64 % 200)),
                whippet_core::ids::Qty(1),
                whippet_core::ids::OrderId(id),
                whippet_core::ids::ParticipantId(1),
                offset,
            );
            engine.process(&command, offset, &mut out).unwrap();
            out.clear();
        }
        let live = engine.state_hash();

        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let mut writer = SnapshotWriter::with_exchange(store.clone(), exchange.clone());
        let mut snapshot = engine.snapshot_state_without_hash();
        assert!(
            snapshot
                .symbols
                .iter()
                .all(|symbol| symbol.state_hash == [0u8; 32])
        );
        writer
            .fill_hashes(&mut snapshot)
            .expect("zero hashes are filled, not rejected");
        let path = writer
            .write(SequencedSnapshot {
                sequence: 1,
                snapshot,
            })
            .unwrap()
            .expect("the filled snapshot is persisted");

        let loaded = store.load(shard_id).unwrap().unwrap();
        let restored = ShardEngine::from_snapshot(&exchange, loaded).unwrap();
        assert_eq!(restored.state_hash(), live);
        assert!(path.exists());
    }

    #[test]
    fn fill_hashes_rejects_a_supplied_hash_that_disagrees() {
        // Recomputing must not silently repair a corrupted stored hash: that
        // would turn a hashing bug into a persisted resume point.
        let exchange = test_exchange();
        let engine = ShardEngine::new(test_shard(&exchange), &exchange);
        let dir = tempfile::tempdir().unwrap();
        let writer = SnapshotWriter::with_exchange(SnapshotStore::new(dir.path()), exchange);

        let mut snapshot = engine.snapshot_state_without_hash();
        snapshot.symbols[0].state_hash[0] ^= 0xff;
        match writer.fill_hashes(&mut snapshot) {
            Err(SnapshotError::HashMismatch { shard, detail }) => {
                assert_eq!(shard, snapshot.shard_id);
                assert!(detail.contains("does not match recomputed"), "{detail}");
            }
            other => panic!("expected HashMismatch, got {other:?}"),
        }
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
    fn older_snapshot_versions_are_rejected_not_misread() {
        // v5 added the per-symbol session field, so a v4 payload has no byte
        // for it and must fail loudly rather than deserialize into the new
        // layout. v4 itself changed the dedup terminal payload from `OrderId`s
        // to `u32` indices, which v3 files would read as ids.
        for version in [1u32, 2, 3, 4, 5] {
            let dir = tempfile::tempdir().unwrap();
            let store = SnapshotStore::new(dir.path());
            let path = store.path(3);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            let mut file = fs::File::create(&path).unwrap();
            file.write_all(MAGIC).unwrap();
            file.write_all(&version.to_le_bytes()).unwrap();
            file.write_all(&[0u8; 8]).unwrap();
            drop(file);

            match store.load(3) {
                Err(SnapshotError::BadVersion(found)) => assert_eq!(found, version),
                other => panic!("expected BadVersion({version}), got {other:?}"),
            }
        }
    }

    #[test]
    fn trailing_bytes_after_a_valid_snapshot_are_rejected() {
        // The old decode used `bincode::deserialize`, which allows trailing
        // bytes: a file with junk appended to a valid payload still loaded.
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let engine = engine_with_one_order(&exchange);
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let path = store.save(&engine.snapshot_state()).unwrap();

        let mut bytes = fs::read(&path).unwrap();
        bytes.push(0x00);
        fs::write(&path, &bytes).unwrap();

        match store.load(shard_id) {
            Err(SnapshotError::Codec(error)) => assert!(
                error
                    .to_string()
                    .contains("remaining after deserialization"),
                "unexpected codec error: {error}"
            ),
            other => panic!("expected a trailing-bytes rejection, got {other:?}"),
        }
    }

    #[test]
    fn an_over_cap_snapshot_file_is_reported_too_large() {
        // Sparse file: costs no disk, but `metadata().len()` is over the cap.
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let path = store.path(0);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let file = fs::File::create(&path).unwrap();
        file.set_len(MAX_SNAPSHOT_BYTES + 1).unwrap();
        drop(file);

        match store.load(0) {
            Err(SnapshotError::TooLarge { detail, .. }) => {
                assert!(detail.contains("cap"), "unexpected detail: {detail}");
            }
            other => panic!("expected TooLarge, got {other:?}"),
        }
    }

    #[test]
    fn a_crafted_symbol_count_is_rejected_before_allocating() {
        // 8 magic bytes + version + shard id + a u64::MAX symbol count. The
        // count is larger than the rest of the file, so it is refused instead
        // of being used to size a container.
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let path = store.path(0);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(MAGIC).unwrap();
        file.write_all(&VERSION.to_le_bytes()).unwrap();
        file.write_all(&0u32.to_le_bytes()).unwrap();
        file.write_all(&u64::MAX.to_le_bytes()).unwrap();
        drop(file);

        match store.load(0) {
            Err(SnapshotError::TooLarge { detail, .. }) => {
                assert!(
                    detail.contains("declared symbol count"),
                    "unexpected detail: {detail}"
                );
            }
            other => panic!("expected TooLarge, got {other:?}"),
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
        let engine = engine_with_one_order(&exchange);
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
        let engine = engine_with_one_order(&exchange);
        let mut writer = SnapshotWriter::with_exchange(store, exchange);

        let result = writer.write(SequencedSnapshot {
            sequence: 4,
            snapshot: engine.snapshot_state(),
        });
        assert!(result.is_err());
        assert_eq!(writer.last_written(), None);
    }

    #[test]
    fn a_live_snapshot_satisfies_the_structural_invariants() {
        let exchange = test_exchange();
        let engine = engine_with_one_order(&exchange);
        let snapshot = engine.snapshot_state();
        snapshot.check_invariants().unwrap();
        assert!(snapshot.is_hashed());
        assert!(
            !engine.snapshot_state_without_hash().is_hashed(),
            "the no-hash constructor is the unhashed phase"
        );
    }

    #[test]
    fn save_rejects_an_unhashed_snapshot_without_creating_a_file() {
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let mut snapshot = engine_with_one_order(&exchange).snapshot_state();
        clear_hashes(&mut snapshot);

        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let error = store.save(&snapshot).unwrap_err();
        assert!(
            matches!(error, SnapshotError::UnhashedSymbol { shard, .. } if shard == shard_id),
            "expected UnhashedSymbol, got {error:?}"
        );
        assert!(
            !store.path(shard_id).exists(),
            "an unhashed snapshot became a file"
        );
        assert!(
            !temp_snapshot_path(&store, shard_id).exists(),
            "the temp file was left behind"
        );
    }

    #[test]
    fn write_unverified_also_requires_filled_hashes() {
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let mut snapshot = engine_with_one_order(&exchange).snapshot_state();
        clear_hashes(&mut snapshot);

        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let mut writer = SnapshotWriter::with_exchange(store.clone(), exchange);
        let result = writer.write_unverified(SequencedSnapshot {
            sequence: 2,
            snapshot,
        });
        assert!(
            matches!(result, Err(SnapshotError::UnhashedSymbol { .. })),
            "the periodic path must still require hashes, got {result:?}"
        );
        assert!(!store.path(shard_id).exists());
        assert_eq!(writer.last_written(), None);
    }

    #[test]
    fn save_rejects_a_snapshot_with_no_symbols() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let snapshot = ShardSnapshot {
            shard_id: 3,
            symbols: Vec::new(),
        };
        let error = store.save(&snapshot).unwrap_err();
        assert!(matches!(error, SnapshotError::EmptySnapshot { shard: 3 }));
        assert!(!store.path(3).exists());
    }

    #[test]
    fn save_rejects_unsorted_and_duplicate_symbols() {
        let exchange = test_exchange();
        let live = engine_with_one_order(&exchange).snapshot_state();

        let mut duplicate = live.clone();
        duplicate.symbols.push(duplicate.symbols[0].clone());

        let mut unsorted = live;
        let mut lower = unsorted.symbols[0].clone();
        let symbol = Symbol::new("AAA-USD").unwrap();
        lower.symbol = symbol;
        lower.book.symbol = symbol;
        // [BTC-USD, AAA-USD] is descending.
        unsorted.symbols.push(lower);

        for snapshot in [duplicate, unsorted] {
            match snapshot.check_invariants() {
                Err(SnapshotError::UnsortedSymbols { .. }) => {}
                other => panic!("expected UnsortedSymbols, got {other:?}"),
            }
            let dir = tempfile::tempdir().unwrap();
            let store = SnapshotStore::new(dir.path());
            assert!(store.save(&snapshot).is_err());
            assert!(!store.path(snapshot.shard_id).exists());
        }
    }

    #[test]
    fn save_rejects_a_snapshot_with_no_commands() {
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let engine = ShardEngine::new(shard_id, &exchange);
        let snapshot = engine.snapshot_state();
        assert!(snapshot.is_hashed(), "an empty engine still hashes cleanly");

        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let error = store.save(&snapshot).unwrap_err();
        assert!(
            matches!(error, SnapshotError::NoResumePoint { shard } if shard == shard_id),
            "expected NoResumePoint, got {error:?}"
        );
        assert!(!store.path(shard_id).exists());
    }

    #[test]
    fn check_invariants_rejects_a_book_symbol_mismatch() {
        let exchange = test_exchange();
        let mut snapshot = engine_with_one_order(&exchange).snapshot_state();
        snapshot.symbols[0].book.symbol = Symbol::new("ETH-USD").unwrap();
        match snapshot.check_invariants() {
            Err(SnapshotError::InvalidSymbol { detail, .. }) => {
                assert!(detail.contains("book carries symbol"), "{detail}");
            }
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }
    }

    #[test]
    fn check_invariants_rejects_impossible_sequences_and_offsets() {
        let exchange = test_exchange();
        let live = engine_with_one_order(&exchange).snapshot_state();

        let mut zero_seq = live.clone();
        zero_seq.symbols[0].exec_seq = Seq(0);
        assert!(matches!(
            zero_seq.check_invariants(),
            Err(SnapshotError::InvalidSymbol { .. })
        ));

        let mut beyond_offset = live.clone();
        let last_offset = beyond_offset.symbols[0].last_offset;
        beyond_offset.symbols[0].dedup[0].1.input_offset = last_offset + 1;
        match beyond_offset.check_invariants() {
            Err(SnapshotError::InvalidSymbol { detail, .. }) => {
                assert!(detail.contains("beyond last_offset"), "{detail}");
            }
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }

        let mut beyond_ts = live;
        let last_ts = beyond_ts.symbols[0].last_ts_ns;
        beyond_ts.symbols[0].dedup[0].1.timestamp_ns = last_ts + 1;
        match beyond_ts.check_invariants() {
            Err(SnapshotError::InvalidSymbol { detail, .. }) => {
                assert!(detail.contains("beyond last_ts_ns"), "{detail}");
            }
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }
    }

    #[test]
    fn check_invariants_rejects_a_commandless_symbol_that_carries_state() {
        let exchange = test_exchange();
        let live = engine_with_one_order(&exchange).snapshot_state();
        let symbol = Symbol::new("AAA-USD").unwrap();

        let mut with_dedup = live.clone();
        let mut idle = with_dedup.symbols[0].clone();
        idle.symbol = symbol;
        idle.book.symbol = symbol;
        idle.commands = 0;
        with_dedup.symbols.insert(0, idle);
        match with_dedup.check_invariants() {
            Err(SnapshotError::InvalidSymbol { detail, .. }) => {
                assert!(detail.contains("dedup entries"), "{detail}");
            }
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }

        let mut with_offset = live;
        let mut idle = with_offset.symbols[0].clone();
        idle.symbol = symbol;
        idle.book.symbol = symbol;
        idle.commands = 0;
        idle.last_offset = 1;
        idle.dedup.clear();
        idle.dedup_terminal_idx.clear();
        with_offset.symbols.insert(0, idle);
        match with_offset.check_invariants() {
            Err(SnapshotError::InvalidSymbol { detail, .. }) => {
                assert!(detail.contains("last_offset"), "{detail}");
            }
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }
    }

    #[test]
    fn check_invariants_checks_the_terminal_queue() {
        let exchange = test_exchange();
        let live = engine_with_two_cancelled_orders(&exchange).snapshot_state();
        assert_eq!(live.symbols[0].dedup_terminal_idx, vec![0, 1]);
        live.check_invariants().unwrap();

        let mut out_of_range = live.clone();
        out_of_range.symbols[0].dedup_terminal_idx = vec![u32::MAX];
        match out_of_range.check_invariants() {
            Err(SnapshotError::InvalidSymbol { detail, .. }) => {
                assert!(detail.contains("out of range"), "{detail}");
            }
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }

        let mut repeated = live.clone();
        repeated.symbols[0].dedup_terminal_idx = vec![0, 0];
        match repeated.check_invariants() {
            Err(SnapshotError::InvalidSymbol { detail, .. }) => {
                assert!(detail.contains("repeated"), "{detail}");
            }
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }

        let mut uncovered = live;
        uncovered.symbols[0].dedup_terminal_idx.clear();
        match uncovered.check_invariants() {
            Err(SnapshotError::InvalidSymbol { detail, .. }) => {
                assert!(detail.contains("covers 0 of 2"), "{detail}");
            }
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }

        // A resting entry is not a terminal one, even if the index is valid.
        let mut resting = engine_with_one_order(&exchange).snapshot_state();
        resting.symbols[0].dedup_terminal_idx = vec![0];
        match resting.check_invariants() {
            Err(SnapshotError::InvalidSymbol { detail, .. }) => {
                assert!(detail.contains("non-terminal"), "{detail}");
            }
            other => panic!("expected InvalidSymbol, got {other:?}"),
        }
    }

    #[test]
    fn a_failed_rename_removes_the_temp_file_and_leaves_the_guard_alone() {
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let engine = engine_with_one_order(&exchange);
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let path = store.path(shard_id);
        // A directory at the destination makes the atomic rename fail after
        // the temp file was fully written.
        fs::create_dir(&path).unwrap();
        let mut writer = SnapshotWriter::with_exchange(store.clone(), exchange);

        let error = writer
            .write(SequencedSnapshot {
                sequence: 1,
                snapshot: engine.snapshot_state(),
            })
            .unwrap_err();
        assert!(matches!(error, SnapshotError::Io(_)), "got {error:?}");
        assert_eq!(writer.last_written(), None);
        assert!(
            !temp_snapshot_path(&store, shard_id).exists(),
            "the temp file was left behind"
        );
        assert!(path.is_dir(), "the destination was replaced");
    }

    #[test]
    fn a_failed_newer_write_does_not_advance_the_guard() {
        let exchange = test_exchange();
        let shard_id = test_shard(&exchange);
        let engine = engine_with_one_order(&exchange);
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path());
        let path = store.path(shard_id);
        let mut writer = SnapshotWriter::with_exchange(store.clone(), exchange);

        writer
            .write(SequencedSnapshot {
                sequence: 1,
                snapshot: engine.snapshot_state(),
            })
            .unwrap()
            .expect("the first snapshot writes");
        assert_eq!(writer.last_written(), Some(1));

        // Replace the file with a directory: the rename for the newer sequence
        // cannot replace it, so the guard must stay on the sequence that did.
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        let error = writer
            .write(SequencedSnapshot {
                sequence: 2,
                snapshot: engine.snapshot_state(),
            })
            .unwrap_err();
        assert!(matches!(error, SnapshotError::Io(_)), "got {error:?}");
        assert_eq!(writer.last_written(), Some(1));
        assert!(!temp_snapshot_path(&store, shard_id).exists());
    }
}
