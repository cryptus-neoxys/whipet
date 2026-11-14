// Licensed under the Apache License, Version 2.0. See LICENSE.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use whippet_engine::ShardSnapshot;

const MAGIC: &[u8; 8] = b"WHIPSNP1";
const VERSION: u32 = 1;
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
}
