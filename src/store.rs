//! The coordinator's state on disk, so a restart loses at most the last second of work.
//!
//! `state.json` holds devices, jobs, tasks and the queue; blobs live beside it as
//! `blobs/<sha256>`. Every write goes to a temporary file that is then renamed, and blobs are
//! written before the state that refers to them, so a crash leaves a consistent snapshot.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use axum::body::Bytes;

/// Blobs by hash.
pub type Blobs = Vec<(String, Bytes)>;

/// What changed since the last snapshot.
pub struct Snapshot {
    pub state: Vec<u8>,
    pub added: Blobs,
    pub removed: Vec<String>,
}

pub struct Store {
    dir: PathBuf,
    /// The last state written: an unchanged pool is not written again.
    last: Vec<u8>,
}

impl Store {
    pub fn open(dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(dir.join("blobs"))
            .with_context(|| format!("creating {}", dir.display()))?;
        Ok(Store {
            dir: dir.into(),
            last: Vec::new(),
        })
    }

    /// The saved state and every saved blob, if a pool was saved here before.
    pub fn load(&mut self) -> Result<Option<(Vec<u8>, Blobs)>> {
        let state = match std::fs::read(self.dir.join("state.json")) {
            Ok(state) => state,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let mut blobs = Vec::new();
        for entry in std::fs::read_dir(self.dir.join("blobs"))? {
            let path = entry?.path();
            let Some(hash) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                blobs.push((hash.to_string(), Bytes::from(std::fs::read(&path)?)));
            }
        }
        self.last = state.clone();
        Ok(Some((state, blobs)))
    }

    pub fn save(&mut self, snap: &Snapshot) -> Result<()> {
        for (hash, data) in &snap.added {
            write_atomic(&self.dir.join("blobs").join(hash), data)?;
        }
        if snap.state != self.last {
            write_atomic(&self.dir.join("state.json"), &snap.state)?;
            self.last = snap.state.clone();
        }
        // Only once no saved state refers to them.
        for hash in &snap.removed {
            match std::fs::remove_file(self.dir.join("blobs").join(hash)) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
                _ => {}
            }
        }
        Ok(())
    }
}

fn write_atomic(path: &Path, data: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

/// Serde for `Instant`s as "seconds ago", so they survive a restart: time the coordinator
/// was down does not count (a device cannot heartbeat to a stopped coordinator).
pub mod ago {
    use std::time::{Duration, Instant};

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(at: &Instant, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_f64(at.elapsed().as_secs_f64())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Instant, D::Error> {
        let ago = Duration::from_secs_f64(f64::deserialize(d)?.max(0.0));
        Ok(Instant::now().checked_sub(ago).unwrap_or_else(Instant::now))
    }

    pub mod option {
        use super::*;

        pub fn serialize<S: Serializer>(at: &Option<Instant>, s: S) -> Result<S::Ok, S::Error> {
            match at {
                Some(at) => super::serialize(at, s),
                None => s.serialize_none(),
            }
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Instant>, D::Error> {
            let ago = Option::<f64>::deserialize(d)?;
            Ok(ago.map(|ago| {
                let ago = Duration::from_secs_f64(ago.max(0.0));
                Instant::now().checked_sub(ago).unwrap_or_else(Instant::now)
            }))
        }
    }
}
