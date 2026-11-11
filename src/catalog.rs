//! Small JSON index. One entry per chunk is enough: a day of ticks at
//! 16,384 rows per chunk is a few thousand entries, not millions.
//!
//! The file is replaced with `rename`, so readers never see a half-written
//! catalog. It is not fsynced; a power loss can drop the last rename.

use crate::wal::SyncPolicy;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

pub const CATALOG_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub id: u32,
    pub min_ts: i64,
    pub max_ts: i64,
    pub rows: u32,
    pub bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SymbolEntry {
    #[serde(default)]
    pub trades: Vec<ChunkMeta>,
    #[serde(default)]
    pub quotes: Vec<ChunkMeta>,
    /// Trade-log sequence covered by sealed trade chunks, and how many
    /// leading rows of that sequence are included. Quote appends use their
    /// own cursor so sealing one stream cannot drop the other's tail.
    #[serde(default)]
    pub trade_wal_seq: u64,
    #[serde(default)]
    pub trade_wal_rows: u32,
    #[serde(default)]
    pub quote_wal_seq: u64,
    #[serde(default)]
    pub quote_wal_rows: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalog {
    pub version: u32,
    pub price_scale: i64,
    pub chunk_rows: u32,
    #[serde(default)]
    pub sync: SyncPolicy,
    pub symbols: BTreeMap<String, SymbolEntry>,
}

impl Catalog {
    pub fn new(price_scale: i64, chunk_rows: u32, sync: SyncPolicy) -> Self {
        Self {
            version: CATALOG_VERSION,
            price_scale,
            chunk_rows,
            sync,
            symbols: BTreeMap::new(),
        }
    }

    pub fn path(root: &Path) -> PathBuf {
        root.join("catalog.json")
    }

    pub fn load(root: &Path) -> Result<Self> {
        let path = Self::path(root);
        let file = File::open(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::bad(format!("no catalog at {}", path.display()))
            } else {
                Error::Io(e)
            }
        })?;
        let cat: Catalog = serde_json::from_reader(file)?;
        if cat.version != CATALOG_VERSION {
            return Err(Error::bad(format!(
                "catalog version {} is not supported",
                cat.version
            )));
        }
        if cat.price_scale <= 0 || cat.chunk_rows == 0 {
            return Err(Error::corrupt("catalog scale or chunk size is invalid"));
        }
        Ok(cat)
    }

    pub fn save(&self, root: &Path) -> Result<()> {
        fs::create_dir_all(root)?;
        let tmp = root.join("catalog.json.tmp");
        let final_path = Self::path(root);
        {
            let mut f = File::create(&tmp)?;
            serde_json::to_writer_pretty(&mut f, self)?;
            f.write_all(b"\n")?;
        }
        fs::rename(&tmp, &final_path)?;
        Ok(())
    }
}

/// Chunks whose time range can overlap `[start, end)`.
///
/// Chunks are non-decreasing in time because appends are monotonic, so this
/// is a binary search plus a short scan, not a filter of the whole index.
pub fn overlapping(chunks: &[ChunkMeta], start: i64, end: i64) -> &[ChunkMeta] {
    if start >= end || chunks.is_empty() {
        return &[];
    }
    let i = chunks.partition_point(|c| c.max_ts < start);
    let rest = &chunks[i..];
    let j = rest
        .iter()
        .position(|c| c.min_ts >= end)
        .unwrap_or(rest.len());
    &rest[..j]
}

/// Quote chunks needed for an as-of join of trades in `[start, end)`.
///
/// Includes the chunk that holds the last quote at or before `start`, so a
/// window that begins between quotes still sees the prevailing book.
pub fn asof_quote_chunks(chunks: &[ChunkMeta], start: i64, end: i64) -> &[ChunkMeta] {
    if chunks.is_empty() || start >= end {
        return &[];
    }
    let mut i = chunks.partition_point(|c| c.min_ts <= start);
    i = i.saturating_sub(1);
    let rest = &chunks[i..];
    let j = rest
        .iter()
        .position(|c| c.min_ts >= end)
        .unwrap_or(rest.len());
    &rest[..j]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(id: u32, min_ts: i64, max_ts: i64) -> ChunkMeta {
        ChunkMeta {
            id,
            min_ts,
            max_ts,
            rows: 4,
            bytes: 10,
        }
    }

    #[test]
    fn prune_edges() {
        let chunks = vec![meta(0, 10, 40), meta(1, 50, 80), meta(2, 90, 90)];
        assert_eq!(
            overlapping(&chunks, 40, 70)
                .iter()
                .map(|c| c.id)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert!(overlapping(&chunks, 41, 50).is_empty());
        assert_eq!(
            overlapping(&chunks, 90, 100)
                .iter()
                .map(|c| c.id)
                .collect::<Vec<_>>(),
            vec![2]
        );
        assert!(overlapping(&chunks, 0, 10).is_empty());
        assert!(overlapping(&chunks, 91, 200).is_empty());
    }

    #[test]
    fn asof_includes_seed_chunk() {
        let chunks = vec![meta(0, 10, 25), meta(1, 50, 80)];
        let ids: Vec<_> = asof_quote_chunks(&chunks, 30, 60)
            .iter()
            .map(|c| c.id)
            .collect();
        assert_eq!(ids, vec![0, 1]);
        let ids: Vec<_> = asof_quote_chunks(&chunks, 10, 20)
            .iter()
            .map(|c| c.id)
            .collect();
        assert_eq!(ids, vec![0]);
    }
}
