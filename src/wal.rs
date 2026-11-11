//! Append-only log of the unsealed tail.
//!
//! A record is committed when its length prefix and CRC are both on disk.
//! A torn trailing record is discarded. `wal_commit_seq` / `wal_commit_rows`
//! in the catalog name the prefix of the log that has already been sealed
//! into chunks, so replaying the log after a crash does not duplicate those
//! rows.

use crate::types::{Quote, Side, Trade};
use crate::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"TWAL";
const VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecKind {
    Trade = 1,
    Quote = 2,
}

#[derive(Clone, Debug)]
pub struct WalRecord {
    pub seq: u64,
    pub symbol: String,
    pub trades: Vec<Trade>,
    pub quotes: Vec<Quote>,
}

pub struct Wal {
    path: PathBuf,
}

impl Wal {
    pub fn create(root: &Path) -> Result<Self> {
        let path = root.join("wal.bin");
        let mut file = File::create(&path)?;
        file.write_all(MAGIC)?;
        file.write_all(&VERSION.to_le_bytes())?;
        file.sync_all()?;
        Ok(Self { path })
    }

    pub fn open(root: &Path) -> Result<Self> {
        let path = root.join("wal.bin");
        if !path.exists() {
            return Self::create(root);
        }
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append_trades(&self, seq: u64, symbol: &str, rows: &[Trade], sync: bool) -> Result<()> {
        let mut body = Vec::new();
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(RecKind::Trade as u8);
        write_symbol(&mut body, symbol)?;
        body.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        for t in rows {
            body.extend_from_slice(&t.ts_ns.to_le_bytes());
            body.extend_from_slice(&t.price_ticks.to_le_bytes());
            body.extend_from_slice(&t.size.to_le_bytes());
            body.extend_from_slice(&u32::from(t.side.as_u8()).to_le_bytes());
        }
        self.append_body(&body, sync)
    }

    pub fn append_quotes(&self, seq: u64, symbol: &str, rows: &[Quote], sync: bool) -> Result<()> {
        let mut body = Vec::new();
        body.extend_from_slice(&seq.to_le_bytes());
        body.push(RecKind::Quote as u8);
        write_symbol(&mut body, symbol)?;
        body.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        for q in rows {
            body.extend_from_slice(&q.ts_ns.to_le_bytes());
            body.extend_from_slice(&q.bid_ticks.to_le_bytes());
            body.extend_from_slice(&q.ask_ticks.to_le_bytes());
            body.extend_from_slice(&q.bid_size.to_le_bytes());
            body.extend_from_slice(&q.ask_size.to_le_bytes());
        }
        self.append_body(&body, sync)
    }

    /// Replace the log with `records`, which must be a suffix of what was there.
    pub fn rewrite(&self, records: &[WalRecord], sync: bool) -> Result<()> {
        let tmp = self.path.with_extension("bin.tmp");
        {
            let mut file = File::create(&tmp)?;
            file.write_all(MAGIC)?;
            file.write_all(&VERSION.to_le_bytes())?;
            for rec in records {
                let body = encode_record(rec)?;
                let crc = crc32fast::hash(&body);
                file.write_all(&(body.len() as u32).to_le_bytes())?;
                file.write_all(&crc.to_le_bytes())?;
                file.write_all(&body)?;
            }
            if sync {
                file.sync_all()?;
            }
        }
        std::fs::rename(&tmp, &self.path)?;
        if sync {
            sync_dir(&self.path)?;
        }
        Ok(())
    }

    fn append_body(&self, body: &[u8], sync: bool) -> Result<()> {
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        let crc = crc32fast::hash(body);
        file.write_all(&(body.len() as u32).to_le_bytes())?;
        file.write_all(&crc.to_le_bytes())?;
        file.write_all(body)?;
        if sync {
            file.sync_all()?;
        }
        Ok(())
    }

    /// Read every intact record. A torn tail is truncated back to the last good offset.
    pub fn replay(&self) -> Result<Vec<WalRecord>> {
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let mut header = [0u8; 8];
        file.read_exact(&mut header)
            .map_err(|_| Error::corrupt("wal header is truncated"))?;
        if &header[0..4] != MAGIC {
            return Err(Error::corrupt("wal magic is not TWAL"));
        }
        let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(Error::corrupt(format!("unsupported wal version {version}")));
        }
        let mut out = Vec::new();
        loop {
            let pos = file.stream_position()?;
            let mut prefix = [0u8; 8];
            let n = file.read(&mut prefix)?;
            if n == 0 {
                break;
            }
            if n < 8 {
                file.set_len(pos)?;
                break;
            }
            let len = u32::from_le_bytes(prefix[0..4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(prefix[4..8].try_into().unwrap());
            let mut body = vec![0u8; len];
            if file.read_exact(&mut body).is_err() || crc32fast::hash(&body) != crc {
                file.set_len(pos)?;
                break;
            }
            out.push(decode_body(&body)?);
        }
        Ok(out)
    }
}

fn encode_record(rec: &WalRecord) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    body.extend_from_slice(&rec.seq.to_le_bytes());
    if !rec.trades.is_empty() {
        body.push(RecKind::Trade as u8);
        write_symbol(&mut body, &rec.symbol)?;
        body.extend_from_slice(&(rec.trades.len() as u32).to_le_bytes());
        for t in &rec.trades {
            body.extend_from_slice(&t.ts_ns.to_le_bytes());
            body.extend_from_slice(&t.price_ticks.to_le_bytes());
            body.extend_from_slice(&t.size.to_le_bytes());
            body.extend_from_slice(&u32::from(t.side.as_u8()).to_le_bytes());
        }
    } else {
        body.push(RecKind::Quote as u8);
        write_symbol(&mut body, &rec.symbol)?;
        body.extend_from_slice(&(rec.quotes.len() as u32).to_le_bytes());
        for q in &rec.quotes {
            body.extend_from_slice(&q.ts_ns.to_le_bytes());
            body.extend_from_slice(&q.bid_ticks.to_le_bytes());
            body.extend_from_slice(&q.ask_ticks.to_le_bytes());
            body.extend_from_slice(&q.bid_size.to_le_bytes());
            body.extend_from_slice(&q.ask_size.to_le_bytes());
        }
    }
    Ok(body)
}

fn write_symbol(body: &mut Vec<u8>, symbol: &str) -> Result<()> {
    let bytes = symbol.as_bytes();
    if bytes.len() > u16::MAX as usize {
        return Err(Error::bad("symbol does not fit in the wal"));
    }
    body.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    body.extend_from_slice(bytes);
    Ok(())
}

fn decode_body(body: &[u8]) -> Result<WalRecord> {
    if body.len() < 8 + 1 + 2 + 4 {
        return Err(Error::corrupt("wal record is truncated"));
    }
    let seq = u64::from_le_bytes(body[0..8].try_into().unwrap());
    let kind = body[8];
    let sym_len = u16::from_le_bytes(body[9..11].try_into().unwrap()) as usize;
    let mut off = 11;
    if off + sym_len + 4 > body.len() {
        return Err(Error::corrupt("wal symbol is truncated"));
    }
    let symbol = std::str::from_utf8(&body[off..off + sym_len])
        .map_err(|_| Error::corrupt("wal symbol is not utf-8"))?
        .to_string();
    off += sym_len;
    let n = u32::from_le_bytes(body[off..off + 4].try_into().unwrap()) as usize;
    off += 4;
    let mut rec = WalRecord {
        seq,
        symbol,
        trades: Vec::new(),
        quotes: Vec::new(),
    };
    if kind == RecKind::Trade as u8 {
        let need = n * 24;
        if off + need != body.len() {
            return Err(Error::corrupt("wal trade payload has the wrong length"));
        }
        for _ in 0..n {
            let ts = i64::from_le_bytes(body[off..off + 8].try_into().unwrap());
            let price = i64::from_le_bytes(body[off + 8..off + 16].try_into().unwrap());
            let size = u32::from_le_bytes(body[off + 16..off + 20].try_into().unwrap());
            let side = u32::from_le_bytes(body[off + 20..off + 24].try_into().unwrap());
            rec.trades.push(Trade {
                ts_ns: ts,
                price_ticks: price,
                size,
                side: Side::from_u8(u8::try_from(side).map_err(|_| Error::corrupt("bad side"))?)?,
            });
            off += 24;
        }
    } else if kind == RecKind::Quote as u8 {
        let need = n * 32;
        if off + need != body.len() {
            return Err(Error::corrupt("wal quote payload has the wrong length"));
        }
        for _ in 0..n {
            rec.quotes.push(Quote {
                ts_ns: i64::from_le_bytes(body[off..off + 8].try_into().unwrap()),
                bid_ticks: i64::from_le_bytes(body[off + 8..off + 16].try_into().unwrap()),
                ask_ticks: i64::from_le_bytes(body[off + 16..off + 24].try_into().unwrap()),
                bid_size: u32::from_le_bytes(body[off + 24..off + 28].try_into().unwrap()),
                ask_size: u32::from_le_bytes(body[off + 28..off + 32].try_into().unwrap()),
            });
            off += 32;
        }
    } else {
        return Err(Error::corrupt(format!("unknown wal record kind {kind}")));
    }
    Ok(rec)
}

pub fn sync_dir(path: &Path) -> Result<()> {
    let dir = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent().unwrap_or(path).to_path_buf()
    };
    File::open(dir)?.sync_all()?;
    Ok(())
}

pub fn wal_path(root: &Path) -> PathBuf {
    root.join("wal.bin")
}

/// When an append or a sealed chunk must hit stable storage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicy {
    /// Write the log, but do not fsync. A process crash still recovers
    /// whatever the kernel kept; a power loss may drop the tail.
    #[default]
    Never,
    /// fsync the log before `append` returns, and fsync any chunk that append seals.
    OnAppend,
    /// fsync chunk files and the catalog when a chunk is sealed. The tail is logged
    /// without fsync.
    OnSeal,
}
