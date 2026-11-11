//! Immutable chunk file (`.tchk`).
//!
//! ```text
//! [0, 64)     header
//! [64, 64+16*ncols)  column directory
//! payloads
//! last 4 bytes  CRC32 of everything before them (IEEE)
//! ```
//!
//! A chunk is one symbol and one stream (trades or quotes). The symbol is not
//! stored in the file; the directory name and the catalog are the dictionary.

use crate::codec::{decode_i64, decode_u64, encode_i64, encode_u64};
use crate::{Error, Result};
use crc32fast::Hasher;

pub const KIND_TRADE: u8 = 1;
pub const KIND_QUOTE: u8 = 2;

pub const COL_TS: u8 = 1;
pub const COL_PRICE: u8 = 2;
pub const COL_SIZE: u8 = 3;
pub const COL_SIDE: u8 = 4;
pub const COL_BID: u8 = 5;
pub const COL_ASK: u8 = 6;
pub const COL_BID_SZ: u8 = 7;
pub const COL_ASK_SZ: u8 = 8;

const HEADER_LEN: usize = 64;
const ENTRY_LEN: usize = 16;
const MAGIC: &[u8; 4] = b"TCHK";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnInfo {
    pub id: u8,
    pub codec: u8,
    pub offset: u32,
    pub len: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkHeader {
    pub version: u16,
    pub kind: u8,
    pub rows: u32,
    pub min_ts: i64,
    pub max_ts: i64,
    pub price_scale: i64,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradeColumns {
    pub scale: i64,
    pub ts: Vec<i64>,
    pub price: Vec<i64>,
    pub size: Vec<u32>,
    pub side: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuoteColumns {
    pub scale: i64,
    pub ts: Vec<i64>,
    pub bid: Vec<i64>,
    pub ask: Vec<i64>,
    pub bid_size: Vec<u32>,
    pub ask_size: Vec<u32>,
}

pub fn column_name(id: u8) -> &'static str {
    match id {
        COL_TS => "ts",
        COL_PRICE => "price",
        COL_SIZE => "size",
        COL_SIDE => "side",
        COL_BID => "bid",
        COL_ASK => "ask",
        COL_BID_SZ => "bid_size",
        COL_ASK_SZ => "ask_size",
        _ => "unknown",
    }
}

struct Built {
    id: u8,
    codec: u8,
    data: Vec<u8>,
}

pub fn encode_trade_chunk(cols: &TradeColumns) -> Result<Vec<u8>> {
    let n = cols.ts.len();
    if n == 0 || cols.price.len() != n || cols.size.len() != n || cols.side.len() != n {
        return Err(Error::bad("trade columns are empty or unequal"));
    }
    if cols.scale <= 0 {
        return Err(Error::bad("price scale must be positive"));
    }
    let size: Vec<u64> = cols.size.iter().map(|&s| u64::from(s)).collect();
    let side: Vec<u64> = cols.side.iter().map(|&s| u64::from(s)).collect();
    let (ts_c, ts_b) = encode_i64(&cols.ts);
    let (px_c, px_b) = encode_i64(&cols.price);
    let (sz_c, sz_b) = encode_u64(&size);
    let (sd_c, sd_b) = encode_u64(&side);
    finish(
        KIND_TRADE,
        cols.scale,
        cols.ts[0],
        cols.ts[n - 1],
        n,
        vec![
            Built {
                id: COL_TS,
                codec: ts_c,
                data: ts_b,
            },
            Built {
                id: COL_PRICE,
                codec: px_c,
                data: px_b,
            },
            Built {
                id: COL_SIZE,
                codec: sz_c,
                data: sz_b,
            },
            Built {
                id: COL_SIDE,
                codec: sd_c,
                data: sd_b,
            },
        ],
    )
}

pub fn encode_quote_chunk(cols: &QuoteColumns) -> Result<Vec<u8>> {
    let n = cols.ts.len();
    if n == 0
        || cols.bid.len() != n
        || cols.ask.len() != n
        || cols.bid_size.len() != n
        || cols.ask_size.len() != n
    {
        return Err(Error::bad("quote columns are empty or unequal"));
    }
    if cols.scale <= 0 {
        return Err(Error::bad("price scale must be positive"));
    }
    let bid_sz: Vec<u64> = cols.bid_size.iter().map(|&s| u64::from(s)).collect();
    let ask_sz: Vec<u64> = cols.ask_size.iter().map(|&s| u64::from(s)).collect();
    let (ts_c, ts_b) = encode_i64(&cols.ts);
    let (bid_c, bid_b) = encode_i64(&cols.bid);
    let (ask_c, ask_b) = encode_i64(&cols.ask);
    let (bs_c, bs_b) = encode_u64(&bid_sz);
    let (as_c, as_b) = encode_u64(&ask_sz);
    finish(
        KIND_QUOTE,
        cols.scale,
        cols.ts[0],
        cols.ts[n - 1],
        n,
        vec![
            Built {
                id: COL_TS,
                codec: ts_c,
                data: ts_b,
            },
            Built {
                id: COL_BID,
                codec: bid_c,
                data: bid_b,
            },
            Built {
                id: COL_ASK,
                codec: ask_c,
                data: ask_b,
            },
            Built {
                id: COL_BID_SZ,
                codec: bs_c,
                data: bs_b,
            },
            Built {
                id: COL_ASK_SZ,
                codec: as_c,
                data: as_b,
            },
        ],
    )
}

fn finish(
    kind: u8,
    scale: i64,
    min_ts: i64,
    max_ts: i64,
    rows: usize,
    cols: Vec<Built>,
) -> Result<Vec<u8>> {
    let rows_u = u32::try_from(rows).map_err(|_| Error::bad("too many rows for one chunk"))?;
    let dir_bytes = cols.len() * ENTRY_LEN;
    let payload_len: usize = cols.iter().map(|c| c.data.len()).sum();
    let payload_u =
        u32::try_from(payload_len).map_err(|_| Error::bad("chunk payload exceeds 4 GiB"))?;
    let total = HEADER_LEN + dir_bytes + payload_len + 4;
    let mut buf = vec![0u8; total];
    buf[0..4].copy_from_slice(MAGIC);
    buf[4..6].copy_from_slice(&1u16.to_le_bytes());
    buf[6] = kind;
    buf[8..12].copy_from_slice(&rows_u.to_le_bytes());
    buf[12..14].copy_from_slice(&(cols.len() as u16).to_le_bytes());
    buf[16..24].copy_from_slice(&min_ts.to_le_bytes());
    buf[24..32].copy_from_slice(&max_ts.to_le_bytes());
    buf[32..40].copy_from_slice(&scale.to_le_bytes());
    buf[40..44].copy_from_slice(&payload_u.to_le_bytes());
    buf[48..52].copy_from_slice(&(dir_bytes as u32).to_le_bytes());

    let mut offset = HEADER_LEN + dir_bytes;
    for (i, col) in cols.iter().enumerate() {
        let e = HEADER_LEN + i * ENTRY_LEN;
        buf[e] = col.id;
        buf[e + 1] = col.codec;
        buf[e + 4..e + 8].copy_from_slice(&(offset as u32).to_le_bytes());
        buf[e + 8..e + 12].copy_from_slice(&(col.data.len() as u32).to_le_bytes());
        buf[offset..offset + col.data.len()].copy_from_slice(&col.data);
        offset += col.data.len();
    }
    debug_assert_eq!(offset, total - 4);
    let mut hasher = Hasher::new();
    hasher.update(&buf[..total - 4]);
    let crc = hasher.finalize();
    buf[total - 4..].copy_from_slice(&crc.to_le_bytes());
    Ok(buf)
}

pub fn parse_chunk(file: &[u8]) -> Result<ChunkHeader> {
    if file.len() < HEADER_LEN + 4 {
        return Err(Error::corrupt("chunk shorter than a header"));
    }
    let (body, crc_bytes) = file.split_at(file.len() - 4);
    let expect = u32::from_le_bytes(crc_bytes.try_into().unwrap());
    let mut hasher = Hasher::new();
    hasher.update(body);
    let got = hasher.finalize();
    if got != expect {
        return Err(Error::corrupt(format!(
            "crc mismatch: stored {expect:#x}, computed {got:#x}"
        )));
    }
    if &file[0..4] != MAGIC {
        return Err(Error::corrupt("chunk magic is not TCHK"));
    }
    let version = u16::from_le_bytes(file[4..6].try_into().unwrap());
    if version != 1 {
        return Err(Error::corrupt(format!(
            "unsupported chunk version {version}"
        )));
    }
    let kind = file[6];
    let rows = u32::from_le_bytes(file[8..12].try_into().unwrap());
    let ncols = u16::from_le_bytes(file[12..14].try_into().unwrap()) as usize;
    let min_ts = i64::from_le_bytes(file[16..24].try_into().unwrap());
    let max_ts = i64::from_le_bytes(file[24..32].try_into().unwrap());
    let price_scale = i64::from_le_bytes(file[32..40].try_into().unwrap());
    let dir_bytes = ncols * ENTRY_LEN;
    if HEADER_LEN + dir_bytes > body.len() {
        return Err(Error::corrupt("column directory overruns the chunk"));
    }
    let mut columns = Vec::with_capacity(ncols);
    for i in 0..ncols {
        let e = HEADER_LEN + i * ENTRY_LEN;
        let id = file[e];
        let codec = file[e + 1];
        let offset = u32::from_le_bytes(file[e + 4..e + 8].try_into().unwrap());
        let len = u32::from_le_bytes(file[e + 8..e + 12].try_into().unwrap());
        let end = (offset as usize)
            .checked_add(len as usize)
            .ok_or_else(|| Error::corrupt("column span overflows"))?;
        if (offset as usize) < HEADER_LEN + dir_bytes || end > body.len() {
            return Err(Error::corrupt(format!(
                "column {} span {offset}+{len} is outside the payload",
                column_name(id)
            )));
        }
        columns.push(ColumnInfo {
            id,
            codec,
            offset,
            len,
        });
    }
    Ok(ChunkHeader {
        version,
        kind,
        rows,
        min_ts,
        max_ts,
        price_scale,
        columns,
    })
}

fn column<'a>(file: &'a [u8], header: &ChunkHeader, id: u8) -> Result<(u8, &'a [u8])> {
    let col = header.columns.iter().find(|c| c.id == id).ok_or_else(|| {
        Error::corrupt(format!("chunk is missing the {} column", column_name(id)))
    })?;
    let start = col.offset as usize;
    let end = start + col.len as usize;
    Ok((col.codec, &file[start..end]))
}

pub fn decode_i64_column(file: &[u8], header: &ChunkHeader, id: u8) -> Result<Vec<i64>> {
    let (codec, bytes) = column(file, header, id)?;
    decode_i64(codec, bytes, header.rows as usize)
}

pub fn decode_u64_column(file: &[u8], header: &ChunkHeader, id: u8) -> Result<Vec<u64>> {
    let (codec, bytes) = column(file, header, id)?;
    decode_u64(codec, bytes, header.rows as usize)
}

pub fn decode_trade_chunk(file: &[u8]) -> Result<TradeColumns> {
    let header = parse_chunk(file)?;
    if header.kind != KIND_TRADE {
        return Err(Error::corrupt("expected a trade chunk"));
    }
    let ts = decode_i64_column(file, &header, COL_TS)?;
    let price = decode_i64_column(file, &header, COL_PRICE)?;
    let size_u = decode_u64_column(file, &header, COL_SIZE)?;
    let side_u = decode_u64_column(file, &header, COL_SIDE)?;
    let mut size = Vec::with_capacity(size_u.len());
    for s in size_u {
        size.push(u32::try_from(s).map_err(|_| Error::corrupt("size does not fit in u32"))?);
    }
    let mut side = Vec::with_capacity(side_u.len());
    for s in side_u {
        let b = u8::try_from(s).map_err(|_| Error::corrupt("side does not fit in u8"))?;
        side.push(b);
    }
    Ok(TradeColumns {
        scale: header.price_scale,
        ts,
        price,
        size,
        side,
    })
}

pub fn decode_quote_chunk(file: &[u8]) -> Result<QuoteColumns> {
    let header = parse_chunk(file)?;
    if header.kind != KIND_QUOTE {
        return Err(Error::corrupt("expected a quote chunk"));
    }
    let ts = decode_i64_column(file, &header, COL_TS)?;
    let bid = decode_i64_column(file, &header, COL_BID)?;
    let ask = decode_i64_column(file, &header, COL_ASK)?;
    let bid_size = narrow_u32(decode_u64_column(file, &header, COL_BID_SZ)?)?;
    let ask_size = narrow_u32(decode_u64_column(file, &header, COL_ASK_SZ)?)?;
    Ok(QuoteColumns {
        scale: header.price_scale,
        ts,
        bid,
        ask,
        bid_size,
        ask_size,
    })
}

fn narrow_u32(values: Vec<u64>) -> Result<Vec<u32>> {
    values
        .into_iter()
        .map(|v| u32::try_from(v).map_err(|_| Error::corrupt("value does not fit in u32")))
        .collect()
}

pub fn codec_of(header: &ChunkHeader, id: u8) -> Option<u8> {
    header.columns.iter().find(|c| c.id == id).map(|c| c.codec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trade_chunk_roundtrip_and_crc() {
        let cols = TradeColumns {
            scale: 10_000,
            ts: (0..300).map(|i| 1_000 + i * 250).collect(),
            price: (0..300).map(|i| 1_850_000 + (i % 7) as i64 - 3).collect(),
            size: (0..300).map(|i| 100 * (1 + (i % 5) as u32)).collect(),
            side: (0..300).map(|i| if i % 2 == 0 { 1 } else { 2 }).collect(),
        };
        let bytes = encode_trade_chunk(&cols).unwrap();
        let got = decode_trade_chunk(&bytes).unwrap();
        assert_eq!(got, cols);
        let mut bad = bytes.clone();
        let last = bad.len() - 8;
        bad[last] ^= 0x5a;
        assert!(decode_trade_chunk(&bad).is_err());
    }

    #[test]
    fn quote_chunk_roundtrip() {
        let cols = QuoteColumns {
            scale: 10_000,
            ts: vec![10, 20, 30, 40],
            bid: vec![100, 100, 110, 110],
            ask: vec![200, 200, 210, 210],
            bid_size: vec![100, 200, 100, 500],
            ask_size: vec![100, 100, 300, 500],
        };
        let bytes = encode_quote_chunk(&cols).unwrap();
        assert_eq!(decode_quote_chunk(&bytes).unwrap(), cols);
    }
}
