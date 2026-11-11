//! CSV and a packed binary interchange format.
//!
//! The binary layout is what the Python harness reads. It is little-endian.
//!
//! ```text
//!  0  magic b"TBIN"
//!  4  version u32 = 1
//!  8  kind u32          1 = trades, 2 = quotes
//! 12  flags u32 = 0
//! 16  scale i64
//! 24  nsymbols u32
//! 28  record_size u32   24 or 40
//! 32  nrows u64
//! 40  data_offset u64
//! 48  symbol table: repeated (u16 len, utf-8 bytes)
//!     zero pad to an 8-byte boundary
//! data_offset  records
//! ```
//!
//! Trade record (24 bytes): `symbol_id u16, side u8, pad u8, size u32, ts i64, price i64`.
//! Quote record (40 bytes): `symbol_id u16, pad u16, bid_size u32, ask_size u32, ts i64, bid i64, ask i64, pad u32`.

use crate::generate::{Dataset, SymbolSeries};
use crate::types::{format_ticks, parse_ticks, Quote, Side, Trade};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

const TRADE_REC: u32 = 24;
const QUOTE_REC: u32 = 40;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatasetMeta {
    pub seed: u64,
    pub scale: i64,
    pub symbols: Vec<String>,
    pub session_start_ns: i64,
    pub session_end_ns: i64,
    pub data_end_ns: i64,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
    pub trade_rows: usize,
    pub quote_rows: usize,
    pub focus_symbol: String,
}

pub fn meta_from_dataset(ds: &Dataset) -> DatasetMeta {
    DatasetMeta {
        seed: ds.seed,
        scale: ds.scale,
        symbols: ds.symbol_names(),
        session_start_ns: ds.session_start_ns,
        session_end_ns: ds.session_end_ns,
        data_end_ns: ds.data_end_ns,
        range_start_ns: ds.range_start_ns,
        range_end_ns: ds.range_end_ns,
        trade_rows: ds.trade_rows(),
        quote_rows: ds.quote_rows(),
        focus_symbol: ds.focus().name.clone(),
    }
}

/// Write `meta.json`, `trades.bin`, `quotes.bin`, `trades.csv`, and `quotes.csv`.
pub fn write_dataset(dir: &Path, ds: &Dataset) -> Result<()> {
    fs::create_dir_all(dir)?;
    let meta = meta_from_dataset(ds);
    let mut meta_file = File::create(dir.join("meta.json"))?;
    serde_json::to_writer_pretty(&mut meta_file, &meta)?;
    meta_file.write_all(b"\n")?;

    let names = meta.symbols.clone();
    let mut trade_bytes = Vec::new();
    let mut quote_bytes = Vec::new();
    let mut trade_rows = 0u64;
    let mut quote_rows = 0u64;
    for (id, series) in ds.symbols.iter().enumerate() {
        let id = id as u16;
        for t in &series.trades {
            push_trade(&mut trade_bytes, id, t);
            trade_rows += 1;
        }
        for q in &series.quotes {
            push_quote(&mut quote_bytes, id, q);
            quote_rows += 1;
        }
    }
    write_bin(
        &dir.join("trades.bin"),
        1,
        ds.scale,
        &names,
        TRADE_REC,
        trade_rows,
        trade_bytes,
    )?;
    write_bin(
        &dir.join("quotes.bin"),
        2,
        ds.scale,
        &names,
        QUOTE_REC,
        quote_rows,
        quote_bytes,
    )?;
    write_trades_csv(&dir.join("trades.csv"), ds)?;
    write_quotes_csv(&dir.join("quotes.csv"), ds)?;
    Ok(())
}

fn push_trade(buf: &mut Vec<u8>, id: u16, t: &Trade) {
    buf.extend_from_slice(&id.to_le_bytes());
    buf.push(t.side.as_u8());
    buf.push(0);
    buf.extend_from_slice(&t.size.to_le_bytes());
    buf.extend_from_slice(&t.ts_ns.to_le_bytes());
    buf.extend_from_slice(&t.price_ticks.to_le_bytes());
}

fn push_quote(buf: &mut Vec<u8>, id: u16, q: &Quote) {
    buf.extend_from_slice(&id.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes());
    buf.extend_from_slice(&q.bid_size.to_le_bytes());
    buf.extend_from_slice(&q.ask_size.to_le_bytes());
    buf.extend_from_slice(&q.ts_ns.to_le_bytes());
    buf.extend_from_slice(&q.bid_ticks.to_le_bytes());
    buf.extend_from_slice(&q.ask_ticks.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
}

fn write_bin(
    path: &Path,
    kind: u32,
    scale: i64,
    symbols: &[String],
    record_size: u32,
    nrows: u64,
    records: Vec<u8>,
) -> Result<()> {
    if records.len() != nrows as usize * record_size as usize {
        return Err(Error::bad("binary record buffer has the wrong length"));
    }
    let mut buf = vec![0u8; 48];
    buf[0..4].copy_from_slice(b"TBIN");
    buf[4..8].copy_from_slice(&1u32.to_le_bytes());
    buf[8..12].copy_from_slice(&kind.to_le_bytes());
    buf[16..24].copy_from_slice(&scale.to_le_bytes());
    buf[24..28].copy_from_slice(&(symbols.len() as u32).to_le_bytes());
    buf[28..32].copy_from_slice(&record_size.to_le_bytes());
    buf[32..40].copy_from_slice(&nrows.to_le_bytes());
    for s in symbols {
        let b = s.as_bytes();
        if b.len() > u16::MAX as usize {
            return Err(Error::bad("symbol longer than u16"));
        }
        buf.extend_from_slice(&(b.len() as u16).to_le_bytes());
        buf.extend_from_slice(b);
    }
    while buf.len() % 8 != 0 {
        buf.push(0);
    }
    let data_offset = buf.len() as u64;
    buf[40..48].copy_from_slice(&data_offset.to_le_bytes());
    buf.extend_from_slice(&records);
    fs::write(path, buf)?;
    Ok(())
}

pub struct BinaryTables {
    pub symbols: Vec<String>,
    pub scale: i64,
    pub trades: Vec<Vec<Trade>>,
    pub quotes: Vec<Vec<Quote>>,
}

pub fn read_binary_tables(dir: &Path) -> Result<BinaryTables> {
    let (symbols, scale, trades) = read_trades_bin(&dir.join("trades.bin"))?;
    let (qsymbols, qscale, quotes) = read_quotes_bin(&dir.join("quotes.bin"))?;
    if symbols != qsymbols || scale != qscale {
        return Err(Error::bad(
            "trade and quote binaries disagree on symbols or scale",
        ));
    }
    Ok(BinaryTables {
        symbols,
        scale,
        trades,
        quotes,
    })
}

pub fn read_trades_bin(path: &Path) -> Result<(Vec<String>, i64, Vec<Vec<Trade>>)> {
    let blob = fs::read(path)?;
    let header = read_header(&blob, 1, TRADE_REC)?;
    let mut groups = vec![Vec::new(); header.symbols.len()];
    let mut off = header.data_offset;
    for _ in 0..header.nrows {
        if off + 24 > blob.len() {
            return Err(Error::corrupt("trade record overruns the file"));
        }
        let id = u16::from_le_bytes(blob[off..off + 2].try_into().unwrap()) as usize;
        let side = Side::from_u8(blob[off + 2])?;
        let size = u32::from_le_bytes(blob[off + 4..off + 8].try_into().unwrap());
        let ts = i64::from_le_bytes(blob[off + 8..off + 16].try_into().unwrap());
        let price = i64::from_le_bytes(blob[off + 16..off + 24].try_into().unwrap());
        let group = groups
            .get_mut(id)
            .ok_or_else(|| Error::corrupt(format!("symbol id {id} is out of range")))?;
        group.push(Trade {
            ts_ns: ts,
            price_ticks: price,
            size,
            side,
        });
        off += 24;
    }
    for g in &mut groups {
        g.sort_by_key(|t| t.ts_ns);
    }
    Ok((header.symbols, header.scale, groups))
}

pub fn read_quotes_bin(path: &Path) -> Result<(Vec<String>, i64, Vec<Vec<Quote>>)> {
    let blob = fs::read(path)?;
    let header = read_header(&blob, 2, QUOTE_REC)?;
    let mut groups = vec![Vec::new(); header.symbols.len()];
    let mut off = header.data_offset;
    for _ in 0..header.nrows {
        if off + 40 > blob.len() {
            return Err(Error::corrupt("quote record overruns the file"));
        }
        let id = u16::from_le_bytes(blob[off..off + 2].try_into().unwrap()) as usize;
        let bid_size = u32::from_le_bytes(blob[off + 4..off + 8].try_into().unwrap());
        let ask_size = u32::from_le_bytes(blob[off + 8..off + 12].try_into().unwrap());
        let ts = i64::from_le_bytes(blob[off + 12..off + 20].try_into().unwrap());
        let bid = i64::from_le_bytes(blob[off + 20..off + 28].try_into().unwrap());
        let ask = i64::from_le_bytes(blob[off + 28..off + 36].try_into().unwrap());
        let group = groups
            .get_mut(id)
            .ok_or_else(|| Error::corrupt(format!("symbol id {id} is out of range")))?;
        group.push(Quote {
            ts_ns: ts,
            bid_ticks: bid,
            ask_ticks: ask,
            bid_size,
            ask_size,
        });
        off += 40;
    }
    for g in &mut groups {
        g.sort_by_key(|q| q.ts_ns);
    }
    Ok((header.symbols, header.scale, groups))
}

struct BinHeader {
    symbols: Vec<String>,
    scale: i64,
    nrows: u64,
    data_offset: usize,
}

fn read_header(blob: &[u8], expect_kind: u32, expect_rec: u32) -> Result<BinHeader> {
    if blob.len() < 48 || &blob[0..4] != b"TBIN" {
        return Err(Error::corrupt("not a TBIN file"));
    }
    let version = u32::from_le_bytes(blob[4..8].try_into().unwrap());
    let kind = u32::from_le_bytes(blob[8..12].try_into().unwrap());
    let scale = i64::from_le_bytes(blob[16..24].try_into().unwrap());
    let nsymbols = u32::from_le_bytes(blob[24..28].try_into().unwrap()) as usize;
    let record_size = u32::from_le_bytes(blob[28..32].try_into().unwrap());
    let nrows = u64::from_le_bytes(blob[32..40].try_into().unwrap());
    let data_offset = u64::from_le_bytes(blob[40..48].try_into().unwrap()) as usize;
    if version != 1 || kind != expect_kind || record_size != expect_rec {
        return Err(Error::corrupt(
            "TBIN header does not match the expected stream",
        ));
    }
    let mut off = 48usize;
    let mut symbols = Vec::with_capacity(nsymbols);
    for _ in 0..nsymbols {
        if off + 2 > blob.len() {
            return Err(Error::corrupt("symbol table is truncated"));
        }
        let nlen = u16::from_le_bytes(blob[off..off + 2].try_into().unwrap()) as usize;
        off += 2;
        if off + nlen > blob.len() {
            return Err(Error::corrupt("symbol table is truncated"));
        }
        let name = std::str::from_utf8(&blob[off..off + nlen])
            .map_err(|_| Error::corrupt("symbol is not utf-8"))?
            .to_string();
        symbols.push(name);
        off += nlen;
    }
    if data_offset > blob.len() || data_offset < off {
        return Err(Error::corrupt("data_offset is out of range"));
    }
    let expect_end = data_offset + nrows as usize * expect_rec as usize;
    if expect_end != blob.len() {
        return Err(Error::corrupt("TBIN length does not match nrows"));
    }
    Ok(BinHeader {
        symbols,
        scale,
        nrows,
        data_offset,
    })
}

pub fn write_trades_csv(path: &Path, ds: &Dataset) -> Result<()> {
    let mut w = BufWriter::new(File::create(path)?);
    writeln!(w, "ts_ns,symbol,price,size,side")?;
    for s in &ds.symbols {
        for t in &s.trades {
            writeln!(
                w,
                "{},{},{},{},{}",
                t.ts_ns,
                s.name,
                format_ticks(t.price_ticks, ds.scale)?,
                t.size,
                t.side
            )?;
        }
    }
    w.flush()?;
    Ok(())
}

pub fn write_quotes_csv(path: &Path, ds: &Dataset) -> Result<()> {
    let mut w = BufWriter::new(File::create(path)?);
    writeln!(w, "ts_ns,symbol,bid,ask,bid_size,ask_size")?;
    for s in &ds.symbols {
        for q in &s.quotes {
            writeln!(
                w,
                "{},{},{},{},{},{}",
                q.ts_ns,
                s.name,
                format_ticks(q.bid_ticks, ds.scale)?,
                format_ticks(q.ask_ticks, ds.scale)?,
                q.bid_size,
                q.ask_size
            )?;
        }
    }
    w.flush()?;
    Ok(())
}

pub fn read_trades_csv(path: &Path, scale: i64) -> Result<BTreeMap<String, Vec<Trade>>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut out: BTreeMap<String, Vec<Trade>> = BTreeMap::new();
    for (idx, line) in reader.lines().enumerate() {
        let line_no = idx + 1;
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if idx == 0 && line.starts_with("ts") {
            continue;
        }
        let mut p = line.split(',');
        let ts = p
            .next()
            .ok_or_else(|| parse_err(line_no, "missing timestamp"))?;
        let symbol = p
            .next()
            .ok_or_else(|| parse_err(line_no, "missing symbol"))?;
        let price = p
            .next()
            .ok_or_else(|| parse_err(line_no, "missing price"))?;
        let size = p.next().ok_or_else(|| parse_err(line_no, "missing size"))?;
        let side = p.next().unwrap_or("unk");
        let ts_ns: i64 = ts
            .trim()
            .parse()
            .map_err(|_| parse_err(line_no, "bad timestamp"))?;
        let price_ticks =
            parse_ticks(price.trim(), scale).map_err(|e| parse_err(line_no, &e.to_string()))?;
        let size: u32 = size
            .trim()
            .parse()
            .map_err(|_| parse_err(line_no, "bad size"))?;
        let side = Side::parse(side).map_err(|e| parse_err(line_no, &e.to_string()))?;
        out.entry(symbol.trim().to_string())
            .or_default()
            .push(Trade {
                ts_ns,
                price_ticks,
                size,
                side,
            });
    }
    for v in out.values_mut() {
        v.sort_by_key(|t| t.ts_ns);
    }
    Ok(out)
}

pub fn read_quotes_csv(path: &Path, scale: i64) -> Result<BTreeMap<String, Vec<Quote>>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut out: BTreeMap<String, Vec<Quote>> = BTreeMap::new();
    for (idx, line) in reader.lines().enumerate() {
        let line_no = idx + 1;
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if idx == 0 && line.starts_with("ts") {
            continue;
        }
        let mut p = line.split(',');
        let ts = field(&mut p, line_no, "timestamp")?;
        let symbol = field(&mut p, line_no, "symbol")?;
        let bid = field(&mut p, line_no, "bid")?;
        let ask = field(&mut p, line_no, "ask")?;
        let bid_size = field(&mut p, line_no, "bid_size")?;
        let ask_size = field(&mut p, line_no, "ask_size")?;
        let ts_ns: i64 = ts
            .parse()
            .map_err(|_| parse_err(line_no, "bad timestamp"))?;
        out.entry(symbol.to_string()).or_default().push(Quote {
            ts_ns,
            bid_ticks: parse_ticks(bid, scale).map_err(|e| parse_err(line_no, &e.to_string()))?,
            ask_ticks: parse_ticks(ask, scale).map_err(|e| parse_err(line_no, &e.to_string()))?,
            bid_size: bid_size
                .parse()
                .map_err(|_| parse_err(line_no, "bad bid_size"))?,
            ask_size: ask_size
                .parse()
                .map_err(|_| parse_err(line_no, "bad ask_size"))?,
        });
    }
    for v in out.values_mut() {
        v.sort_by_key(|q| q.ts_ns);
    }
    Ok(out)
}

fn field<'a>(p: &mut impl Iterator<Item = &'a str>, line: usize, name: &str) -> Result<&'a str> {
    p.next()
        .map(str::trim)
        .ok_or_else(|| parse_err(line, &format!("missing {name}")))
}

fn parse_err(line: usize, message: &str) -> Error {
    Error::Parse {
        line,
        message: message.to_string(),
    }
}

/// Rebuild a [`Dataset`]-shaped list from the binary files. Session fields
/// come from `meta.json` when it is present.
pub fn load_symbol_series(dir: &Path) -> Result<(DatasetMeta, Vec<SymbolSeries>)> {
    let meta_path = dir.join("meta.json");
    let tables = read_binary_tables(dir)?;
    let meta = if meta_path.exists() {
        let f = File::open(meta_path)?;
        serde_json::from_reader(f)?
    } else {
        DatasetMeta {
            seed: 0,
            scale: tables.scale,
            symbols: tables.symbols.clone(),
            session_start_ns: 0,
            session_end_ns: 0,
            data_end_ns: 0,
            range_start_ns: 0,
            range_end_ns: 0,
            trade_rows: tables.trades.iter().map(|t| t.len()).sum(),
            quote_rows: tables.quotes.iter().map(|q| q.len()).sum(),
            focus_symbol: tables.symbols.first().cloned().unwrap_or_default(),
        }
    };
    let mut series = Vec::new();
    for (i, name) in tables.symbols.iter().enumerate() {
        series.push(SymbolSeries {
            name: name.clone(),
            trades: tables.trades.get(i).cloned().unwrap_or_default(),
            quotes: tables.quotes.get(i).cloned().unwrap_or_default(),
        });
    }
    Ok((meta, series))
}
