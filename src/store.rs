//! Symbol-partitioned store.
//!
//! Appends are monotonic per symbol. Full chunks are sealed as immutable
//! `.tchk` files and recorded in `catalog.json`. Queries seal any open tail
//! first, memory-map the chunks whose `[min_ts, max_ts]` overlaps the window,
//! and aggregate one chunk at a time.

use crate::catalog::{self, Catalog, ChunkMeta, SymbolEntry};
use crate::codec::{self, notional_from_delta};
use crate::format::{
    self, decode_i64_column, decode_quote_chunk, decode_trade_chunk, decode_u64_column,
    encode_quote_chunk, encode_trade_chunk, parse_chunk, parse_chunk_inner, QuoteColumns,
    TradeColumns, COL_PRICE, COL_SIZE, COL_TS,
};
use crate::types::{bucket_start, AsofRow, Bar, Quote, Side, Trade, Vwap};
use crate::wal::{self, SyncPolicy, Wal, WalRecord};
use crate::{Error, Result};
use memmap2::Mmap;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct Options {
    pub price_scale: i64,
    pub chunk_rows: usize,
    pub sync: SyncPolicy,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            price_scale: 10_000,
            chunk_rows: 16_384,
            sync: SyncPolicy::Never,
        }
    }
}

/// Test hook. The store returns an error at this point and does not finish the commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrashPoint {
    /// The chunk file is durable. The catalog has not been updated.
    AfterChunkFile,
}

struct Batch<T> {
    seq: u64,
    /// Rows of `seq` sealed before this in-memory suffix.
    already_sealed: u32,
    rows: Vec<T>,
}

struct Piece<T> {
    seq: u64,
    already_sealed_before: u32,
    rows: Vec<T>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct CodecUse {
    pub chunks: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ColStat {
    pub rows: u64,
    pub payload_bytes: u64,
    pub codecs: BTreeMap<String, CodecUse>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StoreStats {
    pub symbols: usize,
    pub price_scale: i64,
    pub chunk_rows: u32,
    pub trade_rows: u64,
    pub quote_rows: u64,
    pub trade_chunks: u64,
    pub quote_chunks: u64,
    pub trade_file_bytes: u64,
    pub quote_file_bytes: u64,
    pub catalog_bytes: u64,
    pub total_bytes: u64,
    pub columns: BTreeMap<String, ColStat>,
}

struct SymbolBuf {
    trades: Vec<Batch<Trade>>,
    quotes: Vec<Batch<Quote>>,
}

enum Stream {
    Trades,
    Quotes,
}

impl Stream {
    fn dir(&self) -> &'static str {
        match self {
            Stream::Trades => "trades",
            Stream::Quotes => "quotes",
        }
    }
}

pub struct TickStore {
    root: PathBuf,
    catalog: Catalog,
    buffers: HashMap<String, SymbolBuf>,
    chunk_rows: usize,
    catalog_dirty: bool,
    /// Chunk paths whose CRC has already been checked in this process.
    verified: HashSet<PathBuf>,
    /// Immutable chunk mappings kept for the life of the store.
    maps: HashMap<PathBuf, Mmap>,
    wal: Wal,
    next_seq: u64,
    sync: SyncPolicy,
    skip_flush: bool,
    crash: Option<CrashPoint>,
}

impl TickStore {
    pub fn create(root: &Path, opts: Options) -> Result<Self> {
        if opts.price_scale <= 0 {
            return Err(Error::bad("price scale must be positive"));
        }
        if !(1..=1_048_576).contains(&opts.chunk_rows) {
            return Err(Error::bad("chunk_rows must be in 1..=1048576"));
        }
        if Catalog::path(root).exists() {
            return Err(Error::bad(format!(
                "store already exists at {}",
                root.display()
            )));
        }
        fs::create_dir_all(root)?;
        let catalog = Catalog::new(opts.price_scale, opts.chunk_rows as u32, opts.sync);
        catalog.save(root)?;
        let wal = Wal::create(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            chunk_rows: opts.chunk_rows,
            catalog,
            buffers: HashMap::new(),
            catalog_dirty: false,
            verified: HashSet::new(),
            maps: HashMap::new(),
            wal,
            next_seq: 1,
            sync: opts.sync,
            skip_flush: false,
            crash: None,
        })
    }

    pub fn open(root: &Path) -> Result<Self> {
        let catalog = Catalog::load(root)?;
        let chunk_rows = catalog.chunk_rows as usize;
        let sync = catalog.sync;
        let wal = Wal::open(root)?;
        let mut store = Self {
            root: root.to_path_buf(),
            chunk_rows,
            catalog,
            buffers: HashMap::new(),
            catalog_dirty: false,
            verified: HashSet::new(),
            maps: HashMap::new(),
            wal,
            next_seq: 1,
            sync,
            skip_flush: false,
            crash: None,
        };
        store.remove_orphans()?;
        store.replay_wal()?;
        store.compact_wal()?;
        Ok(store)
    }

    /// Stop at `point` on the next seal. Drop will not flush the tail.
    pub fn arm_crash(&mut self, point: CrashPoint) {
        self.crash = Some(point);
    }

    /// Drop the store without sealing the tail. Recovery reads the log.
    pub fn abandon(mut self) {
        self.skip_flush = true;
    }

    pub fn price_scale(&self) -> i64 {
        self.catalog.price_scale
    }

    pub fn symbols(&self) -> Vec<String> {
        self.catalog.symbols.keys().cloned().collect()
    }

    /// Half-open span covering every sealed row of one stream.
    pub fn time_span(&self, symbol: &str, trades: bool) -> Result<(i64, i64)> {
        let entry = self.entry(symbol)?;
        let chunks = if trades { &entry.trades } else { &entry.quotes };
        let first = chunks.first().ok_or_else(|| {
            Error::NotFound(format!(
                "{symbol} has no {} chunks",
                if trades { "trade" } else { "quote" }
            ))
        })?;
        let last = chunks.last().expect("non-empty");
        Ok((first.min_ts, last.max_ts.saturating_add(1)))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Seal open tails and write the catalog. Chunk files are replaced with
    /// `rename` and are not fsynced.
    pub fn flush(&mut self) -> Result<()> {
        let symbols: Vec<String> = self.buffers.keys().cloned().collect();
        for s in symbols {
            self.seal_trades(&s, true)?;
            self.seal_quotes(&s, true)?;
        }
        if self.catalog_dirty {
            self.persist_catalog()?;
        }
        self.compact_wal()?;
        Ok(())
    }

    fn compact_wal(&mut self) -> Result<()> {
        let records = self.wal.replay()?;
        let kept: Vec<WalRecord> = records
            .into_iter()
            .filter(|rec| {
                let (seq, rows) = self.symbol_wal_cursor(&rec.symbol, rec);
                !record_is_sealed(rec, seq, rows)
            })
            .collect();
        self.wal.rewrite(&kept, self.sync == SyncPolicy::OnAppend)?;
        Ok(())
    }

    pub fn append_trades(&mut self, symbol: &str, trades: &[Trade]) -> Result<()> {
        Self::check_symbol(symbol)?;
        if trades.is_empty() {
            return Ok(());
        }
        let mut prev = self.last_ts(symbol, Stream::Trades);
        for t in trades {
            if let Some(p) = prev {
                if t.ts_ns < p {
                    return Err(Error::Unsorted {
                        symbol: symbol.to_string(),
                        prev: p,
                        ts: t.ts_ns,
                    });
                }
            }
            prev = Some(t.ts_ns);
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.wal
            .append_trades(seq, symbol, trades, self.sync == SyncPolicy::OnAppend)?;
        self.ensure_buf(symbol);
        {
            let buf = self.buffers.get_mut(symbol).unwrap();
            buf.trades.push(Batch {
                seq,
                already_sealed: 0,
                rows: trades.to_vec(),
            });
        }
        while self.batch_rows(symbol, Stream::Trades) >= self.chunk_rows {
            self.seal_trades(symbol, false)?;
        }
        Ok(())
    }

    pub fn append_quotes(&mut self, symbol: &str, quotes: &[Quote]) -> Result<()> {
        Self::check_symbol(symbol)?;
        if quotes.is_empty() {
            return Ok(());
        }
        let mut prev = self.last_ts(symbol, Stream::Quotes);
        for q in quotes {
            if let Some(p) = prev {
                if q.ts_ns < p {
                    return Err(Error::Unsorted {
                        symbol: symbol.to_string(),
                        prev: p,
                        ts: q.ts_ns,
                    });
                }
            }
            prev = Some(q.ts_ns);
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.wal
            .append_quotes(seq, symbol, quotes, self.sync == SyncPolicy::OnAppend)?;
        self.ensure_buf(symbol);
        {
            let buf = self.buffers.get_mut(symbol).unwrap();
            buf.quotes.push(Batch {
                seq,
                already_sealed: 0,
                rows: quotes.to_vec(),
            });
        }
        while self.batch_rows(symbol, Stream::Quotes) >= self.chunk_rows {
            self.seal_quotes(symbol, false)?;
        }
        Ok(())
    }

    pub fn scan_trades(&mut self, symbol: &str, start: i64, end: i64) -> Result<Vec<Trade>> {
        self.flush()?;
        let metas = self.trade_metas(symbol, start, end)?;
        let mut out = Vec::new();
        for meta in metas {
            let cols = self.load_trades(symbol, &meta)?;
            let full = meta.min_ts >= start && meta.max_ts < end;
            for i in 0..cols.ts.len() {
                let ts = cols.ts[i];
                if !full && (ts < start || ts >= end) {
                    if ts >= end {
                        break;
                    }
                    continue;
                }
                out.push(Trade {
                    ts_ns: ts,
                    price_ticks: cols.price[i],
                    size: cols.size[i],
                    side: Side::from_u8(cols.side[i])?,
                });
            }
        }
        Ok(out)
    }

    /// Count prints in `[start, end)`. Chunks that lie entirely inside the
    /// window contribute their row count from the catalog; only boundary
    /// chunks are decoded.
    pub fn count_trades(&mut self, symbol: &str, start: i64, end: i64) -> Result<u64> {
        self.flush()?;
        let metas = self.trade_metas(symbol, start, end)?;
        let mut n = 0u64;
        for meta in metas {
            if meta.min_ts >= start && meta.max_ts < end {
                n += u64::from(meta.rows);
                continue;
            }
            let cols = self.load_ts(symbol, Stream::Trades, &meta)?;
            for ts in cols {
                if ts < start {
                    continue;
                }
                if ts >= end {
                    break;
                }
                n += 1;
            }
        }
        Ok(n)
    }

    pub fn ohlcv(
        &mut self,
        symbol: &str,
        start: i64,
        end: i64,
        interval_ns: i64,
    ) -> Result<Vec<Bar>> {
        if interval_ns <= 0 {
            return Err(Error::bad("interval must be positive"));
        }
        self.flush()?;
        if start >= end {
            return Ok(Vec::new());
        }
        let metas = self.trade_metas(symbol, start, end)?;
        let mut out = Vec::new();
        let mut cur: Option<BarAcc> = None;
        for meta in &metas {
            let full = meta.min_ts >= start && meta.max_ts < end;
            let (ts, price, size) = self.load_agg_cols(symbol, meta, true)?;
            let n = price.len();
            for i in 0..n {
                if !full {
                    let t = ts[i];
                    if t < start {
                        continue;
                    }
                    if t >= end {
                        break;
                    }
                }
                let b = bucket_start(ts[i], interval_ns);
                let px = price[i];
                let sz = size[i];
                let same = cur.as_ref().is_some_and(|acc| acc.bucket == b);
                if same {
                    cur.as_mut().unwrap().update(px, sz)?;
                    continue;
                }
                if let Some(acc) = cur.take() {
                    out.push(acc.finish());
                }
                cur = Some(BarAcc::new(b, px, sz));
            }
        }
        if let Some(acc) = cur {
            out.push(acc.finish());
        }
        Ok(out)
    }

    pub fn vwap(&mut self, symbol: &str, start: i64, end: i64) -> Result<Vwap> {
        self.flush()?;
        let metas = self.trade_metas(symbol, start, end)?;
        let mut out = Vwap::default();
        for meta in &metas {
            let full = meta.min_ts >= start && meta.max_ts < end;
            if full {
                if let Some((notional, volume, trades)) = self.try_fused_vwap(symbol, meta)? {
                    out.notional_ticks += notional;
                    out.volume += volume;
                    out.trades += trades;
                    continue;
                }
            }
            let (ts, price, size) = self.load_agg_cols(symbol, meta, !full)?;
            if full {
                accumulate_vwap(&mut out, &price, &size);
                continue;
            }
            for i in 0..price.len() {
                let t = ts[i];
                if t < start {
                    continue;
                }
                if t >= end {
                    break;
                }
                add_print(&mut out.notional_ticks, &mut out.volume, price[i], size[i])?;
                out.trades += 1;
            }
        }
        Ok(out)
    }

    pub fn asof_join(&mut self, symbol: &str, start: i64, end: i64) -> Result<Vec<AsofRow>> {
        self.flush()?;
        if start >= end {
            return Ok(Vec::new());
        }
        let entry = self.entry(symbol)?;
        let trade_metas = catalog::overlapping(&entry.trades, start, end).to_vec();
        let quote_metas = catalog::asof_quote_chunks(&entry.quotes, start, end).to_vec();
        let mut cursor = QuoteCursor::load(self, symbol, quote_metas)?;
        let mut out = Vec::new();
        for meta in &trade_metas {
            let cols = self.load_trades(symbol, meta)?;
            for i in 0..cols.ts.len() {
                let ts = cols.ts[i];
                if ts < start {
                    continue;
                }
                if ts >= end {
                    break;
                }
                cursor.advance_to(ts)?;
                let q = cursor.active;
                out.push(AsofRow {
                    ts_ns: ts,
                    price_ticks: cols.price[i],
                    size: cols.size[i],
                    side: Side::from_u8(cols.side[i])?,
                    quote_ts_ns: q.map(|q| q.ts),
                    bid_ticks: q.map(|q| q.bid),
                    ask_ticks: q.map(|q| q.ask),
                    bid_size: q.map(|q| q.bid_size),
                    ask_size: q.map(|q| q.ask_size),
                });
            }
        }
        Ok(out)
    }

    pub fn statistics(&self) -> Result<StoreStats> {
        let mut stats = StoreStats {
            symbols: self.catalog.symbols.len(),
            price_scale: self.catalog.price_scale,
            chunk_rows: self.catalog.chunk_rows,
            trade_rows: 0,
            quote_rows: 0,
            trade_chunks: 0,
            quote_chunks: 0,
            trade_file_bytes: 0,
            quote_file_bytes: 0,
            catalog_bytes: file_len(&Catalog::path(&self.root)).unwrap_or(0),
            total_bytes: dir_bytes(&self.root)?,
            columns: BTreeMap::new(),
        };
        for (symbol, entry) in &self.catalog.symbols {
            stats.trade_rows += entry.trades.iter().map(|c| u64::from(c.rows)).sum::<u64>();
            stats.quote_rows += entry.quotes.iter().map(|c| u64::from(c.rows)).sum::<u64>();
            stats.trade_chunks += entry.trades.len() as u64;
            stats.quote_chunks += entry.quotes.len() as u64;
            for meta in &entry.trades {
                let path = self.chunk_path(symbol, Stream::Trades, meta.id);
                stats.trade_file_bytes += meta.bytes;
                accumulate_columns(&mut stats.columns, "trade", &path)?;
            }
            for meta in &entry.quotes {
                let path = self.chunk_path(symbol, Stream::Quotes, meta.id);
                stats.quote_file_bytes += meta.bytes;
                accumulate_columns(&mut stats.columns, "quote", &path)?;
            }
        }
        Ok(stats)
    }

    fn trade_metas(&self, symbol: &str, start: i64, end: i64) -> Result<Vec<ChunkMeta>> {
        let entry = self.entry(symbol)?;
        Ok(catalog::overlapping(&entry.trades, start, end).to_vec())
    }

    fn entry(&self, symbol: &str) -> Result<&SymbolEntry> {
        self.catalog
            .symbols
            .get(symbol)
            .ok_or_else(|| Error::NotFound(symbol.to_string()))
    }

    fn try_fused_vwap(
        &mut self,
        symbol: &str,
        meta: &ChunkMeta,
    ) -> Result<Option<(i128, u64, u64)>> {
        let path = self.chunk_path(symbol, Stream::Trades, meta.id);
        let check = !self.verified.contains(&path);
        let prepared = {
            let mmap = self.cached_map(&path)?;
            let header = parse_chunk_inner(mmap, check)?;
            let (codec, bytes) = format::column_slice(mmap, &header, COL_PRICE)?;
            let (inner, owned) = codec::unwrap_zstd(codec, bytes)?;
            if inner != codec::CODEC_DELTA {
                return Ok(None);
            }
            let price = owned.unwrap_or_else(|| bytes.to_vec());
            let size_u = decode_u64_column(mmap, &header, COL_SIZE)?;
            let mut sizes = Vec::with_capacity(size_u.len());
            for s in size_u {
                sizes.push(
                    u32::try_from(s).map_err(|_| Error::corrupt("size does not fit in u32"))?,
                );
            }
            (price, sizes)
        };
        if check {
            self.verified.insert(path);
        }
        let (notional, volume) = notional_from_delta(&prepared.0, &prepared.1)?;
        let trades = prepared.1.len() as u64;
        Ok(Some((notional, volume, trades)))
    }

    fn cached_map(&mut self, path: &Path) -> Result<&Mmap> {
        if !self.maps.contains_key(path) {
            let mmap = map_file(path)?;
            self.maps.insert(path.to_path_buf(), mmap);
        }
        Ok(self.maps.get(path).expect("map inserted"))
    }

    /// Price and size, plus timestamps when the caller has to filter or bucket.
    fn load_agg_cols(
        &mut self,
        symbol: &str,
        meta: &ChunkMeta,
        want_ts: bool,
    ) -> Result<(Vec<i64>, Vec<i64>, Vec<u32>)> {
        let path = self.chunk_path(symbol, Stream::Trades, meta.id);
        let check = !self.verified.contains(&path);
        let header = {
            let mmap = self.cached_map(&path)?;
            parse_chunk_inner(mmap, check)?
        };
        if check {
            self.verified.insert(path.clone());
        }
        let mmap = self.cached_map(&path)?;
        let rows = header.rows as usize;
        let ts = if want_ts {
            decode_i64_column(mmap, &header, COL_TS)?
        } else {
            Vec::new()
        };
        let price = decode_i64_column(mmap, &header, COL_PRICE)?;
        let size_u = decode_u64_column(mmap, &header, COL_SIZE)?;
        if price.len() != rows || size_u.len() != rows || (want_ts && ts.len() != rows) {
            return Err(Error::corrupt("aggregated columns have different lengths"));
        }
        let mut size = Vec::with_capacity(rows);
        for s in size_u {
            size.push(u32::try_from(s).map_err(|_| Error::corrupt("size does not fit in u32"))?);
        }
        Ok((ts, price, size))
    }

    fn load_trades(&self, symbol: &str, meta: &ChunkMeta) -> Result<TradeColumns> {
        let path = self.chunk_path(symbol, Stream::Trades, meta.id);
        let mmap = map_file(&path)?;
        decode_trade_chunk(&mmap)
    }

    fn load_quotes(&self, symbol: &str, meta: &ChunkMeta) -> Result<QuoteColumns> {
        let path = self.chunk_path(symbol, Stream::Quotes, meta.id);
        let mmap = map_file(&path)?;
        decode_quote_chunk(&mmap)
    }

    fn load_ts(&self, symbol: &str, stream: Stream, meta: &ChunkMeta) -> Result<Vec<i64>> {
        let path = self.chunk_path(symbol, stream, meta.id);
        let mmap = map_file(&path)?;
        let header = parse_chunk(&mmap)?;
        decode_i64_column(&mmap, &header, COL_TS)
    }

    fn check_symbol(symbol: &str) -> Result<()> {
        let ok = (1..=32).contains(&symbol.len())
            && symbol
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'));
        if ok {
            Ok(())
        } else {
            Err(Error::bad(format!(
                "symbol '{symbol}' must be 1..=32 chars of [A-Za-z0-9._-]"
            )))
        }
    }

    fn ensure_buf(&mut self, symbol: &str) {
        if self.buffers.contains_key(symbol) {
            return;
        }
        self.buffers.insert(
            symbol.to_string(),
            SymbolBuf {
                trades: Vec::new(),
                quotes: Vec::new(),
            },
        );
    }

    fn last_ts(&self, symbol: &str, stream: Stream) -> Option<i64> {
        if let Some(buf) = self.buffers.get(symbol) {
            let cached = match stream {
                Stream::Trades => buf
                    .trades
                    .last()
                    .and_then(|b| b.rows.last().map(|t| t.ts_ns)),
                Stream::Quotes => buf
                    .quotes
                    .last()
                    .and_then(|b| b.rows.last().map(|q| q.ts_ns)),
            };
            if cached.is_some() {
                return cached;
            }
        }
        let entry = self.catalog.symbols.get(symbol)?;
        match stream {
            Stream::Trades => entry.trades.last().map(|c| c.max_ts),
            Stream::Quotes => entry.quotes.last().map(|c| c.max_ts),
        }
    }

    fn batch_rows(&self, symbol: &str, stream: Stream) -> usize {
        let Some(buf) = self.buffers.get(symbol) else {
            return 0;
        };
        match stream {
            Stream::Trades => buf.trades.iter().map(|b| b.rows.len()).sum(),
            Stream::Quotes => buf.quotes.iter().map(|b| b.rows.len()).sum(),
        }
    }

    fn seal_trades(&mut self, symbol: &str, force: bool) -> Result<()> {
        let len = self.batch_rows(symbol, Stream::Trades);
        if len == 0 || (!force && len < self.chunk_rows) {
            return Ok(());
        }
        let n = if force { len } else { self.chunk_rows };
        let id = self
            .catalog
            .symbols
            .get(symbol)
            .map(|s| s.trades.len() as u32)
            .unwrap_or(0);
        let path = self.chunk_path(symbol, Stream::Trades, id);
        let scale = self.catalog.price_scale;
        let pieces = {
            let buf = self.buffers.get_mut(symbol).unwrap();
            take_front(&mut buf.trades, n)
        };
        let trades = flatten_pieces(&pieces);
        let encoded = match encode_taken_trades(scale, &trades) {
            Ok(bytes) => bytes,
            Err(e) => {
                let buf = self.buffers.get_mut(symbol).unwrap();
                restore_front(&mut buf.trades, pieces);
                return Err(e);
            }
        };
        if let Err(e) = atomic_write(&path, &encoded, self.wants_durable_files()) {
            let buf = self.buffers.get_mut(symbol).unwrap();
            restore_front(&mut buf.trades, pieces);
            return Err(e);
        }
        if self.crash == Some(CrashPoint::AfterChunkFile) {
            self.crash = None;
            self.skip_flush = true;
            return Err(Error::bad("simulated crash after chunk file"));
        }
        self.note_sealed(symbol, Stream::Trades, &pieces);
        let meta = ChunkMeta {
            id,
            min_ts: trades[0].ts_ns,
            max_ts: trades[trades.len() - 1].ts_ns,
            rows: trades.len() as u32,
            bytes: encoded.len() as u64,
        };
        self.catalog
            .symbols
            .entry(symbol.to_string())
            .or_default()
            .trades
            .push(meta);
        self.persist_catalog()?;
        Ok(())
    }

    fn seal_quotes(&mut self, symbol: &str, force: bool) -> Result<()> {
        let len = self.batch_rows(symbol, Stream::Quotes);
        if len == 0 || (!force && len < self.chunk_rows) {
            return Ok(());
        }
        let n = if force { len } else { self.chunk_rows };
        let id = self
            .catalog
            .symbols
            .get(symbol)
            .map(|s| s.quotes.len() as u32)
            .unwrap_or(0);
        let path = self.chunk_path(symbol, Stream::Quotes, id);
        let scale = self.catalog.price_scale;
        let pieces = {
            let buf = self.buffers.get_mut(symbol).unwrap();
            take_front(&mut buf.quotes, n)
        };
        let quotes = flatten_pieces(&pieces);
        let encoded = match encode_taken_quotes(scale, &quotes) {
            Ok(bytes) => bytes,
            Err(e) => {
                let buf = self.buffers.get_mut(symbol).unwrap();
                restore_front(&mut buf.quotes, pieces);
                return Err(e);
            }
        };
        if let Err(e) = atomic_write(&path, &encoded, self.wants_durable_files()) {
            let buf = self.buffers.get_mut(symbol).unwrap();
            restore_front(&mut buf.quotes, pieces);
            return Err(e);
        }
        if self.crash == Some(CrashPoint::AfterChunkFile) {
            self.crash = None;
            self.skip_flush = true;
            return Err(Error::bad("simulated crash after chunk file"));
        }
        self.note_sealed(symbol, Stream::Quotes, &pieces);
        let meta = ChunkMeta {
            id,
            min_ts: quotes[0].ts_ns,
            max_ts: quotes[quotes.len() - 1].ts_ns,
            rows: quotes.len() as u32,
            bytes: encoded.len() as u64,
        };
        self.catalog
            .symbols
            .entry(symbol.to_string())
            .or_default()
            .quotes
            .push(meta);
        self.persist_catalog()?;
        Ok(())
    }

    fn wants_durable_files(&self) -> bool {
        matches!(self.sync, SyncPolicy::OnAppend | SyncPolicy::OnSeal)
    }

    fn note_sealed<T>(&mut self, symbol: &str, stream: Stream, pieces: &[Piece<T>]) {
        let entry = self.catalog.symbols.entry(symbol.to_string()).or_default();
        for piece in pieces {
            let through = piece.already_sealed_before + piece.rows.len() as u32;
            match stream {
                Stream::Trades => {
                    entry.trade_wal_seq = piece.seq;
                    entry.trade_wal_rows = through;
                }
                Stream::Quotes => {
                    entry.quote_wal_seq = piece.seq;
                    entry.quote_wal_rows = through;
                }
            }
        }
    }

    fn persist_catalog(&mut self) -> Result<()> {
        let durable = self.wants_durable_files();
        self.catalog.save(&self.root)?;
        if durable {
            File::open(Catalog::path(&self.root))?.sync_all()?;
            wal::sync_dir(&self.root)?;
        }
        self.catalog_dirty = false;
        Ok(())
    }

    fn replay_wal(&mut self) -> Result<()> {
        let records = self.wal.replay()?;
        if let Some(max) = records.iter().map(|r| r.seq).max() {
            self.next_seq = max + 1;
        }
        for rec in records {
            let (commit_seq, commit_rows) = self.symbol_wal_cursor(&rec.symbol, &rec);
            self.install_replayed(rec, commit_seq, commit_rows)?;
        }
        Ok(())
    }

    fn symbol_wal_cursor(&self, symbol: &str, rec: &WalRecord) -> (u64, usize) {
        let Some(entry) = self.catalog.symbols.get(symbol) else {
            return (0, 0);
        };
        if !rec.trades.is_empty() {
            (entry.trade_wal_seq, entry.trade_wal_rows as usize)
        } else {
            (entry.quote_wal_seq, entry.quote_wal_rows as usize)
        }
    }

    fn install_replayed(
        &mut self,
        rec: WalRecord,
        commit_seq: u64,
        commit_rows: usize,
    ) -> Result<()> {
        if rec.seq < commit_seq {
            return Ok(());
        }
        let skip = if rec.seq == commit_seq {
            commit_rows
        } else {
            0
        };
        self.ensure_buf(&rec.symbol);
        if !rec.trades.is_empty() {
            if skip >= rec.trades.len() {
                return Ok(());
            }
            let rows = rec.trades[skip..].to_vec();
            let buf = self.buffers.get_mut(&rec.symbol).unwrap();
            buf.trades.push(Batch {
                seq: rec.seq,
                already_sealed: skip as u32,
                rows,
            });
        }
        if !rec.quotes.is_empty() {
            if skip >= rec.quotes.len() {
                return Ok(());
            }
            let rows = rec.quotes[skip..].to_vec();
            let buf = self.buffers.get_mut(&rec.symbol).unwrap();
            buf.quotes.push(Batch {
                seq: rec.seq,
                already_sealed: skip as u32,
                rows,
            });
        }
        Ok(())
    }

    fn remove_orphans(&self) -> Result<()> {
        let chunks = self.root.join("chunks");
        if !chunks.exists() {
            return Ok(());
        }
        for sym_ent in fs::read_dir(&chunks)? {
            let sym_ent = sym_ent?;
            if !sym_ent.file_type()?.is_dir() {
                continue;
            }
            let sym_name = sym_ent.file_name();
            let sym = sym_name.to_string_lossy();
            let entry = self.catalog.symbols.get(sym.as_ref());
            for (stream, known) in [
                (
                    "trades",
                    entry.map(|e| e.trades.iter().map(|c| c.id).collect::<HashSet<_>>()),
                ),
                (
                    "quotes",
                    entry.map(|e| e.quotes.iter().map(|c| c.id).collect::<HashSet<_>>()),
                ),
            ] {
                let dir = sym_ent.path().join(stream);
                if !dir.exists() {
                    continue;
                }
                for file in fs::read_dir(&dir)? {
                    let file = file?;
                    let name = file.file_name();
                    let name = name.to_string_lossy();
                    if name.ends_with(".tmp") {
                        let _ = fs::remove_file(file.path());
                        continue;
                    }
                    let Some(id) = name
                        .strip_suffix(".tchk")
                        .and_then(|s| s.parse::<u32>().ok())
                    else {
                        continue;
                    };
                    let keep = known.as_ref().is_some_and(|ids| ids.contains(&id));
                    if !keep {
                        let _ = fs::remove_file(file.path());
                    }
                }
            }
        }
        Ok(())
    }

    fn chunk_path(&self, symbol: &str, stream: Stream, id: u32) -> PathBuf {
        self.root
            .join("chunks")
            .join(symbol)
            .join(stream.dir())
            .join(format!("{id:06}.tchk"))
    }
}

impl Drop for TickStore {
    fn drop(&mut self) {
        if !self.skip_flush {
            let _ = self.flush();
        }
    }
}

fn encode_taken_trades(scale: i64, trades: &[Trade]) -> Result<Vec<u8>> {
    let cols = TradeColumns {
        scale,
        ts: trades.iter().map(|t| t.ts_ns).collect(),
        price: trades.iter().map(|t| t.price_ticks).collect(),
        size: trades.iter().map(|t| t.size).collect(),
        side: trades.iter().map(|t| t.side.as_u8()).collect(),
    };
    encode_trade_chunk(&cols)
}

fn encode_taken_quotes(scale: i64, quotes: &[Quote]) -> Result<Vec<u8>> {
    let cols = QuoteColumns {
        scale,
        ts: quotes.iter().map(|q| q.ts_ns).collect(),
        bid: quotes.iter().map(|q| q.bid_ticks).collect(),
        ask: quotes.iter().map(|q| q.ask_ticks).collect(),
        bid_size: quotes.iter().map(|q| q.bid_size).collect(),
        ask_size: quotes.iter().map(|q| q.ask_size).collect(),
    };
    encode_quote_chunk(&cols)
}

#[derive(Clone, Copy)]
struct ActiveQuote {
    ts: i64,
    bid: i64,
    ask: i64,
    bid_size: u32,
    ask_size: u32,
}

struct QuoteCursor {
    chunks: Vec<QuoteColumns>,
    chunk_i: usize,
    pos: usize,
    active: Option<ActiveQuote>,
    exhausted: bool,
}

impl QuoteCursor {
    fn load(store: &TickStore, symbol: &str, metas: Vec<ChunkMeta>) -> Result<Self> {
        let mut chunks = Vec::with_capacity(metas.len());
        for meta in &metas {
            chunks.push(store.load_quotes(symbol, meta)?);
        }
        Ok(Self {
            exhausted: chunks.is_empty(),
            chunks,
            chunk_i: 0,
            pos: 0,
            active: None,
        })
    }

    fn advance_to(&mut self, trade_ts: i64) -> Result<()> {
        if self.exhausted && self.active.is_none() {
            return Ok(());
        }
        loop {
            if self.exhausted {
                return Ok(());
            }
            if self.chunk_i >= self.chunks.len() {
                self.exhausted = true;
                return Ok(());
            }
            let col = &self.chunks[self.chunk_i];
            if self.pos >= col.ts.len() {
                self.chunk_i += 1;
                self.pos = 0;
                continue;
            }
            let qts = col.ts[self.pos];
            if qts <= trade_ts {
                self.active = Some(ActiveQuote {
                    ts: qts,
                    bid: col.bid[self.pos],
                    ask: col.ask[self.pos],
                    bid_size: col.bid_size[self.pos],
                    ask_size: col.ask_size[self.pos],
                });
                self.pos += 1;
            } else {
                return Ok(());
            }
        }
    }
}

struct BarAcc {
    bucket: i64,
    open: i64,
    high: i64,
    low: i64,
    close: i64,
    volume: u64,
    notional: i128,
    trades: u64,
}

impl BarAcc {
    fn new(bucket: i64, price: i64, size: u32) -> Self {
        Self {
            bucket,
            open: price,
            high: price,
            low: price,
            close: price,
            volume: u64::from(size),
            notional: i128::from(price) * i128::from(size),
            trades: 1,
        }
    }

    fn update(&mut self, price: i64, size: u32) -> Result<()> {
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        add_print(&mut self.notional, &mut self.volume, price, size)?;
        self.trades += 1;
        Ok(())
    }

    fn finish(self) -> Bar {
        Bar {
            bucket_ns: self.bucket,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            notional_ticks: self.notional,
            trades: self.trades,
        }
    }
}

fn accumulate_vwap(out: &mut Vwap, price: &[i64], size: &[u32]) {
    debug_assert_eq!(price.len(), size.len());
    let mut notional = 0i128;
    let mut volume = 0u64;
    let n = price.len();
    let mut i = 0;
    while i + 4 <= n {
        notional += i128::from(price[i]) * i128::from(size[i]);
        notional += i128::from(price[i + 1]) * i128::from(size[i + 1]);
        notional += i128::from(price[i + 2]) * i128::from(size[i + 2]);
        notional += i128::from(price[i + 3]) * i128::from(size[i + 3]);
        volume += u64::from(size[i]);
        volume += u64::from(size[i + 1]);
        volume += u64::from(size[i + 2]);
        volume += u64::from(size[i + 3]);
        i += 4;
    }
    while i < n {
        notional += i128::from(price[i]) * i128::from(size[i]);
        volume += u64::from(size[i]);
        i += 1;
    }
    out.notional_ticks += notional;
    out.volume += volume;
    out.trades += n as u64;
}

fn add_print(notional: &mut i128, volume: &mut u64, price: i64, size: u32) -> Result<()> {
    let prod = i128::from(price) * i128::from(size);
    *notional = notional
        .checked_add(prod)
        .ok_or_else(|| Error::Overflow("notional overflow".into()))?;
    *volume = volume
        .checked_add(u64::from(size))
        .ok_or_else(|| Error::Overflow("volume overflow".into()))?;
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8], sync: bool) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tchk.tmp");
    {
        let mut f = File::create(&tmp)?;
        std::io::Write::write_all(&mut f, bytes)?;
        if sync {
            f.sync_all()?;
        }
    }
    fs::rename(&tmp, path)?;
    if sync {
        wal::sync_dir(path)?;
    }
    Ok(())
}

fn take_front<T>(batches: &mut Vec<Batch<T>>, n: usize) -> Vec<Piece<T>> {
    let mut left = n;
    let mut out = Vec::new();
    while left > 0 && !batches.is_empty() {
        let batch = &mut batches[0];
        let k = left.min(batch.rows.len());
        let rows = batch.rows.drain(..k).collect();
        let before = batch.already_sealed;
        batch.already_sealed += k as u32;
        let seq = batch.seq;
        let empty = batch.rows.is_empty();
        if empty {
            batches.remove(0);
        }
        out.push(Piece {
            seq,
            already_sealed_before: before,
            rows,
        });
        left -= k;
    }
    out
}

fn restore_front<T>(batches: &mut Vec<Batch<T>>, mut pieces: Vec<Piece<T>>) {
    while let Some(piece) = pieces.pop() {
        if let Some(front) = batches.first_mut() {
            if front.seq == piece.seq {
                let mut rows = piece.rows;
                rows.append(&mut front.rows);
                front.rows = rows;
                front.already_sealed = piece.already_sealed_before;
                continue;
            }
        }
        batches.insert(
            0,
            Batch {
                seq: piece.seq,
                already_sealed: piece.already_sealed_before,
                rows: piece.rows,
            },
        );
    }
}

fn record_is_sealed(rec: &WalRecord, commit_seq: u64, commit_rows: usize) -> bool {
    if rec.seq < commit_seq {
        return true;
    }
    if rec.seq > commit_seq {
        return false;
    }
    let n = if !rec.trades.is_empty() {
        rec.trades.len()
    } else {
        rec.quotes.len()
    };
    commit_rows >= n
}

fn flatten_pieces<T: Clone>(pieces: &[Piece<T>]) -> Vec<T> {
    pieces.iter().flat_map(|p| p.rows.clone()).collect()
}

fn map_file(path: &Path) -> Result<memmap2::Mmap> {
    let file = File::open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            Error::corrupt(format!("missing chunk {}", path.display()))
        } else {
            Error::from(e)
        }
    })?;
    // Safety: a chunk is immutable after the rename that publishes it. This
    // process never rewrites a sealed chunk, and readers do not mmap a file
    // that a writer still has open.
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    #[cfg(unix)]
    {
        let _ = mmap.advise(memmap2::Advice::Sequential);
    }
    Ok(mmap)
}

fn accumulate_columns(
    into: &mut BTreeMap<String, ColStat>,
    stream: &str,
    path: &Path,
) -> Result<()> {
    let bytes = fs::read(path)?;
    let header = parse_chunk(&bytes)?;
    for col in &header.columns {
        let key = format!("{stream}.{}", format::column_name(col.id));
        let stat = into.entry(key).or_default();
        stat.rows += u64::from(header.rows);
        stat.payload_bytes += u64::from(col.len);
        let codec = crate::codec::codec_label(col.codec);
        let use_ = stat.codecs.entry(codec).or_default();
        use_.chunks += 1;
        use_.bytes += u64::from(col.len);
    }
    Ok(())
}

fn file_len(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path)?.len())
}

fn dir_bytes(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0u64;
    for ent in fs::read_dir(path)? {
        let ent = ent?;
        let ft = ent.file_type()?;
        if ft.is_dir() {
            total += dir_bytes(&ent.path())?;
        } else if ft.is_file() {
            total += ent.metadata()?.len();
        }
    }
    Ok(total)
}
