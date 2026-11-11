use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tickstore::bench::{self, BenchConfig};
use tickstore::dataset_io::{self, load_symbol_series};
use tickstore::generate::{generate, GenConfig};
use tickstore::types::{format_ticks, format_timestamp_utc, parse_interval};
use tickstore::{Options, Result, TickStore};

#[derive(Parser)]
#[command(
    name = "tickstore",
    version,
    about = "Columnar storage engine for financial market tick data"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Generate a synthetic trade and quote tape.
    Gen {
        #[arg(long, default_value = "data/synth")]
        out: PathBuf,
        #[arg(long, default_value_t = 100_000)]
        trades: usize,
        #[arg(long, default_value_t = 2.0)]
        quote_ratio: f64,
        #[arg(long, default_value_t = 4)]
        symbols: usize,
        #[arg(long, default_value_t = 7)]
        seed: u64,
        #[arg(long, default_value_t = 10_000)]
        scale: i64,
    },
    /// Load trades.bin and quotes.bin into a new store.
    Ingest {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        store: PathBuf,
        #[arg(long, default_value_t = 16_384)]
        chunk_rows: usize,
    },
    /// Print storage and codec statistics.
    Stats {
        #[arg(long)]
        store: PathBuf,
    },
    /// Run a query against a store.
    Query {
        #[arg(long)]
        store: PathBuf,
        #[command(subcommand)]
        kind: QueryCmd,
    },
    /// Generate data, ingest it, and write bench/engine.json.
    Bench {
        #[arg(long, default_value_t = 1_000_000)]
        trades: usize,
        #[arg(long, default_value_t = 2.0)]
        quote_ratio: f64,
        #[arg(long, default_value_t = 8)]
        symbols: usize,
        #[arg(long, default_value_t = 7)]
        seed: u64,
        #[arg(long, default_value_t = 10_000)]
        scale: i64,
        #[arg(long, default_value_t = 16_384)]
        chunk_rows: usize,
        #[arg(long, default_value_t = 5)]
        repeats: usize,
        #[arg(long, default_value = "bench/run")]
        workdir: PathBuf,
    },
}

#[derive(Subcommand)]
enum QueryCmd {
    /// Materialize trades in a half-open time range.
    Scan {
        #[arg(long)]
        symbol: String,
        #[arg(long)]
        start: Option<i64>,
        #[arg(long)]
        end: Option<i64>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Count trades. Fully covered chunks are answered from the catalog.
    Count {
        #[arg(long)]
        symbol: String,
        #[arg(long)]
        start: Option<i64>,
        #[arg(long)]
        end: Option<i64>,
    },
    /// Epoch-aligned OHLCV bars. Interval examples: 1s, 1m, 5m, 1h.
    Ohlcv {
        #[arg(long)]
        symbol: String,
        #[arg(long)]
        start: Option<i64>,
        #[arg(long)]
        end: Option<i64>,
        #[arg(long, default_value = "1m")]
        interval: String,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Volume-weighted average price over the window.
    Vwap {
        #[arg(long)]
        symbol: String,
        #[arg(long)]
        start: Option<i64>,
        #[arg(long)]
        end: Option<i64>,
    },
    /// Join each trade to the latest quote at or before that trade.
    Asof {
        #[arg(long)]
        symbol: String,
        #[arg(long)]
        start: Option<i64>,
        #[arg(long)]
        end: Option<i64>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
}

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Gen {
            out,
            trades,
            quote_ratio,
            symbols,
            seed,
            scale,
        } => {
            let ds = generate(&GenConfig {
                trades,
                quote_ratio,
                symbols,
                seed,
                scale,
                ..GenConfig::default()
            })?;
            dataset_io::write_dataset(&out, &ds)?;
            println!(
                "wrote {} trades and {} quotes for {} symbols to {}",
                ds.trade_rows(),
                ds.quote_rows(),
                ds.symbols.len(),
                out.display()
            );
        }
        Cmd::Ingest {
            input,
            store,
            chunk_rows,
        } => {
            let (meta, series) = load_symbol_series(&input)?;
            let mut db = TickStore::create(
                &store,
                Options {
                    price_scale: meta.scale,
                    chunk_rows,
                },
            )?;
            for s in &series {
                db.append_trades(&s.name, &s.trades)?;
                db.append_quotes(&s.name, &s.quotes)?;
            }
            db.flush()?;
            println!(
                "ingested {} trades and {} quotes into {}",
                meta.trade_rows,
                meta.quote_rows,
                store.display()
            );
        }
        Cmd::Stats { store } => {
            let db = TickStore::open(&store)?;
            let stats = db.statistics()?;
            println!("symbols        {}", stats.symbols);
            println!("price scale    {}", stats.price_scale);
            println!("chunk rows     {}", stats.chunk_rows);
            println!(
                "trades         {} rows in {} chunks, {} on disk",
                stats.trade_rows,
                stats.trade_chunks,
                human(stats.trade_file_bytes)
            );
            println!(
                "quotes         {} rows in {} chunks, {} on disk",
                stats.quote_rows,
                stats.quote_chunks,
                human(stats.quote_file_bytes)
            );
            println!("catalog        {}", human(stats.catalog_bytes));
            println!("total          {}", human(stats.total_bytes));
            if stats.trade_rows > 0 {
                println!(
                    "trade bytes/row {:.2}",
                    stats.trade_file_bytes as f64 / stats.trade_rows as f64
                );
            }
            println!("\ncolumn payloads");
            for (name, col) in &stats.columns {
                let bpr = if col.rows == 0 {
                    0.0
                } else {
                    col.payload_bytes as f64 / col.rows as f64
                };
                let codecs: Vec<String> = col
                    .codecs
                    .iter()
                    .map(|(c, u)| format!("{c} ({} chunks, {})", u.chunks, human(u.bytes)))
                    .collect();
                println!(
                    "  {name:<18} {:>10}  {:>6.2} B/row  {}",
                    human(col.payload_bytes),
                    bpr,
                    codecs.join(", ")
                );
            }
        }
        Cmd::Query { store, kind } => {
            let mut db = TickStore::open(&store)?;
            let scale = db.price_scale();
            match kind {
                QueryCmd::Scan {
                    symbol,
                    start,
                    end,
                    limit,
                } => {
                    let (start, end) = resolve_span(&db, &symbol, true, start, end)?;
                    let rows = db.scan_trades(&symbol, start, end)?;
                    println!("{} trades in [{start}, {end})", rows.len());
                    for t in rows.iter().take(limit) {
                        println!(
                            "{}  {}  {:>10}  {}",
                            format_timestamp_utc(t.ts_ns),
                            format_ticks(t.price_ticks, scale)?,
                            t.size,
                            t.side
                        );
                    }
                    if rows.len() > limit {
                        println!("... {} more", rows.len() - limit);
                    }
                }
                QueryCmd::Count { symbol, start, end } => {
                    let (start, end) = resolve_span(&db, &symbol, true, start, end)?;
                    let n = db.count_trades(&symbol, start, end)?;
                    println!("{n}");
                }
                QueryCmd::Ohlcv {
                    symbol,
                    start,
                    end,
                    interval,
                    limit,
                } => {
                    let (start, end) = resolve_span(&db, &symbol, true, start, end)?;
                    let step = parse_interval(&interval)?;
                    let bars = db.ohlcv(&symbol, start, end, step)?;
                    println!("{} bars", bars.len());
                    for b in bars.iter().take(limit) {
                        println!(
                            "{}  o {:<10} h {:<10} l {:<10} c {:<10} vol {:<8} vwap {:>8.4}  n {}",
                            format_timestamp_utc(b.bucket_ns),
                            format_ticks(b.open, scale)?,
                            format_ticks(b.high, scale)?,
                            format_ticks(b.low, scale)?,
                            format_ticks(b.close, scale)?,
                            b.volume,
                            b.vwap(scale).unwrap_or(0.0),
                            b.trades
                        );
                    }
                    if bars.len() > limit {
                        println!("... {} more", bars.len() - limit);
                    }
                }
                QueryCmd::Vwap { symbol, start, end } => {
                    let (start, end) = resolve_span(&db, &symbol, true, start, end)?;
                    let v = db.vwap(&symbol, start, end)?;
                    match v.price(scale) {
                        Some(px) => println!(
                            "{px:.6}  volume {}  trades {}  notional_ticks {}",
                            v.volume, v.trades, v.notional_ticks
                        ),
                        None => println!("no trades"),
                    }
                }
                QueryCmd::Asof {
                    symbol,
                    start,
                    end,
                    limit,
                } => {
                    let (start, end) = resolve_span(&db, &symbol, true, start, end)?;
                    let rows = db.asof_join(&symbol, start, end)?;
                    println!("{} joined trades", rows.len());
                    for r in rows.iter().take(limit) {
                        let bid = r
                            .bid_ticks
                            .map(|p| format_ticks(p, scale))
                            .transpose()?
                            .unwrap_or_else(|| "-".into());
                        let ask = r
                            .ask_ticks
                            .map(|p| format_ticks(p, scale))
                            .transpose()?
                            .unwrap_or_else(|| "-".into());
                        println!(
                            "{}  trade {}  size {:<6} {}  bid {} ask {}  quote {}",
                            format_timestamp_utc(r.ts_ns),
                            format_ticks(r.price_ticks, scale)?,
                            r.size,
                            r.side,
                            bid,
                            ask,
                            r.quote_ts_ns
                                .map(format_timestamp_utc)
                                .unwrap_or_else(|| "-".into())
                        );
                    }
                    if rows.len() > limit {
                        println!("... {} more", rows.len() - limit);
                    }
                }
            }
        }
        Cmd::Bench {
            trades,
            quote_ratio,
            symbols,
            seed,
            scale,
            chunk_rows,
            repeats,
            workdir,
        } => {
            bench::run(&BenchConfig {
                trades,
                quote_ratio,
                symbols,
                seed,
                scale,
                chunk_rows,
                repeats,
                workdir,
            })?;
        }
    }
    Ok(())
}

fn resolve_span(
    store: &TickStore,
    symbol: &str,
    trades: bool,
    start: Option<i64>,
    end: Option<i64>,
) -> Result<(i64, i64)> {
    let (lo, hi) = store.time_span(symbol, trades)?;
    Ok((start.unwrap_or(lo), end.unwrap_or(hi)))
}

fn human(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut u = 0;
    while v >= 1024.0 && u + 1 < UNITS.len() {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{bytes} {}", UNITS[u])
    } else {
        format!("{v:.2} {}", UNITS[u])
    }
}
