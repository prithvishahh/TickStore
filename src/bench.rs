//! In-process benchmark: synthetic tape, TickStore, and a CSV rescan.
//!
//! Parquet and DuckDB numbers are added by `python/bench.py`, which reads the
//! JSON this module writes plus the binary interchange files.

use crate::codec::{
    self, decode_i64, decode_u64, decode_xor_f64, encode_delta, encode_delta_varint,
    encode_dict_u64, encode_dod, encode_for_u64, encode_gorilla_i64, encode_raw_i64,
    encode_raw_u64, encode_rle_u64, encode_varint_u64, encode_xor_f64, CODEC_DELTA,
    CODEC_DELTA_VARINT, CODEC_DICT, CODEC_DOD, CODEC_FOR, CODEC_GORILLA, CODEC_RAW_I64,
    CODEC_RAW_U64, CODEC_RLE, CODEC_VARINT,
};
use crate::dataset_io::{self, write_quotes_csv, write_trades_csv};
use crate::generate::{generate, Dataset, GenConfig};
use crate::reference::{self, asof_checksum, ohlcv_checksum};
use crate::store::{Options, TickStore};
use crate::types::{bucket_start, parse_ticks, Quote, Side, Trade};
use crate::{Error, Result};
use serde_json::{json, Value};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

pub struct BenchConfig {
    pub trades: usize,
    pub quote_ratio: f64,
    pub symbols: usize,
    pub seed: u64,
    pub scale: i64,
    pub chunk_rows: usize,
    pub repeats: usize,
    pub workdir: PathBuf,
}

pub fn run(cfg: &BenchConfig) -> Result<PathBuf> {
    if cfg.repeats == 0 {
        return Err(Error::bad("repeats must be at least 1"));
    }
    fs::create_dir_all(&cfg.workdir)?;
    eprintln!(
        "generating {} trades, ratio {}, {} symbols, seed {}",
        cfg.trades, cfg.quote_ratio, cfg.symbols, cfg.seed
    );
    let gen_started = Instant::now();
    let ds = generate(&GenConfig {
        seed: cfg.seed,
        trades: cfg.trades,
        quote_ratio: cfg.quote_ratio,
        symbols: cfg.symbols,
        scale: cfg.scale,
        ..GenConfig::default()
    })?;
    let gen_s = gen_started.elapsed().as_secs_f64();
    eprintln!(
        "generated {} trades and {} quotes in {:.2}s",
        ds.trade_rows(),
        ds.quote_rows(),
        gen_s
    );

    dataset_io::write_dataset(&cfg.workdir, &ds)?;
    let csv_samples = time_csv_writes(cfg, &ds)?;
    let (ingest_samples, store_bytes) = time_ingests(cfg, &ds)?;

    let focus = ds.focus();
    let full = (ds.session_start_ns, ds.data_end_ns);
    let window = (ds.range_start_ns, ds.range_end_ns);
    let ref_full = reference::sum_price_size(&focus.trades, full.0, full.1);
    let ref_range = reference::sum_price_size(&focus.trades, window.0, window.1);
    let ref_vwap = reference::vwap(&focus.trades, full.0, full.1)?;
    let ref_1m = ohlcv_checksum(&reference::ohlcv(
        &focus.trades,
        full.0,
        full.1,
        60_000_000_000,
    )?);
    let ref_1s = ohlcv_checksum(&reference::ohlcv(
        &focus.trades,
        full.0,
        full.1,
        1_000_000_000,
    )?);
    let ref_asof_full = asof_checksum(&reference::asof_join(
        &focus.trades,
        &focus.quotes,
        full.0,
        full.1,
    ));
    let ref_asof_range = asof_checksum(&reference::asof_join(
        &focus.trades,
        &focus.quotes,
        window.0,
        window.1,
    ));

    eprintln!("timing tickstore queries on {}", focus.name);
    let mut store = TickStore::open(&cfg.workdir.join("store"))?;
    let q_count_full = time_query(cfg.repeats, || {
        store.count_trades(&focus.name, full.0, full.1)
    })?;
    let q_count_range = time_query(cfg.repeats, || {
        store.count_trades(&focus.name, window.0, window.1)
    })?;
    let q_scan_range = time_query(cfg.repeats, || {
        let rows = store.scan_trades(&focus.name, window.0, window.1)?;
        Ok(sum_trades(&rows))
    })?;
    let q_scan_full = time_query(cfg.repeats, || {
        let rows = store.scan_trades(&focus.name, full.0, full.1)?;
        Ok(sum_trades(&rows))
    })?;
    let q_ohlcv_1m = time_query(cfg.repeats, || {
        Ok(ohlcv_checksum(&store.ohlcv(
            &focus.name,
            full.0,
            full.1,
            60_000_000_000,
        )?))
    })?;
    let q_ohlcv_1s = time_query(cfg.repeats, || {
        Ok(ohlcv_checksum(&store.ohlcv(
            &focus.name,
            full.0,
            full.1,
            1_000_000_000,
        )?))
    })?;
    let q_vwap = time_query(cfg.repeats, || store.vwap(&focus.name, full.0, full.1))?;
    let q_asof_full = time_query(cfg.repeats, || {
        Ok(asof_checksum(&store.asof_join(
            &focus.name,
            full.0,
            full.1,
        )?))
    })?;
    let q_asof_range = time_query(cfg.repeats, || {
        Ok(asof_checksum(&store.asof_join(
            &focus.name,
            window.0,
            window.1,
        )?))
    })?;

    check_eq("count full", q_count_full.2, ref_full.0)?;
    check_eq("count range", q_count_range.2, ref_range.0)?;
    check_eq("scan range", q_scan_range.2, ref_range)?;
    check_eq("scan full", q_scan_full.2, ref_full)?;
    check_eq("ohlcv 1m", q_ohlcv_1m.2, ref_1m)?;
    check_eq("ohlcv 1s", q_ohlcv_1s.2, ref_1s)?;
    check_eq("vwap", q_vwap.2, ref_vwap)?;
    check_eq("asof full", q_asof_full.2, ref_asof_full)?;
    check_eq("asof range", q_asof_range.2, ref_asof_range)?;

    eprintln!("timing csv rescan");
    let trades_csv = cfg.workdir.join("trades.csv");
    let quotes_csv = cfg.workdir.join("quotes.csv");
    let scale = ds.scale;
    let symbol = focus.name.clone();
    let c_count_range = time_query(cfg.repeats, || {
        csv_trade_sum(&trades_csv, scale, &symbol, window.0, window.1).map(|s| s.0)
    })?;
    let c_scan_range = time_query(cfg.repeats, || {
        csv_trade_sum(&trades_csv, scale, &symbol, window.0, window.1)
    })?;
    let c_ohlcv_1m = time_query(cfg.repeats, || {
        csv_ohlcv(&trades_csv, scale, &symbol, full.0, full.1, 60_000_000_000)
    })?;
    let c_ohlcv_1s = time_query(cfg.repeats, || {
        csv_ohlcv(&trades_csv, scale, &symbol, full.0, full.1, 1_000_000_000)
    })?;
    let c_vwap = time_query(cfg.repeats, || {
        csv_vwap(&trades_csv, scale, &symbol, full.0, full.1)
    })?;
    let c_asof_range = time_query(cfg.repeats, || {
        csv_asof(&trades_csv, &quotes_csv, scale, &symbol, window.0, window.1)
    })?;
    check_eq("csv count", c_count_range.2, ref_range.0)?;
    check_eq("csv scan", c_scan_range.2, ref_range)?;
    check_eq("csv ohlcv 1m", c_ohlcv_1m.2, ref_1m)?;
    check_eq("csv ohlcv 1s", c_ohlcv_1s.2, ref_1s)?;
    check_eq("csv vwap", c_vwap.2, ref_vwap)?;
    check_eq("csv asof", c_asof_range.2, ref_asof_range)?;

    eprintln!("profiling codecs on {} trades", focus.trades.len());
    let prices: Vec<i64> = focus.trades.iter().map(|t| t.price_ticks).collect();
    let sizes: Vec<u64> = focus.trades.iter().map(|t| u64::from(t.size)).collect();
    let ts: Vec<i64> = focus.trades.iter().map(|t| t.ts_ns).collect();
    let price_f64: Vec<f64> = prices.iter().map(|p| *p as f64 / scale as f64).collect();

    let stats = store.statistics()?;
    let csv_trade_bytes = fs::metadata(&trades_csv)?.len();
    let csv_quote_bytes = fs::metadata(&quotes_csv)?.len();
    let bin_trade_bytes = fs::metadata(cfg.workdir.join("trades.bin"))?.len();
    let bin_quote_bytes = fs::metadata(cfg.workdir.join("quotes.bin"))?.len();

    let report = json!({
        "machine": {
            "rustc": command_version("rustc"),
            "cpu": cpu_model(),
            "arch": std::env::consts::ARCH,
            "os": std::env::consts::OS,
        },
        "dataset": {
            "seed": ds.seed,
            "scale": ds.scale,
            "symbols": ds.symbol_names(),
            "trade_rows": ds.trade_rows(),
            "quote_rows": ds.quote_rows(),
            "session_start_ns": ds.session_start_ns,
            "session_end_ns": ds.session_end_ns,
            "data_end_ns": ds.data_end_ns,
            "focus_symbol": focus.name,
            "focus_trade_rows": focus.trades.len(),
            "focus_quote_rows": focus.quotes.len(),
            "range_start_ns": ds.range_start_ns,
            "range_end_ns": ds.range_end_ns,
            "interval_1m_ns": 60_000_000_000i64,
            "interval_1s_ns": 1_000_000_000i64,
            "chunk_rows": cfg.chunk_rows,
            "generate_s": gen_s,
        },
        "reference": {
            "full_count": ref_full.0,
            "full_price_sum": ref_full.1.to_string(),
            "full_size_sum": ref_full.2,
            "range_count": ref_range.0,
            "range_price_sum": ref_range.1.to_string(),
            "range_size_sum": ref_range.2,
            "vwap_notional": ref_vwap.notional_ticks.to_string(),
            "vwap_volume": ref_vwap.volume,
            "vwap_trades": ref_vwap.trades,
            "ohlcv_1m": checksum_json(ref_1m),
            "ohlcv_1s": checksum_json(ref_1s),
            "asof_full": asof_json(ref_asof_full),
            "asof_range": asof_json(ref_asof_range),
        },
        "tickstore": {
            "disk_bytes": store_bytes,
            "stats": serde_json::to_value(&stats)?,
            "ingest_samples_s": ingest_samples,
            "ingest_median_s": median_ref(&ingest_samples),
            "rows_per_s": (ds.trade_rows() + ds.quote_rows()) as f64 / median_ref(&ingest_samples),
            "queries": {
                "count_full": query_json(q_count_full.0, q_count_full.1),
                "count_range": query_json(q_count_range.0, q_count_range.1),
                "scan_range": query_json(q_scan_range.0, q_scan_range.1),
                "scan_full": query_json(q_scan_full.0, q_scan_full.1),
                "ohlcv_1m": query_json(q_ohlcv_1m.0, q_ohlcv_1m.1),
                "ohlcv_1s": query_json(q_ohlcv_1s.0, q_ohlcv_1s.1),
                "vwap": query_json(q_vwap.0, q_vwap.1),
                "asof_full": query_json(q_asof_full.0, q_asof_full.1),
                "asof_range": query_json(q_asof_range.0, q_asof_range.1),
            },
        },
        "csv": {
            "trade_bytes": csv_trade_bytes,
            "quote_bytes": csv_quote_bytes,
            "write_samples_s": csv_samples,
            "write_median_s": median_ref(&csv_samples),
            "rows_per_s": (ds.trade_rows() + ds.quote_rows()) as f64 / median_ref(&csv_samples),
            "queries": {
                "count_range": query_json(c_count_range.0, c_count_range.1),
                "scan_range": query_json(c_scan_range.0, c_scan_range.1),
                "ohlcv_1m": query_json(c_ohlcv_1m.0, c_ohlcv_1m.1),
                "ohlcv_1s": query_json(c_ohlcv_1s.0, c_ohlcv_1s.1),
                "vwap": query_json(c_vwap.0, c_vwap.1),
                "asof_range": query_json(c_asof_range.0, c_asof_range.1),
            },
        },
        "packed": {
            "trade_bytes": ds.trade_rows() as u64 * 24,
            "quote_bytes": ds.quote_rows() as u64 * 40,
            "trade_file_bytes": bin_trade_bytes,
            "quote_file_bytes": bin_quote_bytes,
        },
        "codecs": {
            "selector_slack": codec::SIZE_SLACK,
            "timestamps": profile_i64(&ts),
            "prices": profile_i64(&prices),
            "sizes": profile_u64(&sizes),
            "price_f64_xor": profile_xor(&price_f64),
        },
    });

    let out = cfg.workdir.join("engine.json");
    let mut f = File::create(&out)?;
    serde_json::to_writer_pretty(&mut f, &report)?;
    f.write_all(b"\n")?;
    eprintln!("wrote {}", out.display());
    print_summary(&report);
    Ok(out)
}

fn print_summary(report: &Value) {
    let ts = &report["tickstore"];
    let csv = &report["csv"];
    eprintln!(
        "tickstore {:.3}s ingest ({:.0} rows/s), {:.3} MiB on disk",
        ts["ingest_median_s"].as_f64().unwrap_or(0.0),
        ts["rows_per_s"].as_f64().unwrap_or(0.0),
        ts["disk_bytes"].as_f64().unwrap_or(0.0) / (1024.0 * 1024.0)
    );
    eprintln!(
        "csv       {:.3}s write   ({:.0} rows/s), {:.3} MiB",
        csv["write_median_s"].as_f64().unwrap_or(0.0),
        csv["rows_per_s"].as_f64().unwrap_or(0.0),
        (csv["trade_bytes"].as_f64().unwrap_or(0.0) + csv["quote_bytes"].as_f64().unwrap_or(0.0))
            / (1024.0 * 1024.0)
    );
    eprintln!("query median ms  count_range scan_range ohlcv_1s vwap asof_range");
    eprintln!(
        "tickstore        {:>11.3} {:>10.3} {:>8.3} {:>7.3} {:>10.3}",
        ts["queries"]["count_range"]["median_ms"]
            .as_f64()
            .unwrap_or(0.0),
        ts["queries"]["scan_range"]["median_ms"]
            .as_f64()
            .unwrap_or(0.0),
        ts["queries"]["ohlcv_1s"]["median_ms"]
            .as_f64()
            .unwrap_or(0.0),
        ts["queries"]["vwap"]["median_ms"].as_f64().unwrap_or(0.0),
        ts["queries"]["asof_range"]["median_ms"]
            .as_f64()
            .unwrap_or(0.0),
    );
    eprintln!(
        "csv              {:>11.3} {:>10.3} {:>8.3} {:>7.3} {:>10.3}",
        csv["queries"]["count_range"]["median_ms"]
            .as_f64()
            .unwrap_or(0.0),
        csv["queries"]["scan_range"]["median_ms"]
            .as_f64()
            .unwrap_or(0.0),
        csv["queries"]["ohlcv_1s"]["median_ms"]
            .as_f64()
            .unwrap_or(0.0),
        csv["queries"]["vwap"]["median_ms"].as_f64().unwrap_or(0.0),
        csv["queries"]["asof_range"]["median_ms"]
            .as_f64()
            .unwrap_or(0.0),
    );
}

fn checksum_json(c: (u64, u64, i128, i128)) -> Value {
    json!({
        "bars": c.0,
        "volume": c.1,
        "open_sum": c.2.to_string(),
        "close_sum": c.3.to_string(),
    })
}

fn asof_json(c: (u64, u64, i128)) -> Value {
    json!({"rows": c.0, "nulls": c.1, "bid_sum": c.2.to_string()})
}

fn query_json(median_ms: f64, first_ms: f64) -> Value {
    json!({"median_ms": median_ms, "first_ms": first_ms})
}

fn time_csv_writes(cfg: &BenchConfig, ds: &Dataset) -> Result<Vec<f64>> {
    let mut samples = Vec::with_capacity(cfg.repeats);
    for _ in 0..cfg.repeats {
        let t0 = Instant::now();
        write_trades_csv(&cfg.workdir.join("trades.csv"), ds)?;
        write_quotes_csv(&cfg.workdir.join("quotes.csv"), ds)?;
        samples.push(t0.elapsed().as_secs_f64());
    }
    Ok(samples)
}

fn time_ingests(cfg: &BenchConfig, ds: &Dataset) -> Result<(Vec<f64>, u64)> {
    let mut samples = Vec::with_capacity(cfg.repeats);
    let mut bytes = 0u64;
    for i in 0..cfg.repeats {
        let dir = if i + 1 == cfg.repeats {
            cfg.workdir.join("store")
        } else {
            cfg.workdir.join("ingest-tmp")
        };
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
        }
        let t0 = Instant::now();
        {
            let mut store = TickStore::create(
                &dir,
                Options {
                    price_scale: ds.scale,
                    chunk_rows: cfg.chunk_rows,
                },
            )?;
            for s in &ds.symbols {
                store.append_trades(&s.name, &s.trades)?;
                store.append_quotes(&s.name, &s.quotes)?;
            }
            store.flush()?;
        }
        samples.push(t0.elapsed().as_secs_f64());
        bytes = dir_bytes(&dir)?;
        if i + 1 != cfg.repeats {
            fs::remove_dir_all(&dir)?;
        }
        eprintln!("  ingest {} / {} in {:.3}s", i + 1, cfg.repeats, samples[i]);
    }
    Ok((samples, bytes))
}

fn time_query<T>(repeats: usize, mut f: impl FnMut() -> Result<T>) -> Result<(f64, f64, T)> {
    let t0 = Instant::now();
    let value = f()?;
    let first = t0.elapsed().as_secs_f64() * 1e3;
    let mut samples = Vec::with_capacity(repeats);
    for _ in 0..repeats {
        let t0 = Instant::now();
        let _ = f()?;
        samples.push(t0.elapsed().as_secs_f64() * 1e3);
    }
    Ok((median(&mut samples), first, value))
}

fn median(xs: &mut [f64]) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    xs[xs.len() / 2]
}

fn median_ref(xs: &[f64]) -> f64 {
    let mut c = xs.to_vec();
    median(&mut c)
}

fn sum_trades(rows: &[Trade]) -> (u64, i128, u64) {
    let mut price = 0i128;
    let mut size = 0u64;
    for t in rows {
        price += i128::from(t.price_ticks);
        size += u64::from(t.size);
    }
    (rows.len() as u64, price, size)
}

fn check_eq<T: PartialEq + std::fmt::Debug>(label: &str, got: T, expect: T) -> Result<()> {
    if got != expect {
        Err(Error::bad(format!(
            "{label} mismatch: {got:?} != {expect:?}"
        )))
    } else {
        Ok(())
    }
}

type EncI64 = fn(&[i64]) -> Vec<u8>;

fn profile_i64(values: &[i64]) -> Value {
    if values.is_empty() {
        return json!([]);
    }
    let specs: &[(&str, u8, EncI64)] = &[
        ("raw_i64", CODEC_RAW_I64, encode_raw_i64),
        ("delta_bitpack", CODEC_DELTA, encode_delta),
        ("dod_bitpack", CODEC_DOD, encode_dod),
        ("delta_varint", CODEC_DELTA_VARINT, encode_delta_varint),
        ("gorilla_dod", CODEC_GORILLA, encode_gorilla_i64),
    ];
    let mut rows = Vec::new();
    for (name, id, enc) in specs {
        let t0 = Instant::now();
        let bytes = enc(values);
        let encode_ns = t0.elapsed().as_nanos() as f64 / values.len() as f64;
        let decoded = decode_i64(*id, &bytes, values.len()).expect("codec roundtrip");
        assert_eq!(&decoded, values);
        let mut best = f64::MAX;
        for _ in 0..3 {
            let t0 = Instant::now();
            let got = decode_i64(*id, &bytes, values.len()).unwrap();
            let sum = got.iter().fold(0i64, |a, b| a.wrapping_add(*b));
            std::hint::black_box(sum);
            best = best.min(t0.elapsed().as_nanos() as f64 / values.len() as f64);
        }
        rows.push(json!({
            "name": name,
            "bytes": bytes.len(),
            "bytes_per_value": bytes.len() as f64 / values.len() as f64,
            "encode_ns": encode_ns,
            "decode_ns": best,
        }));
    }
    let (chosen, chosen_bytes) = codec::encode_i64(values);
    rows.push(json!({
        "name": "selected",
        "codec": codec::codec_name(chosen),
        "bytes": chosen_bytes.len(),
        "bytes_per_value": chosen_bytes.len() as f64 / values.len() as f64,
    }));
    Value::Array(rows)
}

type EncU64 = fn(&[u64]) -> Vec<u8>;

fn profile_u64(values: &[u64]) -> Value {
    if values.is_empty() {
        return json!([]);
    }
    let specs: &[(&str, u8, EncU64)] = &[
        ("raw_u64", CODEC_RAW_U64, encode_raw_u64),
        ("for_bitpack", CODEC_FOR, encode_for_u64),
        ("varint", CODEC_VARINT, encode_varint_u64),
        ("dictionary", CODEC_DICT, encode_dict_u64),
        ("rle", CODEC_RLE, encode_rle_u64),
    ];
    let mut rows = Vec::new();
    for (name, id, enc) in specs {
        let t0 = Instant::now();
        let bytes = enc(values);
        let encode_ns = t0.elapsed().as_nanos() as f64 / values.len() as f64;
        let decoded = decode_u64(*id, &bytes, values.len()).expect("codec roundtrip");
        assert_eq!(&decoded, values);
        let mut best = f64::MAX;
        for _ in 0..3 {
            let t0 = Instant::now();
            let got = decode_u64(*id, &bytes, values.len()).unwrap();
            let sum = got.iter().fold(0u64, |a, b| a.wrapping_add(*b));
            std::hint::black_box(sum);
            best = best.min(t0.elapsed().as_nanos() as f64 / values.len() as f64);
        }
        rows.push(json!({
            "name": name,
            "bytes": bytes.len(),
            "bytes_per_value": bytes.len() as f64 / values.len() as f64,
            "encode_ns": encode_ns,
            "decode_ns": best,
        }));
    }
    let (chosen, chosen_bytes) = codec::encode_u64(values);
    rows.push(json!({
        "name": "selected",
        "codec": codec::codec_name(chosen),
        "bytes": chosen_bytes.len(),
        "bytes_per_value": chosen_bytes.len() as f64 / values.len() as f64,
    }));
    Value::Array(rows)
}

fn profile_xor(values: &[f64]) -> Value {
    if values.is_empty() {
        return json!({});
    }
    let bytes = encode_xor_f64(values);
    let got = decode_xor_f64(&bytes, values.len()).expect("xor");
    for (a, b) in values.iter().zip(got.iter()) {
        assert_eq!(a.to_bits(), b.to_bits());
    }
    json!({
        "bytes": bytes.len(),
        "bytes_per_value": bytes.len() as f64 / values.len() as f64,
    })
}

fn dir_bytes(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    if !path.exists() {
        return Ok(0);
    }
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

fn cpu_model() -> String {
    let Ok(text) = fs::read_to_string("/proc/cpuinfo") else {
        return "unknown".into();
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("model name") {
            return rest
                .split(':')
                .nth(1)
                .unwrap_or("unknown")
                .trim()
                .to_string();
        }
    }
    "unknown".into()
}

fn command_version(bin: &str) -> String {
    std::process::Command::new(bin)
        .arg("--version")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn csv_trade_sum(
    path: &Path,
    scale: i64,
    symbol: &str,
    start: i64,
    end: i64,
) -> Result<(u64, i128, u64)> {
    let mut n = 0u64;
    let mut price = 0i128;
    let mut size = 0u64;
    for_trade_csv(path, scale, symbol, start, end, |t| {
        n += 1;
        price += i128::from(t.price_ticks);
        size += u64::from(t.size);
    })?;
    Ok((n, price, size))
}

fn csv_vwap(
    path: &Path,
    scale: i64,
    symbol: &str,
    start: i64,
    end: i64,
) -> Result<crate::types::Vwap> {
    let mut out = crate::types::Vwap::default();
    for_trade_csv(path, scale, symbol, start, end, |t| {
        out.notional_ticks += i128::from(t.price_ticks) * i128::from(t.size);
        out.volume += u64::from(t.size);
        out.trades += 1;
    })?;
    Ok(out)
}

fn csv_ohlcv(
    path: &Path,
    scale: i64,
    symbol: &str,
    start: i64,
    end: i64,
    interval: i64,
) -> Result<(u64, u64, i128, i128)> {
    let mut bars = 0u64;
    let mut volume = 0u64;
    let mut open_sum = 0i128;
    let mut close_sum = 0i128;
    let mut cur: Option<(i64, i64, i64)> = None; // bucket, open, close
    let mut cur_vol = 0u64;
    for_trade_csv(path, scale, symbol, start, end, |t| {
        let b = bucket_start(t.ts_ns, interval);
        match &mut cur {
            Some((bucket, _open, close)) if *bucket == b => {
                *close = t.price_ticks;
                cur_vol += u64::from(t.size);
            }
            other => {
                if let Some((bucket, open, close)) = other.take() {
                    let _ = bucket;
                    bars += 1;
                    volume += cur_vol;
                    open_sum += i128::from(open);
                    close_sum += i128::from(close);
                }
                cur = Some((b, t.price_ticks, t.price_ticks));
                cur_vol = u64::from(t.size);
            }
        }
    })?;
    if let Some((_b, open, close)) = cur {
        bars += 1;
        volume += cur_vol;
        open_sum += i128::from(open);
        close_sum += i128::from(close);
    }
    Ok((bars, volume, open_sum, close_sum))
}

fn csv_asof(
    trades_path: &Path,
    quotes_path: &Path,
    scale: i64,
    symbol: &str,
    start: i64,
    end: i64,
) -> Result<(u64, u64, i128)> {
    let mut trades = Vec::new();
    for_trade_csv(trades_path, scale, symbol, start, end, |t| trades.push(t))?;
    let mut quotes = Vec::new();
    for_quote_csv(quotes_path, scale, symbol, i64::MIN, end, |q| {
        quotes.push(q)
    })?;
    Ok(asof_checksum(&reference::asof_join(
        &trades, &quotes, start, end,
    )))
}

fn for_trade_csv(
    path: &Path,
    scale: i64,
    symbol: &str,
    start: i64,
    end: i64,
    mut f: impl FnMut(Trade),
) -> Result<()> {
    let reader = BufReader::new(File::open(path)?);
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        if idx == 0 {
            continue;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut p = line.split(',');
        let ts: i64 = p
            .next()
            .unwrap_or("")
            .parse()
            .map_err(|_| Error::bad("csv ts"))?;
        let sym = p.next().unwrap_or("");
        if sym != symbol || ts < start || ts >= end {
            continue;
        }
        let price = parse_ticks(p.next().unwrap_or(""), scale)?;
        let size: u32 = p
            .next()
            .unwrap_or("")
            .parse()
            .map_err(|_| Error::bad("csv size"))?;
        let side = Side::parse(p.next().unwrap_or("unk"))?;
        f(Trade {
            ts_ns: ts,
            price_ticks: price,
            size,
            side,
        });
    }
    Ok(())
}

fn for_quote_csv(
    path: &Path,
    scale: i64,
    symbol: &str,
    start: i64,
    end: i64,
    mut f: impl FnMut(Quote),
) -> Result<()> {
    let reader = BufReader::new(File::open(path)?);
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        if idx == 0 {
            continue;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut p = line.split(',');
        let ts: i64 = p
            .next()
            .unwrap_or("")
            .parse()
            .map_err(|_| Error::bad("csv ts"))?;
        let sym = p.next().unwrap_or("");
        if sym != symbol || ts < start || ts >= end {
            continue;
        }
        let bid = parse_ticks(p.next().unwrap_or(""), scale)?;
        let ask = parse_ticks(p.next().unwrap_or(""), scale)?;
        let bid_size: u32 = p
            .next()
            .unwrap_or("")
            .parse()
            .map_err(|_| Error::bad("csv size"))?;
        let ask_size: u32 = p
            .next()
            .unwrap_or("")
            .parse()
            .map_err(|_| Error::bad("csv size"))?;
        f(Quote {
            ts_ns: ts,
            bid_ticks: bid,
            ask_ticks: ask,
            bid_size,
            ask_size,
        });
    }
    Ok(())
}
