use tickstore::reference::{asof_checksum, asof_join, ohlcv, ohlcv_checksum, vwap};
use tickstore::{generate, Bar, GenConfig, Options, Quote, Side, TickStore, Trade};

struct Tmp {
    path: std::path::PathBuf,
}

impl Tmp {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "tickstore-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn trade(ts: i64, price: i64, size: u32) -> Trade {
    Trade {
        ts_ns: ts,
        price_ticks: price,
        size,
        side: if price % 2 == 0 {
            Side::Buy
        } else {
            Side::Sell
        },
    }
}

#[test]
fn queries_match_the_reference_across_chunk_boundaries() {
    let tmp = Tmp::new();
    let mut store = TickStore::create(
        &tmp.path,
        Options {
            price_scale: 10_000,
            chunk_rows: 4,
        },
    )
    .unwrap();
    let trades = vec![
        trade(10, 10, 2),
        trade(20, 11, 1),
        trade(30, 9, 4),
        trade(40, 12, 3),
        trade(50, 12, 3),
        trade(60, 8, 5),
        trade(70, 15, 1),
        trade(80, 14, 2),
        trade(90, 13, 8),
    ];
    let quotes = vec![
        Quote {
            ts_ns: 10,
            bid_ticks: 1,
            ask_ticks: 2,
            bid_size: 10,
            ask_size: 10,
        },
        Quote {
            ts_ns: 25,
            bid_ticks: 2,
            ask_ticks: 4,
            bid_size: 10,
            ask_size: 11,
        },
        Quote {
            ts_ns: 50,
            bid_ticks: 3,
            ask_ticks: 5,
            bid_size: 12,
            ask_size: 13,
        },
        Quote {
            ts_ns: 95,
            bid_ticks: 9,
            ask_ticks: 10,
            bid_size: 1,
            ask_size: 1,
        },
    ];
    store.append_trades("AAPL", &trades).unwrap();
    store.append_quotes("AAPL", &quotes).unwrap();
    store.flush().unwrap();

    let cases = [
        (i64::MIN, i64::MAX),
        (40, 70),
        (41, 50),
        (40, 41),
        (0, 10),
        (90, 100),
        (80, 90),
        (20, 50),
    ];
    for (start, end) in cases {
        let got = store.scan_trades("AAPL", start, end).unwrap();
        let expect: Vec<_> = trades
            .iter()
            .copied()
            .filter(|t| t.ts_ns >= start && t.ts_ns < end)
            .collect();
        assert_eq!(got, expect, "scan {start}..{end}");
        assert_eq!(
            store.count_trades("AAPL", start, end).unwrap(),
            expect.len() as u64
        );
        assert_eq!(
            store.ohlcv("AAPL", start, end, 25).unwrap(),
            ohlcv(&trades, start, end, 25).unwrap()
        );
        assert_eq!(
            store.vwap("AAPL", start, end).unwrap(),
            vwap(&trades, start, end).unwrap()
        );
        assert_eq!(
            store.asof_join("AAPL", start, end).unwrap(),
            asof_join(&trades, &quotes, start, end)
        );
    }

    // Hand-checked bars. Interval 100, same numbers as a SQL floor-division check.
    let sample = [
        trade(100, 10, 2),
        trade(150, 12, 3),
        trade(200, 11, 1),
        trade(250, 9, 4),
    ];
    store.append_trades("MSFT", &sample).unwrap();
    let bars = store.ohlcv("MSFT", 0, 1_000, 100).unwrap();
    assert_eq!(
        bars,
        vec![
            Bar {
                bucket_ns: 100,
                open: 10,
                high: 12,
                low: 10,
                close: 12,
                volume: 5,
                notional_ticks: 56,
                trades: 2,
            },
            Bar {
                bucket_ns: 200,
                open: 11,
                high: 11,
                low: 9,
                close: 9,
                volume: 5,
                notional_ticks: 47,
                trades: 2,
            },
        ]
    );
}

#[test]
fn reopen_preserves_a_generated_tape() {
    let tmp = Tmp::new();
    let ds = generate(&GenConfig {
        trades: 1_500,
        quote_ratio: 2.0,
        symbols: 2,
        seed: 9,
        ..GenConfig::default()
    })
    .unwrap();
    {
        let mut store = TickStore::create(
            &tmp.path,
            Options {
                price_scale: ds.scale,
                chunk_rows: 64,
            },
        )
        .unwrap();
        for s in &ds.symbols {
            store.append_trades(&s.name, &s.trades).unwrap();
            store.append_quotes(&s.name, &s.quotes).unwrap();
        }
        store.flush().unwrap();
    }
    let mut store = TickStore::open(&tmp.path).unwrap();
    let focus = ds.focus();
    let start = ds.session_start_ns;
    let end = ds.data_end_ns;
    let scanned = store.scan_trades(&focus.name, start, end).unwrap();
    assert_eq!(scanned, focus.trades);
    assert_eq!(
        store
            .ohlcv(&focus.name, start, end, 60_000_000_000)
            .unwrap(),
        ohlcv(&focus.trades, start, end, 60_000_000_000).unwrap()
    );
    let bars = store
        .ohlcv(
            &focus.name,
            ds.range_start_ns,
            ds.range_end_ns,
            1_000_000_000,
        )
        .unwrap();
    assert_eq!(
        ohlcv_checksum(&bars),
        ohlcv_checksum(
            &ohlcv(
                &focus.trades,
                ds.range_start_ns,
                ds.range_end_ns,
                1_000_000_000
            )
            .unwrap()
        )
    );
    let joined = store
        .asof_join(&focus.name, ds.range_start_ns, ds.range_end_ns)
        .unwrap();
    assert_eq!(
        asof_checksum(&joined),
        asof_checksum(&asof_join(
            &focus.trades,
            &focus.quotes,
            ds.range_start_ns,
            ds.range_end_ns
        ))
    );
    // The window starts after the open, so the seed quote is in an earlier chunk.
    assert!(joined.iter().all(|r| r.bid_ticks.is_some()));
}

#[test]
fn rejects_unsorted_appends_and_bad_symbols() {
    let tmp = Tmp::new();
    let mut store = TickStore::create(&tmp.path, Options::default()).unwrap();
    store
        .append_trades("AAPL", &[trade(10, 1, 1), trade(20, 1, 1)])
        .unwrap();
    let err = store.append_trades("AAPL", &[trade(15, 1, 1)]).unwrap_err();
    assert!(err.to_string().contains("unsorted"));
    assert!(store.append_trades("../etc", &[trade(1, 1, 1)]).is_err());
    store.flush().unwrap();
    let mut reopened = TickStore::open(&tmp.path).unwrap();
    let got = reopened.scan_trades("AAPL", 0, 100).unwrap();
    assert_eq!(got.len(), 2);
}

#[test]
fn csv_and_binary_roundtrip() {
    let tmp = Tmp::new();
    let ds = generate(&GenConfig {
        trades: 300,
        quote_ratio: 1.5,
        symbols: 2,
        seed: 3,
        ..GenConfig::default()
    })
    .unwrap();
    tickstore::dataset_io::write_dataset(&tmp.path, &ds).unwrap();
    let tables = tickstore::dataset_io::read_binary_tables(&tmp.path).unwrap();
    assert_eq!(tables.symbols, ds.symbol_names());
    for (i, s) in ds.symbols.iter().enumerate() {
        assert_eq!(tables.trades[i], s.trades);
        assert_eq!(tables.quotes[i], s.quotes);
    }
    let csv =
        tickstore::dataset_io::read_trades_csv(&tmp.path.join("trades.csv"), ds.scale).unwrap();
    for s in &ds.symbols {
        assert_eq!(csv.get(&s.name).unwrap(), &s.trades);
    }
}
