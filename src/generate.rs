//! Synthetic US-cash-session tape.
//!
//! One latent mid is stepped through merged trade and quote times. Quotes
//! publish a new touch; trades lift the ask or hit the bid of the quote that
//! is current at that instant. An as-of join therefore recovers the touch the
//! trade actually executed against.
//!
//! Timestamps are drawn in integer nanoseconds. They are never rounded through
//! `f64`: a session timestamp is ~1.7e18, which `f64` cannot represent exactly.

use crate::rng::Rng;
use crate::types::{Quote, Side, Trade};
use crate::{Error, Result};

pub const DEFAULT_SESSION_START_NS: i64 = 1_704_205_800_000_000_000;
pub const DEFAULT_SESSION_LEN_NS: i64 = 23_400_000_000_000;

#[derive(Clone, Debug)]
pub struct GenConfig {
    pub seed: u64,
    pub trades: usize,
    pub quote_ratio: f64,
    pub symbols: usize,
    pub scale: i64,
    pub session_start_ns: i64,
    pub session_len_ns: i64,
}

impl Default for GenConfig {
    fn default() -> Self {
        Self {
            seed: 7,
            trades: 100_000,
            quote_ratio: 2.0,
            symbols: 8,
            scale: 10_000,
            session_start_ns: DEFAULT_SESSION_START_NS,
            session_len_ns: DEFAULT_SESSION_LEN_NS,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SymbolSeries {
    pub name: String,
    pub trades: Vec<Trade>,
    pub quotes: Vec<Quote>,
}

#[derive(Clone, Debug)]
pub struct Dataset {
    pub symbols: Vec<SymbolSeries>,
    pub scale: i64,
    pub seed: u64,
    pub session_start_ns: i64,
    pub session_end_ns: i64,
    pub data_end_ns: i64,
    pub range_start_ns: i64,
    pub range_end_ns: i64,
}

impl Dataset {
    pub fn focus(&self) -> &SymbolSeries {
        &self.symbols[0]
    }

    pub fn trade_rows(&self) -> usize {
        self.symbols.iter().map(|s| s.trades.len()).sum()
    }

    pub fn quote_rows(&self) -> usize {
        self.symbols.iter().map(|s| s.quotes.len()).sum()
    }

    pub fn symbol_names(&self) -> Vec<String> {
        self.symbols.iter().map(|s| s.name.clone()).collect()
    }
}

struct Spec {
    name: &'static str,
    price: f64,
    rel_vol: f64,
    weight: u32,
    tick: f64,
}

const SPECS: &[Spec] = &[
    Spec {
        name: "AAPL",
        price: 185.0,
        rel_vol: 0.00015,
        weight: 8,
        tick: 0.01,
    },
    Spec {
        name: "MSFT",
        price: 375.0,
        rel_vol: 0.00014,
        weight: 7,
        tick: 0.01,
    },
    Spec {
        name: "NVDA",
        price: 495.0,
        rel_vol: 0.00035,
        weight: 6,
        tick: 0.01,
    },
    Spec {
        name: "AMZN",
        price: 155.0,
        rel_vol: 0.00018,
        weight: 5,
        tick: 0.01,
    },
    Spec {
        name: "META",
        price: 360.0,
        rel_vol: 0.00022,
        weight: 4,
        tick: 0.01,
    },
    Spec {
        name: "SPY",
        price: 472.0,
        rel_vol: 0.00008,
        weight: 4,
        tick: 0.01,
    },
    Spec {
        name: "TSLA",
        price: 248.0,
        rel_vol: 0.00045,
        weight: 3,
        tick: 0.01,
    },
    Spec {
        name: "AMD",
        price: 138.0,
        rel_vol: 0.00032,
        weight: 2,
        tick: 0.01,
    },
    Spec {
        name: "IWM",
        price: 198.0,
        rel_vol: 0.00012,
        weight: 2,
        tick: 0.01,
    },
    Spec {
        name: "QQQ",
        price: 410.0,
        rel_vol: 0.00010,
        weight: 2,
        tick: 0.01,
    },
    Spec {
        name: "JPM",
        price: 172.0,
        rel_vol: 0.00013,
        weight: 2,
        tick: 0.01,
    },
    Spec {
        name: "XOM",
        price: 102.0,
        rel_vol: 0.00014,
        weight: 1,
        tick: 0.01,
    },
];

pub fn generate(cfg: &GenConfig) -> Result<Dataset> {
    if cfg.symbols == 0 {
        return Err(Error::bad("need at least one symbol"));
    }
    if cfg.scale <= 0 {
        return Err(Error::bad("price scale must be positive"));
    }
    if cfg.session_len_ns <= 1 {
        return Err(Error::bad("session length must cover more than 1 ns"));
    }
    if cfg.quote_ratio < 0.0 {
        return Err(Error::bad("quote ratio must be non-negative"));
    }
    let mut rng = Rng::new(cfg.seed);
    let specs = select_specs(cfg.symbols);
    let counts = allocate(cfg.trades, &specs);
    let mut symbols = Vec::with_capacity(specs.len());
    for (spec, n_trades) in specs.iter().zip(counts.iter().copied()) {
        let n_quotes = ((n_trades as f64) * cfg.quote_ratio).round().max(1.0) as usize;
        let (trades, quotes) = gen_symbol(spec, n_trades, n_quotes, cfg, &mut rng);
        symbols.push(SymbolSeries {
            name: spec_name(spec, symbols.len()),
            trades,
            quotes,
        });
    }
    let mut max_ts = cfg.session_start_ns;
    for s in &symbols {
        if let Some(t) = s.trades.last() {
            max_ts = max_ts.max(t.ts_ns);
        }
        if let Some(q) = s.quotes.last() {
            max_ts = max_ts.max(q.ts_ns);
        }
    }
    let five_min = 5 * 60 * 1_000_000_000;
    let ten_min = 10 * 60 * 1_000_000_000;
    let range_start = cfg.session_start_ns + five_min.min(cfg.session_len_ns / 10);
    let range_end = (range_start + ten_min).min(cfg.session_start_ns + cfg.session_len_ns);
    Ok(Dataset {
        symbols,
        scale: cfg.scale,
        seed: cfg.seed,
        session_start_ns: cfg.session_start_ns,
        session_end_ns: cfg.session_start_ns + cfg.session_len_ns,
        data_end_ns: max_ts.saturating_add(1),
        range_start_ns: range_start,
        range_end_ns: range_end.max(range_start + 1),
    })
}

fn spec_name(spec: &Spec, idx: usize) -> String {
    if spec.name.starts_with("SYM") {
        format!("SYM{idx:02}")
    } else {
        spec.name.to_string()
    }
}

fn select_specs(n: usize) -> Vec<Spec> {
    let mut out = Vec::with_capacity(n);
    for spec in SPECS.iter().take(n) {
        out.push(Spec {
            name: spec.name,
            price: spec.price,
            rel_vol: spec.rel_vol,
            weight: spec.weight,
            tick: spec.tick,
        });
    }
    for i in SPECS.len()..n {
        out.push(Spec {
            name: "SYM",
            price: 50.0 + (i as f64) * 3.0,
            rel_vol: 0.0002,
            weight: 1,
            tick: 0.01,
        });
    }
    out
}

fn allocate(trades: usize, specs: &[Spec]) -> Vec<usize> {
    let total_w: u128 = specs.iter().map(|s| s.weight as u128).sum();
    let mut counts = Vec::with_capacity(specs.len());
    let mut assigned = 0usize;
    for s in specs {
        let c = ((trades as u128) * u128::from(s.weight) / total_w) as usize;
        counts.push(c);
        assigned += c;
    }
    if !counts.is_empty() {
        counts[0] += trades - assigned;
    }
    counts
}

fn gen_symbol(
    spec: &Spec,
    n_trades: usize,
    n_quotes: usize,
    cfg: &GenConfig,
    rng: &mut Rng,
) -> (Vec<Trade>, Vec<Quote>) {
    let start = cfg.session_start_ns;
    let dur = cfg.session_len_ns;
    let extra_q = n_quotes.saturating_sub(1);
    let q_times = sample_times(extra_q, dur, rng);
    let t_times = sample_times(n_trades, dur, rng);

    let tick = spec.tick;
    let tick_units = ((tick * cfg.scale as f64).round() as i64).max(1);
    let floor_px = (spec.price * 0.5).max(tick);
    let cap_px = spec.price * 1.8;
    let mut mid = spec.price.clamp(floor_px, cap_px);
    let mut prev_t = start;

    let (b0, a0) = touch_from_mid(mid, tick, 1);
    let mut bid_px = b0 * tick_units;
    let mut ask_px = a0 * tick_units;
    let mut quotes = Vec::with_capacity(extra_q + 1);
    quotes.push(Quote {
        ts_ns: start,
        bid_ticks: bid_px,
        ask_ticks: ask_px,
        bid_size: quote_size(rng),
        ask_size: quote_size(rng),
    });
    let mut trades = Vec::with_capacity(n_trades);

    let mut events = Vec::with_capacity(q_times.len() + t_times.len());
    for off in q_times {
        events.push((start + off, 0u8));
    }
    for off in t_times {
        events.push((start + off, 1u8));
    }
    events.sort_unstable_by_key(|e| (e.0, e.1));

    for (t, kind) in events {
        let dt = (t - prev_t).max(0) as f64 / 1e9;
        if dt > 0.0 {
            let mut ret = rng.normal() * spec.rel_vol * dt.sqrt();
            ret = ret.clamp(-0.02, 0.02);
            mid = (mid * (1.0 + ret)).clamp(floor_px, cap_px);
        }
        prev_t = t;
        if kind == 0 {
            let (b, a) = touch_from_mid(mid, tick, sample_spread(rng));
            bid_px = b * tick_units;
            ask_px = a * tick_units;
            quotes.push(Quote {
                ts_ns: t,
                bid_ticks: bid_px,
                ask_ticks: ask_px,
                bid_size: quote_size(rng),
                ask_size: quote_size(rng),
            });
        } else {
            let buy = rng.f64() < 0.5;
            trades.push(Trade {
                ts_ns: t,
                price_ticks: if buy { ask_px } else { bid_px },
                size: trade_size(rng),
                side: if buy { Side::Buy } else { Side::Sell },
            });
        }
    }
    (trades, quotes)
}

/// Offsets in `[1, dur)`, U-shaped (open and close are heavier), strictly increasing.
fn sample_times(n: usize, dur: i64, rng: &mut Rng) -> Vec<i64> {
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        v.push(shaped_offset(rng, dur).clamp(1, dur - 1));
    }
    v.sort_unstable();
    for i in 1..v.len() {
        if v[i] <= v[i - 1] {
            v[i] = v[i - 1].saturating_add(1);
        }
    }
    v
}

fn shaped_offset(rng: &mut Rng, dur: i64) -> i64 {
    let open = (30 * 60 * 1_000_000_000).min(dur / 4).max(1);
    let u = rng.f64();
    if u < 0.22 {
        rng.uniform_i64(0, open)
    } else if u < 0.44 {
        rng.uniform_i64((dur - open).max(0), dur.max(1))
    } else {
        let lo = open.min(dur.saturating_sub(1));
        let hi = (dur - open).max(lo + 1);
        rng.uniform_i64(lo, hi)
    }
}

fn touch_from_mid(mid: f64, tick: f64, spread_ticks: i64) -> (i64, i64) {
    let mid_ticks = (mid / tick).round().max(1.0) as i64;
    let spread = spread_ticks.max(1);
    let bid = (mid_ticks - spread / 2).max(1);
    let ask = bid + spread;
    (bid, ask)
}

fn sample_spread(rng: &mut Rng) -> i64 {
    let u = rng.f64();
    if u < 0.78 {
        1
    } else if u < 0.95 {
        2
    } else {
        3
    }
}

fn trade_size(rng: &mut Rng) -> u32 {
    let z = rng.normal() * 0.85 + 5.0;
    let raw = z.exp().clamp(1.0, 20_000.0);
    if rng.f64() < 0.7 {
        let lots = (raw / 100.0).round().clamp(1.0, 200.0) as u32;
        lots * 100
    } else {
        raw.round().clamp(1.0, 20_000.0) as u32
    }
}

fn quote_size(rng: &mut Rng) -> u32 {
    const CHOICES: [u32; 8] = [100, 200, 300, 400, 500, 800, 1000, 2000];
    CHOICES[(rng.next_u64() as usize) % CHOICES.len()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference::asof_join;

    #[test]
    fn tape_is_sorted_and_trades_the_touch() {
        let ds = generate(&GenConfig {
            trades: 2_000,
            quote_ratio: 3.0,
            symbols: 3,
            seed: 5,
            ..GenConfig::default()
        })
        .unwrap();
        assert_eq!(ds.trade_rows(), 2_000);
        for s in &ds.symbols {
            assert!(s.quotes.windows(2).all(|w| w[0].ts_ns < w[1].ts_ns));
            assert!(s.trades.windows(2).all(|w| w[0].ts_ns < w[1].ts_ns));
            assert!(s
                .quotes
                .iter()
                .all(|q| q.bid_ticks < q.ask_ticks && q.bid_ticks > 0));
            let joined = asof_join(&s.trades, &s.quotes, i64::MIN, i64::MAX);
            assert_eq!(joined.len(), s.trades.len());
            for row in joined {
                let bid = row.bid_ticks.expect("opening quote covers every trade");
                let ask = row.ask_ticks.unwrap();
                assert!(
                    row.price_ticks == bid || row.price_ticks == ask,
                    "{} trade {} not on touch bid {bid} ask {ask}",
                    s.name,
                    row.price_ticks
                );
            }
        }
    }
}
