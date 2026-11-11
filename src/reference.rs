//! Scalar query specification.
//!
//! The engine and the external benchmarks are checked against these loops.
//! They are intentionally obvious: one row at a time, no pruning, no codecs.

use crate::types::{bucket_start, AsofRow, Bar, Quote, Trade, Vwap};
use crate::{Error, Result};

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

pub fn ohlcv(trades: &[Trade], start: i64, end: i64, interval_ns: i64) -> Result<Vec<Bar>> {
    if interval_ns <= 0 {
        return Err(Error::bad("interval must be positive"));
    }
    if start >= end {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut cur: Option<Acc> = None;
    for t in trades {
        if t.ts_ns < start {
            continue;
        }
        if t.ts_ns >= end {
            break;
        }
        let b = bucket_start(t.ts_ns, interval_ns);
        let same = cur.as_ref().is_some_and(|acc| acc.bucket == b);
        if same {
            cur.as_mut().unwrap().update(t)?;
            continue;
        }
        if let Some(acc) = cur.take() {
            out.push(acc.finish());
        }
        cur = Some(Acc::new(b, t));
    }
    if let Some(acc) = cur {
        out.push(acc.finish());
    }
    Ok(out)
}

struct Acc {
    bucket: i64,
    open: i64,
    high: i64,
    low: i64,
    close: i64,
    volume: u64,
    notional: i128,
    trades: u64,
}

impl Acc {
    fn new(bucket: i64, t: &Trade) -> Self {
        Self {
            bucket,
            open: t.price_ticks,
            high: t.price_ticks,
            low: t.price_ticks,
            close: t.price_ticks,
            volume: u64::from(t.size),
            notional: i128::from(t.price_ticks) * i128::from(t.size),
            trades: 1,
        }
    }

    fn update(&mut self, t: &Trade) -> Result<()> {
        self.high = self.high.max(t.price_ticks);
        self.low = self.low.min(t.price_ticks);
        self.close = t.price_ticks;
        add_print(&mut self.notional, &mut self.volume, t.price_ticks, t.size)?;
        self.trades += 1;
        Ok(())
    }

    fn finish(&self) -> Bar {
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

pub fn vwap(trades: &[Trade], start: i64, end: i64) -> Result<Vwap> {
    let mut out = Vwap::default();
    for t in trades {
        if t.ts_ns < start {
            continue;
        }
        if t.ts_ns >= end {
            break;
        }
        add_print(
            &mut out.notional_ticks,
            &mut out.volume,
            t.price_ticks,
            t.size,
        )?;
        out.trades += 1;
    }
    Ok(out)
}

pub fn asof_join(trades: &[Trade], quotes: &[Quote], start: i64, end: i64) -> Vec<AsofRow> {
    let mut out = Vec::new();
    if start >= end {
        return out;
    }
    let mut qi = 0usize;
    let mut active: Option<&Quote> = None;
    for t in trades {
        if t.ts_ns < start {
            continue;
        }
        if t.ts_ns >= end {
            break;
        }
        while qi < quotes.len() && quotes[qi].ts_ns <= t.ts_ns {
            active = Some(&quotes[qi]);
            qi += 1;
        }
        out.push(row_from(t, active));
    }
    out
}

fn row_from(t: &Trade, q: Option<&Quote>) -> AsofRow {
    AsofRow {
        ts_ns: t.ts_ns,
        price_ticks: t.price_ticks,
        size: t.size,
        side: t.side,
        quote_ts_ns: q.map(|q| q.ts_ns),
        bid_ticks: q.map(|q| q.bid_ticks),
        ask_ticks: q.map(|q| q.ask_ticks),
        bid_size: q.map(|q| q.bid_size),
        ask_size: q.map(|q| q.ask_size),
    }
}

pub fn count_range(trades: &[Trade], start: i64, end: i64) -> u64 {
    trades
        .iter()
        .filter(|t| t.ts_ns >= start && t.ts_ns < end)
        .count() as u64
}

pub fn sum_price_size(trades: &[Trade], start: i64, end: i64) -> (u64, i128, u64) {
    let mut n = 0u64;
    let mut price = 0i128;
    let mut size = 0u64;
    for t in trades {
        if t.ts_ns < start || t.ts_ns >= end {
            continue;
        }
        n += 1;
        price += i128::from(t.price_ticks);
        size += u64::from(t.size);
    }
    (n, price, size)
}

/// Checksum of an as-of result: row count, null quotes, sum of bid ticks.
pub fn asof_checksum(rows: &[AsofRow]) -> (u64, u64, i128) {
    let mut nulls = 0u64;
    let mut bid = 0i128;
    for r in rows {
        match r.bid_ticks {
            Some(b) => bid += i128::from(b),
            None => nulls += 1,
        }
    }
    (rows.len() as u64, nulls, bid)
}

pub fn ohlcv_checksum(bars: &[Bar]) -> (u64, u64, i128, i128) {
    let mut volume = 0u64;
    let mut open_sum = 0i128;
    let mut close_sum = 0i128;
    for b in bars {
        volume += b.volume;
        open_sum += i128::from(b.open);
        close_sum += i128::from(b.close);
    }
    (bars.len() as u64, volume, open_sum, close_sum)
}
