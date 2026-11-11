use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Aggressor side of a trade. Stored in  the side column as a small integer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum Side {
    Unknown = 0,
    Buy = 1,
    Sell = 2,
}

impl Side {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(Side::Unknown),
            1 => Ok(Side::Buy),
            2 => Ok(Side::Sell),
            _ => Err(Error::corrupt(format!("side byte {v} is not 0, 1, or 2"))),
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "0" | "u" | "unk" | "unknown" | "" => Ok(Side::Unknown),
            "1" | "b" | "buy" | "bid" => Ok(Side::Buy),
            "2" | "s" | "sell" | "ask" | "offer" => Ok(Side::Sell),
            other => Err(Error::bad(format!("unknown side '{other}'"))),
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Side::Unknown => "unk",
            Side::Buy => "buy",
            Side::Sell => "sell",
        })
    }
}

/// One trade print. Prices are integer ticks, not floats.
///
/// Currency price = `price_ticks / price_scale`, where `price_scale` lives on
/// the store (default 10_000, so a tick is one hundredth of a cent).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Trade {
    pub ts_ns: i64,
    pub price_ticks: i64,
    pub size: u32,
    pub side: Side,
}

/// Top of book at a point in time. Same tick scale as [`Trade`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Quote {
    pub ts_ns: i64,
    pub bid_ticks: i64,
    pub ask_ticks: i64,
    pub bid_size: u32,
    pub ask_size: u32,
}

/// One OHLCV bucket. Prices are ticks. `notional_ticks` is `sum(price_ticks * size)`.
///
/// Bars are sparse: a bucket with no prints is omitted. Buckets are aligned to
/// the Unix epoch via `ts.div_euclid(interval) * interval`, matching SQL
/// integer division, not to the query start.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bar {
    pub bucket_ns: i64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: u64,
    pub notional_ticks: i128,
    pub trades: u64,
}

impl Bar {
    /// Volume-weighted average price in currency units, if the bar traded.
    pub fn vwap(&self, price_scale: i64) -> Option<f64> {
        if self.volume == 0 || price_scale == 0 {
            None
        } else {
            Some((self.notional_ticks as f64 / self.volume as f64) / price_scale as f64)
        }
    }
}

/// Session or window VWAP accumulated in integer ticks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Vwap {
    pub volume: u64,
    pub notional_ticks: i128,
    pub trades: u64,
}

impl Vwap {
    pub fn price(&self, price_scale: i64) -> Option<f64> {
        if self.volume == 0 || price_scale == 0 {
            None
        } else {
            Some((self.notional_ticks as f64 / self.volume as f64) / price_scale as f64)
        }
    }
}

/// A trade joined to the latest quote at or before the trade timestamp.
///
/// This is the kdb+ `aj` / DuckDB `ASOF` rule: greatest `quote.ts <= trade.ts`.
/// Quotes and trades that share a timestamp match. If no quote exists yet, the
/// quote fields are `None`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AsofRow {
    pub ts_ns: i64,
    pub price_ticks: i64,
    pub size: u32,
    pub side: Side,
    pub quote_ts_ns: Option<i64>,
    pub bid_ticks: Option<i64>,
    pub ask_ticks: Option<i64>,
    pub bid_size: Option<u32>,
    pub ask_size: Option<u32>,
}

/// Epoch-aligned bucket start. `interval_ns` must be positive.
pub fn bucket_start(ts_ns: i64, interval_ns: i64) -> i64 {
    ts_ns.div_euclid(interval_ns) * interval_ns
}

/// Decimal digits of a power-of-ten scale. `10_000 -> Some(4)`, `1 -> Some(0)`.
pub fn scale_digits(scale: i64) -> Option<u32> {
    if scale <= 0 {
        return None;
    }
    let mut s = scale;
    let mut d = 0u32;
    while s % 10 == 0 {
        s /= 10;
        d += 1;
    }
    if s == 1 {
        Some(d)
    } else {
        None
    }
}

/// Format integer ticks as a decimal string that parses back exactly.
pub fn format_ticks(ticks: i64, scale: i64) -> Result<String> {
    let digits = scale_digits(scale)
        .ok_or_else(|| Error::bad("text price format requires a power-of-ten price scale"))?;
    let neg = ticks < 0;
    let v = ticks.unsigned_abs();
    let scale_u = scale as u64;
    let ip = v / scale_u;
    let fp = v % scale_u;
    let sign = if neg { "-" } else { "" };
    if digits == 0 {
        return Ok(format!("{sign}{ip}"));
    }
    Ok(format!("{sign}{ip}.{fp:0width$}", width = digits as usize))
}

/// Parse a decimal price into integer ticks, rounding half away from zero
/// when the input has more fractional digits than the scale.
pub fn parse_ticks(text: &str, scale: i64) -> Result<i64> {
    let digits = scale_digits(scale)
        .ok_or_else(|| Error::bad("text price format requires a power-of-ten price scale"))?;
    let s = text.trim();
    if s.is_empty() {
        return Err(Error::bad("empty price"));
    }
    let neg = s.starts_with('-');
    let s = s.trim_start_matches(['+', '-']);
    if s.is_empty() || s.ends_with('.') && !s[..s.len() - 1].contains('.') {
        // a trailing dot with no fraction is accepted below via split
    }
    let (ip_str, fp_str) = match s.split_once('.') {
        Some((a, b)) => (a, b),
        None => (s, ""),
    };
    if ip_str.is_empty() && fp_str.is_empty() {
        return Err(Error::bad(format!("bad price '{text}'")));
    }
    if !ip_str.bytes().all(|c| c.is_ascii_digit()) || !fp_str.bytes().all(|c| c.is_ascii_digit()) {
        return Err(Error::bad(format!("bad price '{text}'")));
    }
    let ip: i128 = if ip_str.is_empty() {
        0
    } else {
        ip_str
            .parse()
            .map_err(|_| Error::bad(format!("bad price '{text}'")))?
    };
    let mut frac: i128 = 0;
    let mut place = scale as i128;
    for (i, c) in fp_str.bytes().enumerate() {
        let d = (c - b'0') as i128;
        if (i as u32) < digits {
            place /= 10;
            frac += d * place;
        } else {
            if d >= 5 {
                frac += 1;
            }
            break;
        }
    }
    let mut mag = ip
        .checked_mul(scale as i128)
        .and_then(|v| v.checked_add(frac))
        .ok_or_else(|| Error::bad(format!("price '{text}' overflows")))?;
    if mag > i64::MAX as i128 {
        return Err(Error::bad(format!("price '{text}' overflows")));
    }
    if neg {
        mag = -mag;
    }
    Ok(mag as i64)
}

/// Format a nanosecond timestamp as `YYYY-MM-DD HH:MM:SS.nnnnnnnnnZ`.
pub fn format_timestamp_utc(ts_ns: i64) -> String {
    let secs = ts_ns.div_euclid(1_000_000_000);
    let nanos = ts_ns.rem_euclid(1_000_000_000) as u32;
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400) as u32;
    let h = tod / 3600;
    let m = (tod % 3600) / 60;
    let s = tod % 60;
    let (y, mo, d) = civil_from_days(days);
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{m:02}:{s:02}.{nanos:09}Z")
}

/// Howard Hinnant's `civil_from_days`. `z` is days since 1970-01-01.
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = y + if m <= 2 { 1 } else { 0 };
    (y as i32, m as u32, d as u32)
}

/// Parse an interval like `1m`, `500ms`, `1h`, `60s`, or a raw nanosecond count.
pub fn parse_interval(text: &str) -> Result<i64> {
    let s = text.trim();
    let (num, mul): (&str, i64) = if let Some(rest) = s.strip_suffix("ns") {
        (rest, 1)
    } else if let Some(rest) = s.strip_suffix("us") {
        (rest, 1_000)
    } else if let Some(rest) = s.strip_suffix("ms") {
        (rest, 1_000_000)
    } else if let Some(rest) = s.strip_suffix('s') {
        (rest, 1_000_000_000)
    } else if let Some(rest) = s.strip_suffix('m') {
        (rest, 60 * 1_000_000_000)
    } else if let Some(rest) = s.strip_suffix('h') {
        (rest, 3_600 * 1_000_000_000)
    } else {
        (s, 1)
    };
    if num.is_empty() {
        return Err(Error::bad(format!("bad interval '{text}'")));
    }
    let n: i64 = num
        .parse()
        .map_err(|_| Error::bad(format!("bad interval '{text}'")))?;
    if n <= 0 {
        return Err(Error::bad("interval must be positive"));
    }
    n.checked_mul(mul)
        .ok_or_else(|| Error::bad(format!("interval '{text}' overflows")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_roundtrip_and_rounding() {
        let scale = 10_000;
        for ticks in [0, 1, 100, 1850100, -15, -10000, i64::MIN + 1] {
            let text = format_ticks(ticks, scale).unwrap();
            assert_eq!(parse_ticks(&text, scale).unwrap(), ticks, "{text}");
        }
        assert_eq!(parse_ticks("185.01005", scale).unwrap(), 1_850_101);
        assert_eq!(parse_ticks("185.01004", scale).unwrap(), 1_850_100);
        assert_eq!(parse_ticks(".5", 10).unwrap(), 5);
        assert_eq!(format_ticks(1_850_100, scale).unwrap(), "185.0100");
    }

    #[test]
    fn utc_known_instant() {
        assert_eq!(
            format_timestamp_utc(1_704_205_800_000_000_000),
            "2024-01-02 14:30:00.000000000Z"
        );
        assert_eq!(format_timestamp_utc(0), "1970-01-01 00:00:00.000000000Z");
        assert_eq!(format_timestamp_utc(-1), "1969-12-31 23:59:59.999999999Z");
    }

    #[test]
    fn intervals() {
        assert_eq!(parse_interval("1m").unwrap(), 60_000_000_000);
        assert_eq!(parse_interval("500ms").unwrap(), 500_000_000);
        assert_eq!(parse_interval("1h").unwrap(), 3_600_000_000_000);
        assert_eq!(bucket_start(150, 100), 100);
        assert_eq!(bucket_start(-150, 100), -200);
    }
}
