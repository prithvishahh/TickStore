//! Gorilla integer and float codecs.
//!
//! Timestamp scheme follows Pelkonen et al., VLDB 2015, with two deliberate
//! fixes:
//!
//! * Residuals use power-of-two signed ranges. The paper's published bounds
//!   (`[-63, 64]`, `[-255, 256]`, `[-2047, 2048]`) do not fit in 7/9/12 bits.
//! * A 64-bit escape. The paper stops at 32 bits, which is only ~2.1 seconds
//!   when the unit is a nanosecond. Quiet names gap by more than that.
//!
//! The same integer codec is a candidate for prices. It is not float XOR.
//! XOR compression is implemented separately and is exact on `f64` bits, but
//! it is not used for stored prices: a tick grid wants an integer.

use super::bitstream::{BitReader, BitWriter};
use crate::{Error, Result};

pub fn encode_gorilla_i64(values: &[i64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut w = BitWriter::new();
    w.write_bits(values[0] as u64, 64);
    let mut prev = values[0];
    let mut prev_delta: i64 = 0;
    for &v in &values[1..] {
        let delta = v.wrapping_sub(prev);
        let dod = delta.wrapping_sub(prev_delta);
        encode_dod(&mut w, dod);
        prev_delta = delta;
        prev = v;
    }
    w.finish()
}

pub fn decode_gorilla_i64(data: &[u8], n: usize) -> Result<Vec<i64>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let mut r = BitReader::new(data);
    let mut prev = r.read_bits(64)? as i64;
    let mut out = Vec::with_capacity(n);
    out.push(prev);
    let mut prev_delta: i64 = 0;
    for _ in 1..n {
        let dod = decode_dod(&mut r)?;
        let delta = prev_delta.wrapping_add(dod);
        let v = prev.wrapping_add(delta);
        out.push(v);
        prev = v;
        prev_delta = delta;
    }
    Ok(out)
}

fn encode_dod(w: &mut BitWriter, dod: i64) {
    if dod == 0 {
        w.write_bits(0, 1);
    } else if (-64..64).contains(&dod) {
        w.write_bits(0b10, 2);
        w.write_bits(dod as u64, 7);
    } else if (-256..256).contains(&dod) {
        w.write_bits(0b110, 3);
        w.write_bits(dod as u64, 9);
    } else if (-2048..2048).contains(&dod) {
        w.write_bits(0b1110, 4);
        w.write_bits(dod as u64, 12);
    } else if dod >= i64::from(i32::MIN) && dod <= i64::from(i32::MAX) {
        w.write_bits(0b11110, 5);
        w.write_bits(dod as u64, 32);
    } else {
        w.write_bits(0b11111, 5);
        w.write_bits(dod as u64, 64);
    }
}

fn decode_dod(r: &mut BitReader<'_>) -> Result<i64> {
    if r.read_bits(1)? == 0 {
        return Ok(0);
    }
    if r.read_bits(1)? == 0 {
        return Ok(sign_extend(r.read_bits(7)?, 7));
    }
    if r.read_bits(1)? == 0 {
        return Ok(sign_extend(r.read_bits(9)?, 9));
    }
    if r.read_bits(1)? == 0 {
        return Ok(sign_extend(r.read_bits(12)?, 12));
    }
    if r.read_bits(1)? == 0 {
        return Ok(sign_extend(r.read_bits(32)?, 32));
    }
    Ok(r.read_bits(64)? as i64)
}

fn sign_extend(v: u64, bits: u32) -> i64 {
    if bits == 0 || bits >= 64 {
        return v as i64;
    }
    let shift = 64 - bits;
    ((v << shift) as i64) >> shift
}

/// Gorilla XOR float codec. `f64` bit patterns round-trip exactly, including
/// the sign bit of zero and NaN payloads. Meaningful length is stored as
/// `len - 1` so a 64-bit XOR (both the top and bottom bits set) fits in 6 bits.
/// Leading zeros are capped at 31, as in the paper (the field is 5 bits).
pub fn encode_xor_f64(values: &[f64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut w = BitWriter::new();
    let mut prev = values[0].to_bits();
    w.write_bits(prev, 64);
    let mut window: Option<(u32, u32)> = None;
    for v in values.iter().skip(1) {
        let bits = v.to_bits();
        let xor = bits ^ prev;
        if xor == 0 {
            w.write_bit(false);
        } else {
            w.write_bit(true);
            let tz = xor.trailing_zeros().min(63);
            // 5-bit leading-zero field, and always leave at least one significant bit.
            let lz = xor.leading_zeros().min(31).min(63 - tz);
            // Capping lz can only shrink it, and a non-zero word has
            // leading + trailing <= 63, so sig stays in 1..=64.
            let sig = 64 - lz - tz;
            if let Some((plz, ptz)) = window {
                if lz >= plz && tz >= ptz {
                    let width = 64 - plz - ptz;
                    w.write_bit(false);
                    w.write_bits(xor >> ptz, width);
                    prev = bits;
                    continue;
                }
            }
            w.write_bit(true);
            w.write_bits(u64::from(lz), 5);
            w.write_bits(u64::from(sig - 1), 6);
            w.write_bits(xor >> tz, sig);
            window = Some((lz, tz));
        }
        prev = bits;
    }
    w.finish()
}

pub fn decode_xor_f64(data: &[u8], n: usize) -> Result<Vec<f64>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let mut r = BitReader::new(data);
    let mut prev = r.read_bits(64)?;
    let mut out = Vec::with_capacity(n);
    out.push(f64::from_bits(prev));
    let mut window: Option<(u32, u32)> = None;
    for _ in 1..n {
        if r.read_bits(1)? == 0 {
            out.push(f64::from_bits(prev));
            continue;
        }
        let xor = if r.read_bits(1)? == 0 {
            let (plz, ptz) =
                window.ok_or_else(|| Error::corrupt("xor window used before it was set"))?;
            let width = 64 - plz - ptz;
            let meaningful = r.read_bits(width)?;
            meaningful << ptz
        } else {
            let lz = r.read_bits(5)? as u32;
            let sig = r.read_bits(6)? as u32 + 1;
            if lz >= 64 || sig == 0 || lz + sig > 64 {
                return Err(Error::corrupt("xor window header is invalid"));
            }
            let tz = 64 - lz - sig;
            let meaningful = r.read_bits(sig)?;
            window = Some((lz, tz));
            meaningful << tz
        };
        prev ^= xor;
        out.push(f64::from_bits(prev));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::Rng;

    #[test]
    fn regular_timestamps_are_tiny() {
        let ts: Vec<i64> = (0..10_000)
            .map(|i| 1_700_000_000_000_000_000 + i * 1_000_000)
            .collect();
        let enc = encode_gorilla_i64(&ts);
        assert_eq!(decode_gorilla_i64(&enc, ts.len()).unwrap(), ts);
        // Constant spacing collapses to a single bit per sample after the first delta.
        // 10_000 timestamps are ~1.2 KB here versus 80 KB raw.
        assert!(
            enc.len() * 10 < ts.len() * 8,
            "encoded {} bytes, raw {}",
            enc.len(),
            ts.len() * 8
        );
    }

    #[test]
    fn gaps_wider_than_i32_ns() {
        let base = 1_704_205_800_000_000_000i64;
        let hour = 3_600_000_000_000i64;
        let ts = vec![
            base,
            base + 50,
            base + hour,
            base + hour + 50,
            base + 3 * hour,
        ];
        let enc = encode_gorilla_i64(&ts);
        assert_eq!(decode_gorilla_i64(&enc, ts.len()).unwrap(), ts);
    }

    #[test]
    fn gorilla_i64_edges() {
        let samples: Vec<Vec<i64>> = vec![
            vec![],
            vec![0],
            vec![i64::MIN],
            vec![i64::MAX, i64::MIN, -1, 0, 1],
            vec![10, 10, 10, 11, 9, 9],
        ];
        for s in samples {
            let enc = encode_gorilla_i64(&s);
            assert_eq!(decode_gorilla_i64(&enc, s.len()).unwrap(), s);
        }
    }

    #[test]
    fn xor_preserves_bits() {
        let mut rng = Rng::new(3);
        let mut values = vec![
            0.0,
            0.0,
            -0.0,
            1.0,
            1.0000001,
            f64::from_bits(0x8000_0000_0000_0001),
            f64::from_bits(u64::MAX),
            f64::MIN,
            f64::MAX,
            1e-320,
            f64::from_bits(0x7ff8_0000_0000_0001), // NaN payload
        ];
        for _ in 0..200 {
            values.push(f64::from_bits(rng.next_u64()));
        }
        let enc = encode_xor_f64(&values);
        let got = decode_xor_f64(&enc, values.len()).unwrap();
        for (a, b) in values.iter().zip(got.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }
}
