//! Integer column codecs: raw, delta / delta-of-delta mini-block bit-packing,
//! zigzag-varint deltas, frame-of-reference bit-packing, unsigned varint,
//! dictionary, and run-length encoding.
//!
//! Mini-blocks are 128 values. Each block stores its own minimum and bit
//! width, so a single outlier does not widen the whole chunk. Blocks are
//! byte-aligned. Deltas use wrapping arithmetic, so every `i64` sequence
//! round-trips, including the `i64::MIN` / `i64::MAX` boundary.

use super::bitstream::{BitReader, BitWriter};
use super::unpack::unpack_msb;
use crate::cursor::{as_u32_len, push_i64, push_u16, push_u32, push_u64, R};
use crate::{Error, Result};

/// Values per bit-packed mini-block. Matches the order of magnitude of
/// Parquet's delta-binary-packed mini-blocks.
pub const MINI_BLOCK: usize = 128;

pub fn encode_raw_i64(values: &[i64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 8);
    for &v in values {
        push_i64(&mut buf, v);
    }
    buf
}

pub fn decode_raw_i64(data: &[u8], n: usize) -> Result<Vec<i64>> {
    if data.len() != n * 8 {
        return Err(Error::corrupt(format!(
            "raw i64 payload is {} bytes, expected {}",
            data.len(),
            n * 8
        )));
    }
    let mut out = Vec::with_capacity(n);
    let mut r = R::new(data);
    for _ in 0..n {
        out.push(r.i64()?);
    }
    Ok(out)
}

pub fn encode_raw_u64(values: &[u64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(values.len() * 8);
    for &v in values {
        push_u64(&mut buf, v);
    }
    buf
}

pub fn decode_raw_u64(data: &[u8], n: usize) -> Result<Vec<u64>> {
    if data.len() != n * 8 {
        return Err(Error::corrupt(format!(
            "raw u64 payload is {} bytes, expected {}",
            data.len(),
            n * 8
        )));
    }
    let mut out = Vec::with_capacity(n);
    let mut r = R::new(data);
    for _ in 0..n {
        out.push(r.u64()?);
    }
    Ok(out)
}

pub fn encode_delta(values: &[i64]) -> Vec<u8> {
    encode_residual(values, false)
}

pub fn decode_delta(data: &[u8], n: usize) -> Result<Vec<i64>> {
    decode_residual(data, n, false)
}

/// VWAP numerator and volume from a delta-coded price column and aligned sizes.
///
/// Prices are integrated in registers. The caller does not need a price vector.
pub fn notional_from_delta(data: &[u8], sizes: &[u32]) -> Result<(i128, u64)> {
    let n = sizes.len();
    let mut r = R::new(data);
    let count = r.u32()? as usize;
    if count != n {
        return Err(Error::corrupt("delta block count does not match row count"));
    }
    if n == 0 {
        return Ok((0, 0));
    }
    let mut prev = r.i64()?;
    let mut notional = i128::from(prev) * i128::from(sizes[0]);
    let mut volume = u64::from(sizes[0]);
    let residual_count = r.u32()? as usize;
    if residual_count != n - 1 {
        return Err(Error::corrupt("mini-block residual count mismatch"));
    }
    let mut idx = 1usize;
    let mut tmp = [0u64; MINI_BLOCK];
    while idx < n {
        let len = r.u16()? as usize;
        if len == 0 || len > MINI_BLOCK || idx + len > n {
            return Err(Error::corrupt("mini-block length is invalid"));
        }
        let bit_width = r.u8()?;
        let min = r.i64()?;
        if bit_width > 64 {
            return Err(Error::corrupt("mini-block bit width > 64"));
        }
        if bit_width == 0 {
            for _ in 0..len {
                prev = prev.wrapping_add(min);
                notional += i128::from(prev) * i128::from(sizes[idx]);
                volume += u64::from(sizes[idx]);
                idx += 1;
            }
            continue;
        }
        if bit_width == 64 {
            for _ in 0..len {
                let stored = r.u64()? as i128;
                let delta = (i128::from(min) + stored) as i64;
                prev = prev.wrapping_add(delta);
                notional += i128::from(prev) * i128::from(sizes[idx]);
                volume += u64::from(sizes[idx]);
                idx += 1;
            }
            continue;
        }
        let nbytes = (len * bit_width as usize).div_ceil(8);
        let packed = r.take(nbytes)?;
        unpack_msb(packed, u32::from(bit_width), len, &mut tmp)?;
        for stored in tmp.into_iter().take(len) {
            let delta = (i128::from(min) + stored as i128) as i64;
            prev = prev.wrapping_add(delta);
            notional += i128::from(prev) * i128::from(sizes[idx]);
            volume += u64::from(sizes[idx]);
            idx += 1;
        }
    }
    if r.remaining() != 0 {
        return Err(Error::corrupt("trailing bytes after delta column"));
    }
    Ok((notional, volume))
}

pub fn encode_dod(values: &[i64]) -> Vec<u8> {
    encode_residual(values, true)
}

pub fn decode_dod(data: &[u8], n: usize) -> Result<Vec<i64>> {
    decode_residual(data, n, true)
}

fn encode_residual(values: &[i64], dod: bool) -> Vec<u8> {
    let mut buf = Vec::new();
    push_u32(&mut buf, as_u32_len(values.len()));
    if values.is_empty() {
        return buf;
    }
    push_i64(&mut buf, values[0]);
    let residuals = if dod {
        dod_residuals(values)
    } else {
        delta_residuals(values)
    };
    encode_i64_blocks_into(&mut buf, &residuals);
    buf
}

fn decode_residual(data: &[u8], n: usize, dod: bool) -> Result<Vec<i64>> {
    let mut r = R::new(data);
    let count = r.u32()? as usize;
    if count != n {
        return Err(Error::corrupt("delta block count does not match row count"));
    }
    if n == 0 {
        if r.remaining() != 0 {
            return Err(Error::corrupt("trailing bytes after empty delta column"));
        }
        return Ok(Vec::new());
    }
    let first = r.i64()?;
    let residuals = decode_i64_blocks_rest(&mut r, n - 1)?;
    if r.remaining() != 0 {
        return Err(Error::corrupt("trailing bytes after delta column"));
    }
    if dod {
        Ok(integrate_dod(first, &residuals))
    } else {
        Ok(integrate_delta(first, &residuals))
    }
}

fn delta_residuals(values: &[i64]) -> Vec<i64> {
    values.windows(2).map(|w| w[1].wrapping_sub(w[0])).collect()
}

fn dod_residuals(values: &[i64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(values.len().saturating_sub(1));
    let mut prev = values[0];
    let mut prev_d = 0i64;
    for &v in &values[1..] {
        let d = v.wrapping_sub(prev);
        out.push(d.wrapping_sub(prev_d));
        prev_d = d;
        prev = v;
    }
    out
}

fn integrate_delta(first: i64, residuals: &[i64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(residuals.len() + 1);
    out.push(first);
    let mut prev = first;
    for &d in residuals {
        prev = prev.wrapping_add(d);
        out.push(prev);
    }
    out
}

fn integrate_dod(first: i64, residuals: &[i64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(residuals.len() + 1);
    out.push(first);
    let mut prev = first;
    let mut prev_d = 0i64;
    for &dod in residuals {
        let d = prev_d.wrapping_add(dod);
        prev = prev.wrapping_add(d);
        out.push(prev);
        prev_d = d;
    }
    out
}

fn encode_i64_blocks_into(buf: &mut Vec<u8>, values: &[i64]) {
    push_u32(buf, as_u32_len(values.len()));
    let mut off = 0;
    while off < values.len() {
        let end = (off + MINI_BLOCK).min(values.len());
        write_i64_block(buf, &values[off..end]);
        off = end;
    }
}

fn decode_i64_blocks_rest(r: &mut R<'_>, expect: usize) -> Result<Vec<i64>> {
    let count = r.u32()? as usize;
    if count != expect {
        return Err(Error::corrupt("mini-block residual count mismatch"));
    }
    let mut out = Vec::with_capacity(count);
    while out.len() < count {
        let n = r.u16()? as usize;
        if n == 0 || n > MINI_BLOCK || out.len() + n > count {
            return Err(Error::corrupt("mini-block length is invalid"));
        }
        let bit_width = r.u8()?;
        let min = r.i64()?;
        if bit_width > 64 {
            return Err(Error::corrupt("mini-block bit width > 64"));
        }
        if bit_width == 0 {
            out.extend(std::iter::repeat_n(min, n));
            continue;
        }
        if bit_width == 64 {
            for _ in 0..n {
                let stored = r.u64()? as i128;
                out.push((i128::from(min) + stored) as i64);
            }
            continue;
        }
        let nbytes = (n * bit_width as usize).div_ceil(8);
        let packed = r.take(nbytes)?;
        let mut tmp = [0u64; MINI_BLOCK];
        unpack_msb(packed, u32::from(bit_width), n, &mut tmp)?;
        for stored in tmp.into_iter().take(n) {
            out.push((i128::from(min) + stored as i128) as i64);
        }
    }
    Ok(out)
}

fn write_i64_block(buf: &mut Vec<u8>, block: &[i64]) {
    let min = block.iter().copied().min().expect("empty mini-block");
    let max = block.iter().copied().max().expect("empty mini-block");
    let span = i128::from(max) - i128::from(min);
    let bit_width: u8 = if span == 0 {
        0
    } else {
        (128 - (span as u128).leading_zeros()) as u8
    };
    push_u16(buf, block.len() as u16);
    buf.push(bit_width);
    push_i64(buf, min);
    pack_offsets(
        buf,
        bit_width,
        block
            .iter()
            .map(|&v| (i128::from(v) - i128::from(min)) as u64),
    );
}

pub fn encode_for_u64(values: &[u64]) -> Vec<u8> {
    let mut buf = Vec::new();
    push_u32(&mut buf, as_u32_len(values.len()));
    let mut off = 0;
    while off < values.len() {
        let end = (off + MINI_BLOCK).min(values.len());
        write_u64_block(&mut buf, &values[off..end]);
        off = end;
    }
    buf
}

pub fn decode_for_u64(data: &[u8], n: usize) -> Result<Vec<u64>> {
    let mut r = R::new(data);
    let count = r.u32()? as usize;
    if count != n {
        return Err(Error::corrupt("for count does not match row count"));
    }
    let mut out = Vec::with_capacity(count);
    while out.len() < count {
        let len = r.u16()? as usize;
        if len == 0 || len > MINI_BLOCK || out.len() + len > count {
            return Err(Error::corrupt("for block length is invalid"));
        }
        let bit_width = r.u8()?;
        let min = r.u64()?;
        if bit_width > 64 {
            return Err(Error::corrupt("for bit width > 64"));
        }
        if bit_width == 0 {
            out.extend(std::iter::repeat_n(min, len));
            continue;
        }
        if bit_width == 64 {
            for _ in 0..len {
                let stored = r.u64()?;
                out.push(min.wrapping_add(stored));
            }
            continue;
        }
        let nbytes = (len * bit_width as usize).div_ceil(8);
        let packed = r.take(nbytes)?;
        let mut tmp = [0u64; MINI_BLOCK];
        unpack_msb(packed, u32::from(bit_width), len, &mut tmp)?;
        for stored in tmp.into_iter().take(len) {
            out.push(min.wrapping_add(stored));
        }
    }
    if r.remaining() != 0 {
        return Err(Error::corrupt("trailing bytes after for column"));
    }
    Ok(out)
}

fn write_u64_block(buf: &mut Vec<u8>, block: &[u64]) {
    let min = block.iter().copied().min().expect("empty mini-block");
    let max = block.iter().copied().max().expect("empty mini-block");
    let span = max - min;
    let bit_width: u8 = if span == 0 {
        0
    } else {
        (64 - span.leading_zeros()) as u8
    };
    push_u16(buf, block.len() as u16);
    buf.push(bit_width);
    push_u64(buf, min);
    pack_offsets(buf, bit_width, block.iter().map(|&v| v - min));
}

fn pack_offsets(buf: &mut Vec<u8>, bit_width: u8, stored: impl Iterator<Item = u64>) {
    if bit_width == 0 {
        return;
    }
    if bit_width == 64 {
        for v in stored {
            push_u64(buf, v);
        }
        return;
    }
    let mut w = BitWriter::new();
    for v in stored {
        w.write_bits(v, u32::from(bit_width));
    }
    buf.extend_from_slice(&w.finish());
}

pub fn zigzag_encode(n: i64) -> u64 {
    ((n.wrapping_shl(1)) ^ (n >> 63)) as u64
}

pub fn zigzag_decode(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}

pub fn encode_delta_varint(values: &[i64]) -> Vec<u8> {
    let mut buf = Vec::new();
    push_u32(&mut buf, as_u32_len(values.len()));
    if values.is_empty() {
        return buf;
    }
    push_i64(&mut buf, values[0]);
    let mut prev = values[0];
    for &v in &values[1..] {
        let d = v.wrapping_sub(prev);
        push_leb128(&mut buf, zigzag_encode(d));
        prev = v;
    }
    buf
}

pub fn decode_delta_varint(data: &[u8], n: usize) -> Result<Vec<i64>> {
    let mut r = R::new(data);
    let count = r.u32()? as usize;
    if count != n {
        return Err(Error::corrupt("delta-varint count mismatch"));
    }
    if n == 0 {
        if r.remaining() != 0 {
            return Err(Error::corrupt("trailing bytes after empty delta-varint"));
        }
        return Ok(Vec::new());
    }
    let mut prev = r.i64()?;
    let mut out = Vec::with_capacity(n);
    out.push(prev);
    for _ in 1..n {
        let d = zigzag_decode(read_leb128(&mut r)?);
        prev = prev.wrapping_add(d);
        out.push(prev);
    }
    if r.remaining() != 0 {
        return Err(Error::corrupt("trailing bytes after delta-varint"));
    }
    Ok(out)
}

pub fn encode_varint_u64(values: &[u64]) -> Vec<u8> {
    let mut buf = Vec::new();
    for &v in values {
        push_leb128(&mut buf, v);
    }
    buf
}

pub fn decode_varint_u64(data: &[u8], n: usize) -> Result<Vec<u64>> {
    let mut r = R::new(data);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(read_leb128(&mut r)?);
    }
    if r.remaining() != 0 {
        return Err(Error::corrupt("trailing bytes after varint column"));
    }
    Ok(out)
}

fn push_leb128(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut b = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            b |= 0x80;
            buf.push(b);
        } else {
            buf.push(b);
            break;
        }
    }
}

fn read_leb128(r: &mut R<'_>) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        if shift > 63 {
            return Err(Error::corrupt("leb128 overflow"));
        }
        let b = r.u8()?;
        let piece = u64::from(b & 0x7f);
        if piece > (u64::MAX >> shift) {
            return Err(Error::corrupt("leb128 overflow"));
        }
        result |= piece << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
}

/// Dictionary of `u64` values plus bit-packed ids in first-seen order.
pub fn encode_dict_u64(values: &[u64]) -> Vec<u8> {
    use std::collections::HashMap;
    let mut dict = Vec::new();
    let mut index: HashMap<u64, u32> = HashMap::with_capacity(values.len().min(4096));
    let mut ids = Vec::with_capacity(values.len());
    for &v in values {
        let id = if let Some(&id) = index.get(&v) {
            id
        } else {
            let id = dict.len() as u32;
            dict.push(v);
            index.insert(v, id);
            id
        };
        ids.push(id);
    }
    let bits: u8 = if dict.len() <= 1 {
        0
    } else {
        (32 - (dict.len() as u32 - 1).leading_zeros()) as u8
    };
    let mut buf = Vec::new();
    push_u32(&mut buf, as_u32_len(dict.len()));
    for v in dict {
        push_u64(&mut buf, v);
    }
    buf.push(bits);
    if bits > 0 {
        let mut w = BitWriter::new();
        for id in ids {
            w.write_bits(u64::from(id), u32::from(bits));
        }
        buf.extend_from_slice(&w.finish());
    }
    buf
}

pub fn decode_dict_u64(data: &[u8], n: usize) -> Result<Vec<u64>> {
    let mut r = R::new(data);
    let nunique = r.u32()? as usize;
    let mut dict = Vec::with_capacity(nunique);
    for _ in 0..nunique {
        dict.push(r.u64()?);
    }
    let bits = r.u8()?;
    if n == 0 {
        if nunique != 0 || r.remaining() != 0 {
            return Err(Error::corrupt("empty dictionary column is not empty"));
        }
        return Ok(Vec::new());
    }
    if dict.is_empty() {
        return Err(Error::corrupt("dictionary is empty"));
    }
    if bits == 0 {
        if dict.len() != 1 {
            return Err(Error::corrupt("zero-width dictionary is not a singleton"));
        }
        if r.remaining() != 0 {
            return Err(Error::corrupt("trailing bytes after constant dictionary"));
        }
        return Ok(vec![dict[0]; n]);
    }
    if bits > 32 {
        return Err(Error::corrupt("dictionary id width > 32"));
    }
    let nbytes = (n * bits as usize).div_ceil(8);
    let packed = r.take(nbytes)?;
    if r.remaining() != 0 {
        return Err(Error::corrupt("trailing bytes after dictionary"));
    }
    let mut br = BitReader::new(packed);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let id = br.read_bits(u32::from(bits))? as usize;
        let v = *dict
            .get(id)
            .ok_or_else(|| Error::corrupt("dictionary id out of range"))?;
        out.push(v);
    }
    Ok(out)
}

pub fn encode_rle_u64(values: &[u64]) -> Vec<u8> {
    let mut buf = Vec::new();
    if values.is_empty() {
        push_u32(&mut buf, 0);
        return buf;
    }
    let mut runs: Vec<(u64, u32)> = Vec::new();
    let mut cur = values[0];
    let mut len = 1u32;
    for &v in &values[1..] {
        if v == cur && len < u32::MAX {
            len += 1;
        } else {
            runs.push((cur, len));
            cur = v;
            len = 1;
        }
    }
    runs.push((cur, len));
    push_u32(&mut buf, as_u32_len(runs.len()));
    for (v, len) in runs {
        push_u64(&mut buf, v);
        push_u32(&mut buf, len);
    }
    buf
}

pub fn decode_rle_u64(data: &[u8], n: usize) -> Result<Vec<u64>> {
    let mut r = R::new(data);
    let nruns = r.u32()? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..nruns {
        let v = r.u64()?;
        let len = r.u32()? as usize;
        if out.len() + len > n {
            return Err(Error::corrupt("rle run overruns the column"));
        }
        out.extend(std::iter::repeat_n(v, len));
    }
    if out.len() != n {
        return Err(Error::corrupt("rle does not cover the column"));
    }
    if r.remaining() != 0 {
        return Err(Error::corrupt("trailing bytes after rle"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::gorilla::{decode_gorilla_i64, encode_gorilla_i64};
    use crate::codec::{
        decode_i64, decode_u64, encode_i64, encode_u64, CODEC_DELTA, CODEC_DELTA_VARINT,
        CODEC_DICT, CODEC_DOD, CODEC_FOR, CODEC_RAW_I64, CODEC_RAW_U64, CODEC_RLE, CODEC_VARINT,
    };
    use crate::rng::Rng;

    fn samples_i64() -> Vec<Vec<i64>> {
        let mut rng = Rng::new(11);
        let mut walk = Vec::with_capacity(1000);
        let mut p = 0i64;
        for _ in 0..1000 {
            p += (rng.next_u64() % 5) as i64 - 2;
            walk.push(p);
        }
        let mut wide = Vec::new();
        let mut t = 0i64;
        for i in 0..300 {
            t += (i * 17 % 50) as i64 * 1_000_000;
            wide.push(t);
        }
        vec![
            vec![],
            vec![0],
            vec![42, 42, 42, 42],
            vec![i64::MIN, i64::MAX, i64::MIN, -1, 0, 1],
            (0..129).map(|i| i * 3 - 10).collect(),
            (0..128).map(|_| 7).collect(),
            (0..255).map(|i| (i as i64) * (i as i64)).collect(),
            walk,
            wide,
        ]
    }

    #[test]
    fn every_i64_codec_roundtrips() {
        for values in samples_i64() {
            let encoded = [
                (CODEC_RAW_I64, encode_raw_i64(&values)),
                (CODEC_DELTA, encode_delta(&values)),
                (CODEC_DOD, encode_dod(&values)),
                (CODEC_DELTA_VARINT, encode_delta_varint(&values)),
            ];
            for (codec, bytes) in encoded {
                let got = decode_i64(codec, &bytes, values.len()).unwrap();
                assert_eq!(got, values, "codec {codec}");
            }
            let g = encode_gorilla_i64(&values);
            assert_eq!(decode_gorilla_i64(&g, values.len()).unwrap(), values);
            let (id, bytes) = encode_i64(&values);
            assert_eq!(decode_i64(id, &bytes, values.len()).unwrap(), values);
        }
    }

    #[test]
    fn every_u64_codec_roundtrips() {
        let mut rng = Rng::new(19);
        let mut samples: Vec<Vec<u64>> = vec![
            vec![],
            vec![0],
            vec![1, 1, 1, 1, 1],
            vec![0, 1, 2, 3, 2, 2, 2, 9],
            (0..129).map(|i| (i % 7) as u64 * 100).collect(),
            vec![u64::MAX, 0, u64::MAX - 1, 1],
        ];
        let mut random = Vec::new();
        for _ in 0..400 {
            random.push(rng.next_u64() % 10_000);
        }
        samples.push(random);
        for values in samples {
            for (codec, bytes) in [
                (CODEC_RAW_U64, encode_raw_u64(&values)),
                (CODEC_FOR, encode_for_u64(&values)),
                (CODEC_VARINT, encode_varint_u64(&values)),
                (CODEC_DICT, encode_dict_u64(&values)),
                (CODEC_RLE, encode_rle_u64(&values)),
            ] {
                let got = decode_u64(codec, &bytes, values.len()).unwrap();
                assert_eq!(got, values, "codec {codec} n {}", values.len());
            }
            let (id, bytes) = encode_u64(&values);
            assert_eq!(decode_u64(id, &bytes, values.len()).unwrap(), values);
        }
    }

    #[test]
    fn zigzag_extremes() {
        for v in [0, 1, -1, 63, -64, i64::MAX, i64::MIN, 123456789] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }
    }

    #[test]
    fn price_walk_beats_raw() {
        let mut p = 1_850_000i64;
        let mut v = Vec::with_capacity(4096);
        for i in 0..4096 {
            p += (i % 5) as i64 - 2;
            v.push(p);
        }
        let (_, bytes) = encode_i64(&v);
        assert!(
            bytes.len() * 4 < v.len() * 8,
            "encoded {} vs raw {}",
            bytes.len(),
            v.len() * 8
        );
    }
}
