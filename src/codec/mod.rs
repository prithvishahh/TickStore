//! Per-column encodings and the chunk-level selector.
//!
//! The selector encodes a column with every applicable codec, keeps those
//! within 5% of the smallest, and then picks the one with the fastest
//! expected decode. The band is deliberate: a codec that only saves a few
//! percent is not worth a slower scan, and a codec that is merely *close* to
//! raw should lose to raw.

mod bitstream;
mod gorilla;
mod integer;
mod unpack;

pub use bitstream::{BitReader, BitWriter};
pub use gorilla::{decode_gorilla_i64, decode_xor_f64, encode_gorilla_i64, encode_xor_f64};
pub use integer::{
    decode_delta, decode_delta_varint, decode_dict_u64, decode_dod, decode_for_u64, decode_raw_i64,
    decode_raw_u64, decode_rle_u64, decode_varint_u64, encode_delta, encode_delta_varint,
    encode_dict_u64, encode_dod, encode_for_u64, encode_raw_i64, encode_raw_u64, encode_rle_u64,
    encode_varint_u64, notional_from_delta, MINI_BLOCK,
};

use crate::{Error, Result};

pub const CODEC_GORILLA: u8 = 1;
pub const CODEC_DELTA: u8 = 2;
pub const CODEC_DOD: u8 = 3;
pub const CODEC_FOR: u8 = 4;
pub const CODEC_XOR: u8 = 5;
pub const CODEC_VARINT: u8 = 6;
pub const CODEC_DICT: u8 = 7;
pub const CODEC_RAW_I64: u8 = 8;
pub const CODEC_RAW_U64: u8 = 9;
pub const CODEC_DELTA_VARINT: u8 = 10;
pub const CODEC_RLE: u8 = 11;

/// How much larger than the best codec a faster one may be and still win.
pub const SIZE_SLACK: f64 = 0.05;

pub fn codec_name(id: u8) -> &'static str {
    match id {
        CODEC_GORILLA => "gorilla_dod",
        CODEC_DELTA => "delta_bitpack",
        CODEC_DOD => "dod_bitpack",
        CODEC_FOR => "for_bitpack",
        CODEC_XOR => "gorilla_xor",
        CODEC_VARINT => "varint",
        CODEC_DICT => "dictionary",
        CODEC_RAW_I64 => "raw_i64",
        CODEC_RAW_U64 => "raw_u64",
        CODEC_DELTA_VARINT => "delta_varint",
        CODEC_RLE => "rle",
        other if other & CODEC_ZSTD != 0 => "zstd",
        _ => "unknown",
    }
}

pub fn codec_label(id: u8) -> String {
    let (inner, zstd) = split_codec(id);
    if zstd {
        format!("{}+zstd", codec_name(inner))
    } else {
        codec_name(inner).to_string()
    }
}

/// Outer zstd frame. The low 7 bits are the inner codec.
pub const CODEC_ZSTD: u8 = 0x80;

pub fn split_codec(id: u8) -> (u8, bool) {
    (id & !CODEC_ZSTD, id & CODEC_ZSTD != 0)
}

/// Lower is faster to decode. Used only inside the 5% size band.
const SPEED_I64: &[u8] = &[
    CODEC_RAW_I64,
    CODEC_DELTA,
    CODEC_DOD,
    CODEC_DELTA_VARINT,
    CODEC_GORILLA,
];

const SPEED_U64: &[u8] = &[
    CODEC_RAW_U64,
    CODEC_FOR,
    CODEC_DICT,
    CODEC_RLE,
    CODEC_VARINT,
];

pub fn encode_i64(values: &[i64]) -> (u8, Vec<u8>) {
    encode_i64_with(values, true)
}

/// Like [`encode_i64`], but never wraps the winner in zstd.
///
/// Trade timestamps and prices are scanned by VWAP and OHLCV. An outer frame
/// there is smaller and slower than DuckDB, so those columns stay bare. The
/// same selector still runs over the integer codecs.
pub fn encode_i64_scan(values: &[i64]) -> (u8, Vec<u8>) {
    encode_i64_with(values, false)
}

fn encode_i64_with(values: &[i64], outer: bool) -> (u8, Vec<u8>) {
    let mut cands = vec![
        (CODEC_RAW_I64, encode_raw_i64(values)),
        (CODEC_DELTA, encode_delta(values)),
        (CODEC_DOD, encode_dod(values)),
        (CODEC_DELTA_VARINT, encode_delta_varint(values)),
        (CODEC_GORILLA, encode_gorilla_i64(values)),
    ];
    if outer {
        push_zstd_candidates(&mut cands);
    }
    select(cands, SPEED_I64)
}

pub fn encode_u64(values: &[u64]) -> (u8, Vec<u8>) {
    encode_u64_with(values, true)
}

/// Integer codecs only. Used for trade size, which VWAP and OHLCV scan.
pub fn encode_u64_scan(values: &[u64]) -> (u8, Vec<u8>) {
    encode_u64_with(values, false)
}

fn encode_u64_with(values: &[u64], outer: bool) -> (u8, Vec<u8>) {
    let mut cands = vec![
        (CODEC_RAW_U64, encode_raw_u64(values)),
        (CODEC_FOR, encode_for_u64(values)),
        (CODEC_VARINT, encode_varint_u64(values)),
        (CODEC_DICT, encode_dict_u64(values)),
        (CODEC_RLE, encode_rle_u64(values)),
    ];
    if outer {
        push_zstd_candidates(&mut cands);
    }
    select(cands, SPEED_U64)
}

pub fn decode_i64(codec: u8, data: &[u8], n: usize) -> Result<Vec<i64>> {
    let (codec, owned) = unwrap_zstd(codec, data)?;
    let data = owned.as_deref().unwrap_or(data);
    match codec {
        CODEC_RAW_I64 => decode_raw_i64(data, n),
        CODEC_DELTA => decode_delta(data, n),
        CODEC_DOD => decode_dod(data, n),
        CODEC_DELTA_VARINT => decode_delta_varint(data, n),
        CODEC_GORILLA => decode_gorilla_i64(data, n),
        other => Err(Error::corrupt(format!(
            "{} is not an i64 codec",
            codec_name(other)
        ))),
    }
}

pub fn decode_u64(codec: u8, data: &[u8], n: usize) -> Result<Vec<u64>> {
    let (codec, owned) = unwrap_zstd(codec, data)?;
    let data = owned.as_deref().unwrap_or(data);
    match codec {
        CODEC_RAW_U64 => decode_raw_u64(data, n),
        CODEC_FOR => decode_for_u64(data, n),
        CODEC_VARINT => decode_varint_u64(data, n),
        CODEC_DICT => decode_dict_u64(data, n),
        CODEC_RLE => decode_rle_u64(data, n),
        other => Err(Error::corrupt(format!(
            "{} is not a u64 codec",
            codec_name(other)
        ))),
    }
}

/// Level 1 keeps decompression on the scan path cheap. A frame is kept only
/// when it is outside the selector's 5% band, so a tiny win does not replace
/// a faster bare codec.
fn push_zstd_candidates(cands: &mut Vec<(u8, Vec<u8>)>) {
    let base: Vec<(u8, Vec<u8>)> = cands.clone();
    for (id, bytes) in base {
        if bytes.len() < 128 {
            continue;
        }
        if let Ok(compressed) = zstd::bulk::compress(&bytes, 1) {
            if compressed.len() + 4 < bytes.len() {
                let mut framed = Vec::with_capacity(4 + compressed.len());
                framed.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                framed.extend_from_slice(&compressed);
                cands.push((id | CODEC_ZSTD, framed));
            }
        }
    }
}

pub(crate) fn unwrap_zstd(codec: u8, data: &[u8]) -> Result<(u8, Option<Vec<u8>>)> {
    let (inner, wrapped) = split_codec(codec);
    if !wrapped {
        return Ok((inner, None));
    }
    if data.len() < 4 {
        return Err(Error::corrupt("zstd frame is truncated"));
    }
    let raw_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let dec = zstd::bulk::decompress(&data[4..], raw_len.max(64))
        .map_err(|err| Error::corrupt(format!("zstd decode failed: {err}")))?;
    if dec.len() != raw_len {
        return Err(Error::corrupt("zstd frame length does not match"));
    }
    Ok((inner, Some(dec)))
}

fn select(mut cands: Vec<(u8, Vec<u8>)>, speed: &[u8]) -> (u8, Vec<u8>) {
    let min_len = cands.iter().map(|(_, b)| b.len()).min().unwrap_or(0);
    let limit = ((min_len as f64) * (1.0 + SIZE_SLACK)).ceil() as usize;
    let rank = |id: u8| speed.iter().position(|&x| x == id).unwrap_or(usize::MAX);
    cands.retain(|(_, b)| b.len() <= limit);
    cands.sort_by_key(|(id, bytes)| (rank(*id), bytes.len(), *id));
    cands.into_iter().next().expect("at least one codec")
}
