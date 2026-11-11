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

pub use bitstream::{BitReader, BitWriter};
pub use gorilla::{decode_gorilla_i64, decode_xor_f64, encode_gorilla_i64, encode_xor_f64};
pub use integer::{
    decode_delta, decode_delta_varint, decode_dict_u64, decode_dod, decode_for_u64, decode_raw_i64,
    decode_raw_u64, decode_rle_u64, decode_varint_u64, encode_delta, encode_delta_varint,
    encode_dict_u64, encode_dod, encode_for_u64, encode_raw_i64, encode_raw_u64, encode_rle_u64,
    encode_varint_u64, MINI_BLOCK,
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
        _ => "unknown",
    }
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
    select(
        vec![
            (CODEC_RAW_I64, encode_raw_i64(values)),
            (CODEC_DELTA, encode_delta(values)),
            (CODEC_DOD, encode_dod(values)),
            (CODEC_DELTA_VARINT, encode_delta_varint(values)),
            (CODEC_GORILLA, encode_gorilla_i64(values)),
        ],
        SPEED_I64,
    )
}

pub fn encode_u64(values: &[u64]) -> (u8, Vec<u8>) {
    select(
        vec![
            (CODEC_RAW_U64, encode_raw_u64(values)),
            (CODEC_FOR, encode_for_u64(values)),
            (CODEC_VARINT, encode_varint_u64(values)),
            (CODEC_DICT, encode_dict_u64(values)),
            (CODEC_RLE, encode_rle_u64(values)),
        ],
        SPEED_U64,
    )
}

pub fn decode_i64(codec: u8, data: &[u8], n: usize) -> Result<Vec<i64>> {
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

fn select(mut cands: Vec<(u8, Vec<u8>)>, speed: &[u8]) -> (u8, Vec<u8>) {
    let min_len = cands.iter().map(|(_, b)| b.len()).min().unwrap_or(0);
    let limit = ((min_len as f64) * (1.0 + SIZE_SLACK)).ceil() as usize;
    let rank = |id: u8| speed.iter().position(|&x| x == id).unwrap_or(usize::MAX);
    cands.retain(|(_, b)| b.len() <= limit);
    cands.sort_by_key(|(id, bytes)| (rank(*id), bytes.len(), *id));
    cands.into_iter().next().expect("at least one codec")
}
