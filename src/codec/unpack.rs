//! Fast MSB-first bit unpacker.
//!
//! The general [`super::BitReader`] is correct for Gorilla's mixed widths, but
//! a mini-block has one width for every value. This routine keeps a 128-bit
//! window and, on x86_64 with AVX2, widens byte-aligned blocks in vector
//! registers. Other widths and other CPUs use the scalar window.

use crate::{Error, Result};

/// Unpack `n` MSB-first integers of `width` bits into `out`.
///
/// `width == 0` writes zeros. The bit order matches [`super::BitWriter`].
pub fn unpack_msb(data: &[u8], width: u32, n: usize, out: &mut [u64]) -> Result<()> {
    if out.len() < n {
        return Err(Error::corrupt("unpack output is short"));
    }
    if n == 0 {
        return Ok(());
    }
    if width > 64 {
        return Err(Error::corrupt("bit width above 64"));
    }
    if width == 0 {
        out[..n].fill(0);
        return Ok(());
    }
    let need = (n * width as usize).div_ceil(8);
    if data.len() < need {
        return Err(Error::corrupt("bit-packed block is truncated"));
    }
    if width == 8 || width == 16 || width == 32 || width == 64 {
        #[cfg(target_arch = "x86_64")]
        {
            if std::is_x86_feature_detected!("avx2") {
                unsafe { unpack_aligned_avx2(data, width, n, out) };
                return Ok(());
            }
        }
        unpack_aligned_scalar(data, width, n, out);
        return Ok(());
    }
    unpack_scalar(data, width, n, out);
    Ok(())
}

fn unpack_aligned_scalar(data: &[u8], width: u32, n: usize, out: &mut [u64]) {
    match width {
        8 => {
            for i in 0..n {
                out[i] = u64::from(data[i]);
            }
        }
        16 => {
            for i in 0..n {
                out[i] = u64::from(u16::from_be_bytes(
                    data[i * 2..i * 2 + 2].try_into().unwrap(),
                ));
            }
        }
        32 => {
            for i in 0..n {
                out[i] = u64::from(u32::from_be_bytes(
                    data[i * 4..i * 4 + 4].try_into().unwrap(),
                ));
            }
        }
        64 => {
            for i in 0..n {
                out[i] = u64::from_be_bytes(data[i * 8..i * 8 + 8].try_into().unwrap());
            }
        }
        _ => unreachable!("aligned width"),
    }
}

/// Scalar window. Bits already in `acc` sit at the top, matching `BitReader`.
///
/// Whole 8-byte chunks are shifted in whenever the window has room. `width`
/// never exceeds 64, so the window is refilled at most once per value.
fn unpack_scalar(data: &[u8], width: u32, n: usize, out: &mut [u64]) {
    let mask = if width == 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    };
    let mut acc: u128 = 0;
    let mut nbits: u32 = 0;
    let mut idx = 0usize;
    for slot in out.iter_mut().take(n) {
        if nbits < width {
            if idx + 8 <= data.len() {
                let word = u128::from(u64::from_be_bytes(data[idx..idx + 8].try_into().unwrap()));
                acc |= word << (64 - nbits);
                nbits += 64;
                idx += 8;
            } else {
                while nbits < width {
                    let b = u128::from(data[idx]);
                    idx += 1;
                    acc |= b << (120 - nbits);
                    nbits += 8;
                }
            }
        }
        let shift = 128 - width;
        *slot = ((acc >> shift) as u64) & mask;
        acc <<= width;
        nbits -= width;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn unpack_aligned_avx2(data: &[u8], width: u32, n: usize, out: &mut [u64]) {
    use std::arch::x86_64::{
        _mm256_cvtepu16_epi64, _mm256_cvtepu32_epi64, _mm256_cvtepu8_epi64, _mm256_storeu_si256,
        _mm_loadu_si128, _mm_storeu_si128,
    };
    // Each vector load reads 16 bytes. Stop before that would run off the block.
    let mut i = 0;
    match width {
        8 => {
            while i + 8 <= n && i + 16 <= data.len() {
                let lo = _mm256_cvtepu8_epi64(_mm_loadu_si128(data[i..].as_ptr().cast()));
                let hi = _mm256_cvtepu8_epi64(_mm_loadu_si128(data[i + 4..].as_ptr().cast()));
                _mm256_storeu_si256(out[i..].as_mut_ptr().cast(), lo);
                _mm256_storeu_si256(out[i + 4..].as_mut_ptr().cast(), hi);
                i += 8;
            }
        }
        16 => {
            while i + 4 <= n && i * 2 + 16 <= data.len() {
                let swapped = swap_u16x8(_mm_loadu_si128(data[i * 2..].as_ptr().cast()));
                let lo = _mm256_cvtepu16_epi64(swapped);
                _mm256_storeu_si256(out[i..].as_mut_ptr().cast(), lo);
                i += 4;
            }
        }
        32 => {
            while i + 4 <= n && i * 4 + 16 <= data.len() {
                let swapped = swap_u32x4(_mm_loadu_si128(data[i * 4..].as_ptr().cast()));
                let wide = _mm256_cvtepu32_epi64(swapped);
                _mm256_storeu_si256(out[i..].as_mut_ptr().cast(), wide);
                i += 4;
            }
        }
        64 => {
            while i + 2 <= n && i * 8 + 16 <= data.len() {
                let swapped = swap_u64x2(_mm_loadu_si128(data[i * 8..].as_ptr().cast()));
                _mm_storeu_si128(out[i..].as_mut_ptr().cast(), swapped);
                i += 2;
            }
        }
        _ => {}
    }
    if i < n {
        unpack_aligned_scalar(&data[byte_off(width, i)..], width, n - i, &mut out[i..]);
    }
}

#[cfg(target_arch = "x86_64")]
fn byte_off(width: u32, i: usize) -> usize {
    i * (width as usize / 8)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn swap_u16x8(v: std::arch::x86_64::__m128i) -> std::arch::x86_64::__m128i {
    use std::arch::x86_64::{_mm_setr_epi8, _mm_shuffle_epi8};
    let mask = _mm_setr_epi8(1, 0, 3, 2, 5, 4, 7, 6, 9, 8, 11, 10, 13, 12, 15, 14);
    _mm_shuffle_epi8(v, mask)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn swap_u32x4(v: std::arch::x86_64::__m128i) -> std::arch::x86_64::__m128i {
    use std::arch::x86_64::{_mm_setr_epi8, _mm_shuffle_epi8};
    let mask = _mm_setr_epi8(3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12);
    _mm_shuffle_epi8(v, mask)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn swap_u64x2(v: std::arch::x86_64::__m128i) -> std::arch::x86_64::__m128i {
    use std::arch::x86_64::{_mm_setr_epi8, _mm_shuffle_epi8};
    let mask = _mm_setr_epi8(7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8);
    _mm_shuffle_epi8(v, mask)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{BitReader, BitWriter};
    use crate::rng::Rng;

    #[test]
    fn matches_bit_reader() {
        let mut rng = Rng::new(42);
        for width in [1u32, 3, 7, 8, 9, 12, 16, 24, 32, 40, 63, 64] {
            for n in [1usize, 7, 128, 130, 1000] {
                let mut w = BitWriter::new();
                let mut expect = Vec::with_capacity(n);
                for _ in 0..n {
                    let v = if width == 64 {
                        rng.next_u64()
                    } else {
                        rng.next_u64() & ((1u64 << width) - 1)
                    };
                    w.write_bits(v, width);
                    expect.push(v);
                }
                let bytes = w.finish();
                let mut got = vec![0u64; n];
                unpack_msb(&bytes, width, n, &mut got).unwrap();
                assert_eq!(got, expect, "width {width} n {n}");
                let mut r = BitReader::new(&bytes);
                for v in &expect {
                    assert_eq!(r.read_bits(width).unwrap(), *v);
                }
            }
        }
    }
}
