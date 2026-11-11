use crate::{Error, Result};

/// MSB-first bit packer. Bits are flushed as big-endian bytes so the first
/// bit written lands in the high bit of the first byte, matching Gorilla.
#[derive(Clone, Debug)]
pub struct BitWriter {
    data: Vec<u8>,
    acc: u64,
    nbits: u8,
}

impl BitWriter {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            acc: 0,
            nbits: 0,
        }
    }

    pub fn write_bit(&mut self, bit: bool) {
        self.write_bits(u64::from(bit), 1);
    }

    /// Write the low `n` bits of `value`, high bit first. `n` is in `0..=64`.
    pub fn write_bits(&mut self, mut value: u64, mut n: u32) {
        debug_assert!(n <= 64);
        if n == 0 {
            return;
        }
        if n < 64 {
            value &= (1u64 << n) - 1;
        }
        while n > 0 {
            let space = 64 - u32::from(self.nbits);
            if n <= space {
                let shift = space - n;
                self.acc |= value << shift;
                self.nbits += n as u8;
                n = 0;
                if self.nbits == 64 {
                    self.flush_full();
                }
            } else {
                let shift = n - space;
                self.acc |= value >> shift;
                self.flush_full();
                let mask = (1u64 << shift) - 1;
                value &= mask;
                n = shift;
            }
        }
    }

    fn flush_full(&mut self) {
        self.data.extend_from_slice(&self.acc.to_be_bytes());
        self.acc = 0;
        self.nbits = 0;
    }

    pub fn finish(mut self) -> Vec<u8> {
        if self.nbits > 0 {
            let nbytes = usize::from(self.nbits).div_ceil(8);
            let bytes = self.acc.to_be_bytes();
            self.data.extend_from_slice(&bytes[..nbytes]);
            self.nbits = 0;
            self.acc = 0;
        }
        self.data
    }
}

impl Default for BitWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// MSB-first reader. A `u128` window means a read of 64 bits never has to
/// span a refill that the accumulator cannot hold.
pub struct BitReader<'a> {
    data: &'a [u8],
    idx: usize,
    acc: u128,
    nbits: u32,
}

impl<'a> BitReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            idx: 0,
            acc: 0,
            nbits: 0,
        }
    }

    fn fill(&mut self) {
        while self.nbits <= 120 && self.idx < self.data.len() {
            let b = u128::from(self.data[self.idx]);
            self.idx += 1;
            let shift = 120 - self.nbits;
            self.acc |= b << shift;
            self.nbits += 8;
        }
    }

    pub fn read_bits(&mut self, n: u32) -> Result<u64> {
        if n == 0 {
            return Ok(0);
        }
        if n > 64 {
            return Err(Error::corrupt("bit read wider than 64"));
        }
        if self.nbits < n {
            self.fill();
        }
        if self.nbits < n {
            return Err(Error::corrupt("unexpected end of bit stream"));
        }
        let shift = 128 - n;
        let val = (self.acc >> shift) as u64;
        self.acc <<= n;
        self.nbits -= n;
        Ok(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::Rng;

    #[test]
    fn mixed_widths_roundtrip() {
        let mut rng = Rng::new(99);
        let mut values = Vec::new();
        let mut w = BitWriter::new();
        for i in 0..500 {
            let n = (i % 64) + 1;
            let v = if n == 64 {
                rng.next_u64()
            } else {
                rng.next_u64() & ((1u64 << n) - 1)
            };
            w.write_bits(v, n as u32);
            values.push((v, n as u32));
        }
        let data = w.finish();
        let mut r = BitReader::new(&data);
        for (v, n) in values {
            assert_eq!(r.read_bits(n).unwrap(), v, "width {n}");
        }
    }

    #[test]
    fn single_bit() {
        let mut w = BitWriter::new();
        w.write_bit(true);
        w.write_bit(false);
        w.write_bit(true);
        let data = w.finish();
        assert_eq!(data, vec![0b1010_0000]);
        let mut r = BitReader::new(&data);
        assert_eq!(r.read_bits(1).unwrap(), 1);
        assert_eq!(r.read_bits(1).unwrap(), 0);
        assert_eq!(r.read_bits(1).unwrap(), 1);
    }

    #[test]
    fn eof_is_an_error() {
        let mut r = BitReader::new(&[0xFF]);
        assert!(r.read_bits(9).is_err());
    }
}
