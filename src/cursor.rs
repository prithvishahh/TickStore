use crate::{Error, Result};

/// Little-endian cursor over a column payload.
pub(crate) struct R<'a> {
    data: &'a [u8],
    i: usize,
}

impl<'a> R<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data, i: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.i)
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let end = self
            .i
            .checked_add(N)
            .ok_or_else(|| Error::corrupt("cursor overflow"))?;
        if end > self.data.len() {
            return Err(Error::corrupt("unexpected end of column"));
        }
        let mut a = [0u8; N];
        a.copy_from_slice(&self.data[self.i..end]);
        self.i = end;
        Ok(a)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.take_array::<1>()?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take_array()?))
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take_array()?))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take_array()?))
    }

    pub(crate) fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.take_array()?))
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .i
            .checked_add(n)
            .ok_or_else(|| Error::corrupt("cursor overflow"))?;
        if end > self.data.len() {
            return Err(Error::corrupt("unexpected end of column"));
        }
        let s = &self.data[self.i..end];
        self.i = end;
        Ok(s)
    }
}

pub(crate) fn push_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn push_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn push_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn push_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn as_u32_len(n: usize) -> u32 {
    u32::try_from(n).expect("column longer than u32::MAX")
}
