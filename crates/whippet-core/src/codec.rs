// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Fixed-layout little-endian wire codec.
//!
//! Whippet payloads are deliberately boring: little-endian integers, fixed
//! widths, no varints, no floats. A payload can be decoded without allocation
//! (except the `Vec` a caller asks for), and the byte layout is stable enough
//! to compare across releases.

use crate::error::{CoreError, Result};

#[derive(Default, Debug)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
        }
    }

    pub fn u8(&mut self, value: u8) -> &mut Self {
        self.buf.push(value);
        self
    }

    pub fn u16(&mut self, value: u16) -> &mut Self {
        self.buf.extend_from_slice(&value.to_le_bytes());
        self
    }

    pub fn u32(&mut self, value: u32) -> &mut Self {
        self.buf.extend_from_slice(&value.to_le_bytes());
        self
    }

    pub fn u64(&mut self, value: u64) -> &mut Self {
        self.buf.extend_from_slice(&value.to_le_bytes());
        self
    }

    pub fn i64(&mut self, value: i64) -> &mut Self {
        self.buf.extend_from_slice(&value.to_le_bytes());
        self
    }

    pub fn u128(&mut self, value: u128) -> &mut Self {
        self.buf.extend_from_slice(&value.to_le_bytes());
        self
    }

    pub fn raw(&mut self, value: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(value);
        self
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn finish(self) -> Vec<u8> {
        self.buf
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }
}

#[derive(Debug, Clone)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    pub fn ensure(&self, need: usize) -> Result<()> {
        if self.remaining() < need {
            return Err(CoreError::BufferTooShort {
                need,
                have: self.remaining(),
            });
        }
        Ok(())
    }

    pub fn u8(&mut self) -> Result<u8> {
        self.ensure(1)?;
        let value = self.buf[self.pos];
        self.pos += 1;
        Ok(value)
    }

    pub fn u16(&mut self) -> Result<u16> {
        self.ensure(2)?;
        let value = u16::from_le_bytes(self.buf[self.pos..self.pos + 2].try_into().unwrap());
        self.pos += 2;
        Ok(value)
    }

    pub fn u32(&mut self) -> Result<u32> {
        self.ensure(4)?;
        let value = u32::from_le_bytes(self.buf[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(value)
    }

    pub fn u64(&mut self) -> Result<u64> {
        self.ensure(8)?;
        let value = u64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(value)
    }

    pub fn i64(&mut self) -> Result<i64> {
        self.ensure(8)?;
        let value = i64::from_le_bytes(self.buf[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(value)
    }

    pub fn u128(&mut self) -> Result<u128> {
        self.ensure(16)?;
        let value = u128::from_le_bytes(self.buf[self.pos..self.pos + 16].try_into().unwrap());
        self.pos += 16;
        Ok(value)
    }

    pub fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        self.ensure(len)?;
        let value = &self.buf[self.pos..self.pos + len];
        self.pos += len;
        Ok(value)
    }

    pub fn finish(&self) -> Result<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(CoreError::InvalidValue(format!(
                "{} trailing bytes",
                self.remaining()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_all_widths() {
        let mut writer = Writer::new();
        writer
            .u8(0xab)
            .u16(0x1234)
            .u32(0xdead_beef)
            .u64(0x0102_0304_0506_0708)
            .i64(-42)
            .u128(0x1111_2222_3333_4444_5555_6666_7777_8888)
            .raw(b"hello");

        let bytes = writer.finish();
        let mut reader = Reader::new(&bytes);
        assert_eq!(reader.u8().unwrap(), 0xab);
        assert_eq!(reader.u16().unwrap(), 0x1234);
        assert_eq!(reader.u32().unwrap(), 0xdead_beef);
        assert_eq!(reader.u64().unwrap(), 0x0102_0304_0506_0708);
        assert_eq!(reader.i64().unwrap(), -42);
        assert_eq!(
            reader.u128().unwrap(),
            0x1111_2222_3333_4444_5555_6666_7777_8888
        );
        assert_eq!(reader.bytes(5).unwrap(), b"hello");
        reader.finish().unwrap();
    }

    #[test]
    fn short_buffer_is_an_error() {
        let mut reader = Reader::new(&[1, 2]);
        assert!(reader.u32().is_err());
    }

    #[test]
    fn trailing_bytes_are_an_error() {
        let reader = Reader::new(&[0]);
        assert!(reader.finish().is_err());
    }
}
