use anyhow::{bail, Result};
use bytebuffer::ByteBuffer;
use chrono::{Datelike, NaiveDate};
use core::fmt;

#[derive(Debug)]
enum PageError {
    BufferSizeExceeded,
}

impl std::error::Error for PageError {}

impl fmt::Display for PageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PageError::BufferSizeExceeded => write!(f, "buffer size exceeded"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Page {
    buf: ByteBuffer,
}

impl Page {
    pub fn new(block_size: usize) -> Self {
        Self {
            buf: ByteBuffer::from_vec(vec![0; block_size]),
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Page {
        Self {
            buf: ByteBuffer::from_vec(bytes),
        }
    }

    pub fn get_short(&mut self, offset: usize) -> Result<i16> {
        self.buf.set_rpos(offset);
        Ok(self.buf.read_i16()?)
    }

    pub fn set_short(&mut self, offset: usize, n: i16) -> Result<()> {
        if offset + std::mem::size_of::<i16>() > self.buf.len() {
            bail!(PageError::BufferSizeExceeded)
        }

        self.buf.set_wpos(offset);
        self.buf.write_i16(n);
        Ok(())
    }

    pub fn get_int(&mut self, offset: usize) -> Result<i32> {
        self.buf.set_rpos(offset);
        Ok(self.buf.read_i32()?)
    }

    pub fn set_int(&mut self, offset: usize, n: i32) -> Result<()> {
        if offset + std::mem::size_of::<i32>() > self.buf.len() {
            bail!(PageError::BufferSizeExceeded)
        }

        self.buf.set_wpos(offset);
        self.buf.write_i32(n);
        Ok(())
    }

    pub fn get_bytes(&mut self, offset: usize) -> Result<Vec<u8>> {
        self.buf.set_rpos(offset);
        let len = self.buf.read_i32()? as usize;
        Ok(self.buf.read_bytes(len)?)
    }

    pub fn set_bytes(&mut self, offset: usize, bytes: &[u8]) -> Result<()> {
        if offset + std::mem::size_of::<i32>() + bytes.len() > self.buf.len() {
            bail!(PageError::BufferSizeExceeded)
        }

        self.buf.set_wpos(offset);
        self.buf.write_i32(bytes.len() as i32);
        self.buf.write_bytes(bytes);
        Ok(())
    }

    pub fn get_string(&mut self, offset: usize) -> Result<String> {
        self.buf.set_rpos(offset);
        Ok(self.buf.read_string()?)
    }

    pub fn set_string(&mut self, offset: usize, s: &str) -> Result<()> {
        if offset + s.len() > self.buf.len() {
            bail!(PageError::BufferSizeExceeded)
        }

        self.buf.set_wpos(offset);
        self.buf.write_string(s);
        Ok(())
    }

    pub fn get_bool(&mut self, offset: usize) -> Result<bool> {
        self.buf.set_rpos(offset);
        Ok(self.buf.read_u8().map(|n| n != 0)?)
    }

    pub fn set_bool(&mut self, offset: usize, b: bool) -> Result<()> {
        if offset + std::mem::size_of::<u8>() > self.buf.len() {
            bail!(PageError::BufferSizeExceeded)
        }

        self.buf.set_wpos(offset);
        self.buf.write_u8(b as u8);
        Ok(())
    }

    pub fn get_date(&mut self, offset: usize) -> Result<NaiveDate> {
        self.buf.set_rpos(offset);
        Ok(self
            .buf
            .read_i32()
            .map(|days| NaiveDate::from_num_days_from_ce_opt(days).unwrap())?)
    }

    pub fn set_date(&mut self, offset: usize, date: NaiveDate) -> Result<()> {
        if offset + std::mem::size_of::<i32>() > self.buf.len() {
            bail!(PageError::BufferSizeExceeded)
        }

        self.buf.set_wpos(offset);
        self.buf.write_i32(date.num_days_from_ce());
        Ok(())
    }

    pub fn max_length(strlen: usize) -> usize {
        std::mem::size_of::<i32>() + (strlen * std::mem::size_of::<char>())
    }

    // a package private method, needed by FileManager
    pub(crate) fn contents(&mut self) -> &mut ByteBuffer {
        self.buf.set_rpos(0);
        &mut self.buf
    }
}
