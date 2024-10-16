use anyhow::{Ok, Result};
use bytebuffer::ByteBuffer;
use std::mem;

#[derive(Debug)]
pub struct Page {
    buf: ByteBuffer,
}

impl Page {
    pub fn new(blocksize: usize) -> Self {
        Self {
            buf: ByteBuffer::from_vec(vec![0; blocksize]),
        }
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Page {
        Self {
            buf: ByteBuffer::from_vec(bytes),
        }
    }

    pub fn get_int(&mut self, offset: usize) -> Result<i32> {
        self.buf.set_rpos(offset);
        Ok(self.buf.read_i32()?)
    }

    pub fn set_int(&mut self, offset: usize, n: i32) -> Result<()> {
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
        self.buf.set_wpos(offset);
        self.buf.write_string(s);
        Ok(())
    }

    pub fn max_length(strlen: usize) -> usize {
        mem::size_of::<i32>() + (strlen * mem::size_of::<u8>())
    }

    // a package private method, needed by FileManager
    pub(crate) fn contents(&mut self) -> &mut ByteBuffer {
        self.buf.set_rpos(0);
        &mut self.buf
    }
}
