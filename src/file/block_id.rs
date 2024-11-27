use core::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockId {
    filename: String,
    block_number: usize,
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[file {}, block {}]", self.filename, self.block_number)
    }
}

impl BlockId {
    pub fn new(filename: &str, block_number: usize) -> Self {
        Self {
            filename: filename.to_string(),
            block_number,
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn block_number(&self) -> usize {
        self.block_number
    }
}
