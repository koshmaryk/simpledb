#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BlockId {
    filename: String,
    block_number: u64,
}

impl BlockId {
    pub fn new(filename: String, block_number: u64) -> Self {
        Self {
            filename,
            block_number,
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn block_number(&self) -> u64 {
        self.block_number
    }
}
