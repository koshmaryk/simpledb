#[derive(Debug, PartialEq, Eq, Hash)]
pub struct BlockId {
    filename: String,
    block_number: usize,
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
