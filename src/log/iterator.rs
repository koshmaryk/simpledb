use anyhow::Result;
use std::sync::{Arc, Mutex};

use crate::file::block_id::BlockId;
use crate::file::manager::FileManager;
use crate::file::page::Page;

#[derive(Debug)]
pub struct LogIterator {
    file_manager: Arc<Mutex<FileManager>>,
    block: BlockId,
    page: Page,
    current_pos: usize,
    boundary: usize,
}

impl LogIterator {
    pub fn new(file_manager: Arc<Mutex<FileManager>>, block: &BlockId) -> Result<Self> {
        let block_size = file_manager.lock().unwrap().block_size();
        let mut iterator = Self {
            file_manager: Arc::clone(&file_manager),
            block: block.clone(),
            page: Page::from_bytes(vec![0; block_size]),
            current_pos: 0,
            boundary: 0,
        };

        iterator.move_to_block(block)?;
        Ok(iterator)
    }

    pub fn has_next(&self) -> bool {
        self.current_pos < self.file_manager.lock().unwrap().block_size()
            || self.block.block_number() > 0
    }

    fn move_to_block(&mut self, block: &BlockId) -> Result<()> {
        self.file_manager
            .lock()
            .unwrap()
            .read(block, &mut self.page)?;
        self.boundary = self.page.get_int(0)? as usize;
        self.current_pos = self.boundary;
        Ok(())
    }
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos == self.file_manager.lock().unwrap().block_size() {
            self.block = BlockId::new(self.block.filename(), self.block.block_number() - 1);
            self.move_to_block(&self.block.clone()).ok();
        }

        self.page
            .get_bytes(self.current_pos)
            .inspect(|rec| self.current_pos += std::mem::size_of::<i32>() + rec.len())
            .ok()
    }
}
