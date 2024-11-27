use anyhow::Result;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{buffer::manager::BufferManager, file::block_id::BlockId};

#[derive(Debug)]
pub struct BufferList {
    buffer_manager: Arc<Mutex<BufferManager>>,
    buffers: HashMap<BlockId, usize>,
    pins: Vec<BlockId>,
}

/// Manages the transaction's currently-pinned buffers.
impl BufferList {
    pub fn new(buffer_manager: Arc<Mutex<BufferManager>>) -> Self {
        Self {
            buffer_manager,
            buffers: HashMap::new(),
            pins: Vec::new(),
        }
    }

    pub fn get_buffer_idx(&self, block: &BlockId) -> Option<usize> {
        self.buffers.get(block).copied()
    }

    pub fn pin(&mut self, block: &BlockId) -> Result<()> {
        let buffer = self.buffer_manager.lock().unwrap().pin(block)?;

        self.buffers.insert(block.clone(), buffer);
        self.pins.push(block.clone());
        Ok(())
    }

    pub fn unpin(&mut self, block: &BlockId) -> Result<()> {
        if let Some(idx) = self.buffers.get(block) {
            self.buffer_manager.lock().unwrap().unpin(*idx)?;

            self.pins.retain(|e| e != block);

            if !self.pins.contains(block) {
                self.buffers.remove(block);
            }
        }
        Ok(())
    }

    pub fn unpin_all(&mut self) -> Result<()> {
        for block in &self.pins {
            if let Some(idx) = self.buffers.get(block) {
                self.buffer_manager.lock().unwrap().unpin(*idx)?
            }
        }

        self.buffers.clear();
        self.pins.clear();
        Ok(())
    }
}
