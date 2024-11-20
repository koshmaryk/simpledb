use anyhow::{Ok, Result};
use std::sync::{Arc, Mutex};

use crate::{
    file::{block_id::BlockId, manager::FileManager, page::Page},
    log::manager::LogManager,
    Lsn,
};

#[derive(Debug, Clone)]
pub struct Buffer {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    pub contents: Page,
    pub block: Option<BlockId>,
    pins: u32,
    pub txnum: Lsn,
    lsn: Lsn,
}

impl Buffer {
    pub fn new(file_manager: Arc<Mutex<FileManager>>, log_manager: Arc<Mutex<LogManager>>) -> Self {
        let block_size = file_manager.lock().unwrap().block_size();
        Self {
            file_manager,
            log_manager,
            contents: Page::new(block_size),
            block: None,
            pins: 0,
            txnum: -1,
            lsn: -1,
        }
    }

    pub fn set_modified(&mut self, txnum: i64, lsn: Lsn) -> Result<()> {
        self.txnum = txnum;
        if lsn >= 0 {
            self.lsn = lsn;
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.txnum >= 0 {
            self.log_manager.lock().unwrap().flush(self.lsn)?;
            if let Some(block) = &self.block {
                self.file_manager
                    .lock()
                    .unwrap()
                    .write(block, &mut self.contents)?;
            }
            self.txnum = -1;
        }
        Ok(())
    }

    pub fn assign_to_block(&mut self, block: &BlockId) -> Result<()> {
        self.flush()?;
        self.file_manager
            .lock()
            .unwrap()
            .read(block, &mut self.contents)?;
        self.block = Some(block.clone());
        self.pins = 0;
        Ok(())
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn pin(&mut self) {
        self.pins += 1;
    }

    pub fn unpin(&mut self) {
        self.pins -= 1;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use crate::{
        buffer::manager::BufferManager,
        file::{block_id::BlockId, manager::FileManager},
        log::manager::LogManager,
    };

    #[test]
    fn test_buffer() {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().to_str().unwrap();
        let test_log_file = temp_dir
            .path()
            .join("simpledb.log")
            .to_str()
            .unwrap()
            .to_string();
        
        let block_size = 400;
        let num_buffers = 3; // only 3 buffers

        let file_manager = Arc::new(Mutex::new(
            FileManager::new(db_dir, block_size).unwrap(),
        ));
        let log_manager = Arc::new(Mutex::new(
            LogManager::new(Arc::clone(&file_manager), &test_log_file).unwrap(),
        ));
        let buffer_manager = BufferManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            num_buffers,
        );

        // Test pinning and modifying first buffer
        let idx1 = buffer_manager.pin(&BlockId::new("testfile", 1)).unwrap();

        {
            let (lock, _) = &*buffer_manager.state;
            let mut state = lock.lock().unwrap();

            let n = state.buffer_pool[idx1].contents.get_int(80).unwrap();
            state.buffer_pool[idx1].contents.set_int(80, n + 1).unwrap();
            state.buffer_pool[idx1].set_modified(1, 0).unwrap();// placeholder values
            assert_eq!(1, n + 1);
        }

        buffer_manager.unpin(idx1).unwrap();

        // One of these pins will flush buff1 to disk:
        let mut idx2 = buffer_manager.pin(&BlockId::new("testfile", 2)).unwrap();
        let idx3 = buffer_manager.pin(&BlockId::new("testfile", 3)).unwrap();
        let idx4 = buffer_manager.pin(&BlockId::new("testfile", 4)).unwrap();

        buffer_manager.unpin(idx2).unwrap();

        // Try to pin block 1 again
        idx2 = buffer_manager.pin(&BlockId::new("testfile", 1)).unwrap();

        {
            let (lock, _) = &*buffer_manager.state;
            let mut state = lock.lock().unwrap();

            state.buffer_pool[idx2].contents.set_int(80, 9999).unwrap();
            state.buffer_pool[idx2].set_modified(1, 0).unwrap(); // This modification won't get written to disk
        }

        // Cleanup
        buffer_manager.unpin(idx3).unwrap();
        buffer_manager.unpin(idx4).unwrap();
        buffer_manager.unpin(idx2).unwrap();
    }
}
