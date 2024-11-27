use core::fmt;
use std::{
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use anyhow::{Error, Ok, Result};
use option_ext::OptionExt;

use crate::{
    file::{block_id::BlockId, manager::FileManager},
    log::manager::LogManager,
};

use super::buffer::Buffer;

const MAX_TIME: u128 = 10_000; // 10 seconds

#[derive(Debug)]
enum BufferManagerError {
    BufferAbort,
}

impl std::error::Error for BufferManagerError {}
impl fmt::Display for BufferManagerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BufferManagerError::BufferAbort => write!(f, "buffer abort"),
        }
    }
}

#[derive(Debug)]
pub struct BufferPoolState {
    pub buffer_pool: Vec<Buffer>,
    num_available: usize,
}

#[derive(Debug)]
pub struct BufferManager {
    pub state: Arc<(Mutex<BufferPoolState>, Condvar)>,
}

impl BufferManager {
    pub fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        num_buffers: usize,
    ) -> Self {
        let buffers = (0..num_buffers)
            .map(|_| Buffer::new(Arc::clone(&file_manager), Arc::clone(&log_manager)))
            .collect();

        let state = BufferPoolState {
            buffer_pool: buffers,
            num_available: num_buffers,
        };

        Self {
            state: Arc::new((Mutex::new(state), Condvar::new())),
        }
    }

    pub fn available(&self) -> usize {
        let (lock, _) = &*self.state;
        let state = lock.lock().unwrap();
        state.num_available
    }

    pub fn unpin(&self, idx: usize) -> Result<()> {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();

        state.buffer_pool[idx].unpin();
        if !state.buffer_pool[idx].is_pinned() {
            state.num_available += 1;
            cvar.notify_one();
        }
        Ok(())
    }

    pub fn pin(&self, block: &BlockId) -> Result<usize> {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();

        loop {
            if let Some(idx) = self.try_to_pin(block, &mut state) {
                return Ok(idx);
            }

            let (new_state, timeout) = cvar
                .wait_timeout(state, Duration::from_millis(MAX_TIME as u64))
                .map_err(|_| BufferManagerError::BufferAbort)?;

            state = new_state;

            if timeout.timed_out() {
                return Err(Error::new(BufferManagerError::BufferAbort));
            }
        }
    }

    pub fn flush_all(&self, txnum: i32) -> Result<()> {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();

        state
            .buffer_pool
            .iter_mut()
            .filter(|buf| buf.txnum == txnum)
            .try_for_each(|buf| buf.flush())
            .map_or_else(
                |err| {
                    cvar.notify_all();
                    Err(err)
                },
                |ok| {
                    cvar.notify_all();
                    Ok(ok)
                },
            )
    }

    fn try_to_pin<'a>(&self, block: &'a BlockId, state: &'a mut BufferPoolState) -> Option<usize> {
        if let Some(idx) = self.find_existing_buffer(block, state) {
            if !state.buffer_pool[idx].is_pinned() {
                state.num_available -= 1;
            }
            state.buffer_pool[idx].pin();
            return Some(idx);
        }

        if let Some(idx) = self.find_unpinned_buffer(state) {
            state.buffer_pool[idx].assign_to_block(block).unwrap();
            state.num_available -= 1;
            state.buffer_pool[idx].pin();
            return Some(idx);
        }

        None
    }

    fn find_existing_buffer<'a>(
        &self,
        block: &'a BlockId,
        state: &'a BufferPoolState,
    ) -> Option<usize> {
        state
            .buffer_pool
            .iter()
            .enumerate()
            .find(|(_, buffer)| buffer.block().contains(block))
            .map(|(idx, _)| idx)
    }

    fn find_unpinned_buffer(&self, state: &BufferPoolState) -> Option<usize> {
        state
            .buffer_pool
            .iter()
            .enumerate()
            .find(|(_, buffer)| !buffer.is_pinned())
            .map(|(idx, _)| idx)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use tempfile::tempdir;

    use crate::{
        buffer::manager::BufferManager,
        file::{block_id::BlockId, manager::FileManager},
        log::manager::LogManager,
    };

    #[test]
    fn test_buffer_manager() {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().to_str().unwrap();
        let test_file = temp_dir
            .path()
            .join("simpledb.log")
            .to_str()
            .unwrap()
            .to_string();

        let block_size = 400;
        let num_buffers = 3;

        let file_manager = Arc::new(Mutex::new(FileManager::new(db_dir, block_size).unwrap()));
        let log_manager = Arc::new(Mutex::new(
            LogManager::new(Arc::clone(&file_manager), &test_file).unwrap(),
        ));
        let buffer_manager = BufferManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            num_buffers,
        );

        let mut buffers = vec![0usize; 6];
        buffers[0] = buffer_manager.pin(&BlockId::new("testfile", 0)).unwrap();
        buffers[1] = buffer_manager.pin(&BlockId::new("testfile", 1)).unwrap();
        buffers[2] = buffer_manager.pin(&BlockId::new("testfile", 2)).unwrap();

        buffer_manager.unpin(buffers[1]).unwrap();
        buffers[1] = 42;

        buffers[3] = buffer_manager.pin(&BlockId::new("testfile", 0)).unwrap(); // block 0 pinned twice
        buffers[4] = buffer_manager.pin(&BlockId::new("testfile", 1)).unwrap(); // block 1 repinned
        assert_eq!(0, buffer_manager.available());

        assert!(buffer_manager.pin(&BlockId::new("testfile", 3)).is_err()); // will not work; no buffers left

        buffer_manager.unpin(buffers[2]).unwrap();
        buffers[2] = 42;

        buffers[5] = buffer_manager.pin(&BlockId::new("testfile", 3)).unwrap(); // now this works

        let expected = HashMap::from([
            (0, BlockId::new("testfile", 0)),
            (3, BlockId::new("testfile", 0)),
            (4, BlockId::new("testfile", 1)),
            (5, BlockId::new("testfile", 3)),
        ]);

        {
            let (lock, _) = &*buffer_manager.state;
            let mut state = lock.lock().unwrap();

            for (i, &idx) in buffers.iter().enumerate() {
                if idx != 42 {
                    let x: Vec<Option<BlockId>> = state
                        .buffer_pool
                        .iter_mut()
                        .map(|buf| buf.block().clone())
                        .collect();
                    println!("{:?}", x);

                    let actual = state.buffer_pool[idx].block().as_ref();
                    println!("i: {}, idx: {}", i, idx);
                    println!("buff[{}] pinned to block {:?}", i, actual.unwrap());
                    assert_eq!(expected.get(&i).unwrap(), actual.unwrap());
                } else {
                    assert!(i == 1 || i == 2);
                }
            }
        }
    }
}
