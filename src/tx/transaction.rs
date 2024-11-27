use anyhow::{Ok, Result};
use core::fmt;
use std::sync::{atomic::{AtomicI32, Ordering}, Arc, Mutex};

use crate::{
    buffer::manager::BufferManager,
    file::{block_id::BlockId, manager::FileManager},
    log::manager::LogManager,
};

use super::{
    bufferlist::BufferList,
    concurrency::{lock_table::LockTable, manager::ConcurrencyManager},
    recovery::manager::RecoveryManager,
};

static NEXT_TX_NUM: AtomicI32 = AtomicI32::new(0);
static END_OF_FILE: i32 = -1;

#[derive(Debug)]
enum TransactionError {
    TransactionAbort,
}

impl std::error::Error for TransactionError {}
impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TransactionError::TransactionAbort => write!(f, "transaction abort"),
        }
    }
}

#[derive(Debug)]
pub struct Transaction {
    recovery_manager: RecoveryManager,
    concurrency_manager: ConcurrencyManager,
    buffer_manager: Arc<Mutex<BufferManager>>,
    file_manager: Arc<Mutex<FileManager>>,
    buffers: BufferList,
    txnum: i32,
}

/// Provides transaction management for clients, ensuring that all transactions are serializable, recoverable, and in general satisfy the ACID properties.
impl Transaction {
    pub fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
        lock_table: Arc<Mutex<LockTable>>,
    ) -> Result<Self> {
        let txnum = NEXT_TX_NUM.fetch_add(1, Ordering::SeqCst);
        let recovery_manager = RecoveryManager::new(
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            txnum,
        )?;
        let concurrency_manager = ConcurrencyManager::new(Arc::clone(&lock_table));
        let tx_buffers = BufferList::new(Arc::clone(&buffer_manager));

        Ok(Self {
            recovery_manager,
            concurrency_manager,
            buffer_manager,
            file_manager,
            buffers: tx_buffers,
            txnum,
        })
    }

    /// Commit the current transaction.
    /// Flush all modified buffers (and their log records), write and flush a commit record to the log, release all locks, and unpin any pinned buffers.
    pub fn commit(&mut self) -> Result<()> {
        self.recovery_manager.commit()?;
        self.concurrency_manager.release()?;
        self.buffers.unpin_all()?;
        Ok(())
    }

    /// Rollback the current transaction.
    /// Undo any modified values, flush those buffers, write and flush a rollback record to the log, release all locks, and unpin any pinned buffers.
    pub fn rollback(&mut self) -> Result<()> {
        let recovery_manager = self.recovery_manager.clone();
        recovery_manager.rollback(self)?; // think about redesigning to not need this
        self.concurrency_manager.release()?;
        self.buffers.unpin_all()?;
        Ok(())
    }

    /// Flush all modified buffers.
    /// Then go through the log, rolling back all uncommitted transactions.
    /// Finally, write a quiescent checkpoint record to the log.
    /// This method is called during system startup, before user transactions begin.
    pub fn recover(&mut self) -> Result<()> {
        self.buffer_manager.lock().unwrap().flush_all(self.txnum)?;
        let recovery_manager = self.recovery_manager.clone();
        recovery_manager.recover(self)?; // think about redesigning to not need this
        Ok(())
    }

    /// The transaction pin the specified block, and manages the buffer for the client.
    pub fn pin(&mut self, block: &BlockId) -> Result<()> {
        self.buffers.pin(block)?;
        Ok(())
    }

    /// The transaction looks up the buffer pinned to this block, and unpins it.
    pub fn unpin(&mut self, block: &BlockId) -> Result<()> {
        self.buffers.unpin(block)?;
        Ok(())
    }

    /// Return the integer value stored at the specified offset of the specified block.
    /// The method first obtains an slock on the block, then it calls the buffer to retrieve the value.
    pub fn get_int(&mut self, block: &BlockId, offset: usize) -> Result<i32> {
        self.concurrency_manager.slock(block)?;

        if let Some(idx) = self.buffers.get_buffer_idx(block) {
            let (lock, _) = &*self.buffer_manager.lock().unwrap().state;
            let mut state = lock.lock().unwrap();
            return Ok(state.buffer_pool[idx].contents().get_int(offset)?);
        }

        Err(TransactionError::TransactionAbort.into())
    }

    /// Return the string value stored at the specified offset of the specified block.
    /// The method first obtains an slock on the block, then it calls the buffer to retrieve the value.
    pub fn get_string(&mut self, block: &BlockId, offset: usize) -> Result<String> {
        self.concurrency_manager.slock(block)?;

        if let Some(idx) = self.buffers.get_buffer_idx(block) {
            let (lock, _) = &*self.buffer_manager.lock().unwrap().state;
            let mut state = lock.lock().unwrap();
            return Ok(state.buffer_pool[idx].contents().get_string(offset)?);
        }

        Err(TransactionError::TransactionAbort.into())
    }

    /// Store an integer at the specified offset of the specified block.
    /// The method first obtains an xlock on the block.
    /// It then reads the current value at that offset, puts it into an update log record, and writes that record to the log.
    /// Finally, it calls the buffer to store the value, passing in the LSN of the log record and the transaction's id.
    pub fn set_int(
        &mut self,
        block: &BlockId,
        offset: usize,
        val: i32,
        ok_to_log: bool,
    ) -> Result<()> {
        self.concurrency_manager.xlock(block)?;

        if let Some(idx) = self.buffers.get_buffer_idx(block) {
            let (lock, _) = &*self.buffer_manager.lock().unwrap().state;
            let mut state = lock.lock().unwrap();
            let mut lsn = -1;
            if ok_to_log {
                lsn = self.recovery_manager.set_int(&mut state.buffer_pool[idx], offset, val)?;
            }

            state.buffer_pool[idx].contents().set_int(offset, val)?;
            state.buffer_pool[idx].set_modified(self.txnum, lsn)?;
            return Ok(());
        }

        Err(TransactionError::TransactionAbort.into())
    }

    /// Store an string at the specified offset of the specified block.
    /// The method first obtains an xlock on the block.
    /// It then reads the current value at that offset, puts it into an update log record, and writes that record to the log.
    /// Finally, it calls the buffer to store the value, passing in the LSN of the log record and the transaction's id.
    pub fn set_string(
        &mut self,
        block: &BlockId,
        offset: usize,
        val: &str,
        ok_to_log: bool,
    ) -> Result<()> {
        self.concurrency_manager.xlock(block)?;

        if let Some(idx) = self.buffers.get_buffer_idx(block) {
            let (lock, _) = &*self.buffer_manager.lock().unwrap().state;
            let mut state = lock.lock().unwrap();
            let mut lsn = -1;
            if ok_to_log {
                lsn = self.recovery_manager.set_string(&mut state.buffer_pool[idx], offset, val)?;
            }

            state.buffer_pool[idx].contents().set_string(offset, val)?;
            state.buffer_pool[idx].set_modified(self.txnum, lsn)?;
            return Ok(());
        }

        Err(TransactionError::TransactionAbort.into())
    }

    /// Return the number of blocks in the specified file.
    /// This method first obtains an SLock on the "end of the file", before asking the file manager to return the file size.
    pub fn size(&mut self, filename: &str) -> Result<usize> {
        let dummy_block = BlockId::new(filename, END_OF_FILE as usize);
        self.concurrency_manager.slock(&dummy_block)?;
        Ok(self.file_manager.lock().unwrap().length(filename)?)
    }

    /// Append a new block to the end of the specified file and returns a reference to it.
    /// This method first obtains an xlock on the "end of the file", before performing the append.
    pub fn append(&mut self, filename: &str) -> Result<BlockId> {
        let dummy_block = BlockId::new(filename, END_OF_FILE as usize);
        self.concurrency_manager.xlock(&dummy_block)?;
        Ok(self.file_manager.lock().unwrap().append(filename)?)
    }

    pub fn block_size(&self) -> usize {
        self.file_manager.lock().unwrap().block_size()
    }

    pub fn available_buffs(&self) -> usize {
        self.buffer_manager.lock().unwrap().available()
    }
}

#[cfg(test)]
mod tests {

    use std::sync::{Arc, Mutex};

    use tempfile::tempdir;

    use crate::{
        buffer::manager::BufferManager, file::{block_id::BlockId, manager::FileManager}, log::manager::LogManager, tx::concurrency::lock_table::LockTable
    };

    use super::Transaction;

    #[test]
    fn test_transaction_lifecycle() {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().to_str().unwrap();
        let test_file = temp_dir
            .path()
            .join("simpledb.log")
            .to_str()
            .unwrap()
            .to_string();

        let block_size = 400;
        let num_buffers = 8;

        let file_manager = Arc::new(Mutex::new(FileManager::new(db_dir, block_size).unwrap()));
        let log_manager = Arc::new(Mutex::new(
            LogManager::new(Arc::clone(&file_manager), &test_file).unwrap(),
        ));
        let buffer_manager = Arc::new(Mutex::new(BufferManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            num_buffers,
        )));

        let lock_table = Arc::new(Mutex::new(LockTable::new()));

         // The block initially contains unknown bytes, so we don't log the initial values
        let block = BlockId::new("testfile", 1);

        // Transaction 1: Initialize the block's values
        let mut tx1 = Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            Arc::clone(&lock_table),
        ).unwrap();

        tx1.pin(&block).unwrap();
        tx1.set_int(&block, 80, 1, false).unwrap();
        tx1.set_string(&block, 40, "one", false).unwrap();
        tx1.commit().unwrap();

        // Transaction 2: Read initial values and modify them
        let mut tx2 = Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            Arc::clone(&lock_table),
        ).unwrap();

        tx2.pin(&block).unwrap();
            
        // Read and verify initial values
        let ival = tx2.get_int(&block, 80).unwrap();
        let sval = tx2.get_string(&block, 40).unwrap();
        assert_eq!(ival, 1, "Initial integer value should be 1");
        assert_eq!(sval, "one", "Initial string value should be 'one'");
        
        // Modify values - increment int and append "!" to string
        tx2.set_int(&block, 80, ival + 1, true).unwrap();
        tx2.set_string(&block, 40, &format!("{}!", sval), true).unwrap();
        tx2.commit().unwrap();

        // Transaction 3: Verify modifications and test rollback
        let mut tx3 = Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            Arc::clone(&lock_table),
        ).unwrap();

        tx3.pin(&block).unwrap();
            
        // Verify the modifications from tx2
        assert_eq!(tx3.get_int(&block, 80).unwrap(), 2, "Integer should be incremented to 2");
        assert_eq!(tx3.get_string(&block, 40).unwrap(), "one!", "String should have exclamation mark added");
        
        // Make a change that will be rolled back
        tx3.set_int(&block, 80, 9999, true).unwrap();
        assert_eq!(tx3.get_int(&block, 80).unwrap(), 9999, "Value should be 9999");
        
        // Rollback the transaction
        tx3.rollback().unwrap();

        // Transaction 4: Verify rollback was successful
        let mut tx4 = Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            Arc::clone(&lock_table),
        ).unwrap();

        tx4.pin(&block).unwrap();
            
        // Verify that the value is back to what it was before tx3
        assert_eq!(tx4.get_int(&block, 80).unwrap(), 2, "After rollback, integer should be back to 2");
        tx4.commit().unwrap();
    }
}