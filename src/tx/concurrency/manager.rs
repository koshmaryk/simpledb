use anyhow::{Ok, Result};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::file::block_id::BlockId;

use super::lock_table::LockTable;

#[derive(Debug)]
enum Lock {
    Exclusive,
    Shared,
}

#[derive(Debug)]
pub struct ConcurrencyManager {
    lock_table: Arc<Mutex<LockTable>>,
    locks: HashMap<BlockId, Lock>,
}

/// Each transaction has its own concurrency manager.
/// The concurrency manager keeps track of which locks the transaction currently has, and interacts with the global lock table as needed.
/// The concurrency manager uses locking to guarantee that schedules are serializable.
/// In particular, it requires all transactions to follow the lock protocol, which states:
/// - Before reading a block, acquire a shared lock on it.
/// - Before modifying a block, acquire an exclusive lock on it.
/// - Release all locks after commit or rollback.
impl ConcurrencyManager {
    pub fn new(lock_table: Arc<Mutex<LockTable>>) -> Self {
        Self {
            lock_table: Arc::clone(&lock_table),
            locks: HashMap::new(),
        }
    }

    pub fn slock(&mut self, block: &BlockId) -> Result<()> {
        match self.locks.get(block) {
            Some(Lock::Exclusive) => Ok(()),
            Some(Lock::Shared) => Ok(()),
            _ => {
                self.lock_table.lock().unwrap().slock(block)?;
                self.locks.insert(block.clone(), Lock::Shared);
                Ok(())
            }
        }
    }

    pub fn xlock(&mut self, block: &BlockId) -> Result<()> {
        match self.locks.get(block) {
            Some(Lock::Exclusive) => Ok(()),
            _ => {
                self.slock(block)?;
                self.lock_table.lock().unwrap().xlock(block)?;
                self.locks.insert(block.clone(), Lock::Exclusive);
                Ok(())
            }
        }
    }

    pub fn release(&mut self) -> Result<()> {
        for block in self.locks.keys() {
            self.lock_table.lock().unwrap().unlock(block)?;
        }
        self.locks.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{
        file::block_id::BlockId,
        tx::concurrency::{
            lock_table::LockTable,
            manager::{ConcurrencyManager, Lock},
        },
    };

    #[test]
    fn test_slock_idempotent() {
        let lock_table = Arc::new(Mutex::new(LockTable::new()));
        let mut manager = ConcurrencyManager::new(Arc::clone(&lock_table));
        let block = BlockId::new("test.tbl", 0);

        assert!(manager.slock(&block).is_ok());
        assert!(manager.slock(&block).is_ok());
        assert!(matches!(manager.locks.get(&block), Some(Lock::Shared)));
    }

    #[test]
    fn test_xlock_idempotent() {
        let lock_table = Arc::new(Mutex::new(LockTable::new()));
        let mut manager = ConcurrencyManager::new(Arc::clone(&lock_table));
        let block = BlockId::new("test.tbl", 0);

        assert!(manager.xlock(&block).is_ok());
        assert!(manager.xlock(&block).is_ok());
        assert!(matches!(manager.locks.get(&block), Some(Lock::Exclusive)));
    }

    #[test]
    fn test_xlock_after_slock() {
        let lock_table = Arc::new(Mutex::new(LockTable::new()));
        let mut manager = ConcurrencyManager::new(Arc::clone(&lock_table));
        let block = BlockId::new("test.tbl", 0);

        assert!(manager.slock(&block).is_ok());
        assert!(manager.xlock(&block).is_ok());
        assert!(matches!(manager.locks.get(&block), Some(Lock::Exclusive)));
    }

    #[test]
    fn test_slock_after_xlock() {
        let lock_table = Arc::new(Mutex::new(LockTable::new()));
        let mut manager = ConcurrencyManager::new(Arc::clone(&lock_table));
        let block = BlockId::new("test.tbl", 0);

        assert!(manager.xlock(&block).is_ok());
        assert!(manager.slock(&block).is_ok());
        assert!(matches!(manager.locks.get(&block), Some(Lock::Exclusive))); // lock should remain exclusive
    }

    #[test]
    fn test_xlock_succeeds_on_unlocked_block() {
        let lock_table = Arc::new(Mutex::new(LockTable::new()));
        let mut manager1 = ConcurrencyManager::new(Arc::clone(&lock_table));
        let mut manager2 = ConcurrencyManager::new(Arc::clone(&lock_table));
        let block = BlockId::new("test.tbl", 0);

        assert!(manager1.slock(&block).is_ok());
        assert!(matches!(manager1.locks.get(&block), Some(Lock::Shared)));

        assert!(manager1.release().is_ok());

        assert!(manager2.xlock(&block).is_ok());
        assert!(matches!(manager2.locks.get(&block), Some(Lock::Exclusive)));
    }

    #[test]
    fn test_xlock_fails_on_locked_block() {
        let lock_table = Arc::new(Mutex::new(LockTable::new()));
        let mut manager1 = ConcurrencyManager::new(Arc::clone(&lock_table));
        let mut manager2 = ConcurrencyManager::new(Arc::clone(&lock_table));
        let block = BlockId::new("test.tbl", 0);

        assert!(manager1.slock(&block).is_ok());
        assert!(matches!(manager1.locks.get(&block), Some(Lock::Shared)));

        assert!(manager2.xlock(&block).is_err());
    }
}
