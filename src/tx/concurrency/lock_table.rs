use anyhow::{Ok, Result};
use core::fmt;
use std::{
    collections::HashMap,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use crate::file::block_id::BlockId;

const MAX_TIME: u128 = 10_000; // 10 seconds

#[derive(Debug)]
pub struct LockAbortError;

impl std::error::Error for LockAbortError {}
impl fmt::Display for LockAbortError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LockAbortError => write!(f, "lock abort"),
        }
    }
}

#[derive(Debug)]
enum Lock {
    Exclusive,
    Shared(usize),
}

#[derive(Debug, Clone)]
pub struct LockTable {
    state: Arc<(Mutex<HashMap<BlockId, Lock>>, Condvar)>,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            state: Arc::new((Mutex::new(HashMap::new()), Condvar::new())),
        }
    }

    /// If an xlock exists when the method is called, then the calling thread will be placed on a wait list until the lock is released.
    /// If the thread remains on the wait list for a certain amount of time, then an exception is thrown.
    pub fn slock(&self, block: &BlockId) -> Result<()> {
        let (lock, cvar) = &*self.state;
        let mut locks = lock.lock().unwrap();

        loop {
            match locks.get(&block) {
                Some(Lock::Exclusive) => {
                    let (new_locks, timeout) = cvar
                        .wait_timeout(locks, Duration::from_millis(MAX_TIME as u64))
                        .map_err(|_| LockAbortError)?;

                    locks = new_locks;

                    if timeout.timed_out() {
                        return Err(LockAbortError.into());
                    }
                }
                Some(Lock::Shared(count)) => {
                    let new_count = *count + 1;
                    locks.insert(block.clone(), Lock::Shared(new_count));
                    return Ok(());
                }
                None => {
                    locks.insert(block.clone(), Lock::Shared(1));
                    return Ok(());
                }
            }
        }
    }

    /// If a lock of any type exists when the method is called, then the calling thread will be placed on a wait list until the locks are released.
    /// If the thread remains on the wait list for a certain amount of time, then an exception is thrown.
    pub fn xlock(&self, block: &BlockId) -> Result<()> {
        let (lock, cvar) = &*self.state;
        let mut locks = lock.lock().unwrap();

        loop {
            match locks.get(&block) {
                Some(Lock::Shared(count)) if *count > 1 => {
                    let (new_locks, timeout) = cvar
                        .wait_timeout(locks, Duration::from_millis(MAX_TIME as u64))
                        .map_err(|_| LockAbortError)?;

                    locks = new_locks;

                    if timeout.timed_out() {
                        return Err(LockAbortError.into());
                    }
                }
                _ => {
                    locks.insert(block.clone(), Lock::Exclusive);
                    return Ok(());
                }
            }
        }
    }

    // If this lock is the last lock on that block, then the waiting transactions are notified.
    pub fn unlock(&self, block: &BlockId) -> Result<()> {
        let (lock, cvar) = &*self.state;
        let mut locks = lock.lock().unwrap();

        match locks.get(block) {
            Some(Lock::Shared(count)) if *count > 1 => {
                let new_count = *count - 1;
                locks.insert(block.clone(), Lock::Shared(new_count));
            }
            _ => {
                locks.remove(block);
                cvar.notify_all();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::MutexGuard};

    use crate::{
        file::block_id::BlockId,
        tx::concurrency::lock_table::{Lock, LockTable},
    };

    #[test]
    fn test_slocks() {
        let lock_table = LockTable::new();
        let block = BlockId::new("test.tbl", 1);

        assert!(lock_table.slock(&block).is_ok());
        assert!(lock_table.slock(&block).is_ok());
        assert!(matches!(
            get_locks(&lock_table).get(&block),
            Some(Lock::Shared(2))
        ));

        assert!(lock_table.unlock(&block).is_ok());
        assert!(matches!(
            get_locks(&lock_table).get(&block),
            Some(Lock::Shared(1))
        ));

        assert!(lock_table.unlock(&block).is_ok());
        assert!(get_locks(&lock_table).get(&block).is_none());
    }

    #[test]
    fn test_xlock() {
        let lock_table = LockTable::new();
        let block = BlockId::new("test.tbl", 1);

        assert!(lock_table.xlock(&block).is_ok());
        assert!(matches!(
            get_locks(&lock_table).get(&block),
            Some(Lock::Exclusive)
        ));

        assert!(lock_table.unlock(&block).is_ok());
        assert!(get_locks(&lock_table).get(&block).is_none());
    }

    #[test]
    fn test_xlock_timeout() {
        let lock_table = LockTable::new();
        let block = BlockId::new("test.tbl", 1);

        assert!(lock_table.slock(&block).is_ok());
        assert!(lock_table.slock(&block).is_ok());

        assert!(lock_table.xlock(&block).is_err());
    }

    #[test]
    fn test_slock_timeout() {
        let lock_table = LockTable::new();
        let block = BlockId::new("test.tbl", 1);

        assert!(lock_table.xlock(&block).is_ok());

        assert!(lock_table.slock(&block).is_err());
    }

    fn get_locks(lock_table: &LockTable) -> MutexGuard<HashMap<BlockId, Lock>> {
        let (lock, _) = &*lock_table.state;
        lock.lock().unwrap()
    }
}
