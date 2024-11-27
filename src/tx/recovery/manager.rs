use anyhow::{Ok, Result};
use core::fmt;
use std::sync::{Arc, Mutex};

use crate::{
    buffer::{buffer::Buffer, manager::BufferManager},
    log::manager::LogManager,
    tx::transaction::Transaction,
    Lsn,
};

use super::log_record::{
    create_log_record, CheckpointRecord, CommitRecord, LogOperation, RollbackRecord, SetIntRecord,
    SetStringRecord, StartRecord,
};

#[derive(Debug)]
enum RecoveryManagerError {
    RecoveryError,
}

impl std::error::Error for RecoveryManagerError {}
impl fmt::Display for RecoveryManagerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RecoveryManagerError::RecoveryError => write!(f, "recovery error"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecoveryManager {
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    txnum: i32,
}

/// Each transaction has its own recovery manager.
impl RecoveryManager {
    pub fn new(
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
        txnum: i32,
    ) -> Result<Self> {
        StartRecord::write_to_log(Arc::clone(&log_manager), txnum)?;
        Ok(Self {
            log_manager,
            buffer_manager,
            txnum,
        })
    }

    /// Write a commit record to the log, and flushes it to disk.
    pub fn commit(&self) -> Result<()> {
        self.buffer_manager.lock().unwrap().flush_all(self.txnum)?;
        let lsn = CommitRecord::write_to_log(Arc::clone(&self.log_manager), self.txnum)?;
        self.log_manager.lock().unwrap().flush(lsn)?;
        Ok(())
    }

    /// Write a rollback record to the log and flush it to disk.
    pub fn rollback(&self, tx: &mut Transaction) -> Result<()> {
        self.do_rollback(tx)?;
        self.buffer_manager.lock().unwrap().flush_all(self.txnum)?;
        let lsn = RollbackRecord::write_to_log(Arc::clone(&self.log_manager), self.txnum)?;
        self.log_manager.lock().unwrap().flush(lsn)?;
        Ok(())
    }

    /// Recover uncompleted transactions from the log and then write a quiescent checkpoint record to the log and flush it to disk.
    pub fn recover(&self, tx: &mut Transaction) -> Result<()> {
        self.do_recover(tx)?;
        self.buffer_manager.lock().unwrap().flush_all(self.txnum)?;
        let lsn = CheckpointRecord::write_to_log(Arc::clone(&self.log_manager))?;
        self.log_manager.lock().unwrap().flush(lsn)?;
        Ok(())
    }

    /// Write a setint record to the log, flushes it to disk and return its lsn.
    pub fn set_int(&self, buf: &mut Buffer, offset: usize, _: i32) -> Result<Lsn> {
        let old_val = buf.contents().get_int(offset)?;
        if let Some(block) = buf.block() {
            return SetIntRecord::write_to_log(
                Arc::clone(&self.log_manager),
                self.txnum,
                block,
                offset,
                old_val,
            );
        }
        Err(RecoveryManagerError::RecoveryError.into())
    }

    /// Write a setstring record to the log, flushes it to disk and return its lsn.
    pub fn set_string(&self, buf: &mut Buffer, offset: usize, _: &str) -> Result<Lsn> {
        let old_val = buf.contents().get_string(offset)?;
        if let Some(block) = buf.block() {
            return SetStringRecord::write_to_log(
                Arc::clone(&self.log_manager),
                self.txnum,
                block,
                offset,
                &old_val,
            );
        }
        Err(RecoveryManagerError::RecoveryError.into())
    }

    /// Rollback the transaction, by iterating through the log records until it finds the transaction's START record,
    /// calling undo() for each of the transaction's log records.
    fn do_rollback(&self, tx: &mut Transaction) -> Result<()> {
        let mut iter = self.log_manager.lock().unwrap().iterator()?;
        while iter.has_next() {
            if let Some(bytes) = iter.next() {
                let rec = create_log_record(bytes)?;
                if rec.tx_number() == self.txnum {
                    if rec.op() == LogOperation::Start {
                        return Ok(());
                    }

                    rec.undo(tx)?;
                }
            }
        }

        Ok(())
    }

    /// Do a complete database recovery. The method iterates through the log records.
    /// Whenever it finds a log record for an unfinished transaction, it calls undo() on that record.
    /// The method stops when it encounters a CHECKPOINT record or the end of the log.
    fn do_recover(&self, tx: &mut Transaction) -> Result<()> {
        let mut finished_txs = vec![];
        let mut iter = self.log_manager.lock().unwrap().iterator()?;
        while iter.has_next() {
            if let Some(bytes) = iter.next() {
                let rec = create_log_record(bytes)?;
                match rec.op() {
                    LogOperation::Checkpoint => return Ok(()),
                    LogOperation::Commit | LogOperation::Rollback => {
                        finished_txs.push(rec.tx_number())
                    }
                    _ => {
                        if !finished_txs.contains(&rec.tx_number()) {
                            rec.undo(tx)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
