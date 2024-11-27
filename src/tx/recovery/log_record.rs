use core::fmt;
use std::sync::{Arc, Mutex};

use anyhow::{Ok, Result};
use num_enum::TryFromPrimitive;

use crate::{
    file::{block_id::BlockId, page::Page},
    log::manager::LogManager,
    tx::transaction::Transaction,
    Lsn,
};

#[derive(Debug)]
enum LogRecordError {
    UnkownLogOperation(i32),
}

impl std::error::Error for LogRecordError {}
impl fmt::Display for LogRecordError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LogRecordError::UnkownLogOperation(op) => write!(f, "unkown log operation: {}", op),
        }
    }
}

#[derive(Debug, PartialEq, TryFromPrimitive)]
#[repr(i32)]
pub enum LogOperation {
    Checkpoint = 0,
    Start = 1,
    Commit = 2,
    Rollback = 3,
    SetInt = 4,
    SetString = 5,
}

pub trait LogRecord {
    fn op(&self) -> LogOperation;

    fn tx_number(&self) -> i32;

    /// Undoes the operation encoded by this log record.
    /// The only log record types for which this method does anything interesting are SETINT and SETSTRING.
    fn undo(&self, tx: &mut Transaction) -> Result<()>;
}

pub fn create_log_record(bytes: Vec<u8>) -> Result<Box<dyn LogRecord>> {
    let mut p = Page::from_bytes(bytes);
    let value = p.get_int(0)?;
    match LogOperation::try_from(value)
        .map_err(|err| LogRecordError::UnkownLogOperation(err.number))?
    {
        LogOperation::Checkpoint => Ok(Box::new(CheckpointRecord::new()?)),
        LogOperation::Start => Ok(Box::new(StartRecord::new(&mut p)?)),
        LogOperation::Commit => Ok(Box::new(CommitRecord::new(&mut p)?)),
        LogOperation::Rollback => Ok(Box::new(RollbackRecord::new(&mut p)?)),
        LogOperation::SetInt => Ok(Box::new(SetIntRecord::new(&mut p)?)),
        LogOperation::SetString => Ok(Box::new(SetStringRecord::new(&mut p)?)),
    }
}

pub struct CheckpointRecord {}

impl fmt::Display for CheckpointRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<CHECKPOINT>")
    }
}

impl CheckpointRecord {
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }

    pub fn write_to_log(log_manager: Arc<Mutex<LogManager>>) -> Result<Lsn> {
        let mut p = Page::new(std::mem::size_of::<i32>());
        p.set_int(0, LogOperation::Checkpoint as i32)?;

        log_manager.lock().unwrap().append(p.contents().as_bytes())
    }
}

impl LogRecord for CheckpointRecord {
    fn op(&self) -> LogOperation {
        LogOperation::Checkpoint
    }
    fn tx_number(&self) -> i32 {
        -1
    }

    fn undo(&self, _: &mut Transaction) -> Result<()> {
        Ok(()) //noop
    }
}

pub struct StartRecord {
    txnum: i32,
}

impl fmt::Display for StartRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<START {}>", self.txnum)
    }
}

impl StartRecord {
    pub fn new(p: &mut Page) -> Result<Self> {
        let tpos = std::mem::size_of::<i32>();
        Ok(Self {
            txnum: p.get_int(tpos)?,
        })
    }

    pub fn write_to_log(log_manager: Arc<Mutex<LogManager>>, txnum: i32) -> Result<Lsn> {
        let tpos = std::mem::size_of::<i32>();
        let mut p = Page::new(tpos + std::mem::size_of::<i32>());
        p.set_int(0, LogOperation::Start as i32)?;
        p.set_int(tpos, txnum)?;

        log_manager.lock().unwrap().append(p.contents().as_bytes())
    }
}

impl LogRecord for StartRecord {
    fn op(&self) -> LogOperation {
        LogOperation::Start
    }
    fn tx_number(&self) -> i32 {
        -1
    }

    fn undo(&self, _: &mut Transaction) -> Result<()> {
        Ok(()) //noop
    }
}

pub struct CommitRecord {
    txnum: i32,
}

impl fmt::Display for CommitRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<COMMIT {}>", self.txnum)
    }
}

impl CommitRecord {
    pub fn new(p: &mut Page) -> Result<Self> {
        let tpos = std::mem::size_of::<i32>();
        Ok(Self {
            txnum: p.get_int(tpos)?,
        })
    }

    pub fn write_to_log(log_manager: Arc<Mutex<LogManager>>, txnum: i32) -> Result<Lsn> {
        let tpos = std::mem::size_of::<i32>();
        let mut p = Page::new(tpos + std::mem::size_of::<i32>());
        p.set_int(0, LogOperation::Commit as i32)?;
        p.set_int(tpos, txnum)?;

        log_manager.lock().unwrap().append(p.contents().as_bytes())
    }
}

impl LogRecord for CommitRecord {
    fn op(&self) -> LogOperation {
        LogOperation::Commit
    }
    fn tx_number(&self) -> i32 {
        -1
    }

    fn undo(&self, _: &mut Transaction) -> Result<()> {
        Ok(()) //noop
    }
}

pub struct RollbackRecord {
    txnum: i32,
}

impl fmt::Display for RollbackRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<ROLLBACK {}>", self.txnum)
    }
}

impl RollbackRecord {
    pub fn new(p: &mut Page) -> Result<Self> {
        let tpos = std::mem::size_of::<i32>();
        Ok(Self {
            txnum: p.get_int(tpos)?,
        })
    }

    pub fn write_to_log(log_manager: Arc<Mutex<LogManager>>, txnum: i32) -> Result<Lsn> {
        let tpos = std::mem::size_of::<i32>();
        let mut p = Page::new(tpos + std::mem::size_of::<i32>());
        p.set_int(0, LogOperation::Rollback as i32)?;
        p.set_int(tpos, txnum)?;

        log_manager.lock().unwrap().append(p.contents().as_bytes())
    }
}

impl LogRecord for RollbackRecord {
    fn op(&self) -> LogOperation {
        LogOperation::Rollback
    }
    fn tx_number(&self) -> i32 {
        -1
    }

    fn undo(&self, _: &mut Transaction) -> Result<()> {
        Ok(()) //noop
    }
}

pub struct SetIntRecord {
    txnum: i32,
    offset: usize,
    val: i32,
    block: BlockId,
}

impl fmt::Display for SetIntRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "<SETINT {} {} {} {}>",
            self.txnum, self.block, self.offset, self.val
        )
    }
}

impl SetIntRecord {
    pub fn new(p: &mut Page) -> Result<Self> {
        let tpos = std::mem::size_of::<i64>();
        let txnum = p.get_int(tpos)?;
        let fpos = tpos + std::mem::size_of::<i32>();
        let filename = p.get_string(fpos)?;
        let bpos = fpos + Page::max_length(filename.len());
        let block_number = p.get_int(bpos)? as usize;
        let block = BlockId::new(&filename, block_number);
        let opos = bpos + std::mem::size_of::<i32>();
        let offset = p.get_int(opos)? as usize;
        let vpos = opos + std::mem::size_of::<i32>();
        let val = p.get_int(vpos)?;

        Ok(Self {
            txnum,
            offset,
            val,
            block,
        })
    }

    /// A static method to write a SetInt record to the log.
    /// This log record contains the SETINT operator, followed by the transaction id, the filename, number,
    /// and offset of the modified block, and the previous integer value at that offset.
    pub fn write_to_log(
        log_manager: Arc<Mutex<LogManager>>,
        txnum: i32,
        block: &BlockId,
        offset: usize,
        val: i32,
    ) -> Result<Lsn> {
        let tpos = std::mem::size_of::<i64>();
        let fpos = tpos + std::mem::size_of::<i32>();
        let bpos = fpos + Page::max_length(block.filename().len());
        let opos = bpos + std::mem::size_of::<i32>();
        let vpos = opos + std::mem::size_of::<i32>();
        let mut p = Page::new(vpos + std::mem::size_of::<i32>());
        p.set_int(0, LogOperation::SetInt as i32)?;
        p.set_int(tpos, txnum)?;
        p.set_string(fpos, block.filename())?;
        p.set_int(bpos, block.block_number() as i32)?;
        p.set_int(opos, offset as i32)?;
        p.set_int(vpos, val)?;

        log_manager.lock().unwrap().append(p.contents().as_bytes())
    }
}

impl LogRecord for SetIntRecord {
    fn op(&self) -> LogOperation {
        LogOperation::SetInt
    }
    fn tx_number(&self) -> i32 {
        self.txnum
    }

    /// Replace the specified data value with the value saved in the log record.
    /// The method pins a buffer to the specified block, calls set_int to restore the saved value, and unpins the buffer.
    fn undo(&self, tx: &mut Transaction) -> Result<()> {
        tx.pin(&self.block)?;
        tx.set_int(&self.block, self.offset, self.val, false)?; // don't log the undo!
        tx.unpin(&self.block)?;
        Ok(())
    }
}

pub struct SetStringRecord {
    txnum: i32,
    offset: usize,
    val: String,
    block: BlockId,
}

impl fmt::Display for SetStringRecord {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "<SETSTRING {} {} {} {}>",
            self.txnum, self.block, self.offset, self.val
        )
    }
}

impl SetStringRecord {
    pub fn new(p: &mut Page) -> Result<Self> {
        let tpos = std::mem::size_of::<i64>();
        let txnum = p.get_int(tpos)?;
        let fpos = tpos + std::mem::size_of::<i32>();
        let filename = p.get_string(fpos)?;
        let bpos = fpos + Page::max_length(filename.len());
        let block_number = p.get_int(bpos)?;
        let block = BlockId::new(&filename, block_number as usize);
        let opos = bpos + std::mem::size_of::<i32>();
        let offset = p.get_int(opos)? as usize;
        let vpos = opos + std::mem::size_of::<i32>();
        let val = p.get_string(vpos)?;

        Ok(Self {
            txnum,
            offset,
            val,
            block,
        })
    }

    /// A static method to write a SetString record to the log.
    /// This log record contains the SETSTRING operator, followed by the transaction id, the filename, number,
    /// and offset of the modified block, and the previous integer value at that offset.
    pub fn write_to_log(
        log_manager: Arc<Mutex<LogManager>>,
        txnum: i32,
        block: &BlockId,
        offset: usize,
        val: &str,
    ) -> Result<Lsn> {
        let tpos = std::mem::size_of::<i64>();
        let fpos = tpos + std::mem::size_of::<i32>();
        let bpos = fpos + Page::max_length(block.filename().len());
        let opos = bpos + std::mem::size_of::<i32>();
        let vpos = opos + std::mem::size_of::<i32>();
        let mut p = Page::new(vpos + Page::max_length(val.len()));
        p.set_int(0, LogOperation::SetString as i32)?;
        p.set_int(tpos, txnum)?;
        p.set_string(fpos, block.filename())?;
        p.set_int(bpos, block.block_number() as i32)?;
        p.set_int(opos, offset as i32)?;
        p.set_string(vpos, val)?;

        log_manager.lock().unwrap().append(p.contents().as_bytes())
    }
}

impl LogRecord for SetStringRecord {
    fn op(&self) -> LogOperation {
        LogOperation::SetString
    }
    fn tx_number(&self) -> i32 {
        self.txnum
    }

    /// Replace the specified data value with the value saved in the log record.
    /// The method pins a buffer to the specified block, calls set_string to restore the saved value, and unpins the buffer.
    fn undo(&self, tx: &mut Transaction) -> Result<()> {
        tx.pin(&self.block)?;
        tx.set_string(&self.block, self.offset, &self.val, false)?; // don't log the undo!
        tx.unpin(&self.block)?;
        Ok(())
    }
}
