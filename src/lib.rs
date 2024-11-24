/// A SimpleDB Log Sequence Number (LSN).
pub type Lsn = i64;

pub mod buffer;
pub mod file;
pub mod log;
pub mod tx;