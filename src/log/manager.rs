use anyhow::{Ok, Result};
use std::sync::{Arc, Mutex};

use crate::{file::{block_id::BlockId, manager::FileManager, page::Page}, Lsn};

use super::iterator::LogIterator;

#[derive(Debug)]
pub struct LogManager {
    file_manager: Arc<Mutex<FileManager>>,
    logfile: String,
    logpage: Page,
    current_block: BlockId,
    // log sequence number
    latest_lsn: Lsn,
    last_saved_lsn: Lsn,
}

/// The log manager is responsible for writing log records to the log file from right to left.
/// A log sequence number (or LSN)identifies identifies a log record.
/// The log manager keeps track of the next available LSN and the LSN of the most recent log record written to disk.
impl LogManager {
    pub fn new(file_manager: Arc<Mutex<FileManager>>, log_file: &str) -> Result<Self> {
        let block_size = file_manager.lock().unwrap().block_size();

        let mut log_manager = LogManager {
            file_manager: Arc::clone(&file_manager),
            logfile: log_file.to_string(),
            logpage: Page::from_bytes(vec![0; block_size]),
            current_block: BlockId::new(log_file, 0),
            latest_lsn: 0,
            last_saved_lsn: 0,
        };

        // If the log file does not yet exist, create it with an empty first block
        let log_size = file_manager.lock().unwrap().length(log_file)?;
        log_manager.current_block = if log_size == 0 {
            log_manager.append_new_block()?
        } else {
            let block = BlockId::new(log_file, log_size - 1);
            file_manager
                .lock()
                .unwrap()
                .read(&block, &mut log_manager.logpage)?;
            block
        };

        Ok(log_manager)
    }

    /// Ensures that the log record with specified LSN (and all previous log records) is written to disk
    pub fn flush(&mut self, lsn: Lsn) -> Result<()> {
        if lsn >= self.last_saved_lsn {
            self.do_flush()?;
        }

        Ok(())
    }

    /// Ensures that the log records are written to disk by flushing it, and only then return iterator
    pub fn iterator(&mut self) -> Result<LogIterator> {
        self.do_flush()?;
        LogIterator::new(Arc::clone(&self.file_manager), &self.current_block)
    }

    /// The log records are placed in the page from right to left,
    /// which enables the log iterator to read records in reverse order.
    /// The first 4 bytes of the page is the ofsset of the most recently added record,
    /// so that the iterator will know where the records begin.
    pub fn append(&mut self, logrec: &[u8]) -> Result<i64> {
        let mut boundary = self.logpage.get_int(0)?;
        let int_bytes = std::mem::size_of::<i32>() as i32;
        let recsize = logrec.len() as i32;
        let bytes_needed = recsize + int_bytes;
        // the log record doesn't fit, so write the current page to disk,
        // clear the page and append the now-empty page to the log file
        if boundary - bytes_needed < int_bytes {
            // so move to the next block
            self.do_flush()?;
            self.current_block = self.append_new_block()?;
            boundary = self.logpage.get_int(0)?;
        }

        let recpos = boundary - bytes_needed;
        self.logpage.set_bytes(recpos as usize, logrec)?;
        self.logpage.set_int(0, recpos)?; // the new boundary
        self.latest_lsn += 1;
        Ok(self.latest_lsn)
    }

    fn do_flush(&mut self) -> Result<()> {
        self.file_manager
            .lock()
            .unwrap()
            .write(&self.current_block, &mut self.logpage)?;

        self.last_saved_lsn = self.latest_lsn;

        Ok(())
    }

    /// writes
    fn append_new_block(&mut self) -> Result<BlockId> {
        let mut guard = self.file_manager.lock().unwrap();
        let block = guard.append(&self.logfile)?;
        self.logpage.set_int(0, guard.block_size() as i32)?;
        guard.write(&block, &mut self.logpage)?;
        Ok(block)
    }
}

#[cfg(test)]
mod tests {

    use std::sync::{Arc, Mutex};
    use tempfile::tempdir;

    use crate::{file::manager::FileManager, file::page::Page, log::manager::LogManager};

    #[test]
    fn logtest() {
        let temp_dir = tempdir().unwrap();
        let test_log_file = temp_dir
            .path()
            .join("logtest")
            .to_str()
            .unwrap()
            .to_string();

        let block_size = 512;
        let file_manager = Arc::new(Mutex::new(
            FileManager::new(temp_dir.path().to_str().unwrap(), block_size).unwrap(),
        ));
        let log_manager = Arc::new(Mutex::new(
            LogManager::new(Arc::clone(&file_manager), &test_log_file).unwrap(),
        ));

        create_records(Arc::clone(&log_manager), 1, 25);
        assert_log_records(Arc::clone(&log_manager), 25, 1);
        create_records(Arc::clone(&log_manager), 26, 50);
        assert_log_records(Arc::clone(&log_manager), 50, 1);
    }

    fn assert_log_records(log_manager: Arc<Mutex<LogManager>>, start: i32, end: i32) {
        let mut iter = log_manager.lock().unwrap().iterator().unwrap();
        let mut current = start;

        while iter.has_next() {
            if let Some(rec) = iter.next() {
                let mut p = Page::from_bytes(rec);
                let s = p.get_string(0).unwrap();
                let npos = Page::max_length(s.len());
                let val = p.get_int(npos).unwrap();

                assert_eq!(s, format!("record{}", current));
                assert_eq!(val, current + 100);

                current -= 1;
            }
        }

        assert_eq!(current + 1, end);
    }

    fn create_records(log_manager: Arc<Mutex<LogManager>>, start: i32, end: i32) {
        for i in start..=end {
            let s = format!("record{}", i);
            let rec = create_log_record(s.as_str(), i + 100);
            let lsn = log_manager.lock().unwrap().append(&rec).unwrap();
            assert_eq!(i as i64, lsn);
        }
    }

    fn create_log_record(s: &str, n: i32) -> Vec<u8> {
        let npos = Page::max_length(s.len());
        let bytes: Vec<u8> = vec![0; npos + 4];
        let mut p = Page::from_bytes(bytes);
        p.set_string(0, s).unwrap();
        p.set_int(npos, n).unwrap();
        p.contents().as_bytes().to_vec()
    }
}
