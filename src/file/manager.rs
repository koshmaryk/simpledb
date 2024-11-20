use anyhow::Result;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use super::{block_id::BlockId, page::Page};

#[derive(Debug)]
pub struct FileManager {
    db_dir: String,
    block_size: usize,
    is_new: bool,
    open_files: HashMap<String, Arc<Mutex<File>>>,
    total_blocks_read: usize,
    total_blocks_write: usize,
}

impl FileManager {
    pub fn new(db_dir: &str, block_size: usize) -> Result<Self> {
        let path = Path::new(db_dir);
        let is_new = !path.exists();

        if is_new {
            std::fs::create_dir_all(path)?;
        }

        // Remove any leftover temporary tables
        std::fs::read_dir(path)?
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_file() && path.starts_with("temp"))
            .try_for_each(std::fs::remove_file)?;

        Ok(Self {
            db_dir: db_dir.to_string(),
            block_size,
            is_new,
            open_files: HashMap::new(),
            total_blocks_read: 0,
            total_blocks_write: 0,
        })
    }

    pub fn read(&mut self, block: &BlockId, page: &mut Page) -> Result<()> {
        let file = self.get_file(block.filename())?;
        let mut guard = file.lock().unwrap();
        let pos = (block.block_number() * self.block_size) as u64;
        guard.seek(SeekFrom::Start(pos))?;

        if guard.metadata()?.len() >= pos + page.contents().len() as u64 {
            let mut temp_buf = vec![0u8; page.contents().len()];
            guard.read_exact(&mut temp_buf)?;
            page.contents().clear();
            page.contents().write_bytes(&temp_buf);
        }

        self.total_blocks_read += 1;

        Ok(())
    }

    pub fn write(&mut self, block: &BlockId, page: &mut Page) -> Result<()> {
        let file = self.get_file(block.filename())?;
        let mut guard = file.lock().unwrap();
        guard.seek(SeekFrom::Start(
            (block.block_number() * self.block_size) as u64,
        ))?;
        guard.write_all(page.contents().as_bytes())?;
        guard.sync_all()?;

        self.total_blocks_write += 1;

        Ok(())
    }

    pub fn append(&mut self, filename: &str) -> Result<BlockId> {
        let new_block_number = self.length(filename)?;
        let block = BlockId::new(filename, new_block_number);
        let bytes = vec![0u8; self.block_size];

        let file = self.get_file(block.filename())?;
        let mut guard = file.lock().unwrap();
        guard.seek(SeekFrom::Start(
            (block.block_number() * self.block_size) as u64,
        ))?;
        guard.write_all(&bytes)?;
        guard.sync_all()?;

        self.total_blocks_write += 1;

        Ok(block)
    }

    pub fn length(&mut self, filename: &str) -> Result<usize> {
        let file = self.get_file(filename)?;
        let guard = file.lock().unwrap();
        let len = guard.metadata()?.len();

        //ceiling
        Ok((len as usize + self.block_size - 1) / self.block_size)
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn get_total_blocks_read(&self) -> usize {
        self.total_blocks_read
    }

    pub fn get_total_blocks_write(&self) -> usize {
        self.total_blocks_write
    }

    fn get_file(&mut self, filename: &str) -> Result<Arc<Mutex<File>>> {
        if let Some(file) = self.open_files.get(filename) {
            Ok(Arc::clone(file))
        } else {
            let path = Path::new(&self.db_dir).join(filename);

            let file = Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&path)?,
            ));

            self.open_files
                .insert(filename.to_string(), Arc::clone(&file));

            Ok(file)
        }
    }
}

#[cfg(test)]
mod test {

    use chrono::NaiveDate;
    use tempfile::tempdir;

    use super::FileManager;
    use crate::file::{block_id::BlockId, page::Page};

    #[test]
    fn test_read_write_short() {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().to_str().unwrap();
        let block_size = 512;

        let mut file_manager = FileManager::new(db_dir, block_size).unwrap();

        let filename = "simple_short.tbl";
        let block = BlockId::new(filename, 0);

        let mut page = Page::new(block_size);
        page.set_short(0, 10).unwrap();

        // write the page
        file_manager.write(&block, &mut page).unwrap();

        // read the page
        file_manager.read(&block, &mut page).unwrap();

        assert_eq!(page.get_short(0).unwrap(), 10);
        assert_eq!(file_manager.length(filename).unwrap(), 1);
    }

    #[test]
    fn test_read_write_int() {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().to_str().unwrap();
        let block_size = 512;

        let mut file_manager = FileManager::new(db_dir, block_size).unwrap();

        let filename = "simple_int.tbl";
        let block = BlockId::new(filename, 0);

        let mut page = Page::new(block_size);
        page.set_int(0, 42).unwrap();

        // write the page
        file_manager.write(&block, &mut page).unwrap();

        // read the page
        file_manager.read(&block, &mut page).unwrap();

        assert_eq!(page.get_int(0).unwrap(), 42);
        assert_eq!(file_manager.length(filename).unwrap(), 1);
    }

    #[test]
    fn test_read_write_bytes() {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().to_str().unwrap();
        let block_size = 512;

        let mut file_manager = FileManager::new(db_dir, block_size).unwrap();

        let filename = "simple_bytes.tbl";
        let block = BlockId::new(filename, 0);

        let mut page = Page::new(block_size);
        let test_bytes = vec![1, 2, 3, 4, 5];
        page.set_bytes(0, &test_bytes).unwrap();

        // write the page
        file_manager.write(&block, &mut page).unwrap();

        // read the page
        file_manager.read(&block, &mut page).unwrap();

        assert_eq!(page.get_bytes(0).unwrap(), test_bytes);
        assert_eq!(file_manager.length(filename).unwrap(), 1);
    }

    #[test]
    fn test_read_write_string() {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().to_str().unwrap();
        let block_size = 512;

        let mut file_manager = FileManager::new(db_dir, block_size).unwrap();

        let filename = "simple_string.tbl";
        let block = BlockId::new(filename, 0);

        let mut page = Page::new(block_size);
        page.set_string(0, "Hello, SimpleDB!").unwrap();

        // write the page
        file_manager.write(&block, &mut page).unwrap();

        // read the page
        file_manager.read(&block, &mut page).unwrap();

        assert_eq!(page.get_string(0).unwrap(), "Hello, SimpleDB!");
        assert_eq!(file_manager.length(filename).unwrap(), 1);
    }

    #[test]
    fn test_append() {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().to_str().unwrap();
        let block_size = 512;

        let mut file_manager = FileManager::new(db_dir, block_size).unwrap();

        let filename = "simple.tbl";

        let block1 = file_manager.append(filename).unwrap();
        assert_eq!(block1.block_number(), 0);

        let block2 = file_manager.append(filename).unwrap();
        assert_eq!(block2.block_number(), 1);

        assert_eq!(file_manager.length(filename).unwrap(), 2);
    }

    #[test]
    fn test_read_write_bool() {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().to_str().unwrap();
        let block_size = 512;

        let mut file_manager = FileManager::new(db_dir, block_size).unwrap();

        let filename = "simple_bool.tbl";
        let block = BlockId::new(filename, 0);

        let mut page = Page::new(block_size);
        page.set_bool(0, true).unwrap();
        page.set_bool(1, false).unwrap();

        // write the page
        file_manager.write(&block, &mut page).unwrap();

        // read the page
        file_manager.read(&block, &mut page).unwrap();

        assert!(page.get_bool(0).unwrap());
        assert!(!page.get_bool(1).unwrap());
        assert_eq!(file_manager.length(filename).unwrap(), 1);
    }

    #[test]
    fn test_read_write_date() {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().to_str().unwrap();
        let block_size = 512;

        let mut file_manager = FileManager::new(db_dir, block_size).unwrap();

        let filename = "simple_date.tbl";
        let block = BlockId::new(filename, 0);

        let mut page = Page::new(block_size);
        let test_date = NaiveDate::from_ymd_opt(2024, 10, 1).unwrap();
        page.set_date(0, test_date).unwrap();

        // write the page
        file_manager.write(&block, &mut page).unwrap();

        // read the page
        file_manager.read(&block, &mut page).unwrap();

        assert_eq!(page.get_date(0).unwrap(), test_date);
        assert_eq!(file_manager.length(filename).unwrap(), 1);
    }

    #[test]
    fn test_statistics() {
        let temp_dir = tempdir().unwrap();
        let db_dir = temp_dir.path().to_str().unwrap();
        let block_size = 512;

        let mut file_manager = FileManager::new(db_dir, block_size).unwrap();

        let filename = "stats_test.tbl";
        let block = BlockId::new(filename, 0);

        let mut page = Page::new(block_size);
        page.set_int(0, 42).unwrap();

        assert_eq!(file_manager.get_total_blocks_read(), 0);
        assert_eq!(file_manager.get_total_blocks_write(), 0);

        file_manager.write(&block, &mut page).unwrap();
        assert_eq!(file_manager.get_total_blocks_write(), 1);

        file_manager.read(&block, &mut page).unwrap();
        assert_eq!(file_manager.get_total_blocks_read(), 1);

        file_manager.append(filename).unwrap();
        assert_eq!(file_manager.get_total_blocks_write(), 2);
    }
}
