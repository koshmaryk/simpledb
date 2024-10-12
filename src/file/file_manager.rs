use anyhow::{Ok, Result};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use super::block_id::BlockId;
use super::page::Page;

#[derive(Debug)]
pub struct FileManager {
    db_dir: String,
    block_size: usize,
    is_new: bool,
    open_files: HashMap<String, Arc<Mutex<File>>>,
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
        })
    }

    pub fn read(&mut self, block: &BlockId, page: &mut Page) -> Result<()> {
        let file = self.get_file(block.filename())?;
        let mut guard = file.lock().unwrap();
        guard.seek(SeekFrom::Start(
            (block.block_number() * self.block_size) as u64,
        ))?;
        guard.read_exact(&mut page.contents().as_bytes().to_vec())?;

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

        Ok(block)
    }

    pub fn length(&mut self, filename: &str) -> Result<usize> {
        let file = self.get_file(filename)?;
        let guard = file.lock().unwrap();
        let len = guard.metadata()?.len();

        //ceiling
        Ok((len as usize + self.block_size - 1) / self.block_size)
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

    use super::FileManager;
    use crate::file::{block_id::BlockId, page::Page};
    use tempfile::tempdir;

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

        let filename = "simple_string";
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
}
