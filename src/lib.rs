mod page;
mod page_scanner;
use crate::page::{DataField, Page};
use std::fs::{File, OpenOptions};
use std::io::{Read, Result, Seek, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};

pub struct Storage {
    pages: RwLock<Vec<Arc<Page>>>,
}

impl Storage {
    pub fn insert_data(file_path: &str, data_fields: &Vec<DataField>) -> Result<()> {
        let path = Path::new(file_path);
        let page_size = 8192;
        let (mut file, page) = if path.exists() {
            let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;
            let mut buffer = vec![0u8; page_size];
            file.read_exact(&mut buffer)?;
            let page = Page::new(page_size, Some(buffer));
            (file, page)
        } else {
            let file = File::create(file_path)?;
            let page = Page::new(page_size, None);
            (file, page)
        };
        page.write(data_fields);
        file.seek(std::io::SeekFrom::Start(0))?;
        file.write_all(&page.get_buffer())?;

        Ok(())
    }

    pub fn read_data(file_path: &str) {}

    fn add_page(&self, page: Page) {
        let mut pages = self.pages.write().unwrap();
        pages.push(Arc::new(page));
    }
}
