use std::fs::{File, OpenOptions};
use std::io::{Read, Result, Seek, Write};
use std::path::Path;

const PAGE_SIZE: usize = 8192;

struct PageHeaderOffsets {
    pub id: (usize, usize),
    pub lower: (usize, usize),
    pub higher: (usize, usize),
}

enum DataField {
    Text(String),
    Integer(u16),
}

pub struct Storage {
    page_header_offsets: PageHeaderOffsets,
}

impl Storage {
    pub fn new() -> Self {
        Self {
            page_header_offsets: PageHeaderOffsets {
                id: (0, 2),
                lower: (2, 4),
                higher: (4, 6),
            },
        }
    }

    pub fn insert_data(
        &self,
        file_path: &str,
        data_fields: &Vec<DataField>,
        data_len: u16,
    ) -> Result<()> {
        let path = Path::new(file_path);
        let (mut file, mut page) = if path.exists() {
            let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;
            let mut page = self.default_page();
            file.read_exact(&mut page)?;
            (file, page)
        } else {
            let file = File::create(file_path)?;
            let page = self.create_page()?;
            (file, page)
        };
        self.write_page(&mut page, data_fields, data_len);
        file.seek(std::io::SeekFrom::Start(0))?;
        file.write_all(&page)?;

        Ok(())
    }

    fn default_page(&self) -> Vec<u8> {
        [0u8; PAGE_SIZE].to_vec()
    }

    fn write_metadata(&self, page: &mut Vec<u8>) {
        let id = 1 as u16;
        let page_header_offsets = &self.page_header_offsets;
        page[page_header_offsets.id.0..page_header_offsets.id.1].copy_from_slice(&id.to_le_bytes());
        let lower = 6 as u16;
        page[page_header_offsets.lower.0..page_header_offsets.lower.1]
            .copy_from_slice(&lower.to_le_bytes());
        let higher = PAGE_SIZE;
        page[page_header_offsets.higher.0..page_header_offsets.higher.1]
            .copy_from_slice(&higher.to_le_bytes());
    }

    fn write_page(&self, page: &mut Vec<u8>, data_fields: &Vec<DataField>, data_len: u16) {
        let page_header_offsets = &self.page_header_offsets;
        let mut lower = u16::from_le_bytes(
            page[page_header_offsets.lower.0..page_header_offsets.lower.1]
                .try_into()
                .unwrap(),
        );
        let mut higher = u16::from_le_bytes(
            page[page_header_offsets.higher.0..page_header_offsets.higher.1]
                .try_into()
                .unwrap(),
        );
        higher -= data_len;
        let pointer_offset = lower as usize;
        // adds a pointer thats value is the: original higher value - data_len
        page[pointer_offset..pointer_offset + 2].copy_from_slice(&higher.to_le_bytes());
        // lower value updated due to added pointer
        lower += 2;
        let mut data_offset = higher as usize;
        for field in data_fields {
            match field {
                DataField::Text(text) => {
                    let text_to_bytes = text.as_bytes();
                    let text_len = text_to_bytes.len() as u16;
                    page[data_offset..data_offset + 2].copy_from_slice(&text_len.to_le_bytes());
                    data_offset += 2;
                    page[data_offset..data_offset + text_len as usize]
                        .copy_from_slice(text_to_bytes);
                    data_offset += text_len as usize;
                }
                DataField::Integer(int) => {
                    page[data_offset..data_offset + 2].copy_from_slice(&int.to_le_bytes());
                    data_offset += 2;
                }
            }
        }
        // modifies lower and higher values within the page
        page[page_header_offsets.lower.0..page_header_offsets.lower.1]
            .copy_from_slice(&lower.to_le_bytes());
        page[page_header_offsets.higher.0..page_header_offsets.higher.1]
            .copy_from_slice(&higher.to_le_bytes());
    }

    fn create_page(&self) -> Result<Vec<u8>> {
        let mut page = self.default_page();
        self.write_metadata(&mut page);
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
