use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Result, Seek, Write};
use std::path::Path;

const PAGE_SIZE: usize = 8192;

#[derive(Debug, Clone)]
struct PageHeaderOffsets {
    pub id: (usize, usize),
    pub lower: (usize, usize),
    pub higher: (usize, usize),
    pub end_headers: u16,
}

#[derive(Debug, Clone)]
struct PageHeader {
    pub id: u16,
    pub lower: u16,
    pub higher: u16,
}

enum DataField {
    Text(String),
    Integer(u16),
}

impl DataField {
    pub fn to_int(&self) -> u16 {
        match self {
            DataField::Integer(_) => 1,
            DataField::Text(_) => 2,
        }
    }
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
                end_headers: 6,
            },
        }
    }

    pub fn insert_data(&self, file_path: &str, data_fields: &Vec<DataField>) -> Result<()> {
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
        self.write_page(&mut page, data_fields);
        file.seek(std::io::SeekFrom::Start(0))?;
        file.write_all(&page)?;

        Ok(())
    }

    fn default_page(&self) -> Vec<u8> {
        [0u8; PAGE_SIZE].to_vec()
    }

    fn write_metadata(&self, page: &mut Vec<u8>, page_header: &PageHeader) {
        let id = page_header.id;
        let page_header_offsets = &self.page_header_offsets;
        page[page_header_offsets.id.0..page_header_offsets.id.1].copy_from_slice(&id.to_le_bytes());
        let lower = page_header.lower;
        page[page_header_offsets.lower.0..page_header_offsets.lower.1]
            .copy_from_slice(&lower.to_le_bytes());
        let higher = page_header.higher;
        page[page_header_offsets.higher.0..page_header_offsets.higher.1]
            .copy_from_slice(&higher.to_le_bytes());
    }

    fn read_metadata(&self, page: &Vec<u8>) -> PageHeader {
        let page_header_offsets = &self.page_header_offsets;
        let id = u16::from_le_bytes(
            page[page_header_offsets.id.0..page_header_offsets.id.1]
                .try_into()
                .unwrap(),
        );
        let lower = u16::from_le_bytes(
            page[page_header_offsets.lower.0..page_header_offsets.lower.1]
                .try_into()
                .unwrap(),
        );
        let higher = u16::from_le_bytes(
            page[page_header_offsets.higher.0..page_header_offsets.higher.1]
                .try_into()
                .unwrap(),
        );
        PageHeader { id, lower, higher }
    }

    fn write_page(&self, page: &mut Vec<u8>, data_fields: &Vec<DataField>) {
        let mut row = Vec::new();
        let mut data = Vec::new();
        let mut data_len = 0 as u16;
        row.extend_from_slice(&(data_fields.len() as u16).to_le_bytes());
        data_len += 2;
        for field in data_fields {
            row.extend_from_slice(&field.to_int().to_le_bytes());
            data_len += 2;
            match field {
                DataField::Text(text) => {
                    let text_to_bytes = text.as_bytes();
                    let text_len = text_to_bytes.len() as u16;
                    data.extend_from_slice(&text_len.to_le_bytes());
                    data_len += 2;
                    data.extend_from_slice(text_to_bytes);
                    data_len += text_len;
                }
                DataField::Integer(int) => {
                    data.extend_from_slice(&int.to_le_bytes());
                    data_len += 2;
                }
            }
        }
        row.extend_from_slice(&data);
        let mut page_header = self.read_metadata(&page);
        page_header.higher -= data_len;
        let data_offset = page_header.higher;
        page[data_offset as usize..(data_offset + data_len) as usize].copy_from_slice(&row);
        let pointer_offset = page_header.lower;
        page[pointer_offset as usize..pointer_offset as usize + 2]
            .copy_from_slice(&data_offset.to_le_bytes());
        page_header.lower += 2;
        self.write_metadata(page, &page_header);
    }

    fn read_page(&self, page: &Vec<u8>) {
        let mut pointers = Vec::new();
        let page_header = self.read_metadata(page);
        let mut offset = self.page_header_offsets.end_headers as usize;
        while offset <= (page_header.lower - 2) as usize {
            pointers.push(u16::from_le_bytes(
                page[offset..offset + 2].try_into().unwrap(),
            ));
            offset += 2;
        }
        for pointer in pointers {
            let mut offset = pointer as usize;
            let mut num_of_fields =
                u16::from_le_bytes(page[offset..offset + 2].try_into().unwrap());
            offset += 2;
            while num_of_fields != 0 {
                let datatype_num = u16::from_le_bytes(page[offset..offset + 2].try_into().unwrap());
                println!("{}", offset);
                offset += 2;
                match datatype_num {
                    1 => {
                        let integer =
                            u16::from_le_bytes(page[offset..offset + 2].try_into().unwrap());
                        println!("{}", integer);
                        offset += 2;
                    }
                    2 => {
                        let text_length =
                            u16::from_le_bytes(page[offset..offset + 2].try_into().unwrap())
                                as usize;
                        offset += 2;
                        let text =
                            String::from_utf8(page[offset..offset + text_length].to_vec()).unwrap();
                        println!("{}", text);
                        offset += text_length;
                    }
                    _ => panic!("invalid number"),
                }
                num_of_fields -= 1;
            }
        }
    }

    fn create_page(&self) -> Result<Vec<u8>> {
        let mut page = self.default_page();
        let page_header = PageHeader {
            id: 1,
            lower: 6,
            higher: PAGE_SIZE as u16,
        };
        self.write_metadata(&mut page, &page_header);
        Ok(page)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_page() {
        let storage = Storage::new();
        let mut page = storage.create_page().unwrap();
        let data_fields = vec![DataField::Text("data".to_string()), DataField::Integer(10)];
        storage.write_page(&mut page, &data_fields);
        let page_headers = storage.read_metadata(&page);
        storage.read_page(&page);
    }
}
