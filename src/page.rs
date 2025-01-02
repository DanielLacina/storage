use std::fs::{File, OpenOptions};
use std::io::{Read, Result, Seek, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

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

#[derive(Debug, Clone, PartialEq)]
pub enum DataField {
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

pub struct Page {
    header_offsets: PageHeaderOffsets,
    page_size: usize,
    buffer: Arc<Mutex<Vec<u8>>>,
}

//pub fn insert_data(file_path: &str, data_fields: &Vec<DataField>) -> Result<()> {
//    let path = Path::new(file_path);
//    let page_size = 8192;
//    let (mut file, mut page) = if path.exists() {
//        let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;
//        let mut buffer = [u8; page_size].to_vec();
//        file.read_exact(&mut buffer)?;
//        let page = Page::new(page_size, Some(buffer));
//        (file, page)
//    } else {
//        let file = File::create(file_path)?;
//        let page = Page::new(page_size, None);
//        (file, page)
//    }; page.write(data_fields); file.seek(std::io::SeekFrom::Start(0))?; file.write_all(&buffer)?;
//
//    Ok(())
//}

impl Page {
    pub fn new(page_size: usize, buffer: Option<Vec<u8>>) -> Self {
        let (buffer, write_metadata) = if let Some(buffer) = buffer {
            (buffer, false)
        } else {
            (vec![0u8; page_size], true)
        };
        let page = Self {
            header_offsets: PageHeaderOffsets {
                id: (0, 2),
                lower: (2, 4),
                higher: (4, 6),
                end_headers: 6,
            },
            page_size,
            buffer: Arc::new(Mutex::new(buffer)),
        };
        if write_metadata {
            page.write_metadata(&PageHeader {
                id: 0,
                lower: page.header_offsets.end_headers,
                higher: page_size as u16,
            });
        }
        page
    }

    fn write_metadata(&self, page_header: &PageHeader) {
        let mut buffer = self.buffer.lock().unwrap();
        let id = page_header.id;
        let header_offsets = &self.header_offsets;
        buffer[header_offsets.id.0..header_offsets.id.1].copy_from_slice(&id.to_le_bytes());
        let lower = page_header.lower;
        buffer[header_offsets.lower.0..header_offsets.lower.1]
            .copy_from_slice(&lower.to_le_bytes());
        let higher = page_header.higher;
        buffer[header_offsets.higher.0..header_offsets.higher.1]
            .copy_from_slice(&higher.to_le_bytes());
    }

    fn read_metadata(&self) -> PageHeader {
        let buffer = self.buffer.lock().unwrap();
        let header_offsets = &self.header_offsets;
        let id = u16::from_le_bytes(
            buffer[header_offsets.id.0..header_offsets.id.1]
                .try_into()
                .unwrap(),
        );
        let lower = u16::from_le_bytes(
            buffer[header_offsets.lower.0..header_offsets.lower.1]
                .try_into()
                .unwrap(),
        );
        let higher = u16::from_le_bytes(
            buffer[header_offsets.higher.0..header_offsets.higher.1]
                .try_into()
                .unwrap(),
        );
        PageHeader { id, lower, higher }
    }

    pub fn write(&self, data_fields: &Vec<DataField>) {
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
        let mut buffer = self.buffer.lock().unwrap();
        let mut page_header = self.read_metadata();
        page_header.higher -= data_len;
        let data_offset = page_header.higher;
        buffer[data_offset as usize..(data_offset + data_len) as usize].copy_from_slice(&row);
        let pointer_offset = page_header.lower;
        buffer[pointer_offset as usize..pointer_offset as usize + 2]
            .copy_from_slice(&data_offset.to_le_bytes());
        page_header.lower += 2;
        drop(buffer);
        self.write_metadata(&page_header);
    }

    pub fn read(&self) -> Vec<Vec<DataField>> {
        let mut pointers = Vec::new();
        let page_header = self.read_metadata();
        let mut offset = self.header_offsets.end_headers as usize;
        let buffer = self.buffer.lock().unwrap();
        while offset <= (page_header.lower - 2) as usize {
            pointers.push(u16::from_le_bytes(
                buffer[offset..offset + 2].try_into().unwrap(),
            ));
            offset += 2;
        }
        let mut rows = Vec::new();
        for pointer in pointers {
            let mut row = Vec::new();
            let mut offset = pointer as usize;
            let mut num_of_fields =
                u16::from_le_bytes(buffer[offset..offset + 2].try_into().unwrap());
            offset += 2;
            let mut datatype_nums = Vec::new();
            while num_of_fields != 0 {
                let datatype_num =
                    u16::from_le_bytes(buffer[offset..offset + 2].try_into().unwrap());
                datatype_nums.push(datatype_num);
                offset += 2;
                num_of_fields -= 1;
            }
            for datatype_num in datatype_nums {
                match datatype_num {
                    1 => {
                        let integer =
                            u16::from_le_bytes(buffer[offset..offset + 2].try_into().unwrap());
                        row.push(DataField::Integer(integer));
                        offset += 2;
                    }
                    2 => {
                        let text_length =
                            u16::from_le_bytes(buffer[offset..offset + 2].try_into().unwrap())
                                as usize;
                        offset += 2;
                        let text = String::from_utf8(buffer[offset..offset + text_length].to_vec())
                            .unwrap();
                        row.push(DataField::Text(text));
                        offset += text_length;
                    }
                    _ => panic!("invalid number"),
                }
            }
            rows.push(row);
        }
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_page() {
        let page = Page::new(8192, None);
        let data_fields = vec![DataField::Text("data".to_string()), DataField::Integer(10)];
        page.write(&data_fields);
        let rows = page.read();
        assert_eq!(rows, vec![data_fields]);
    }
}
