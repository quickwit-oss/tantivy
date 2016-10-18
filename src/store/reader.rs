use Result;

use directory::ReadOnlySource;
use std::cell::RefCell;
use DocId;
use schema::Document;
use schema::FieldValue;
use common::BinarySerializable;
use std::mem::size_of;
use std::io::Read;
use std::io;
use std::cmp::Ordering;
use lz4;

use super::OffsetIndex;

pub struct StoreReader {
    pub data: ReadOnlySource,
    pub offsets: Vec<OffsetIndex>,
    current_block: RefCell<Vec<u8>>,
}

impl StoreReader {

    fn block_offset(&self, seek: DocId) -> OffsetIndex {
        fn search(offsets: &[OffsetIndex], seek: DocId) -> OffsetIndex {
            let m = offsets.len() / 2;
            let pivot_offset = &offsets[m];
            if offsets.len() <= 1 {
                return pivot_offset.clone()
            }
            match pivot_offset.0.cmp(&seek) {
                Ordering::Less => search(&offsets[m..], seek),
                Ordering::Equal => pivot_offset.clone(),
                Ordering::Greater => search(&offsets[..m], seek),
            }
        }
        search(&self.offsets, seek)
    }

    fn read_block(&self, block_offset: usize) -> io::Result<()> {
        let mut current_block_mut = self.current_block.borrow_mut();
        current_block_mut.clear();
        let total_buffer = self.data.as_slice();
        let mut cursor = &total_buffer[block_offset..];
        let block_length = u32::deserialize(&mut cursor).unwrap();
        let block_array: &[u8] = &total_buffer[(block_offset + 4 as usize)..(block_offset + 4 + block_length as usize)];
        let mut lz4_decoder = try!(lz4::Decoder::new(block_array));
        lz4_decoder.read_to_end(&mut current_block_mut).map(|_| ())
    }

    pub fn get(&self, doc_id: DocId) -> Result<Document> {
        let OffsetIndex(first_doc_id, block_offset) = self.block_offset(doc_id);
        try!(self.read_block(block_offset as usize));
        let current_block_mut = self.current_block.borrow_mut();
        let mut cursor = &current_block_mut[..];
        for _ in first_doc_id..doc_id  {
            let block_length = try!(u32::deserialize(&mut cursor));
            cursor = &cursor[block_length as usize..];
        }
        try!(u32::deserialize(&mut cursor));
        let mut field_values = Vec::new();
        let num_fields = try!(u32::deserialize(&mut cursor));
        for _ in 0..num_fields {
            let field_value = try!(FieldValue::deserialize(&mut cursor));
            field_values.push(field_value);
        }
        Ok(Document::from(field_values))
    }
}


fn split_source(data: ReadOnlySource) -> (ReadOnlySource, ReadOnlySource) {
    let data_len = data.len();
    let serialized_offset: ReadOnlySource = data.slice(data_len - size_of::<u64>(), data_len);
    let mut serialized_offset_buf = serialized_offset.as_slice(); 
    let offset = u64::deserialize(&mut serialized_offset_buf).expect("any 8-byte slice can be deserialize to u64.");
    let offset = offset as usize;
    (data.slice(0, offset), data.slice(offset, data_len - size_of::<u64>()))
}

fn read_header(data: &ReadOnlySource) -> Vec<OffsetIndex> {
    // TODO err
    // the first offset is implicitely (0, 0)
    let mut offsets = vec!(OffsetIndex(0, 0));
    let mut buffer: &[u8] = data.as_slice();
    offsets.append(&mut Vec::deserialize(&mut buffer).unwrap()); 
    offsets
}

impl From<ReadOnlySource> for StoreReader {
    fn from(data: ReadOnlySource) -> StoreReader {
        let (data_source, offset_index_source) = split_source(data);
        let offsets = read_header(&offset_index_source);
        StoreReader {
            data: data_source,
            offsets: offsets,
            current_block: RefCell::new(Vec::new()),
        }
    }
}
