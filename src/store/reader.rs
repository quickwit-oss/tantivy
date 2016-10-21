use Result;

use directory::ReadOnlySource;
use std::cell::RefCell;
use DocId;
use schema::Document;
use schema::FieldValue;
use common::BinarySerializable;
use std::mem::size_of;
use std::io::{self, Read};
use datastruct::SkipList;
use lz4;

pub struct StoreReader {
    pub data: ReadOnlySource,
    pub offset_index_source: ReadOnlySource,
    current_block: RefCell<Vec<u8>>,
    pub max_doc: DocId,
}

impl StoreReader {
    fn block_offset(&self, doc_id: DocId) -> (DocId, u64) {
        SkipList::read(self.offset_index_source.as_slice())
            .seek(doc_id)
            .unwrap_or((0u32, 0u64))
    }

    fn read_block(&self, block_offset: usize) -> io::Result<()> {
        let mut current_block_mut = self.current_block.borrow_mut();
        current_block_mut.clear();
        let total_buffer = self.data.as_slice();
        let mut cursor = &total_buffer[block_offset..];
        let block_length = u32::deserialize(&mut cursor).unwrap();
        let block_array: &[u8] =
            &total_buffer[(block_offset + 4 as usize)..(block_offset + 4 + block_length as usize)];
        let mut lz4_decoder = try!(lz4::Decoder::new(block_array));
        lz4_decoder.read_to_end(&mut current_block_mut).map(|_| ())
    }

    pub fn get(&self, doc_id: DocId) -> Result<Document> {
        let (first_doc_id, block_offset) = self.block_offset(doc_id);
        try!(self.read_block(block_offset as usize));
        let current_block_mut = self.current_block.borrow_mut();
        let mut cursor = &current_block_mut[..];
        for _ in first_doc_id..doc_id {
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


fn split_source(data: ReadOnlySource) -> (ReadOnlySource, ReadOnlySource, DocId) {
    let data_len = data.len();
    let footer_offset = data_len - size_of::<u64>() - size_of::<u32>();
    let serialized_offset: ReadOnlySource = data.slice(footer_offset, data_len);
    let mut serialized_offset_buf = serialized_offset.as_slice();
    let offset = u64::deserialize(&mut serialized_offset_buf).unwrap();
    let offset = offset as usize;
    let max_doc = u32::deserialize(&mut serialized_offset_buf).unwrap();
    (data.slice(0, offset), data.slice(offset, footer_offset), max_doc)
}


impl From<ReadOnlySource> for StoreReader {
    fn from(data: ReadOnlySource) -> StoreReader {
        let (data_source, offset_index_source, max_doc) = split_source(data);
        StoreReader {
            data: data_source,
            offset_index_source: offset_index_source,
            current_block: RefCell::new(Vec::new()),
            max_doc: max_doc,
        }
    }
}
