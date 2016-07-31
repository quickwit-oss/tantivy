use directory::ReadOnlySource;
use std::cell::RefCell;
use DocId;
use schema::Document;
use schema::FieldValue;
use common::BinarySerializable;

use std::io::Read;
use std::io::Cursor;
use std::io;
use std::io::SeekFrom;
use std::io::Seek;
use std::cmp::Ordering;
use lz4;

use super::OffsetIndex;

pub struct StoreReader {
    pub data: ReadOnlySource,
    pub offsets: Vec<OffsetIndex>,
    current_block: RefCell<Vec<u8>>,
}

impl StoreReader {

    fn read_header(data: &ReadOnlySource) -> Vec<OffsetIndex> {
        // TODO err
        // the first offset is implicitely (0, 0)
        let mut offsets = vec!(OffsetIndex(0, 0));
        let mut cursor = Cursor::new(data.as_slice());
        cursor.seek(SeekFrom::End(-8)).unwrap();
        let offset = u64::deserialize(&mut cursor).unwrap();
        cursor.seek(SeekFrom::Start(offset)).unwrap();
        offsets.append(&mut Vec::deserialize(&mut cursor).unwrap());
        offsets
    }

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
        let mut cursor = Cursor::new(&total_buffer[block_offset..]);
        let block_length = u32::deserialize(&mut cursor).unwrap();
        let block_array: &[u8] = &total_buffer[(block_offset + 4 as usize)..(block_offset + 4 + block_length as usize)];
        let mut lz4_decoder = lz4::Decoder::new(Cursor::new(block_array)).unwrap();
        lz4_decoder.read_to_end(&mut current_block_mut).map(|_| ())
    }

    pub fn get(&self, doc_id: DocId) -> io::Result<Document> {
        let OffsetIndex(first_doc_id, block_offset) = self.block_offset(doc_id);
        try!(self.read_block(block_offset as usize));
        let mut current_block_mut = self.current_block.borrow_mut();
        let mut cursor = Cursor::new(&mut current_block_mut[..]);
        for _ in first_doc_id..doc_id  {
            let block_length = try!(u32::deserialize(&mut cursor));
            try!(cursor.seek(SeekFrom::Current(block_length as i64)));
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

    pub fn new(data: ReadOnlySource) -> StoreReader {
        let offsets = StoreReader::read_header(&data);
        StoreReader {
            data: data,
            offsets: offsets,
            current_block: RefCell::new(Vec::new()),
        }
    }
}
