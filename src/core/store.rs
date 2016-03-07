use core::directory::WritePtr;
use std::cell::RefCell;
use core::schema::DocId;
use core::schema::Document;
use core::schema::FieldValue;
use core::serialize::BinarySerializable;
use core::serialize::Size;
use core::directory::ReadOnlySource;
use std::io::Write;
use std::io::Read;
use std::io::Cursor;
use std::io;
use std::io::SeekFrom;
use std::io::Seek;
use lz4;

// TODO cache uncompressed pages

const BLOCK_SIZE: usize = 131_072;

pub struct StoreWriter {
    doc: DocId,
    offsets: Vec<OffsetIndex>, // TODO have a better index.
    written: u64,
    writer: WritePtr,
    intermediary_buffer: Vec<u8>,
    current_block: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OffsetIndex(DocId, u64);

impl BinarySerializable for OffsetIndex {

    const SIZE: Size = Size::Constant(4 + 8);

    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        let OffsetIndex(a, b) = *self;
        Ok(try!(a.serialize(writer)) + try!(b.serialize(writer)))
    }
    fn deserialize(reader: &mut Read) -> io::Result<OffsetIndex> {
        let a = try!(DocId::deserialize(reader));
        let b = try!(u64::deserialize(reader));
        Ok(OffsetIndex(a, b))
    }
}

impl StoreWriter {

    pub fn new(writer: WritePtr) -> StoreWriter {
        StoreWriter {
            doc: 0,
            written: 0,
            offsets: Vec::new(),
            writer: writer,
            intermediary_buffer: Vec::new(),
            current_block: Vec::new(),
        }
    }

    pub fn store<'a>(&mut self, field_values: &Vec<&'a FieldValue>) {
        self.intermediary_buffer.clear();
        (field_values.len() as u32).serialize(&mut self.intermediary_buffer);
        for field_value in field_values.iter() {
            (*field_value).serialize(&mut self.intermediary_buffer);
        }
        (self.intermediary_buffer.len() as u32).serialize(&mut self.current_block);
        self.current_block.write_all(&self.intermediary_buffer[..]);
        self.doc += 1;
        if self.current_block.len() > BLOCK_SIZE {
            self.write_and_compress_block();
        }
    }

    fn write_and_compress_block(&mut self,) -> io::Result<()> {
        // err handling
        self.intermediary_buffer.clear();
        {
            let mut encoder = lz4::EncoderBuilder::new()
                    .build(&mut self.intermediary_buffer)
                    .unwrap();
            try!(encoder.write_all(&self.current_block));
            let (_, encoder_result) = encoder.finish();
            try!(encoder_result);
        }
        let compressed_block_size = self.intermediary_buffer.len() as u64;
        self.written += try!((compressed_block_size as u32).serialize(&mut self.writer)) as u64;
        try!(self.writer.write_all(&self.intermediary_buffer));
        self.written += compressed_block_size;
        self.offsets.push(OffsetIndex(self.doc, self.written));
        self.current_block.clear();
        Ok(())
    }

    pub fn close(&mut self,) -> io::Result<()> {
        if self.current_block.len() > 0 {
            self.write_and_compress_block();
        }
        let header_offset: u64 = self.written;
        try!(self.offsets.serialize(&mut self.writer));
        try!(header_offset.serialize(&mut self.writer));
        self.writer.flush()
    }

}


pub struct StoreReader {
    data: ReadOnlySource,
    offsets: Vec<OffsetIndex>,
    current_block: RefCell<Vec<u8>>,
}

impl StoreReader {

    pub fn num_docs(&self,) -> DocId {
        self.offsets.len() as DocId
    }

    fn read_header(data: &ReadOnlySource) -> Vec<OffsetIndex> {
        // todo err
        let mut cursor = Cursor::new(data.as_slice());
        cursor.seek(SeekFrom::End(-8)).unwrap();
        let offset = u64::deserialize(&mut cursor).unwrap();
        cursor.seek(SeekFrom::Start(offset)).unwrap();
        Vec::deserialize(&mut cursor).unwrap()
    }

    fn block_offset(&self, doc_id: &DocId) -> OffsetIndex {
        let mut offset = OffsetIndex(0, 0);
        for &OffsetIndex(first_doc_id, block_offset) in self.offsets.iter() {
            if first_doc_id > *doc_id {
                break;
            }
            else {
                offset = OffsetIndex(first_doc_id, block_offset);
            }
        }
        return offset;
    }

    fn read_block(&self, block_offset: usize) {
        let mut current_block_mut = self.current_block.borrow_mut();
        current_block_mut.clear();
        let total_buffer = self.data.as_slice();
        let mut cursor = Cursor::new(&total_buffer[block_offset..]);
        let block_length = u32::deserialize(&mut cursor).unwrap();
        let block_array: &[u8] = &total_buffer[(block_offset + 4 as usize)..(block_offset + 4 + block_length as usize)];
        let mut lz4_decoder = lz4::Decoder::new(Cursor::new(block_array)).unwrap();
        lz4_decoder.read_to_end(&mut current_block_mut);
    }

    pub fn get(&self, doc_id: &DocId) -> io::Result<Document> {
        let OffsetIndex(first_doc_id, block_offset) = self.block_offset(doc_id);
        self.read_block(block_offset as usize);
        let mut current_block_mut = self.current_block.borrow_mut();
        let mut cursor = Cursor::new(&mut current_block_mut[..]);
        for _ in first_doc_id..*doc_id  {
            let block_length = try!(u32::deserialize(&mut cursor));
            cursor.seek(SeekFrom::Current(block_length as i64));
        }
        try!(u32::deserialize(&mut cursor));
        let mut field_values = Vec::new();
        let num_fields = u32::deserialize(&mut cursor).unwrap();
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


#[cfg(test)]
mod tests {

    use super::*;
    use test::Bencher;
    use std::path::PathBuf;
    use core::schema::Schema;
    use core::schema::FieldOptions;
    use core::schema::FieldValue;
    use core::directory::{RAMDirectory, Directory, MmapDirectory, WritePtr};

    fn write_lorem_ipsum_store(writer: WritePtr) -> Schema {
        let mut schema = Schema::new();
        let field_body = schema.add_field("body", &FieldOptions::new().set_stored());
        let field_title = schema.add_field("title", &FieldOptions::new().set_stored());
        let lorem = String::from("Doc Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.");
        {
            let mut store_writer = StoreWriter::new(writer);
            for i in 0..1000 {
                let mut fields: Vec<FieldValue> = Vec::new();
                {
                    let field_value = FieldValue {
                        field: field_body.clone(),
                        text: lorem.clone(),
                    };
                    fields.push(field_value);
                }
                {
                    let title_text = format!("Doc {}", i);
                    let field_value = FieldValue {
                        field: field_title.clone(),
                        text: title_text,
                    };
                    fields.push(field_value);
                }
                let fields_refs: Vec<&FieldValue> = fields.iter().collect();
                store_writer.store(&fields_refs);
            }
            store_writer.close();
        }
        schema
    }


    #[test]
    fn test_store() {
        let path = PathBuf::from("store");
        let mut directory = RAMDirectory::create();
        let store_file = directory.open_write(&path).unwrap();
        let schema = write_lorem_ipsum_store(store_file);
        let field_title = schema.field("title").unwrap();
        let store_source = directory.open_read(&path).unwrap();
        let store = StoreReader::new(store_source);
        for i in (0..10).map(|i| i * 3 / 2) {
            assert_eq!(*store.get(&i).unwrap().get_one(&field_title).unwrap(), format!("Doc {}", i));
        }
    }

    #[bench]
    fn bench_store_encode(b: &mut Bencher) {
        let mut directory = MmapDirectory::create_from_tempdir().unwrap();
        let path = PathBuf::from("store");
        b.iter(|| {
            write_lorem_ipsum_store(directory.open_write(&path).unwrap());
        });
    }


    #[bench]
    fn bench_store_decode(b: &mut Bencher) {
        let mut directory = MmapDirectory::create_from_tempdir().unwrap();
        let path = PathBuf::from("store");
        write_lorem_ipsum_store(directory.open_write(&path).unwrap());
        let store_source = directory.open_read(&path).unwrap();
        let store = StoreReader::new(store_source);
        b.iter(|| {
            store.get(&12);
        });

    }
}
