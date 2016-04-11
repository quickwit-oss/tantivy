use core::directory::WritePtr;
use std::cell::RefCell;
use core::schema::DocId;
use core::schema::Document;
use core::schema::TextFieldValue;
use core::serialize::BinarySerializable;
use core::directory::ReadOnlySource;
use std::io::Write;
use std::io::Read;
use std::io::Cursor;
use std::io;
use std::io::SeekFrom;
use std::io::Seek;
use std::cmp::Ordering;
use lz4;

// TODO cache uncompressed pages

const BLOCK_SIZE: usize = 131_072;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
struct OffsetIndex(DocId, u64);

pub struct StoreWriter {
    doc: DocId,
    offsets: Vec<OffsetIndex>, // TODO have a better index.
    written: u64,
    writer: WritePtr,
    intermediary_buffer: Vec<u8>,
    current_block: Vec<u8>,
}

impl BinarySerializable for OffsetIndex {
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

    pub fn stack_reader(&mut self, reader: &StoreReader) -> io::Result<()> {
        if self.current_block.len() > 0 {
            try!(self.write_and_compress_block());
        }
        match reader.offsets.last() {
            Some(&OffsetIndex(ref num_docs, ref body_size)) => {
                try!(self.writer.write_all(&reader.data.as_slice()[0..*body_size as usize]));
                for &OffsetIndex(doc, offset) in reader.offsets.iter() {
                    self.offsets.push(OffsetIndex(self.doc + doc, self.written + offset));
                }
                self.written += *body_size;
                self.doc += *num_docs;
                Ok(())
            },
            None => {
                Err(io::Error::new(io::ErrorKind::Other, "No offset for reader"))
            }
        }
    }

    pub fn store<'a>(&mut self, field_values: &Vec<&'a TextFieldValue>) -> io::Result<()> {
        self.intermediary_buffer.clear();
        try!((field_values.len() as u32).serialize(&mut self.intermediary_buffer));
        for field_value in field_values.iter() {
            try!((*field_value).serialize(&mut self.intermediary_buffer));
        }
        try!((self.intermediary_buffer.len() as u32).serialize(&mut self.current_block));
        try!(self.current_block.write_all(&self.intermediary_buffer[..]));
        self.doc += 1;
        if self.current_block.len() > BLOCK_SIZE {
            try!(self.write_and_compress_block());
        }
        Ok(())
    }

    fn write_and_compress_block(&mut self,) -> io::Result<()> {
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
            try!(self.write_and_compress_block());
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

    fn block_offset(&self, seek: &DocId) -> OffsetIndex {
        fn search(offsets: &[OffsetIndex], seek: &DocId) -> OffsetIndex {
            let m = offsets.len() / 2;
            let pivot_offset = &offsets[m];
            if offsets.len() <= 1 {
                return pivot_offset.clone()
            }
            match pivot_offset.0.cmp(seek) {
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

    pub fn get(&self, doc_id: &DocId) -> io::Result<Document> {
        let OffsetIndex(first_doc_id, block_offset) = self.block_offset(doc_id);
        try!(self.read_block(block_offset as usize));
        let mut current_block_mut = self.current_block.borrow_mut();
        let mut cursor = Cursor::new(&mut current_block_mut[..]);
        for _ in first_doc_id..*doc_id  {
            let block_length = try!(u32::deserialize(&mut cursor));
            try!(cursor.seek(SeekFrom::Current(block_length as i64)));
        }
        try!(u32::deserialize(&mut cursor));
        let mut text_field_values = Vec::new();
        let num_fields = try!(u32::deserialize(&mut cursor));
        for _ in 0..num_fields {
            let text_field_value = try!(TextFieldValue::deserialize(&mut cursor));
            text_field_values.push(text_field_value);
        }
        let u32_field_values = Vec::new();
        Ok(Document {
            text_field_values: text_field_values,
            u32_field_values: u32_field_values,
        })
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
    use core::schema::TextOptions;
    use core::schema::TextFieldValue;
    use core::directory::{RAMDirectory, Directory, MmapDirectory, WritePtr};

    fn write_lorem_ipsum_store(writer: WritePtr) -> Schema {
        let mut schema = Schema::new();
        let field_body = schema.add_text_field("body", &TextOptions::new().set_stored());
        let field_title = schema.add_text_field("title", &TextOptions::new().set_stored());
        let lorem = String::from("Doc Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.");
        {
            let mut store_writer = StoreWriter::new(writer);
            for i in 0..1000 {
                let mut fields: Vec<TextFieldValue> = Vec::new();
                {
                    let field_value = TextFieldValue {
                        field: field_body.clone(),
                        text: lorem.clone(),
                    };
                    fields.push(field_value);
                }
                {
                    let title_text = format!("Doc {}", i);
                    let field_value = TextFieldValue {
                        field: field_title.clone(),
                        text: title_text,
                    };
                    fields.push(field_value);
                }
                let fields_refs: Vec<&TextFieldValue> = fields.iter().collect();
                store_writer.store(&fields_refs).unwrap();
            }
            store_writer.close().unwrap();
        }
        schema
    }


    #[test]
    fn test_store() {
        let path = PathBuf::from("store");
        let mut directory = RAMDirectory::create();
        let store_file = directory.open_write(&path).unwrap();
        let schema = write_lorem_ipsum_store(store_file);
        let field_title = schema.text_field("title");
        let store_source = directory.open_read(&path).unwrap();
        let store = StoreReader::new(store_source);
        for i in (0..10).map(|i| i * 3 / 2) {
            assert_eq!(*store.get(&i).unwrap().get_first_text(&field_title).unwrap(), format!("Doc {}", i));
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
            store.get(&12).unwrap();
        });

    }
}
