use std::io::BufWriter;
use std::fs::File;
use core::global::DocId;
use core::schema::Document;
use core::schema::FieldValue;
use core::error;
use core::serialize::BinarySerializable;
use std::io::Write;
use std::io::Read;
use lz4;

const BLOCK_SIZE: usize = 262144;

pub struct StoreWriter {
    doc: DocId,
    offsets: Vec<OffsetIndex>,
    written: u64,
    writer: BufWriter<File>,
    current_block: Vec<u8>,
}

#[derive(Debug)]
struct OffsetIndex(DocId, u64);

impl BinarySerializable for OffsetIndex {
    fn serialize(&self, writer: &mut Write) -> error::Result<usize> {
        let OffsetIndex(a, b) = *self;
        Ok(try!(a.serialize(writer)) + try!(b.serialize(writer)))
    }
    fn deserialize(reader: &mut Read) -> error::Result<OffsetIndex> {
        let a = try!(DocId::deserialize(reader));
        let b = try!(u64::deserialize(reader));
        Ok(OffsetIndex(a, b))
    }
}



impl StoreWriter {

    pub fn new(file: File) -> StoreWriter {
        StoreWriter {
            doc: 0,
            written: 0,
            offsets: Vec::new(),
            writer: BufWriter::new(file),
            current_block: Vec::new(),
        }
    }

    pub fn store(&mut self, field_values: &Vec<&FieldValue>) {
        (field_values.len() as u32).serialize(&mut self.current_block);
        for field_value in field_values.iter() {
            (*field_value).serialize(&mut self.current_block);
        }
        self.doc += 1;
        if self.current_block.len() > BLOCK_SIZE {
            self.write_and_compress_block();
        }
    }

    fn write_and_compress_block(&mut self,) {
        // err handling
        let mut encoder = lz4::EncoderBuilder::new()
                .build(&mut self.writer)
                .unwrap();
        encoder.write_all(&self.current_block);
        self.written = self.current_block.len() as u64;
        self.offsets.push(OffsetIndex(self.doc, self.written));
        self.current_block.clear();
    }

    pub fn close(&mut self,) {
        if self.current_block.len() > 0 {
            self.write_and_compress_block();
        }
    }

}




// impl<T: BinarySerializable> BinarySerializable for Vec<T> {
//     fn serialize(&self, field_values: &mut Iterator<Item=&FieldValue>) -> error::Result<usize> {
//         let mut total_size = 0;
//         writer.write_u32::<BigEndian>(self.len() as u32);
//         total_size += 4;
//         for it in self.iter() {
//             let item_size = try!(it.serialize(writer));
//             total_size += item_size;
//         }
//         Ok(total_size)
//     }
// }
