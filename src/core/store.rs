use std::io::BufWriter;
use std::fs::File;
use core::schema::Document;
use core::schema::FieldValue;
use core::error;
use core::serialize::BinarySerializable;
use std::io::Write;
use std::io::Read;

pub struct StoreWriter {
    doc: usize,
    offsets: Vec<usize>,
    writer: BufWriter<File>,
}

impl StoreWriter {

    pub fn new(file: File) -> StoreWriter {
        StoreWriter {
            doc: 0,
            offsets: Vec::new(),
            writer: BufWriter::new(file),
        }
    }

    pub fn store(&mut self, fields: &Vec<&FieldValue>) {
        
        self.doc += 1;
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
