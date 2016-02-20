use std::io::BufWriter;
use std::fs::File;
use core::schema::Document;
use core::schema::FieldValue;

pub struct StoreWriter {
    writer: BufWriter<File>,
}

impl StoreWriter {

    pub fn new(file: File) -> StoreWriter {
        StoreWriter {
            writer: BufWriter::new(file),
        }
    }

    pub fn store(&mut self, fields: &mut Iterator<Item=&FieldValue>) {
        for field in fields {
            println!("{:?}", field);
        }
    }
}
