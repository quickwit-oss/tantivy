use std::io::BufWriter;
use std::fs::File;
use core::schema::Document;

pub struct StoreWriter {
    writer: BufWriter<File>,
}

impl StoreWriter {

    pub fn new(file: File) -> StoreWriter {
        StoreWriter {
            writer: BufWriter::new(file),
        }
    }

    pub fn store(doc: &Document) {

    }
}
