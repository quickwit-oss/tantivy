use std::io::Write;
use std::io::BufWriter;
use std::io::Read;
use std::io::Cursor;
use std::io::SeekFrom;
use std::io::Seek;
use core::DocId;
use std::ops::DerefMut;
use serde::Serialize;
use serde;
use bincode;
use byteorder;
use core::error;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};


struct LayerBuilder {
    period: usize,
    buffer: Vec<u8>,
    remaining: usize,
    len: usize,
}

impl LayerBuilder {

    fn written_size(&self,) -> usize {
        self.buffer.len()
    }

    fn write(&self, output: &mut Write) -> Result<(), byteorder::Error> {
        try!(output.write_u32::<BigEndian>(self.len() as u32));
        try!(output.write_u32::<BigEndian>(self.buffer.len() as u32));
        try!(output.write_all(&self.buffer));
        Ok(())
    }

    fn len(&self,) -> usize {
        self.len
    }

    fn with_period(period: usize) -> LayerBuilder {
        LayerBuilder {
            period: period,
            buffer: Vec::new(),
            remaining: period,
            len: 0,
        }
    }

    fn insert<S: Serialize>(&mut self, doc_id: DocId, dest: S) -> InsertResult {
        self.remaining -= 1;
        self.len += 1;
        if self.remaining == 0 {
            let offset = self.written_size();
            dest.serialize(&mut bincode::serde::Serializer::new(&mut self.buffer));
            self.remaining = self.period;
            InsertResult::SkipPointer(offset)
        }
        else {
            doc_id.serialize(&mut bincode::serde::Serializer::new(&mut self.buffer));
            dest.serialize(&mut bincode::serde::Serializer::new(&mut self.buffer));
            InsertResult::NoNeedForSkip
        }
    }
}


pub struct SkipListBuilder {
    period: usize,
    layers: Vec<LayerBuilder>,
}

enum InsertResult {
    SkipPointer(usize),
    NoNeedForSkip,
}

impl SkipListBuilder {

    pub fn new(period: usize) -> SkipListBuilder {
        SkipListBuilder {
            period: period,
            layers: Vec::new(),
        }
    }


    fn get_layer<'a>(&'a mut self, layer_id: usize) -> &mut LayerBuilder {
        if layer_id == self.layers.len() {
            let layer_builder = LayerBuilder::with_period(self.period);
            self.layers.push(layer_builder);
        }
        &mut self.layers[layer_id]
    }

    pub fn insert<S: Serialize>(&mut self, doc_id: DocId, dest: S) {
        let mut layer_id = 0;
        match self.get_layer(0).insert(doc_id, dest) {
            InsertResult::SkipPointer(mut offset) => {
                loop {
                    layer_id += 1;
                    let skip_result = self.get_layer(layer_id)
                        .insert(doc_id, offset);
                    match skip_result {
                        InsertResult::SkipPointer(next_offset) => {
                            offset = next_offset;
                        },
                        InsertResult::NoNeedForSkip => {
                            return;
                        }
                    }
                }
            },
            InsertResult::NoNeedForSkip => {
                return;
            }
        }
    }

    pub fn write<W: Write>(self, output: &mut Write) -> error::Result<()> {
        output.write_u8(self.layers.len() as u8);
        for layer in self.layers.iter() {
            match layer.write(output) {
                Ok(())=> {},
                Err(someerr)=> { return Err(error::Error::WriteError(format!("Could not write skiplist {:?}", someerr) )) }
            }
        }
        Ok(())
    }
}


// ---------------------------


struct Layer<R: Read + Seek> {
    reader: R,
    num_items: u32,
}

impl<R: Read + Seek + Clone> Layer<R> {
    fn read(reader: &mut R) -> Layer<R> {
        // TODO error handling?
        let num_items = reader.read_u32::<BigEndian>().unwrap() as u32;
        let num_bytes = reader.read_u32::<BigEndian>().unwrap() as u32;
        let reader_clone = reader.clone();
        reader.seek(SeekFrom::Current(num_bytes as i64));
        Layer {
            reader: reader_clone,
            num_items: num_items,
        }
    }
}

pub struct SkipList<R: Read + Seek> {
    layers: Vec<Layer<R>>,
}

impl<R: Read + Seek> SkipList<R> {

    pub fn read(data: &[u8]) -> SkipList<Cursor<&[u8]>> {
        let mut cursor = Cursor::new(data);
        // TODO error handling?
        let num_layers = cursor.read_u8().unwrap();
        let mut layers = Vec::new();
        for _ in (0..num_layers) {
            layers.push(Layer::read(&mut cursor));
        }
        SkipList {
            layers: layers
        }
    }

}
