use std::io::Write;
use std::io::BufWriter;
use core::DocId;
use std::ops::DerefMut;
use serde::Serialize;
use serde;
use bincode;
use byteorder;
use core::error;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};



// writer

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
