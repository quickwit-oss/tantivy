use std::io::Write;
use common::BinarySerializable;
use std::marker::PhantomData;
use DocId;
use std::io;

struct LayerBuilder<T: BinarySerializable> {
    period: usize,
    buffer: Vec<u8>,
    remaining: usize,
    len: usize,
    _phantom_: PhantomData<T>,
}

impl<T: BinarySerializable> LayerBuilder<T> {
    fn written_size(&self) -> usize {
        self.buffer.len()
    }

    fn write(&self, output: &mut Write) -> Result<(), io::Error> {
        output.write_all(&self.buffer)?;
        Ok(())
    }

    fn with_period(period: usize) -> LayerBuilder<T> {
        LayerBuilder {
            period: period,
            buffer: Vec::new(),
            remaining: period,
            len: 0,
            _phantom_: PhantomData,
        }
    }

    fn insert(&mut self, doc_id: DocId, value: &T) -> io::Result<Option<(DocId, u32)>> {
        self.remaining -= 1;
        self.len += 1;
        let offset = self.written_size() as u32;
        doc_id.serialize(&mut self.buffer)?;
        value.serialize(&mut self.buffer)?;
        Ok(if self.remaining == 0 {
            self.remaining = self.period;
            Some((doc_id, offset))
        } else {
            None
        })
    }
}


pub struct SkipListBuilder<T: BinarySerializable> {
    period: usize,
    data_layer: LayerBuilder<T>,
    skip_layers: Vec<LayerBuilder<u32>>,
}


impl<T: BinarySerializable> SkipListBuilder<T> {
    pub fn new(period: usize) -> SkipListBuilder<T> {
        SkipListBuilder {
            period: period,
            data_layer: LayerBuilder::with_period(period),
            skip_layers: Vec::new(),
        }
    }

    fn get_skip_layer(&mut self, layer_id: usize) -> &mut LayerBuilder<u32> {
        if layer_id == self.skip_layers.len() {
            let layer_builder = LayerBuilder::with_period(self.period);
            self.skip_layers.push(layer_builder);
        }
        &mut self.skip_layers[layer_id]
    }

    pub fn insert(&mut self, doc_id: DocId, dest: &T) -> io::Result<()> {
        let mut layer_id = 0;
        let mut skip_pointer = try!(self.data_layer.insert(doc_id, dest));
        loop {
            skip_pointer = match skip_pointer {
                Some((skip_doc_id, skip_offset)) => {
                    try!(self.get_skip_layer(layer_id).insert(
                        skip_doc_id,
                        &skip_offset,
                    ))
                }
                None => {
                    return Ok(());
                }
            };
            layer_id += 1;
        }
    }

    pub fn write<W: Write>(self, output: &mut W) -> io::Result<()> {
        let mut size: u32 = 0;
        let mut layer_sizes: Vec<u32> = Vec::new();
        size += self.data_layer.buffer.len() as u32;
        layer_sizes.push(size);
        for layer in self.skip_layers.iter().rev() {
            size += layer.buffer.len() as u32;
            layer_sizes.push(size);
        }
        layer_sizes.serialize(output)?;
        self.data_layer.write(output)?;
        for layer in self.skip_layers.iter().rev() {
            layer.write(output)?;
        }
        Ok(())
    }
}
