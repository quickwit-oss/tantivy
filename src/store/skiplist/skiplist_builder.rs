use common::{is_power_of_2, BinarySerializable, VInt};
use std::io;
use std::io::Write;
use std::marker::PhantomData;

struct LayerBuilder<T: BinarySerializable> {
    period_mask: usize,
    buffer: Vec<u8>,
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
        assert!(is_power_of_2(period), "The period has to be a power of 2.");
        LayerBuilder {
            period_mask: (period - 1),
            buffer: Vec::new(),
            len: 0,
            _phantom_: PhantomData,
        }
    }

    fn insert(&mut self, key: u64, value: &T) -> io::Result<Option<(u64, u64)>> {
        self.len += 1;
        let offset = self.written_size() as u64;
        VInt(key).serialize_into_vec(&mut self.buffer);
        value.serialize(&mut self.buffer)?;
        let emit_skip_info = (self.period_mask & self.len) == 0;
        if emit_skip_info {
            Ok(Some((key, offset)))
        } else {
            Ok(None)
        }
    }
}

pub struct SkipListBuilder<T: BinarySerializable> {
    period: usize,
    data_layer: LayerBuilder<T>,
    skip_layers: Vec<LayerBuilder<u64>>,
}

impl<T: BinarySerializable> SkipListBuilder<T> {
    pub fn new(period: usize) -> SkipListBuilder<T> {
        SkipListBuilder {
            period,
            data_layer: LayerBuilder::with_period(period),
            skip_layers: Vec::new(),
        }
    }

    fn get_skip_layer(&mut self, layer_id: usize) -> &mut LayerBuilder<u64> {
        if layer_id == self.skip_layers.len() {
            let layer_builder = LayerBuilder::with_period(self.period);
            self.skip_layers.push(layer_builder);
        }
        &mut self.skip_layers[layer_id]
    }

    pub fn insert(&mut self, key: u64, dest: &T) -> io::Result<()> {
        let mut layer_id = 0;
        let mut skip_pointer = self.data_layer.insert(key, dest)?;
        loop {
            skip_pointer = match skip_pointer {
                Some((skip_doc_id, skip_offset)) => self.get_skip_layer(layer_id)
                    .insert(skip_doc_id, &skip_offset)?,
                None => {
                    return Ok(());
                }
            };
            layer_id += 1;
        }
    }

    pub fn write<W: Write>(self, output: &mut W) -> io::Result<()> {
        let mut size: u64 = self.data_layer.buffer.len() as u64;
        let mut layer_sizes = vec![VInt(size)];
        for layer in self.skip_layers.iter().rev() {
            size += layer.buffer.len() as u64;
            layer_sizes.push(VInt(size));
        }
        layer_sizes.serialize(output)?;
        self.data_layer.write(output)?;
        for layer in self.skip_layers.iter().rev() {
            layer.write(output)?;
        }
        Ok(())
    }
}
