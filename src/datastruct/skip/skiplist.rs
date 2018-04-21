use common::{BinarySerializable, VInt};
use std::cmp::max;
use std::marker::PhantomData;

static EMPTY: [u8; 0] = [];

struct Layer<'a, T> {
    data: &'a [u8],
    cursor: &'a [u8],
    next_id: Option<u64>,
    _phantom_: PhantomData<T>,
}

impl<'a, T: BinarySerializable> Iterator for Layer<'a, T> {
    type Item = (u64, T);

    fn next(&mut self) -> Option<(u64, T)> {
        if let Some(cur_id) = self.next_id {
            let cur_val = T::deserialize(&mut self.cursor).unwrap();
            self.next_id = VInt::deserialize_u64(&mut self.cursor).ok();
            Some((cur_id, cur_val))
        } else {
            None
        }
    }
}

impl<'a, T: BinarySerializable> From<&'a [u8]> for Layer<'a, T> {
    fn from(data: &'a [u8]) -> Layer<'a, T> {
        let mut cursor = data;
        let next_id = VInt::deserialize_u64(&mut cursor).ok();
        Layer {
            data,
            cursor,
            next_id,
            _phantom_: PhantomData,
        }
    }
}

impl<'a, T: BinarySerializable> Layer<'a, T> {
    fn empty() -> Layer<'a, T> {
        Layer {
            data: &EMPTY,
            cursor: &EMPTY,
            next_id: None,
            _phantom_: PhantomData,
        }
    }

    fn seek_offset(&mut self, offset: usize) {
        self.cursor = &self.data[offset..];
        self.next_id = VInt::deserialize_u64(&mut self.cursor).ok();
    }

    // Returns the last element (key, val)
    // such that (key < doc_id)
    //
    // If there is no such element anymore,
    // returns None.
    //
    // If the element exists, it will be returned
    // at the next call to `.next()`.
    fn seek(&mut self, key: u64) -> Option<(u64, T)> {
        let mut result: Option<(u64, T)> = None;
        loop {
            if let Some(next_id) = self.next_id {
                if next_id < key {
                    if let Some(v) = self.next() {
                        result = Some(v);
                        continue;
                    }
                }
            }
            return result;
        }
    }
}

pub struct SkipList<'a, T: BinarySerializable> {
    data_layer: Layer<'a, T>,
    skip_layers: Vec<Layer<'a, u64>>,
}

impl<'a, T: BinarySerializable> Iterator for SkipList<'a, T> {
    type Item = (u64, T);

    fn next(&mut self) -> Option<(u64, T)> {
        self.data_layer.next()
    }
}

impl<'a, T: BinarySerializable> SkipList<'a, T> {
    pub fn seek(&mut self, key: u64) -> Option<(u64, T)> {
        let mut next_layer_skip: Option<(u64, u64)> = None;
        for skip_layer in &mut self.skip_layers {
            if let Some((_, offset)) = next_layer_skip {
                skip_layer.seek_offset(offset as usize);
            }
            next_layer_skip = skip_layer.seek(key);
        }
        if let Some((_, offset)) = next_layer_skip {
            self.data_layer.seek_offset(offset as usize);
        }
        self.data_layer.seek(key)
    }
}

impl<'a, T: BinarySerializable> From<&'a [u8]> for SkipList<'a, T> {
    fn from(mut data: &'a [u8]) -> SkipList<'a, T> {
        let offsets: Vec<u64> = Vec::<VInt>::deserialize(&mut data)
            .unwrap()
            .into_iter()
            .map(|el| el.0)
            .collect();
        let num_layers = offsets.len();
        let layers_data: &[u8] = data;
        let data_layer: Layer<'a, T> = if num_layers == 0 {
            Layer::empty()
        } else {
            let first_layer_data: &[u8] = &layers_data[..offsets[0] as usize];
            Layer::from(first_layer_data)
        };
        let skip_layers = (0..max(1, num_layers) - 1)
            .map(|i| (offsets[i] as usize, offsets[i + 1] as usize))
            .map(|(start, stop)| Layer::from(&layers_data[start..stop]))
            .collect();
        SkipList {
            skip_layers,
            data_layer,
        }
    }
}
