use crate::common::{BinarySerializable, VInt};
use crate::directory::ReadOnlySource;
use std::cmp::max;
use std::io::{Seek, SeekFrom};
use std::marker::PhantomData;

struct Layer<T> {
    data: ReadOnlySource,
    cursor: ReadOnlySource,
    next_id: Option<u64>,
    _phantom_: PhantomData<T>,
}

impl<T: BinarySerializable> Iterator for Layer<T> {
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

impl<T: BinarySerializable> From<ReadOnlySource> for Layer<T> {
    fn from(data: ReadOnlySource) -> Layer<T> {
        let mut cursor = data.clone();
        let next_id = VInt::deserialize_u64(&mut cursor).ok();
        Layer {
            data,
            cursor,
            next_id,
            _phantom_: PhantomData,
        }
    }
}

impl<T: BinarySerializable> Layer<T> {
    fn empty() -> Layer<T> {
        let data = ReadOnlySource::empty();
        Layer {
            data: data.clone(),
            cursor: data,
            next_id: None,
            _phantom_: PhantomData,
        }
    }

    fn seek_offset(&mut self, offset: usize) {
        // self.cursor = &self.data[offset..];
        self.cursor = self.data.slice_from(offset);
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

pub struct SkipList<T: BinarySerializable> {
    data_layer: Layer<T>,
    skip_layers: Vec<Layer<u64>>,
}

impl<T: BinarySerializable> Iterator for SkipList<T> {
    type Item = (u64, T);

    fn next(&mut self) -> Option<(u64, T)> {
        self.data_layer.next()
    }
}

impl<T: BinarySerializable> SkipList<T> {
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

impl<T: BinarySerializable> From<ReadOnlySource> for SkipList<T> {
    fn from(mut data: ReadOnlySource) -> SkipList<T> {
        let data_pos = data
            .seek(SeekFrom::Current(0))
            .expect("Can't seek in skiplist");
        let offsets: Vec<u64> = Vec::<VInt>::deserialize(&mut data)
            .unwrap()
            .into_iter()
            .map(|el| el.0)
            .collect();
        let num_layers = offsets.len();

        let new_pos = data
            .seek(SeekFrom::Current(0))
            .expect("Can't seek in skiplist");
        let slice_length = new_pos - data_pos;
        let layers_data = data.slice_from(slice_length as usize);

        let data_layer: Layer<T> = if num_layers == 0 {
            Layer::empty()
        } else {
            let first_layer_data = layers_data.slice_to(offsets[0] as usize);
            Layer::from(first_layer_data)
        };

        let skip_layers: Vec<Layer<u64>> = (0..max(1, num_layers) - 1)
            .map(|i| (offsets[i] as usize, offsets[i + 1] as usize))
            .map(|(start, stop)| Layer::from(layers_data.slice(start, stop)))
            .collect();

        SkipList {
            skip_layers,
            data_layer,
        }
    }
}

impl<T: BinarySerializable> From<Vec<u8>> for SkipList<T> {
    fn from(data: Vec<u8>) -> SkipList<T> {
        SkipList::from(ReadOnlySource::from(data))
    }
}
