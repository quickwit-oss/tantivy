use common::BinarySerializable;
use std::marker::PhantomData;
use DocId;
use std::cmp::max;

static EMPTY: [u8; 0] = [];

struct Layer<'a, T> {
    data: &'a [u8],
    cursor: &'a [u8],
    next_id: DocId,
    _phantom_: PhantomData<T>,
}

impl<'a, T: BinarySerializable> Iterator for Layer<'a, T> {

    type Item = (DocId, T);

    fn next(&mut self,)-> Option<(DocId, T)> {
        if self.next_id == u32::max_value() {
            None
        }
        else {
            let cur_val = T::deserialize(&mut self.cursor).unwrap();
            let cur_id = self.next_id;
            self.next_id = u32::deserialize(&mut self.cursor).unwrap_or(u32::max_value());
            Some((cur_id, cur_val))
        }
    }
}

impl<'a, T: BinarySerializable> From<&'a [u8]> for Layer<'a, T> {
    fn from(data: &'a [u8]) -> Layer<'a, T> {
        let mut cursor = data; 
        let next_id = u32::deserialize(&mut cursor).unwrap_or(u32::max_value());
        Layer {
            data: data,
            cursor: cursor,
            next_id: next_id,
            _phantom_: PhantomData,
        }
    }
}

impl<'a, T: BinarySerializable> Layer<'a, T> {

    fn empty() -> Layer<'a, T> {
        Layer {
            data: &EMPTY,
            cursor: &EMPTY,
            next_id: DocId::max_value(),
            _phantom_: PhantomData,
        }
    }

    fn seek_offset(&mut self, offset: usize)  {
        self.cursor = &self.data[offset..];
        self.next_id = u32::deserialize(&mut self.cursor).unwrap_or(u32::max_value());
    }
    
    // Returns the last element (key, val)
    // such that (key < doc_id)
    //
    // If there is no such element anymore,
    // returns None.
    fn seek(&mut self, doc_id: DocId) -> Option<(DocId, T)> {
        let mut val = None;
        while self.next_id < doc_id {
            match self.next() {
                None => { break; },
                v => { val = v; }
            }
        }
        val
    }
}


pub struct SkipList<'a, T: BinarySerializable> {
    data_layer: Layer<'a, T>,
    skip_layers: Vec<Layer<'a, u32>>,
}

impl<'a, T: BinarySerializable> Iterator for SkipList<'a, T> {

    type Item = (DocId, T);

    fn next(&mut self,)-> Option<(DocId, T)> {
        self.data_layer.next()
    }
}

impl<'a, T: BinarySerializable> SkipList<'a, T> {

    pub fn seek(&mut self, doc_id: DocId) -> Option<(DocId, T)> {
        let mut next_layer_skip: Option<(DocId, u32)> = None;
        for skip_layer in &mut self.skip_layers {
            if let Some((_, offset)) = next_layer_skip {
                skip_layer.seek_offset(offset as usize);
            }
            next_layer_skip = skip_layer.seek(doc_id);
         }
         if let Some((_, offset)) = next_layer_skip {
             self.data_layer.seek_offset(offset as usize);
         }
         self.data_layer.seek(doc_id)
    }

    
}


impl<'a, T: BinarySerializable> From<&'a [u8]> for SkipList<'a, T> {
    
    fn from(mut data: &'a [u8]) -> SkipList<'a, T> {
        let offsets: Vec<u32> = Vec::deserialize(&mut data).unwrap();
        let num_layers = offsets.len();
        let layers_data: &[u8] = data;
        let data_layer: Layer<'a, T> =
            if num_layers == 0 { Layer::empty() }
            else {
                let first_layer_data: &[u8] = &layers_data[..offsets[0] as usize];
                Layer::from(first_layer_data)
            };
        let skip_layers = (0..max(1, num_layers) - 1)
            .map(|i| (offsets[i] as usize, offsets[i + 1] as usize))
            .map(|(start, stop)| {
                Layer::from(&layers_data[start..stop])
            })
            .collect();
        SkipList {
            skip_layers: skip_layers,
            data_layer: data_layer,
        }
    }

}
