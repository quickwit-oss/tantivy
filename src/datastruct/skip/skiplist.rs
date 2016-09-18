use common::BinarySerializable;
use std::io::Cursor;
use std::io::SeekFrom;
use std::io::Seek;
use std::marker::PhantomData;
use DocId;


struct Layer<'a, T> {
    cursor: Cursor<&'a [u8]>,
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
            self.next_id =
                match u32::deserialize(&mut self.cursor) {
                    Ok(val) => val,
                    Err(_) => u32::max_value()
                };
            Some((cur_id, cur_val))
        }
    }
}


static EMPTY: [u8; 0] = [];

impl<'a, T: BinarySerializable> Layer<'a, T> {

    fn read(mut cursor: Cursor<&'a [u8]>) -> Layer<'a, T> {
        // TODO error handling?
        let next_id = match u32::deserialize(&mut cursor) {
            Ok(val) => val,
            Err(_) => u32::max_value(),
        };
        Layer {
            cursor: cursor,
            next_id: next_id,
            _phantom_: PhantomData,
        }
    }

    fn empty() -> Layer<'a, T> {
        Layer {
            cursor: Cursor::new(&EMPTY),
            next_id: DocId::max_value(),
            _phantom_: PhantomData,
        }
    }


    fn seek_offset(&mut self, offset: usize)  {
        self.cursor.seek(SeekFrom::Start(offset as u64)).unwrap();
        self.next_id = match u32::deserialize(&mut self.cursor) {
            Ok(val) => val,
            Err(_) => u32::max_value(),
        };
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
        for skip_layer_id in 0..self.skip_layers.len() {
            let mut skip_layer: &mut Layer<'a, u32> = &mut self.skip_layers[skip_layer_id];
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

    pub fn read(data: &'a [u8]) -> SkipList<'a, T> {
        let mut cursor = Cursor::new(data);
        let offsets: Vec<u32> = Vec::deserialize(&mut cursor).unwrap();
        let num_layers = offsets.len();
        let start_position = cursor.position() as usize;
        let layers_data: &[u8] = &data[start_position..data.len()];
        let data_layer: Layer<'a, T> =
            if num_layers == 0 { Layer::empty() }
            else {
                let first_layer_data: &[u8] = &layers_data[..offsets[0] as usize];
                let first_layer_cursor = Cursor::new(first_layer_data);
                Layer::read(first_layer_cursor)
            };
        let mut skip_layers =
            if num_layers > 0 {
                offsets.iter()
                    .zip(&offsets[1..])
                    .map(|(start, stop)| {
                        let layer_data: &[u8] = &layers_data[*start as usize..*stop as usize];
                        let cursor = Cursor::new(layer_data);
                        Layer::read(cursor)
                    })
                    .collect()
            }
            else {
                Vec::new()
            };
        skip_layers.reverse();
        SkipList {
            skip_layers: skip_layers,
            data_layer: data_layer,
        }
    }
}
