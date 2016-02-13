use std::io::Write;
use std::io::BufWriter;
use std::io::Read;
use std::io::Cursor;
use std::io::SeekFrom;
use std::io::Seek;
use std::marker::PhantomData;
use core::DocId;
use std::ops::DerefMut;
use bincode;
use byteorder;
use core::error;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;


pub trait BinarySerializable : fmt::Debug + Sized {
    // TODO move Result from Error.
    fn serialize(&self, writer: &mut Write) -> error::Result<usize>;
    fn deserialize(reader: &mut Read) -> error::Result<Self>;
}

impl BinarySerializable for () {
    fn serialize(&self, writer: &mut Write) -> error::Result<usize> {
        Ok(0)
    }
    fn deserialize(reader: &mut Read) -> error::Result<Self> {
        Ok(())
    }
}

impl<T: BinarySerializable> BinarySerializable for Vec<T> {
    fn serialize(&self, writer: &mut Write) -> error::Result<usize> {
        let mut total_size = 0;
        writer.write_u32::<BigEndian>(self.len() as u32);
        total_size += 4;
        for it in self.iter() {
            let item_size = try!(it.serialize(writer));
            total_size += item_size;
        }
        Ok(total_size)
    }
    fn deserialize(reader: &mut Read) -> error::Result<Vec<T>> {
        // TODO error
        let num_items = reader.read_u32::<BigEndian>().unwrap();
        let mut items: Vec<T> = Vec::with_capacity(num_items as usize);
        for i in 0..num_items {
            let item = try!(T::deserialize(reader));
            items.push(item);
        }
        Ok(items)
    }
}

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

    fn insert<S: BinarySerializable>(&mut self, doc_id: DocId, value: &S) -> InsertResult {
        self.remaining -= 1;
        self.len += 1;
        let offset = self.written_size(); // TODO not sure if we want after or here
        self.buffer.write_u32::<BigEndian>(doc_id);
        value.serialize(&mut self.buffer);
        if self.remaining == 0 {
            self.remaining = self.period;
            InsertResult::SkipPointer(offset as u32)
        }
        else {
            InsertResult::NoNeedForSkip
        }
    }
}


pub struct SkipListBuilder {
    period: usize,
    layers: Vec<LayerBuilder>,
}


enum InsertResult {
    SkipPointer(u32),
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

    pub fn insert<S: BinarySerializable>(&mut self, doc_id: DocId, dest: &S) {
        let mut layer_id = 0;
        match self.get_layer(0).insert(doc_id, dest) {
            InsertResult::SkipPointer(mut offset) => {
                loop {
                    layer_id += 1;
                    let skip_result = self.get_layer(layer_id)
                        .insert(doc_id, &offset);
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
        let mut size: u32 = 0;
        let mut layer_sizes: Vec<u32> = Vec::new();
        for layer in self.layers.iter() {
            size += layer.buffer.len() as u32;
            layer_sizes.push(size);
        }
        layer_sizes.serialize(output);
        for layer in self.layers.iter() {
            match layer.write(output) {
                Ok(())=> {},
                Err(someerr)=> { return Err(error::Error::WriteError(format!("Could not write skiplist {:?}", someerr) )) }
            }
        }
        Ok(())
    }
}


impl BinarySerializable for u32 {
    fn serialize(&self, writer: &mut Write) -> error::Result<usize> {
        // TODO error handling
        writer.write_u32::<BigEndian>(self.clone());
        Ok(4)
    }

    fn deserialize(reader: &mut Read) -> error::Result<Self> {
        // TODO error handling
        Ok(reader.read_u32::<BigEndian>().unwrap())
    }
}


fn rebase_cursor<'a>(cursor: &Cursor<&'a [u8]>) -> Cursor<&'a [u8]>{
    let data: &'a[u8] = *cursor.get_ref();
    let from_idx = cursor.position() as usize;
    let rebased_data = &data[from_idx..];
    Cursor::new(rebased_data)
}


#[test]
fn test_rebase_cursor() {
    {
        let a: Vec<u8> = vec!(1, 2, 3);
        let mut cur: Cursor<&[u8]> = Cursor::new(&a);
        assert_eq!(cur.read_u8().unwrap(), 1);
        let mut rebased_cursor = rebase_cursor(&cur);
        assert_eq!(cur.read_u8().unwrap(), 2);
        assert_eq!(rebased_cursor.read_u8().unwrap(), 2);
        assert_eq!(cur.position(), 2);
        assert_eq!(rebased_cursor.position(), 1);
        cur.seek(SeekFrom::Start(0));
        assert_eq!(cur.read_u8().unwrap(), 1);
        rebased_cursor.seek(SeekFrom::Start(0));
        assert_eq!(rebased_cursor.read_u8().unwrap(), 2);
    }
}


struct Layer<'a, T> {
    cursor: Cursor<&'a [u8]>,
    next_item_idx: usize,
    num_items: usize,
    next_id: u32,
    _phantom_: PhantomData<T>,
}


impl<'a, T: BinarySerializable> Iterator for Layer<'a, T> {

    type Item = (DocId, T);

    fn next(&mut self,)-> Option<(DocId, T)> {
        if self.next_item_idx >= self.num_items {
            None
        }
        else {
            let cur_val = T::deserialize(&mut self.cursor).unwrap();
            let cur_id = self.next_id;
            self.next_item_idx += 1;
            if self.next_item_idx < self.num_items {
                self.next_id = u32::deserialize(&mut self.cursor).unwrap();
            }
            Some((cur_id, cur_val))
        }

    }
}


static EMPTY: [u8; 0] = [];

impl<'a, T: BinarySerializable> Layer<'a, T> {

    fn len(&self,) -> usize {
        self.num_items
    }

    fn read(cursor: &mut Cursor<&'a [u8]>) -> Layer<'a, T> {
        // TODO error handling?
        let num_items = cursor.read_u32::<BigEndian>().unwrap() as u32;
        println!("{} items", num_items);
        let num_bytes = cursor.read_u32::<BigEndian>().unwrap() as u32;
        println!("{} bytes", num_bytes);
        let mut rebased_cursor = rebase_cursor(cursor);
        cursor.seek(SeekFrom::Current(num_bytes as i64));
        let next_id =
            if num_items == 0 { 0 }
            else { rebased_cursor.read_u32::<BigEndian>().unwrap() };
        println!("next_id {:?}", next_id);
        Layer {
            cursor: rebased_cursor,
            next_item_idx: 0,
            num_items: num_items as usize,
            next_id: next_id,
            _phantom_: PhantomData,
        }
    }

    fn empty() -> Layer<'a, T> {
        Layer {
            cursor: Cursor::new(&EMPTY),
            next_item_idx: 0,
            num_items: 0,
            next_id: 0,
            _phantom_: PhantomData,
        }
    }

    fn seek(doc_id: DocId) {
        // while self.next_doc_id < doc_id {
        //     self.next_doc_id = cursor.read_u32::<BigEndian>();
        //     self.cur_val = self.next_val;
        //     self.next_doc_id = bincode::Deserializer::new(self.cursor, 8).read_u32();
        //     self.next_val =  bincode::Deserializer::new(self.cursor, 8).read_u32();
        // }
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

    pub fn seek(&mut self, doc_id: DocId) {
        // let mut next_layer_offset: u64 = 0;
        // for skip_layer_id in 0..self.skip_layers.len() {
        //     println!("LAYER {}", skip_layer_id);
        //     let mut skip_layer: &mut Layer<'a, u32> = &mut self.skip_layers[skip_layer_id];
        //     println!("seek {}", next_layer_offset);
        //     if next_layer_offset > 0 {
        //         skip_layer.cursor.seek(SeekFrom::Start(next_layer_offset));
        //         next_layer_offset = 0;
        //     }
        //     println!("next id {}", skip_layer.next_id);
        //     while skip_layer.next_id < doc_id {
        //         match skip_layer.next() {
        //             Some((_, offset)) => {
        //                 println!("bipoffset {}", offset);
        //                 next_layer_offset = offset as u64;
        //             },
        //             None => {
        //                 break;
        //             }
        //         }
        //     }
        // }
        // for skip_layer in self.skip_layers.iter() {
        //     println!("{}", skip_layer.len());
        // }
        // println!("last seek {}", next_layer_offset);
        // if next_layer_offset > 0 {
        //     self.data_layer.cursor.seek(SeekFrom::Start(next_layer_offset));
        // }
        while self.data_layer.next_id < doc_id {
            match self.data_layer.next() {
                None => { break; },
                _ => {}
            }
        }

    }

    pub fn read(data: &'a [u8]) -> SkipList<'a, T> {
        let mut cursor = Cursor::new(data);
        let offsets: Vec<u32> = Vec::deserialize(&mut cursor).unwrap();
        let num_layers = offsets.len();
        println!("{} layers ", num_layers);


        let start_position = cursor.position() as usize;
        let layers_data: &[u8] = &data[start_position..data.len()];

        let mut cur_offset = start_position;

        let data_layer: Layer<'a, T> =
            if num_layers == 0 { Layer::empty() }
            else {
                let first_layer_data: &[u8] = &layers_data[..offsets[0] as usize];
                let mut first_layer_cursor = Cursor::new(first_layer_data);
                Layer::read(&mut cursor)
            };
        let mut skip_layers: Vec<Layer<u32>>;
        if num_layers > 0 {
            skip_layers = offsets.iter()
                .zip(&offsets[1..])
                .map(|(start, stop)| {
                    let layer_data: &[u8] = &data[*start as usize..*stop as usize];
                    let mut cursor = Cursor::new(layer_data);
                    let skip_layer: Layer<'a, u32> = Layer::read(&mut cursor);
                    skip_layer
                })
                .collect();
        }
        else {
            skip_layers = Vec::new();
        }
        skip_layers.reverse();
        SkipList {
            skip_layers: skip_layers,
            data_layer: data_layer,
        }
    }
}
