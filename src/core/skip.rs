use std::io::Write;
use std::io::BufWriter;
use std::io::Read;
use std::io::Cursor;
use std::io::SeekFrom;
use std::io::Seek;
use std::marker;
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


// the lower layer contains only the list of doc ids.
// A docset is represented
// SkipList<'a, Void>

struct SkipLayer<'a, T> {
    cursor: Cursor<&'a [u8]>,
    num_items: u32,
    next_item: Option<T>,
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
    _phantom_: marker::PhantomData<T>,
    cursor: Cursor<&'a [u8]>,
    item_idx: usize,
    num_items: usize,
    cur_id: u32,
    next_id: Option<u32>,
}


impl<'a, T: BinarySerializable> Iterator for Layer<'a, T> {

    type Item = (DocId, T);

    fn next(&mut self,)-> Option<(DocId, T)> {
        if self.item_idx >= self.num_items {
            None
        }
        else {
            let cur_val = T::deserialize(&mut self.cursor).unwrap();
            let cur_id = self.next_id;
            self.item_idx += 1;
            if self.item_idx < self.num_items - 1 {
                self.next_id = Some(u32::deserialize(&mut self.cursor).unwrap());
            }
            else {
                self.next_id = None;
            }
            Some((self.cur_id.clone(), cur_val))
        }

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



impl<'a, T: BinarySerializable> Layer<'a, T> {
    fn read(cursor: &mut Cursor<&'a [u8]>) -> Layer<'a, T> {
        // TODO error handling?
        let num_items = cursor.read_u32::<BigEndian>().unwrap() as u32;
        println!("{} items ", num_items);
        let num_bytes = cursor.read_u32::<BigEndian>().unwrap() as u32;
        println!("{} bytes ", num_bytes);
        let mut rebased_cursor = rebase_cursor(cursor);
        cursor.seek(SeekFrom::Current(num_bytes as i64));
        // println!("cur val {:?}", cur_val);
        let next_id: Option<u32> = match rebased_cursor.read_u32::<BigEndian>() {
                    Ok(val) => Some(val),
                    Err(_) => None
                };
        Layer {
            cursor: rebased_cursor,
            item_idx: 0,
            num_items: num_items as usize,
            next_id: next_id,

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

    pub fn read(data: &'a [u8]) -> SkipList<'a, T> {
        let mut cursor = Cursor::new(data);
        let num_layers = cursor.read_u8().unwrap();
        println!("{} layers ", num_layers);
        let mut skip_layers = Vec::new();
        for _ in (0..num_layers - 1) {
            let skip_layer: Layer<'a, u32> = Layer::read(&mut cursor);
            skip_layers.push(skip_layer);
        }
        let data_layer: Layer<'a, T> = Layer::read(&mut cursor);
        SkipList {
            skip_layers: skip_layers,
            data_layer: data_layer,
        }
    }
}
