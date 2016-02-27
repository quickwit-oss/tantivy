use std::io;
use std::io::Seek;
use std::io::Write;
use std::io::Cursor;
use std::fs::File;
use fst::Map;
use fst::MapBuilder;
use std::rc::Rc;
use fst::raw::Fst;
use core::serialize::BinarySerializable;
use std::marker::PhantomData;
use fst;
use fst::raw::MmapReadOnly;
use std::ops::Deref;


fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

pub struct FstMapBuilder<W: Write, V: BinarySerializable> {
    fst_builder: MapBuilder<W>,
    data: Vec<u8>,
    _phantom_: PhantomData<V>,
}

impl<W: Write, V: BinarySerializable> FstMapBuilder<W, V> {

    fn new(w: W) -> io::Result<FstMapBuilder<W, V>> {
        let fst_builder = try!(MapBuilder::new(w).map_err(convert_fst_error));
        Ok(FstMapBuilder {
            fst_builder: fst_builder,
            data: Vec::new(),
            _phantom_: PhantomData,
        })
    }

    fn insert(&mut self, key: &[u8], value: &V) -> io::Result<()>{
        try!(self.fst_builder
            .insert(key, self.data.len() as u64)
            .map_err(convert_fst_error));
        value.serialize(&mut self.data);
        Ok(())
    }

    fn close(self,) -> io::Result<W> {
        let mut file = try!(self.fst_builder
                 .into_inner()
                 .map_err(convert_fst_error));
        let footer_size = self.data.len();
        file.write_all(&self.data);
        (footer_size as u32).serialize(&mut file);
        Ok(file)
    }
}


pub struct FstMap<V: BinarySerializable> {
    fst_index: fst::Map,
    values_mmap: MmapReadOnly,
    _phantom_: PhantomData<V>,
}


impl<V: BinarySerializable> FstMap<V> {
    pub fn open(file: &File) -> io::Result<FstMap<V>> {
        let mmap = try!(MmapReadOnly::open(&file));
        let mut cursor = Cursor::new(unsafe {mmap.as_slice()});
        try!(cursor.seek(io::SeekFrom::End(-4)));
        let footer_size = try!(u32::deserialize(&mut cursor)) as  usize;
        let split_len = mmap.len() - 4 - footer_size;
        let fst_mmap = mmap.range(0, split_len);
        let values_mmap = mmap.range(split_len, mmap.len() - 4);
        let fst = try!(fst::raw::Fst::from_mmap(fst_mmap).map_err(convert_fst_error));
        Ok(FstMap {
            fst_index: fst::Map::from(fst),
            values_mmap: values_mmap,
            _phantom_: PhantomData,
        })
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<V> {
        self.fst_index
            .get(key)
            .map(|offset| {
                let buffer = unsafe { self.values_mmap.as_slice()};
                let mut cursor = Cursor::new(&buffer[(offset as usize)..]);
                V::deserialize(&mut cursor).unwrap()
            })
    }
}


mod tests {
    use super::{FstMapBuilder, FstMap};

    #[test]
    fn test_fstmap() {
        let mut fst_map_builder: FstMapBuilder<Vec<u8>, u32> = FstMapBuilder::new(Vec::new()).unwrap();
        fst_map_builder.insert("abc".as_bytes(), &34).unwrap();
        fst_map_builder.insert("abcd".as_bytes(), &343).unwrap();
        let data = fst_map_builder.close().unwrap();
    }
}
