use std::io;
use std::io::Write;
use std::fs::File;
use fst::Map;
use fst::MapBuilder;
use std::rc::Rc;
use core::serialize::BinarySerializable;
use std::marker::PhantomData;
use fst;
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
        file.write_all(&self.data);
        Ok(file)
    }

}



pub struct FstMap<R: Deref<Target=[u8]>, V: BinarySerializable> {
    //fst::Map,
    data: R,
    _phantom_: PhantomData<V>,
}

impl<R: Deref<Target=[u8]>, V: BinarySerializable> FstMap<R, V> {
    pub fn new(data: R) -> FstMap<R, V> {
        FstMap {
            data: data,
            _phantom_: PhantomData,
        }
    }

    pub fn read(key: &[u8]) -> Option<V> {
        None
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
