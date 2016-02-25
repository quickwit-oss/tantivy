use std::io;
use std::io::Write;
use std::fs::File;
use fst::MapBuilder;
use std::rc::Rc;
use core::serialize::BinarySerializable;
use std::marker::PhantomData;
use fst;

fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

struct FstMapBuilder<V: BinarySerializable> {
    fst_builder: MapBuilder<File>,
    data: Vec<u8>,
    _phantom_: PhantomData<V>,
}


impl<V: BinarySerializable> FstMapBuilder<V> {

    fn new(file: File) -> io::Result<FstMapBuilder<V>> {
        let fst_builder = try!(MapBuilder::new(file).map_err(convert_fst_error));
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

    fn close(self,) -> io::Result<()> {
        let mut file = try!(self.fst_builder
                 .into_inner()
                 .map_err(convert_fst_error));
        file.write_all(&self.data);
        Ok(())
    }

}
