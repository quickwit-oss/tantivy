use std::io;
use std::io::Seek;
use std::io::Write;
use std::io::Cursor;
use std::fs::File;
use fst;
use fst::raw::MmapReadOnly;
use core::serialize::BinarySerializable;
use std::marker::PhantomData;

fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

pub struct FstMapBuilder<W: Write, V: BinarySerializable> {
    fst_builder: fst::MapBuilder<W>,
    data: Vec<u8>,
    _phantom_: PhantomData<V>,
}

impl<W: Write, V: BinarySerializable> FstMapBuilder<W, V> {

    fn new(w: W) -> io::Result<FstMapBuilder<W, V>> {
        let fst_builder = try!(fst::MapBuilder::new(w).map_err(convert_fst_error));
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

    fn finish(self,) -> io::Result<W> {
        let mut file = try!(
            self.fst_builder
                 .into_inner()
                 .map_err(convert_fst_error));
        let footer_size = self.data.len() as u32;
        file.write_all(&self.data);
        (footer_size as u32).serialize(&mut file);
        file.flush();
        Ok(file)
    }
}


pub struct FstMap<V: BinarySerializable> {
    fst_index: fst::Map,
    values_mmap: MmapReadOnly,
    _phantom_: PhantomData<V>,
}


impl<V: BinarySerializable> FstMap<V> {
    pub fn open(file: File) -> io::Result<FstMap<V>> {
        let mmap = try!(MmapReadOnly::open(&file));
        let mut cursor = Cursor::new(unsafe {mmap.as_slice()});
        try!(cursor.seek(io::SeekFrom::End(-4)));
        let footer_size = try!(u32::deserialize(&mut cursor)) as  usize;
        let split_len = mmap.len() - 4 - footer_size;
        let fst_mmap = mmap.range(0, split_len);
        let values_mmap = mmap.range(split_len, footer_size);
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
    use super::*;
    use tempfile;

    #[test]
    fn test_fstmap() {
        let fstmap_file;
        {
            let tempfile = tempfile::tempfile().unwrap(); // 41
            let mut fstmap_builder = FstMapBuilder::new(tempfile).unwrap();
            fstmap_builder.insert("abc".as_bytes(), &34u32).unwrap();
            fstmap_builder.insert("abcd".as_bytes(), &346u32).unwrap();
            fstmap_file = fstmap_builder.finish().unwrap();
        }
        let fstmap = FstMap::open(fstmap_file).unwrap();
        assert_eq!(fstmap.get("abc"), Some(34u32));
        assert_eq!(fstmap.get("abcd"), Some(346u32));
    }
}
