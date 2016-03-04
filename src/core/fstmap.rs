use std::io;
use std::io::Seek;
use std::io::Write;
use std::io::Cursor;
use fst;
use fst::raw::Fst;
use fst::Streamer;
use core::directory::ReadOnlySource;
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

    pub fn new(w: W) -> io::Result<FstMapBuilder<W, V>> {
        let fst_builder = try!(fst::MapBuilder::new(w).map_err(convert_fst_error));
        Ok(FstMapBuilder {
            fst_builder: fst_builder,
            data: Vec::new(),
            _phantom_: PhantomData,
        })
    }

    pub fn insert(&mut self, key: &[u8], value: &V) -> io::Result<()>{
        try!(self.fst_builder
            .insert(key, self.data.len() as u64)
            .map_err(convert_fst_error));
        value.serialize(&mut self.data);
        Ok(())
    }

    pub fn finish(self,) -> io::Result<W> {
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
    values_mmap: ReadOnlySource,
    _phantom_: PhantomData<V>,
}


fn open_fst_index(source: ReadOnlySource) -> io::Result<fst::Map> {
    Ok(fst::Map::from(match source {
        ReadOnlySource::Anonymous(data) => try!(Fst::from_bytes(data).map_err(convert_fst_error)),
        ReadOnlySource::Mmap(mmap_readonly) => try!(Fst::from_mmap(mmap_readonly).map_err(convert_fst_error)),
    }))
}

struct FstMapIter<'a, V: 'static + BinarySerializable> {
    streamer: fst::map::Stream<'a>,
    fst_map: &'a FstMap<V>,
    __phantom__: PhantomData<V>
}

impl<'a, V: 'static + BinarySerializable> Iterator for FstMapIter<'a, V> {

    // type Item = (Vec<u8>, V);
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let next_item = self.streamer.next();
        match next_item {
            Some((key, offset)) => { Some(4u32) },
            None => None
        }
    }
}

impl<V: BinarySerializable> FstMap<V> {

    pub fn stream<'a>(&'a self,) -> FstMapIter<'a, V> {
        FstMapIter {
            streamer: self.fst_index.stream(),
            fst_map: self,
            __phantom__: PhantomData,
        }
    }

    pub fn from_source(source: ReadOnlySource)  -> io::Result<FstMap<V>> {
        println!("Source Len : {}", source.as_slice().len());
        let mut cursor = Cursor::new(source.as_slice());
        try!(cursor.seek(io::SeekFrom::End(-4)));
        let footer_size = try!(u32::deserialize(&mut cursor)) as  usize;
        println!("Cursor : {}", footer_size);
        let split_len = source.len() - 4 - footer_size;
        let fst_source = source.slice(0, split_len);
        let values_source = source.slice(split_len, source.len() - 4);
        let fst_index = try!(open_fst_index(fst_source));
        Ok(FstMap {
            fst_index: fst_index,
            values_mmap: values_source,
            _phantom_: PhantomData,
        })
    }

    fn read_value(&self, offset: u64) -> V {
        let buffer = self.values_mmap.as_slice();
        let mut cursor = Cursor::new(&buffer[(offset as usize)..]);
        V::deserialize(&mut cursor).unwrap()
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<V> {
        self.fst_index
            .get(key)
            .map(|offset| self.read_value(offset))
    }
}

#[cfg(test)]
mod tests {
    use super::{FstMapBuilder, FstMap};
    use core::directory::{RAMDirectory, Directory};
    use std::path::PathBuf;
    use fst::Streamer;

    #[test]
    fn test_fstmap() {
        let mut directory = RAMDirectory::create();
        let path = PathBuf::from("fstmap");
        {
            let write = directory.open_write(&path).unwrap();
            let mut fstmap_builder = FstMapBuilder::new(write).unwrap();
            fstmap_builder.insert("abc".as_bytes(), &34u32).unwrap();
            fstmap_builder.insert("abcd".as_bytes(), &346u32).unwrap();
            fstmap_builder.finish().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        let fstmap = FstMap::from_source(source).unwrap();
        assert_eq!(fstmap.get("abc"), Some(34u32));
        assert_eq!(fstmap.get("abcd"), Some(346u32));
        let mut stream = fstmap.stream();
        let mut items = Vec::new();
        {
            stream.next();
        }
        {
            stream.next();
        }
        // loop {
        //     match stream.next() {
        //         Some(it) => {
        //             items.push(it);
        //         }
        //         None => {
        //             break;
        //         }
        //     }
        //
        // }
        // //
        // {
        //     let item = stream.next();
        //     assert_eq!(item, Some((Vec::from("abc"), 34u32)) );
        // }
        // {
        //     let item = stream.next();
        //     assert_eq!(item, Some((Vec::from("abcd"), 346u32)) );
        // }
        // {
        //     assert_eq!(stream.next(), None);
        // }
    }

}
