use std::io::{self, Write};
use fst;
use fst::raw::Fst;
use super::{FstMapStreamerBuilder, FstMapStreamer};
use directory::ReadOnlySource;
use common::BinarySerializable;
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
        let fst_builder = fst::MapBuilder::new(w).map_err(convert_fst_error)?;
        Ok(FstMapBuilder {
               fst_builder: fst_builder,
               data: Vec::new(),
               _phantom_: PhantomData,
           })
    }

    /// Horribly unsafe, nobody should ever do that... except me :)
    ///
    /// If used, it must be used by systematically alternating calls
    /// to insert_key and insert_value.
    ///
    /// TODO see if I can bend Rust typesystem to enforce that
    /// in a nice way.
    pub fn insert_key(&mut self, key: &[u8]) -> io::Result<()> {
        self.fst_builder
                 .insert(key, self.data.len() as u64)
                 .map_err(convert_fst_error)?;
        Ok(())
    }

    /// Horribly unsafe, nobody should ever do that... except me :)
    pub fn insert_value(&mut self, value: &V) -> io::Result<()> {
        value.serialize(&mut self.data)?;
        Ok(())
    }

    #[cfg(test)]
    pub fn insert(&mut self, key: &[u8], value: &V) -> io::Result<()> {
        self.fst_builder
                 .insert(key, self.data.len() as u64)
                 .map_err(convert_fst_error)?;
        value.serialize(&mut self.data)?;
        Ok(())
    }

    pub fn finish(self) -> io::Result<W> {
        let mut file = self.fst_builder.into_inner().map_err(convert_fst_error)?;
        let footer_size = self.data.len() as u32;
        file.write_all(&self.data)?;
        (footer_size as u32).serialize(&mut file)?;
        file.flush()?;
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
                          ReadOnlySource::Anonymous(data) => {
                              Fst::from_shared_bytes(data.data, data.start, data.len)
                                       .map_err(convert_fst_error)?
                          }
                          ReadOnlySource::Mmap(mmap_readonly) => {
                              Fst::from_mmap(mmap_readonly).map_err(convert_fst_error)?
                          }
                      }))
}

impl<V> FstMap<V> where V: BinarySerializable {

    pub fn from_source(source: ReadOnlySource) -> io::Result<FstMap<V>> {
        let total_len = source.len();
        let length_offset = total_len - 4;
        let mut split_len_buffer: &[u8] = &source.as_slice()[length_offset..];
        let footer_size = u32::deserialize(&mut split_len_buffer)? as usize;
        let split_len = length_offset - footer_size;
        let fst_source = source.slice(0, split_len);
        let values_source = source.slice(split_len, length_offset);
        let fst_index = open_fst_index(fst_source)?;
        Ok(FstMap {
               fst_index: fst_index,
               values_mmap: values_source,
               _phantom_: PhantomData,
        })
    }

    pub(crate) fn read_value(&self, offset: u64) -> V {
        let buffer = self.values_mmap.as_slice();
        let mut cursor = &buffer[(offset as usize)..];
        V::deserialize(&mut cursor).expect("Data in FST is corrupted")
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<V> {
        self.fst_index
            .get(key)
            .map(|offset| self.read_value(offset))
    }

    pub fn stream(&self) -> FstMapStreamer<V> {
        self.range().into_stream()
    }

    pub fn range(&self) -> FstMapStreamerBuilder<V> {
        FstMapStreamerBuilder::new(&self, self.fst_index.range())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use directory::{RAMDirectory, Directory};
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
        let fstmap: FstMap<u32> = FstMap::from_source(source).unwrap();
        assert_eq!(fstmap.get("abc"), Some(34u32));
        assert_eq!(fstmap.get("abcd"), Some(346u32));
        let mut stream = fstmap.stream();
        assert_eq!(stream.next().unwrap(), "abc".as_bytes());
        assert_eq!(stream.key(), "abc".as_bytes());
        assert_eq!(stream.value(), 34u32);
        assert_eq!(stream.next().unwrap(), "abcd".as_bytes());
        assert_eq!(stream.key(), "abcd".as_bytes());
        assert_eq!(stream.value(), 346u32);
        assert!(!stream.advance());
    }

}
