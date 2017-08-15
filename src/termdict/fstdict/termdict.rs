use std::io::{self, Write};
use fst;
use fst::raw::Fst;
use directory::ReadOnlySource;
use common::BinarySerializable;
use std::marker::PhantomData;
use postings::TermInfo;
use termdict::{TermDictionary, TermDictionaryBuilder};
use super::{TermStreamerImpl, TermStreamerBuilderImpl};

fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

/// See [`TermDictionaryBuilder`](./trait.TermDictionaryBuilder.html)
pub struct TermDictionaryBuilderImpl<W, V = TermInfo>
{
    fst_builder: fst::MapBuilder<W>,
    data: Vec<u8>,
    _phantom_: PhantomData<V>,
}

impl<W, V> TermDictionaryBuilderImpl<W, V>
    where W: Write,
          V: BinarySerializable + Default
{
    /// # Warning
    /// Horribly dangerous internal API
    ///
    /// If used, it must be used by systematically alternating calls
    /// to insert_key and insert_value.
    ///
    /// Prefer using `.insert(key, value)`
    pub(crate) fn insert_key(&mut self, key: &[u8]) -> io::Result<()> {
        self.fst_builder
            .insert(key, self.data.len() as u64)
            .map_err(convert_fst_error)?;
        Ok(())
    }

    /// # Warning
    ///
    /// Horribly dangerous internal API. See `.insert_key(...)`.
    pub(crate) fn insert_value(&mut self, value: &V) -> io::Result<()> {
        value.serialize(&mut self.data)?;
        Ok(())
    }
}

impl<W, V> TermDictionaryBuilder<W, V> for TermDictionaryBuilderImpl<W, V>
    where W: Write,
          V: BinarySerializable + Default
{
    fn new(w: W) -> io::Result<Self> {
        let fst_builder = fst::MapBuilder::new(w).map_err(convert_fst_error)?;
        Ok(TermDictionaryBuilderImpl {
               fst_builder: fst_builder,
               data: Vec::new(),
               _phantom_: PhantomData,
           })
    }

    fn insert<K: AsRef<[u8]>>(&mut self, key_ref: K, value: &V) -> io::Result<()> {
        let key = key_ref.as_ref();
        self.fst_builder
            .insert(key, self.data.len() as u64)
            .map_err(convert_fst_error)?;
        value.serialize(&mut self.data)?;
        Ok(())
    }

    fn finish(self) -> io::Result<W> {
        let mut file = self.fst_builder.into_inner().map_err(convert_fst_error)?;
        let footer_size = self.data.len() as u32;
        file.write_all(&self.data)?;
        (footer_size as u32).serialize(&mut file)?;
        file.flush()?;
        Ok(file)
    }
}

fn open_fst_index(source: ReadOnlySource) -> io::Result<fst::Map> {
    let fst = match source {
        ReadOnlySource::Anonymous(data) => {
            Fst::from_shared_bytes(data.data, data.start, data.len)
                .map_err(convert_fst_error)?
        }
        ReadOnlySource::Mmap(mmap_readonly) => {
            Fst::from_mmap(mmap_readonly).map_err(convert_fst_error)?
        }
    };
    Ok(fst::Map::from(fst))
}

/// See [`TermDictionary`](./trait.TermDictionary.html)
pub struct TermDictionaryImpl<V = TermInfo>
{
    fst_index: fst::Map,
    values_mmap: ReadOnlySource,
    _phantom_: PhantomData<V>,
}

impl<V> TermDictionaryImpl<V>
    where V: BinarySerializable + Default
{
    /// Deserialize and returns the value at address `offset`
    pub(crate) fn read_value(&self, offset: u64) -> io::Result<V> {
        let buffer = self.values_mmap.as_slice();
        let mut cursor = &buffer[(offset as usize)..];
        V::deserialize(&mut cursor)
    }
}


impl<'a, V> TermDictionary<'a, V> for TermDictionaryImpl<V>
    where V: BinarySerializable + Default + 'a
{
    type Streamer = TermStreamerImpl<'a, V>;

    type StreamBuilder = TermStreamerBuilderImpl<'a, V>;

    fn from_source(source: ReadOnlySource) -> io::Result<Self> {
        let total_len = source.len();
        let length_offset = total_len - 4;
        let mut split_len_buffer: &[u8] = &source.as_slice()[length_offset..];
        let footer_size = u32::deserialize(&mut split_len_buffer)? as usize;
        let split_len = length_offset - footer_size;
        let fst_source = source.slice(0, split_len);
        let values_source = source.slice(split_len, length_offset);
        let fst_index = open_fst_index(fst_source)?;
        Ok(TermDictionaryImpl {
               fst_index: fst_index,
               values_mmap: values_source,
               _phantom_: PhantomData,
           })
    }

    fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<V> {
        self.fst_index
            .get(key)
            .map(|offset| {
                     self.read_value(offset)
                         .expect("The fst is corrupted. Failed to deserialize a value.")
                 })
    }

    fn range(&self) -> TermStreamerBuilderImpl<V> {
        TermStreamerBuilderImpl::new(self, self.fst_index.range())
    }
}
