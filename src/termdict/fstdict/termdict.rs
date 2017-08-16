use std::io::{self, Write};
use fst;
use fst::raw::Fst;
use directory::ReadOnlySource;
use common::BinarySerializable;
use bincode;
use postings::TermInfo;
use termdict::{TermDictionary, TermDictionaryBuilder};
use super::{TermStreamerImpl, TermStreamerBuilderImpl};

fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

/// See [`TermDictionaryBuilder`](./trait.TermDictionaryBuilder.html)
pub struct TermDictionaryBuilderImpl<W>
{
    fst_builder: fst::MapBuilder<W>,
    data: Vec<u8>,
}

impl<W> TermDictionaryBuilderImpl<W>
    where W: Write
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

impl<W> TermDictionaryBuilder<W> for TermDictionaryBuilderImpl<W>
    where W: Write
{
    fn new(w: W, field_option: FieldOption) -> io::Result<Self> {
        let fst_builder = fst::MapBuilder::new(w).map_err(convert_fst_error)?;
        Ok(TermDictionaryBuilderImpl {
               fst_builder: fst_builder,
               data: Vec::new(),
           })
    }

    fn insert<K: AsRef<[u8]>>(&mut self, key_ref: K, value: &TermInfo) -> io::Result<()> {
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
pub struct TermDictionaryImpl
{
    fst_index: fst::Map,
    values_mmap: ReadOnlySource,
}

impl TermDictionaryImpl
{
    /// Deserialize and returns the value at address `offset`
    pub(crate) fn read_value(&self, offset: u64) -> io::Result<V> {
        let buffer = self.values_mmap.as_slice();
        let mut cursor = &buffer[(offset as usize)..];
        V::deserialize(&mut cursor)
    }
}


impl<'a> TermDictionary<'a> for TermDictionaryImpl
{
    type Streamer = TermStreamerImpl<'a>;

    type StreamBuilder = TermStreamerBuilderImpl<'a>;

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

    fn range(&self) -> TermStreamerBuilderImpl {
        TermStreamerBuilderImpl::new(self, self.fst_index.range())
    }
}
