use std::io::{self, Write};
use fst;
use fst::raw::Fst;
use super::{TermStreamerBuilder, TermStreamer};
use directory::ReadOnlySource;
use common::BinarySerializable;
use std::marker::PhantomData;
use schema::{Field, Term};
use postings::TermInfo;


fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}


/// Builder for the new term dictionary.
///
/// Just like for the fst crate, all terms must be inserted in order.
pub struct TermDictionaryBuilder<W: Write, V = TermInfo>
    where V: BinarySerializable
{
    fst_builder: fst::MapBuilder<W>,
    data: Vec<u8>,
    _phantom_: PhantomData<V>,
}

impl<W: Write, V: BinarySerializable> TermDictionaryBuilder<W, V> {
    /// Creates a new `TermDictionaryBuilder`
    pub fn new(w: W) -> io::Result<TermDictionaryBuilder<W, V>> {
        let fst_builder = fst::MapBuilder::new(w).map_err(convert_fst_error)?;
        Ok(TermDictionaryBuilder {
               fst_builder: fst_builder,
               data: Vec::new(),
               _phantom_: PhantomData,
           })
    }

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

    /// Inserts a `(key, value)` pair in the term dictionary.
    ///
    /// *Keys have to be inserted in order.*
    pub fn insert(&mut self, key: &[u8], value: &V) -> io::Result<()> {
        self.fst_builder
            .insert(key, self.data.len() as u64)
            .map_err(convert_fst_error)?;
        value.serialize(&mut self.data)?;
        Ok(())
    }

    /// Finalize writing the builder, and returns the underlying
    /// `Write` object.
    pub fn finish(self) -> io::Result<W> {
        let mut file = self.fst_builder.into_inner().map_err(convert_fst_error)?;
        let footer_size = self.data.len() as u32;
        file.write_all(&self.data)?;
        (footer_size as u32).serialize(&mut file)?;
        file.flush()?;
        Ok(file)
    }
}

/// Datastructure to access the `terms` of a segment.
pub struct TermDictionary<V = TermInfo>
    where V: BinarySerializable
{
    fst_index: fst::Map,
    values_mmap: ReadOnlySource,
    _phantom_: PhantomData<V>,
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

impl<V> TermDictionary<V>
    where V: BinarySerializable
{
    /// Opens a `TermDictionary` given a data source.
    pub fn from_source(source: ReadOnlySource) -> io::Result<TermDictionary<V>> {
        let total_len = source.len();
        let length_offset = total_len - 4;
        let mut split_len_buffer: &[u8] = &source.as_slice()[length_offset..];
        let footer_size = u32::deserialize(&mut split_len_buffer)? as usize;
        let split_len = length_offset - footer_size;
        let fst_source = source.slice(0, split_len);
        let values_source = source.slice(split_len, length_offset);
        let fst_index = open_fst_index(fst_source)?;
        Ok(TermDictionary {
               fst_index: fst_index,
               values_mmap: values_source,
               _phantom_: PhantomData,
           })
    }

    /// Deserialize and returns the value at address `offset`
    pub(crate) fn read_value(&self, offset: u64) -> io::Result<V> {
        let buffer = self.values_mmap.as_slice();
        let mut cursor = &buffer[(offset as usize)..];
        V::deserialize(&mut cursor)
    }

    /// Lookups the value corresponding to the key.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<V> {
        self.fst_index
            .get(key)
            .map(|offset| {
                     self.read_value(offset)
                         .expect("The fst is corrupted. Failed to deserialize a value.")
                 })
    }

    /// A stream of all the sorted terms. [See also `.stream_field()`](#method.stream_field)
    pub fn stream(&self) -> TermStreamer<V> {
        self.range().into_stream()
    }

    /// A stream of all the sorted terms in the given field.
    pub fn stream_field(&self, field: Field) -> TermStreamer<V> {
        let start_term = Term::from_field_text(field, "");
        let stop_term = Term::from_field_text(Field(field.0 + 1), "");
        self.range()
            .ge(start_term.as_slice())
            .lt(stop_term.as_slice())
            .into_stream()
    }

    /// Returns a range builder, to stream all of the terms
    /// within an interval.
    pub fn range(&self) -> TermStreamerBuilder<V> {
        TermStreamerBuilder::new(self, self.fst_index.range())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use directory::{RAMDirectory, Directory};
    use std::path::PathBuf;
    use fst::Streamer;

    #[test]
    fn test_term_dictionary() {
        let mut directory = RAMDirectory::create();
        let path = PathBuf::from("TermDictionary");
        {
            let write = directory.open_write(&path).unwrap();
            let mut term_dictionary_builder = TermDictionaryBuilder::new(write).unwrap();
            term_dictionary_builder
                .insert("abc".as_bytes(), &34u32)
                .unwrap();
            term_dictionary_builder
                .insert("abcd".as_bytes(), &346u32)
                .unwrap();
            term_dictionary_builder.finish().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        let term_dict: TermDictionary<u32> = TermDictionary::from_source(source).unwrap();
        assert_eq!(term_dict.get("abc"), Some(34u32));
        assert_eq!(term_dict.get("abcd"), Some(346u32));
        let mut stream = term_dict.stream();
        assert_eq!(stream.next().unwrap(), ("abc".as_bytes(), 34u32));
        assert_eq!(stream.key(), "abc".as_bytes());
        assert_eq!(stream.value(), 34u32);
        assert_eq!(stream.next().unwrap(), ("abcd".as_bytes(), 346u32));
        assert_eq!(stream.key(), "abcd".as_bytes());
        assert_eq!(stream.value(), 346u32);
        assert!(!stream.advance());
    }

}
