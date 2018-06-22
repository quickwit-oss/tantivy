use super::term_info_store::{TermInfoStore, TermInfoStoreWriter};
use super::{TermStreamer, TermStreamerBuilder};
use common::BinarySerializable;
use common::CountingWriter;
use directory::ReadOnlySource;
use fst;
use fst::raw::Fst;
use fst::Automaton;
use postings::TermInfo;
use schema::FieldType;
use std::io::{self, Write};
use termdict::TermOrdinal;

fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

/// Builder for the new term dictionary.
///
/// Inserting must be done in the order of the `keys`.
pub struct TermDictionaryBuilder<W> {
    fst_builder: fst::MapBuilder<W>,
    term_info_store_writer: TermInfoStoreWriter,
    term_ord: u64,
}

impl<W> TermDictionaryBuilder<W>
where
    W: Write,
{
    /// Creates a new `TermDictionaryBuilder`
    pub fn new(w: W, _field_type: FieldType) -> io::Result<Self> {
        let fst_builder = fst::MapBuilder::new(w).map_err(convert_fst_error)?;
        Ok(TermDictionaryBuilder {
            fst_builder,
            term_info_store_writer: TermInfoStoreWriter::new(),
            term_ord: 0,
        })
    }

    /// Inserts a `(key, value)` pair in the term dictionary.
    ///
    /// *Keys have to be inserted in order.*
    pub fn insert<K: AsRef<[u8]>>(&mut self, key_ref: K, value: &TermInfo) -> io::Result<()> {
        let key = key_ref.as_ref();
        self.insert_key(key)?;
        self.insert_value(value)?;
        Ok(())
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
            .insert(key, self.term_ord)
            .map_err(convert_fst_error)?;
        self.term_ord += 1;
        Ok(())
    }

    /// # Warning
    ///
    /// Horribly dangerous internal API. See `.insert_key(...)`.
    pub(crate) fn insert_value(&mut self, term_info: &TermInfo) -> io::Result<()> {
        self.term_info_store_writer.write_term_info(term_info)?;
        Ok(())
    }

    /// Finalize writing the builder, and returns the underlying
    /// `Write` object.
    pub fn finish(mut self) -> io::Result<W> {
        let mut file = self.fst_builder.into_inner().map_err(convert_fst_error)?;
        {
            let mut counting_writer = CountingWriter::wrap(&mut file);
            self.term_info_store_writer.serialize(&mut counting_writer)?;
            let footer_size = counting_writer.written_bytes();
            (footer_size as u64).serialize(&mut counting_writer)?;
            counting_writer.flush()?;
        }
        Ok(file)
    }
}

fn open_fst_index(source: ReadOnlySource) -> fst::Map {
    let fst = match source {
        ReadOnlySource::Anonymous(data) => {
            Fst::from_shared_bytes(data.data, data.start, data.len).expect("FST data is corrupted")
        }
        #[cfg(feature = "mmap")]
        ReadOnlySource::Mmap(mmap_readonly) => {
            Fst::from_mmap(mmap_readonly).expect("FST data is corrupted")
        }
    };
    fst::Map::from(fst)
}

/// The term dictionary contains all of the terms in
/// `tantivy index` in a sorted manner.
///
/// The `Fst` crate is used to associate terms to their
/// respective `TermOrdinal`. The `TermInfoStore` then makes it
/// possible to fetch the associated `TermInfo`.
pub struct TermDictionary {
    fst_index: fst::Map,
    term_info_store: TermInfoStore,
}

impl TermDictionary {
    /// Opens a `TermDictionary` given a data source.
    pub fn from_source(source: ReadOnlySource) -> Self {
        let total_len = source.len();
        let length_offset = total_len - 8;
        let mut split_len_buffer: &[u8] = &source.as_slice()[length_offset..];
        let footer_size = u64::deserialize(&mut split_len_buffer)
            .expect("Deserializing 8 bytes should always work") as usize;
        let split_len = length_offset - footer_size;
        let fst_source = source.slice(0, split_len);
        let values_source = source.slice(split_len, length_offset);
        let fst_index = open_fst_index(fst_source);
        TermDictionary {
            fst_index,
            term_info_store: TermInfoStore::open(&values_source),
        }
    }

    /// Creates an empty term dictionary which contains no terms.
    pub fn empty(field_type: FieldType) -> Self {
        let term_dictionary_data: Vec<u8> =
            TermDictionaryBuilder::new(Vec::<u8>::new(), field_type)
                .expect("Creating a TermDictionaryBuilder in a Vec<u8> should never fail")
                .finish()
                .expect("Writing in a Vec<u8> should never fail");
        let source = ReadOnlySource::from(term_dictionary_data);
        Self::from_source(source)
    }

    /// Returns the number of terms in the dictionary.
    /// Term ordinals range from 0 to `num_terms() - 1`.
    pub fn num_terms(&self) -> usize {
        self.term_info_store.num_terms()
    }

    /// Returns the ordinal associated to a given term.
    pub fn term_ord<K: AsRef<[u8]>>(&self, key: K) -> Option<TermOrdinal> {
        self.fst_index.get(key)
    }

    /// Returns the term associated to a given term ordinal.
    ///
    /// Term ordinals are defined as the position of the term in
    /// the sorted list of terms.
    ///
    /// Returns true iff the term has been found.
    ///
    /// Regardless of whether the term is found or not,
    /// the buffer may be modified.
    pub fn ord_to_term(&self, mut ord: TermOrdinal, bytes: &mut Vec<u8>) -> bool {
        bytes.clear();
        let fst = self.fst_index.as_fst();
        let mut node = fst.root();
        while ord != 0 || !node.is_final() {
            if let Some(transition) = node
                .transitions()
                .take_while(|transition| transition.out.value() <= ord)
                .last()
            {
                ord -= transition.out.value();
                bytes.push(transition.inp);
                let new_node_addr = transition.addr;
                node = fst.node(new_node_addr);
            } else {
                return false;
            }
        }
        true
    }

    /// Returns the number of terms in the dictionary.
    pub fn term_info_from_ord(&self, term_ord: TermOrdinal) -> TermInfo {
        self.term_info_store.get(term_ord)
    }

    /// Lookups the value corresponding to the key.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<TermInfo> {
        self.term_ord(key)
            .map(|term_ord| self.term_info_from_ord(term_ord))
    }

    /// Returns a range builder, to stream all of the terms
    /// within an interval.
    pub fn range<'a>(&'a self) -> TermStreamerBuilder<'a> {
        TermStreamerBuilder::new(self, self.fst_index.range())
    }

    /// A stream of all the sorted terms. [See also `.stream_field()`](#method.stream_field)
    pub fn stream<'a>(&'a self) -> TermStreamer<'a> {
        self.range().into_stream()
    }

    /// Returns a search builder, to stream all of the terms
    /// within the Automaton
    pub fn search<'a, A: Automaton + 'a>(&'a self, automaton: A) -> TermStreamerBuilder<'a, A> {
        let stream_builder = self.fst_index.search(automaton);
        TermStreamerBuilder::<A>::new(self, stream_builder)
    }
}
