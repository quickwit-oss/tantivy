use std::io::{self, Write};
use fst;
use fst::raw::Fst;
use directory::ReadOnlySource;
use common::BinarySerializable;
use schema::FieldType;
use postings::TermInfo;
use termdict::{TermDictionary, TermDictionaryBuilder, TermOrdinal};
use super::{TermStreamerBuilderImpl, TermStreamerImpl};

fn convert_fst_error(e: fst::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

/// See [`TermDictionaryBuilder`](./trait.TermDictionaryBuilder.html)
pub struct TermDictionaryBuilderImpl<W> {
    fst_builder: fst::MapBuilder<W>,
    data: Vec<u8>,
    term_ord: u64,
}

impl<W> TermDictionaryBuilderImpl<W>
where
    W: Write,
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
            .insert(key, self.term_ord)
            .map_err(convert_fst_error)?;
        self.term_ord += 1;
        Ok(())
    }

    /// # Warning
    ///
    /// Horribly dangerous internal API. See `.insert_key(...)`.
    pub(crate) fn insert_value(&mut self, value: &TermInfo) -> io::Result<()> {
        value.serialize(&mut self.data)?;
        Ok(())
    }
}

impl<W> TermDictionaryBuilder<W> for TermDictionaryBuilderImpl<W>
where
    W: Write,
{
    fn new(w: W, _field_type: FieldType) -> io::Result<Self> {
        let fst_builder = fst::MapBuilder::new(w).map_err(convert_fst_error)?;
        Ok(TermDictionaryBuilderImpl {
            fst_builder: fst_builder,
            data: Vec::new(),
            term_ord: 0,
        })
    }

    fn insert<K: AsRef<[u8]>>(&mut self, key_ref: K, value: &TermInfo) -> io::Result<()> {
        let key = key_ref.as_ref();
        self.insert_key(key)?;
        self.insert_value(value)?;
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

fn open_fst_index(source: ReadOnlySource) -> fst::Map {
    let fst = match source {
        ReadOnlySource::Anonymous(data) => {
            Fst::from_shared_bytes(data.data, data.start, data.len).expect("FST data is corrupted")
        }
        ReadOnlySource::Mmap(mmap_readonly) => {
            Fst::from_mmap(mmap_readonly).expect("FST data is corrupted")
        }
    };
    fst::Map::from(fst)
}

/// See [`TermDictionary`](./trait.TermDictionary.html)
pub struct TermDictionaryImpl {
    fst_index: fst::Map,
    values_mmap: ReadOnlySource,
}

impl<'a> TermDictionary<'a> for TermDictionaryImpl {
    type Streamer = TermStreamerImpl<'a>;

    type StreamBuilder = TermStreamerBuilderImpl<'a>;

    fn from_source(source: ReadOnlySource) -> Self {
        let total_len = source.len();
        let length_offset = total_len - 4;
        let mut split_len_buffer: &[u8] = &source.as_slice()[length_offset..];
        let footer_size = u32::deserialize(&mut split_len_buffer)
            .expect("Deserializing 4 bytes should always work") as usize;
        let split_len = length_offset - footer_size;
        let fst_source = source.slice(0, split_len);
        let values_source = source.slice(split_len, length_offset);
        let fst_index = open_fst_index(fst_source);
        TermDictionaryImpl {
            fst_index: fst_index,
            values_mmap: values_source,
        }
    }

    fn num_terms(&self) -> usize {
        self.values_mmap.len() / TermInfo::SIZE_IN_BYTES
    }

    fn ord_to_term(&self, mut ord: TermOrdinal, bytes: &mut Vec<u8>) -> bool {
        bytes.clear();
        let fst = self.fst_index.as_fst();
        let mut node = fst.root();
        while ord != 0 || !node.is_final() {
            if let Some(transition) = node.transitions()
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

    fn term_ord<K: AsRef<[u8]>>(&self, key: K) -> Option<TermOrdinal> {
        self.fst_index.get(key)
    }

    fn term_info_from_ord(&self, term_ord: TermOrdinal) -> TermInfo {
        let buffer = self.values_mmap.as_slice();
        let offset = term_ord as usize * TermInfo::SIZE_IN_BYTES;
        let mut cursor = &buffer[offset..];
        TermInfo::deserialize(&mut cursor)
            .expect("The fst is corrupted. Failed to deserialize a value.")
    }

    fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<TermInfo> {
        self.term_ord(key)
            .map(|term_ord| self.term_info_from_ord(term_ord))
    }

    fn range(&self) -> TermStreamerBuilderImpl {
        TermStreamerBuilderImpl::new(self, self.fst_index.range())
    }
}
