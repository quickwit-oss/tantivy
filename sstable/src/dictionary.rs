use std::io;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use common::file_slice::FileSlice;
use common::{BinarySerializable, OwnedBytes};
use tantivy_fst::automaton::AlwaysMatch;
use tantivy_fst::Automaton;

use crate::streamer::{Streamer, StreamerBuilder};
use crate::{BlockAddr, DeltaReader, Reader, SSTable, SSTableIndex, TermOrdinal, VoidSSTable};

/// An SSTable is a sorted map that associates sorted `&[u8]` keys
/// to any kind of typed values.
///
/// The SSTable is organized in blocks.
/// In each block, keys and values are encoded separately.
///
/// The keys are encoded using incremental encoding.
/// The values on the other hand, are encoded according to a value-specific
/// codec defined in the TSSTable generic argument.
///
/// Finally, an index is joined to the Dictionary to make it possible,
/// given a key to identify which block contains this key.
///
/// The codec was designed in such a way that the sstable
/// reader is not aware of block, and yet can read any sequence of blocks,
/// as long as the slice of bytes it is given starts and stops at
/// block boundary.
///
/// (See also README.md)
#[derive(Debug, Clone)]
pub struct Dictionary<TSSTable: SSTable = VoidSSTable> {
    pub sstable_slice: FileSlice,
    pub sstable_index: SSTableIndex,
    num_terms: u64,
    phantom_data: PhantomData<TSSTable>,
}

impl Dictionary<VoidSSTable> {
    pub fn build_for_tests(terms: &[&str]) -> Dictionary {
        let mut terms = terms.to_vec();
        terms.sort();
        let mut buffer = Vec::new();
        let mut dictionary_writer = Self::builder(&mut buffer).unwrap();
        for term in terms {
            dictionary_writer.insert(term, &()).unwrap();
        }
        dictionary_writer.finish().unwrap();
        Dictionary::from_bytes(OwnedBytes::new(buffer)).unwrap()
    }
}

impl<TSSTable: SSTable> Dictionary<TSSTable> {
    pub fn builder<W: io::Write>(wrt: W) -> io::Result<crate::Writer<W, TSSTable::ValueWriter>> {
        Ok(TSSTable::writer(wrt))
    }

    pub(crate) fn sstable_reader_block(
        &self,
        block_addr: BlockAddr,
    ) -> io::Result<Reader<'static, TSSTable::ValueReader>> {
        let data = self.sstable_slice.read_bytes_slice(block_addr.byte_range)?;
        Ok(TSSTable::reader(data))
    }

    pub(crate) async fn sstable_reader_block_async(
        &self,
        block_addr: BlockAddr,
    ) -> io::Result<Reader<'static, TSSTable::ValueReader>> {
        let data = self
            .sstable_slice
            .read_bytes_slice_async(block_addr.byte_range)
            .await?;
        Ok(TSSTable::reader(data))
    }

    pub(crate) fn sstable_delta_reader_for_key_range(
        &self,
        key_range: impl RangeBounds<[u8]>,
        limit: Option<u64>,
    ) -> io::Result<DeltaReader<'static, TSSTable::ValueReader>> {
        let slice = self.file_slice_for_range(key_range, limit);
        let data = slice.read_bytes()?;
        Ok(TSSTable::delta_reader(data))
    }

    /// This function returns a file slice covering a set of sstable blocks
    /// that include the key range passed in arguments. Optionally returns
    /// only block for up to `limit` matching terms.
    ///
    /// It works by identifying
    /// - `first_block`: the block containing the start boudary key
    /// - `last_block`: the block containing the end boundary key.
    ///
    /// And then returning the range that spans over all blocks between.
    /// and including first_block and last_block, aka:
    /// `[first_block.start_offset .. last_block.end_offset)`
    ///
    /// Technically this function does not provide the tightest fit, as
    /// for simplification, it treats the start bound of the `key_range`
    /// as if it was inclusive, even if it is exclusive.
    /// On the rare edge case where a user asks for `(start_key, end_key]`
    /// and `start_key` happens to be the last key of a block, we return a
    /// slice that is the first block was not necessary.
    pub fn file_slice_for_range(
        &self,
        key_range: impl RangeBounds<[u8]>,
        limit: Option<u64>,
    ) -> FileSlice {
        let first_block_id = match key_range.start_bound() {
            Bound::Included(key) | Bound::Excluded(key) => {
                let Some(first_block_id) = self.sstable_index.locate_with_key(key) else {
                    return FileSlice::empty();
                };
                Some(first_block_id)
            }
            Bound::Unbounded => None,
        };

        let last_block_id = match key_range.end_bound() {
            Bound::Included(key) | Bound::Excluded(key) => self.sstable_index.locate_with_key(key),
            Bound::Unbounded => None,
        };

        let start_bound = if let Some(first_block_id) = first_block_id {
            let Some(block_addr) = self.sstable_index.get_block(first_block_id) else {
                return FileSlice::empty();
            };
            Bound::Included(block_addr.byte_range.start)
        } else {
            Bound::Unbounded
        };

        let last_block_id = if let Some(limit) = limit {
            let second_block_id = first_block_id.map(|id| id + 1).unwrap_or(0);
            if let Some(block_addr) = self.sstable_index.get_block(second_block_id) {
                let ordinal_limit = block_addr.first_ordinal + limit;
                let last_block_limit = self.sstable_index.locate_with_ord(ordinal_limit);
                if let Some(last_block_id) = last_block_id {
                    Some(last_block_id.min(last_block_limit))
                } else {
                    Some(last_block_limit)
                }
            } else {
                last_block_id
            }
        } else {
            last_block_id
        };
        let end_bound = last_block_id
            .and_then(|block_id| self.sstable_index.get_block(block_id))
            .map(|block_addr| Bound::Excluded(block_addr.byte_range.end))
            .unwrap_or(Bound::Unbounded);

        self.sstable_slice.slice((start_bound, end_bound))
    }

    /// Opens a `TermDictionary`.
    pub fn open(term_dictionary_file: FileSlice) -> io::Result<Self> {
        let (main_slice, footer_len_slice) = term_dictionary_file.split_from_end(16);
        let mut footer_len_bytes: OwnedBytes = footer_len_slice.read_bytes()?;
        let index_offset = u64::deserialize(&mut footer_len_bytes)?;
        let num_terms = u64::deserialize(&mut footer_len_bytes)?;
        let (sstable_slice, index_slice) = main_slice.split(index_offset as usize);
        let sstable_index_bytes = index_slice.read_bytes()?;
        let sstable_index = SSTableIndex::load(sstable_index_bytes.as_slice())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "SSTable corruption"))?;
        Ok(Dictionary {
            sstable_slice,
            sstable_index,
            num_terms,
            phantom_data: PhantomData,
        })
    }

    /// Creates a term dictionary from the supplied bytes.
    pub fn from_bytes(owned_bytes: OwnedBytes) -> io::Result<Self> {
        Dictionary::open(FileSlice::new(Arc::new(owned_bytes)))
    }

    /// Creates an empty term dictionary which contains no terms.
    pub fn empty() -> Self {
        let term_dictionary_data: Vec<u8> = Self::builder(Vec::<u8>::new())
            .expect("Creating a TermDictionaryBuilder in a Vec<u8> should never fail")
            .finish()
            .expect("Writing in a Vec<u8> should never fail");
        let empty_dict_file = FileSlice::from(term_dictionary_data);
        Dictionary::open(empty_dict_file).unwrap()
    }

    /// Returns the number of terms in the dictionary.
    /// Term ordinals range from 0 to `num_terms() - 1`.
    pub fn num_terms(&self) -> usize {
        self.num_terms as usize
    }

    /// Returns the ordinal associated with a given term.
    pub fn term_ord<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TermOrdinal>> {
        let key_bytes = key.as_ref();

        let Some(block_addr) = self.sstable_index.get_block_with_key(key_bytes) else {
            return Ok(None);
        };

        let mut term_ord = block_addr.first_ordinal;
        let mut sstable_reader = self.sstable_reader_block(block_addr)?;
        while sstable_reader.advance()? {
            if sstable_reader.key() == key_bytes {
                return Ok(Some(term_ord));
            }
            term_ord += 1;
        }
        Ok(None)
    }

    /// Returns the term associated with a given term ordinal.
    ///
    /// Term ordinals are defined as the position of the term in
    /// the sorted list of terms.
    ///
    /// Returns true if and only if the term has been found.
    ///
    /// Regardless of whether the term is found or not,
    /// the buffer may be modified.
    pub fn ord_to_term(&self, ord: TermOrdinal, bytes: &mut Vec<u8>) -> io::Result<bool> {
        // find block in which the term would be
        let block_addr = self.sstable_index.get_block_with_ord(ord);
        let first_ordinal = block_addr.first_ordinal;

        // then search inside that block only
        let mut sstable_reader = self.sstable_reader_block(block_addr)?;
        for _ in first_ordinal..=ord {
            if !sstable_reader.advance()? {
                return Ok(false);
            }
        }
        bytes.clear();
        bytes.extend_from_slice(sstable_reader.key());
        Ok(true)
    }

    /// Returns the number of terms in the dictionary.
    pub fn term_info_from_ord(&self, term_ord: TermOrdinal) -> io::Result<Option<TSSTable::Value>> {
        // find block in which the term would be
        let block_addr = self.sstable_index.get_block_with_ord(term_ord);
        let first_ordinal = block_addr.first_ordinal;

        // then search inside that block only
        let mut sstable_reader = self.sstable_reader_block(block_addr)?;
        for _ in first_ordinal..=term_ord {
            if !sstable_reader.advance()? {
                return Ok(None);
            }
        }
        Ok(Some(sstable_reader.value().clone()))
    }

    /// Lookups the value corresponding to the key.
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TSSTable::Value>> {
        if let Some(block_addr) = self.sstable_index.get_block_with_key(key.as_ref()) {
            let mut sstable_reader = self.sstable_reader_block(block_addr)?;
            let key_bytes = key.as_ref();
            while sstable_reader.advance()? {
                if sstable_reader.key() == key_bytes {
                    let value = sstable_reader.value().clone();
                    return Ok(Some(value));
                }
            }
        }
        Ok(None)
    }

    /// Lookups the value corresponding to the key.
    pub async fn get_async<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<TSSTable::Value>> {
        if let Some(block_addr) = self.sstable_index.get_block_with_key(key.as_ref()) {
            let mut sstable_reader = self.sstable_reader_block_async(block_addr).await?;
            let key_bytes = key.as_ref();
            while sstable_reader.advance()? {
                if sstable_reader.key() == key_bytes {
                    let value = sstable_reader.value().clone();
                    return Ok(Some(value));
                }
            }
        }
        Ok(None)
    }

    /// Returns a range builder, to stream all of the terms
    /// within an interval.
    pub fn range(&self) -> StreamerBuilder<'_, TSSTable> {
        StreamerBuilder::new(self, AlwaysMatch)
    }

    /// A stream of all the sorted terms.
    pub fn stream(&self) -> io::Result<Streamer<'_, TSSTable>> {
        self.range().into_stream()
    }

    /// Returns a search builder, to stream all of the terms
    /// within the Automaton
    pub fn search<'a, A: Automaton + 'a>(
        &'a self,
        automaton: A,
    ) -> StreamerBuilder<'a, TSSTable, A>
    where
        A::State: Clone,
    {
        StreamerBuilder::<TSSTable, A>::new(self, automaton)
    }

    #[doc(hidden)]
    pub async fn warm_up_dictionary(&self) -> io::Result<()> {
        self.sstable_slice.read_bytes_async().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::{Arc, Mutex};

    use common::OwnedBytes;

    use super::Dictionary;
    use crate::MonotonicU64SSTable;

    #[derive(Debug)]
    struct PermissionedHandle {
        bytes: OwnedBytes,
        allowed_range: Mutex<Range<usize>>,
    }

    impl PermissionedHandle {
        fn new(bytes: Vec<u8>) -> Self {
            let bytes = OwnedBytes::new(bytes);
            PermissionedHandle {
                allowed_range: Mutex::new(0..bytes.len()),
                bytes,
            }
        }

        fn restrict(&self, range: Range<usize>) {
            *self.allowed_range.lock().unwrap() = range;
        }
    }

    impl common::HasLen for PermissionedHandle {
        fn len(&self) -> usize {
            self.bytes.len()
        }
    }

    impl common::file_slice::FileHandle for PermissionedHandle {
        fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
            let allowed_range = self.allowed_range.lock().unwrap();
            if !allowed_range.contains(&range.start) || !allowed_range.contains(&(range.end - 1)) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("invalid range, allowed {allowed_range:?}, requested {range:?}"),
                ));
            }

            Ok(self.bytes.slice(range))
        }
    }

    fn make_test_sstable() -> (Dictionary<MonotonicU64SSTable>, Arc<PermissionedHandle>) {
        let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new()).unwrap();

        // this makes 256k keys, enough to fill multiple blocks.
        for elem in 0..0x3ffff {
            let key = format!("{elem:05X}").into_bytes();
            builder.insert_cannot_fail(&key, &elem);
        }

        let table = builder.finish().unwrap();
        let table = Arc::new(PermissionedHandle::new(table));
        let slice = common::file_slice::FileSlice::new(table.clone());

        let dictionary = Dictionary::<MonotonicU64SSTable>::open(slice).unwrap();

        // if the last block is id 0, tests are meaningless
        assert_ne!(dictionary.sstable_index.locate_with_ord(u64::MAX), 0);
        assert_eq!(dictionary.num_terms(), 0x3ffff);
        (dictionary, table)
    }

    #[test]
    fn test_ord_term_conversion() {
        let (dic, slice) = make_test_sstable();

        let block = dic.sstable_index.get_block_with_ord(100_000);
        slice.restrict(block.byte_range);

        let mut res = Vec::new();

        // middle of a block
        assert!(dic.ord_to_term(100_000, &mut res).unwrap());
        assert_eq!(res, format!("{:05X}", 100_000).into_bytes());
        assert_eq!(dic.term_info_from_ord(100_000).unwrap().unwrap(), 100_000);
        assert_eq!(dic.get(&res).unwrap().unwrap(), 100_000);
        assert_eq!(dic.term_ord(&res).unwrap().unwrap(), 100_000);

        // start of a block
        assert!(dic.ord_to_term(block.first_ordinal, &mut res).unwrap());
        assert_eq!(res, format!("{:05X}", block.first_ordinal).into_bytes());
        assert_eq!(
            dic.term_info_from_ord(block.first_ordinal)
                .unwrap()
                .unwrap(),
            block.first_ordinal
        );
        assert_eq!(dic.get(&res).unwrap().unwrap(), block.first_ordinal);
        assert_eq!(dic.term_ord(&res).unwrap().unwrap(), block.first_ordinal);

        // end of a block
        let ordinal = block.first_ordinal - 1;
        let new_range = dic.sstable_index.get_block_with_ord(ordinal).byte_range;
        slice.restrict(new_range);
        assert!(dic.ord_to_term(ordinal, &mut res).unwrap());
        assert_eq!(res, format!("{:05X}", ordinal).into_bytes());
        assert_eq!(dic.term_info_from_ord(ordinal).unwrap().unwrap(), ordinal);
        assert_eq!(dic.get(&res).unwrap().unwrap(), ordinal);
        assert_eq!(dic.term_ord(&res).unwrap().unwrap(), ordinal);

        // before first block
        // 1st block must be loaded for key-related operations
        let block = dic.sstable_index.get_block_with_ord(0);
        slice.restrict(block.byte_range);

        assert!(dic.get(&b"$$$").unwrap().is_none());
        assert!(dic.term_ord(&b"$$$").unwrap().is_none());

        // after last block
        // last block must be loaded for ord related operations
        let ordinal = 0x40000 + 10;
        let new_range = dic.sstable_index.get_block_with_ord(ordinal).byte_range;
        slice.restrict(new_range);
        assert!(!dic.ord_to_term(ordinal, &mut res).unwrap());
        assert!(dic.term_info_from_ord(ordinal).unwrap().is_none());

        // last block isn't required to be loaded for key related operations
        slice.restrict(0..0);
        assert!(dic.get(&b"~~~").unwrap().is_none());
        assert!(dic.term_ord(&b"~~~").unwrap().is_none());
    }

    #[test]
    fn test_range() {
        let (dic, slice) = make_test_sstable();

        let start = dic
            .sstable_index
            .get_block_with_key(b"10000")
            .unwrap()
            .byte_range;
        let end = dic
            .sstable_index
            .get_block_with_key(b"18000")
            .unwrap()
            .byte_range;
        slice.restrict(start.start..end.end);

        let mut stream = dic.range().ge(b"10000").lt(b"18000").into_stream().unwrap();

        for i in 0x10000..0x18000 {
            assert!(stream.advance());
            assert_eq!(stream.term_ord(), i);
            assert_eq!(stream.value(), &i);
            assert_eq!(stream.key(), format!("{i:05X}").into_bytes());
        }
        assert!(!stream.advance());

        // verify limiting the number of results reduce the size read
        slice.restrict(start.start..(end.end - 1));

        let mut stream = dic
            .range()
            .ge(b"10000")
            .lt(b"18000")
            .limit(0xfff)
            .into_stream()
            .unwrap();

        for i in 0x10000..0x10fff {
            assert!(stream.advance());
            assert_eq!(stream.term_ord(), i);
            assert_eq!(stream.value(), &i);
            assert_eq!(stream.key(), format!("{i:05X}").into_bytes());
        }
        // there might be more successful elements after, though how many is undefined

        slice.restrict(0..slice.bytes.len());

        let mut stream = dic.stream().unwrap();
        for i in 0..0x3ffff {
            assert!(stream.advance());
            assert_eq!(stream.term_ord(), i);
            assert_eq!(stream.value(), &i);
            assert_eq!(stream.key(), format!("{i:05X}").into_bytes());
        }
        assert!(!stream.advance());
    }
}
