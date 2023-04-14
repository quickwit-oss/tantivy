use std::io::{self, Write};
use std::ops::Range;

use common::OwnedBytes;

use crate::{common_prefix_len, SSTable, SSTableDataCorruption, TermOrdinal};

#[derive(Default, Debug, Clone)]
pub struct SSTableIndex {
    blocks: Vec<BlockMeta>,
}

impl SSTableIndex {
    /// Load an index from its binary representation
    pub fn load(data: OwnedBytes) -> Result<SSTableIndex, SSTableDataCorruption> {
        let mut reader = IndexSSTable::reader(data);
        let mut blocks = Vec::new();

        while reader.advance().map_err(|_| SSTableDataCorruption)? {
            blocks.push(BlockMeta {
                last_key_or_greater: reader.key().to_vec(),
                block_addr: reader.value().clone(),
            });
        }

        Ok(SSTableIndex { blocks })
    }

    /// Get the [`BlockAddr`] of the requested block.
    pub(crate) fn get_block(&self, block_id: usize) -> Option<BlockAddr> {
        self.blocks
            .get(block_id)
            .map(|block_meta| block_meta.block_addr.clone())
    }

    /// Get the block id of the block that would contain `key`.
    ///
    /// Returns None if `key` is lexicographically after the last key recorded.
    pub(crate) fn locate_with_key(&self, key: &[u8]) -> Option<usize> {
        let pos = self
            .blocks
            .binary_search_by_key(&key, |block| &block.last_key_or_greater);
        match pos {
            Ok(pos) => Some(pos),
            Err(pos) => {
                if pos < self.blocks.len() {
                    Some(pos)
                } else {
                    // after end of last block: no block matches
                    None
                }
            }
        }
    }

    /// Get the [`BlockAddr`] of the block that would contain `key`.
    ///
    /// Returns None if `key` is lexicographically after the last key recorded.
    pub fn get_block_with_key(&self, key: &[u8]) -> Option<BlockAddr> {
        self.locate_with_key(key).and_then(|id| self.get_block(id))
    }

    pub(crate) fn locate_with_ord(&self, ord: TermOrdinal) -> usize {
        let pos = self
            .blocks
            .binary_search_by_key(&ord, |block| block.block_addr.first_ordinal);

        match pos {
            Ok(pos) => pos,
            // Err(0) can't happen as the sstable starts with ordinal zero
            Err(pos) => pos - 1,
        }
    }

    /// Get the [`BlockAddr`] of the block containing the `ord`-th term.
    pub(crate) fn get_block_with_ord(&self, ord: TermOrdinal) -> BlockAddr {
        // locate_with_ord always returns an index within range
        self.get_block(self.locate_with_ord(ord)).unwrap()
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct BlockAddr {
    pub byte_range: Range<usize>,
    pub first_ordinal: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct BlockMeta {
    /// Any byte string that is lexicographically greater or equal to
    /// the last key in the block,
    /// and yet strictly smaller than the first key in the next block.
    pub last_key_or_greater: Vec<u8>,
    pub block_addr: BlockAddr,
}

#[derive(Default)]
pub struct SSTableIndexBuilder {
    index: SSTableIndex,
}

/// Given that left < right,
/// mutates `left into a shorter byte string left'` that
/// matches `left <= left' < right`.
fn find_shorter_str_in_between(left: &mut Vec<u8>, right: &[u8]) {
    assert!(&left[..] < right);
    let common_len = common_prefix_len(left, right);
    if left.len() == common_len {
        return;
    }
    // It is possible to do one character shorter in some case,
    // but it is not worth the extra complexity
    for pos in (common_len + 1)..left.len() {
        if left[pos] != u8::MAX {
            left[pos] += 1;
            left.truncate(pos + 1);
            return;
        }
    }
}

impl SSTableIndexBuilder {
    /// In order to make the index as light as possible, we
    /// try to find a shorter alternative to the last key of the last block
    /// that is still smaller than the next key.
    pub(crate) fn shorten_last_block_key_given_next_key(&mut self, next_key: &[u8]) {
        if let Some(last_block) = self.index.blocks.last_mut() {
            find_shorter_str_in_between(&mut last_block.last_key_or_greater, next_key);
        }
    }

    pub fn add_block(&mut self, last_key: &[u8], byte_range: Range<usize>, first_ordinal: u64) {
        self.index.blocks.push(BlockMeta {
            last_key_or_greater: last_key.to_vec(),
            block_addr: BlockAddr {
                byte_range,
                first_ordinal,
            },
        })
    }

    pub fn serialize<W: std::io::Write>(&self, wrt: W) -> io::Result<()> {
        // we can't use a plain writer as it would generate an index
        let mut sstable_writer = IndexSSTable::delta_writer(wrt);

        // in tests, set a smaller block size to stress-test
        #[cfg(test)]
        sstable_writer.set_block_len(16);

        let mut previous_key = Vec::with_capacity(crate::DEFAULT_KEY_CAPACITY);
        for block in self.index.blocks.iter() {
            let keep_len = common_prefix_len(&previous_key, &block.last_key_or_greater);

            sstable_writer.write_suffix(keep_len, &block.last_key_or_greater[keep_len..]);
            sstable_writer.write_value(&block.block_addr);
            sstable_writer.flush_block_if_required()?;

            previous_key.clear();
            previous_key.extend_from_slice(&block.last_key_or_greater);
        }
        sstable_writer.flush_block()?;
        sstable_writer.finish().write_all(&0u32.to_le_bytes())?;
        Ok(())
    }
}

/// SSTable representing an index
///
/// `last_key_or_greater` is used as the key, the value contains the
/// length and first ordinal of each block. The start offset is implicitly
/// obtained from lengths.
struct IndexSSTable;

impl SSTable for IndexSSTable {
    type Value = BlockAddr;

    type ValueReader = crate::value::index::IndexValueReader;

    type ValueWriter = crate::value::index::IndexValueWriter;
}

#[cfg(test)]
mod tests {
    use common::OwnedBytes;

    use super::{BlockAddr, SSTableIndex, SSTableIndexBuilder};
    use crate::SSTableDataCorruption;

    #[test]
    fn test_sstable_index() {
        let mut sstable_builder = SSTableIndexBuilder::default();
        sstable_builder.add_block(b"aaa", 10..20, 0u64);
        sstable_builder.add_block(b"bbbbbbb", 20..30, 5u64);
        sstable_builder.add_block(b"ccc", 30..40, 10u64);
        sstable_builder.add_block(b"dddd", 40..50, 15u64);
        let mut buffer: Vec<u8> = Vec::new();
        sstable_builder.serialize(&mut buffer).unwrap();
        let buffer = OwnedBytes::new(buffer);
        let sstable_index = SSTableIndex::load(buffer).unwrap();
        assert_eq!(
            sstable_index.get_block_with_key(b"bbbde"),
            Some(BlockAddr {
                first_ordinal: 10u64,
                byte_range: 30..40
            })
        );

        assert_eq!(sstable_index.locate_with_key(b"aa").unwrap(), 0);
        assert_eq!(sstable_index.locate_with_key(b"aaa").unwrap(), 0);
        assert_eq!(sstable_index.locate_with_key(b"aab").unwrap(), 1);
        assert_eq!(sstable_index.locate_with_key(b"ccc").unwrap(), 2);
        assert!(sstable_index.locate_with_key(b"e").is_none());

        assert_eq!(sstable_index.locate_with_ord(0), 0);
        assert_eq!(sstable_index.locate_with_ord(1), 0);
        assert_eq!(sstable_index.locate_with_ord(4), 0);
        assert_eq!(sstable_index.locate_with_ord(5), 1);
        assert_eq!(sstable_index.locate_with_ord(100), 3);
    }

    #[test]
    fn test_sstable_with_corrupted_data() {
        let mut sstable_builder = SSTableIndexBuilder::default();
        sstable_builder.add_block(b"aaa", 10..20, 0u64);
        sstable_builder.add_block(b"bbbbbbb", 20..30, 5u64);
        sstable_builder.add_block(b"ccc", 30..40, 10u64);
        sstable_builder.add_block(b"dddd", 40..50, 15u64);
        let mut buffer: Vec<u8> = Vec::new();
        sstable_builder.serialize(&mut buffer).unwrap();
        buffer[2] = 9u8;
        let buffer = OwnedBytes::new(buffer);
        let data_corruption_err = SSTableIndex::load(buffer).err().unwrap();
        assert!(matches!(data_corruption_err, SSTableDataCorruption));
    }

    #[track_caller]
    fn test_find_shorter_str_in_between_aux(left: &[u8], right: &[u8]) {
        let mut left_buf = left.to_vec();
        super::find_shorter_str_in_between(&mut left_buf, right);
        assert!(left_buf.len() <= left.len());
        assert!(left <= &left_buf);
        assert!(&left_buf[..] < right);
    }

    #[test]
    fn test_find_shorter_str_in_between() {
        test_find_shorter_str_in_between_aux(b"", b"hello");
        test_find_shorter_str_in_between_aux(b"abc", b"abcd");
        test_find_shorter_str_in_between_aux(b"abcd", b"abd");
        test_find_shorter_str_in_between_aux(&[0, 0, 0], &[1]);
        test_find_shorter_str_in_between_aux(&[0, 0, 0], &[0, 0, 1]);
        test_find_shorter_str_in_between_aux(&[0, 0, 255, 255, 255, 0u8], &[0, 1]);
    }

    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn test_proptest_find_shorter_str(left in any::<Vec<u8>>(), right in any::<Vec<u8>>()) {
            if left < right {
                test_find_shorter_str_in_between_aux(&left, &right);
            }
        }
    }
}
