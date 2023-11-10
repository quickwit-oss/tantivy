use std::io::{self, Write};
use std::ops::Range;

use common::OwnedBytes;

use crate::{common_prefix_len, SSTable, SSTableDataCorruption, TermOrdinal};

#[derive(Debug, Clone)]
pub struct SSTableIndex {
    root_blocks: Vec<BlockMeta>,
    layer_count: u32,
    index_bytes: OwnedBytes,
}

impl Default for SSTableIndex {
    fn default() -> Self {
        SSTableIndex {
            root_blocks: Vec::new(),
            layer_count: 1,
            index_bytes: OwnedBytes::empty(),
        }
    }
}

impl SSTableIndex {
    /// Load an index from its binary representation
    pub fn load(
        data: OwnedBytes,
        layer_count: u32,
        first_layer_offset: usize,
    ) -> Result<SSTableIndex, SSTableDataCorruption> {
        let (index_bytes, first_layer_slice) = data.split(first_layer_offset);
        let mut reader = IndexSSTable::reader(first_layer_slice);
        let mut root_blocks = Vec::new();

        while reader.advance().map_err(|_| SSTableDataCorruption)? {
            root_blocks.push(BlockMeta {
                last_key_or_greater: reader.key().to_vec(),
                block_addr: reader.value().clone(),
            });
        }

        Ok(SSTableIndex {
            root_blocks,
            layer_count,
            index_bytes,
            // index_bytes: OwnedBytes::empty(),
        })
    }

    /// Get the [`BlockAddr`] of the block that would contain `key`.
    ///
    /// Returns None if `key` is lexicographically after the last key recorded.
    pub fn get_block_with_key(&self, key: &[u8]) -> io::Result<Option<BlockAddr>> {
        self.iterate_from_key(key).map(|iter| iter.value().cloned())
    }

    /// Get the [`BlockAddr`] of the block containing the `ord`-th term.
    pub fn get_block_with_ord(&self, ord: TermOrdinal) -> io::Result<BlockAddr> {
        let pos = self
            .root_blocks
            .binary_search_by_key(&ord, |block| block.block_addr.first_ordinal);

        let root_pos = match pos {
            Ok(pos) => pos,
            // Err(0) can't happen as the sstable starts with ordinal zero
            Err(pos) => pos - 1,
        };

        if self.layer_count == 1 {
            return Ok(self.root_blocks[root_pos].block_addr.clone());
        }

        let mut next_layer_block_addr = self.root_blocks[root_pos].block_addr.clone();
        for _ in 1..self.layer_count {
            let mut sstable_delta_reader = IndexSSTable::delta_reader(
                self.index_bytes
                    .slice(next_layer_block_addr.byte_range.clone()),
            );
            while sstable_delta_reader.advance()? {
                if sstable_delta_reader.value().first_ordinal > ord {
                    break;
                }
                next_layer_block_addr = sstable_delta_reader.value().clone();
            }
        }
        Ok(next_layer_block_addr)
    }

    pub(crate) fn iterate_from_key(&self, key: &[u8]) -> io::Result<ReaderOrSlice<'_>> {
        let root_pos = self
            .root_blocks
            .binary_search_by_key(&key, |block| &block.last_key_or_greater);
        let root_pos = match root_pos {
            Ok(pos) => pos,
            Err(pos) => {
                if pos < self.root_blocks.len() {
                    pos
                } else {
                    // after end of last block: no block matches
                    return Ok(ReaderOrSlice::End);
                }
            }
        };

        let mut next_layer_block_addr = self.root_blocks[root_pos].block_addr.clone();
        let mut last_delta_reader = None;
        for _ in 1..self.layer_count {
            // we don't enter this loop for 1 layer index
            let mut sstable_delta_reader = IndexSSTable::delta_reader(
                self.index_bytes.slice(next_layer_block_addr.byte_range),
            );
            crate::dictionary::decode_up_to_key(key, &mut sstable_delta_reader)?;
            next_layer_block_addr = sstable_delta_reader.value().clone();
            last_delta_reader = Some(sstable_delta_reader);
        }

        if let Some(delta_reader) = last_delta_reader {
            // reconstruct the current key. We stopped either on the exact key, or just after
            // either way, common_prefix_len is something that did not change between the
            // last-key-before-target and the current pos, so those bytes must match the prefix of
            // `key`. The next bytes can be obtained from the delta reader
            let mut result_key = Vec::with_capacity(crate::DEFAULT_KEY_CAPACITY);
            let common_prefix_len = delta_reader.common_prefix_len();
            let suffix = delta_reader.suffix();
            let new_len = delta_reader.common_prefix_len() + suffix.len();
            result_key.resize(new_len, 0u8);
            result_key[..common_prefix_len].copy_from_slice(&key[..common_prefix_len]);
            result_key[common_prefix_len..].copy_from_slice(suffix);

            let reader = crate::Reader {
                key: result_key,
                delta_reader,
            };
            Ok(ReaderOrSlice::Reader(reader))
        } else {
            // self.layer_count == 1, there is no lvl2 sstable to decode.
            Ok(ReaderOrSlice::Iter(&self.root_blocks, root_pos))
        }
    }
}

pub(crate) enum ReaderOrSlice<'a> {
    Reader(crate::Reader<crate::value::index::IndexValueReader>),
    Iter(&'a [BlockMeta], usize),
    End,
}

impl<'a> ReaderOrSlice<'a> {
    pub fn advance(&mut self) -> Result<bool, SSTableDataCorruption> {
        match self {
            ReaderOrSlice::Reader(reader) => {
                let res = reader.advance().map_err(|_| SSTableDataCorruption);
                if !matches!(res, Ok(true)) {
                    *self = ReaderOrSlice::End;
                }
                res
            }
            ReaderOrSlice::Iter(slice, index) => {
                *index += 1;
                if *index < slice.len() {
                    Ok(true)
                } else {
                    *self = ReaderOrSlice::End;
                    Ok(false)
                }
            }
            ReaderOrSlice::End => Ok(false),
        }
    }

    /// Get current key. Always Some(_) unless last call to advance returned something else than
    /// Ok(true)
    pub fn key(&self) -> Option<&[u8]> {
        match self {
            ReaderOrSlice::Reader(reader) => Some(reader.key()),
            ReaderOrSlice::Iter(slice, index) => Some(&slice[*index].last_key_or_greater),
            ReaderOrSlice::End => None,
        }
    }

    /// Get current value. Always Some(_) unless last call to advance returned something else than
    /// Ok(true)
    pub fn value(&self) -> Option<&BlockAddr> {
        match self {
            ReaderOrSlice::Reader(reader) => Some(reader.value()),
            ReaderOrSlice::Iter(slice, index) => Some(&slice[*index].block_addr),
            ReaderOrSlice::End => None,
        }
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
        if let Some(last_block) = self.index.root_blocks.last_mut() {
            find_shorter_str_in_between(&mut last_block.last_key_or_greater, next_key);
        }
    }

    pub fn add_block(&mut self, last_key: &[u8], byte_range: Range<usize>, first_ordinal: u64) {
        self.index.root_blocks.push(BlockMeta {
            last_key_or_greater: last_key.to_vec(),
            block_addr: BlockAddr {
                byte_range,
                first_ordinal,
            },
        })
    }

    pub fn serialize<W: std::io::Write>(
        &self,
        wrt: W,
        index_max_root_blocks: std::num::NonZeroU64,
    ) -> io::Result<(u32, u64)> {
        let index_max_root_blocks = index_max_root_blocks.get();

        let mut wrt = common::CountingWriter::wrap(wrt);
        let mut next_layer = write_sstable_layer(&mut wrt, &self.index.root_blocks, 0)?;

        let mut layer_count = 1;
        let mut offset = 0;
        while next_layer.len() as u64 > index_max_root_blocks {
            offset = wrt.written_bytes();
            layer_count += 1;

            next_layer = write_sstable_layer(&mut wrt, &next_layer, offset as usize)?;
        }
        Ok((layer_count, offset))
    }
}

fn write_sstable_layer<W: std::io::Write>(
    wrt: W,
    layer_content: &[BlockMeta],
    offset: usize,
) -> io::Result<Vec<BlockMeta>> {
    // we can't use a plain writer as it would generate an index
    // also disable compression, the index is small anyway, and it's the most costly part of
    // opening that kind of sstable
    let mut sstable_writer =
        crate::DeltaWriter::<_, crate::value::index::IndexValueWriter>::new_no_compression(wrt);

    // in tests, set a smaller block size to stress-test
    #[cfg(test)]
    sstable_writer.set_block_len(16);

    let mut next_layer = Vec::new();
    let mut previous_key = Vec::with_capacity(crate::DEFAULT_KEY_CAPACITY);
    let mut first_ordinal = None;
    for block in layer_content.iter() {
        if first_ordinal.is_none() {
            first_ordinal = Some(block.block_addr.first_ordinal);
        }
        let keep_len = common_prefix_len(&previous_key, &block.last_key_or_greater);

        sstable_writer.write_suffix(keep_len, &block.last_key_or_greater[keep_len..]);
        sstable_writer.write_value(&block.block_addr);
        if let Some(range) = sstable_writer.flush_block_if_required()? {
            let real_range = (range.start + offset)..(range.end + offset);
            let block_meta = BlockMeta {
                last_key_or_greater: block.last_key_or_greater.clone(),
                block_addr: BlockAddr {
                    byte_range: real_range,
                    first_ordinal: first_ordinal.take().unwrap(),
                },
            };
            next_layer.push(block_meta);
            previous_key.clear();
        } else {
            previous_key.extend_from_slice(&block.last_key_or_greater);
            previous_key.resize(block.last_key_or_greater.len(), 0u8);
            previous_key[keep_len..].copy_from_slice(&block.last_key_or_greater[keep_len..]);
        }
    }
    if let Some(range) = sstable_writer.flush_block()? {
        if let Some(last_block) = layer_content.last() {
            // not going here means an empty table (?!)
            let real_range = (range.start + offset)..(range.end + offset);
            let block_meta = BlockMeta {
                last_key_or_greater: last_block.last_key_or_greater.clone(),
                block_addr: BlockAddr {
                    byte_range: real_range,
                    first_ordinal: first_ordinal.take().unwrap(),
                },
            };
            next_layer.push(block_meta);
        }
    }
    sstable_writer.finish().write_all(&0u32.to_le_bytes())?;

    Ok(next_layer)
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
        sstable_builder
            .serialize(&mut buffer, crate::DEFAULT_MAX_ROOT_BLOCKS)
            .unwrap();
        let buffer = OwnedBytes::new(buffer);
        let sstable_index = SSTableIndex::load(buffer, 1, 0).unwrap();
        assert_eq!(
            sstable_index.get_block_with_key(b"bbbde").unwrap(),
            Some(BlockAddr {
                first_ordinal: 10u64,
                byte_range: 30..40
            })
        );

        assert_eq!(
            sstable_index
                .get_block_with_key(b"aa")
                .unwrap()
                .unwrap()
                .first_ordinal,
            0
        );
        assert_eq!(
            sstable_index
                .get_block_with_key(b"aaa")
                .unwrap()
                .unwrap()
                .first_ordinal,
            0
        );
        assert_eq!(
            sstable_index
                .get_block_with_key(b"aab")
                .unwrap()
                .unwrap()
                .first_ordinal,
            5
        );
        assert_eq!(
            sstable_index
                .get_block_with_key(b"ccc")
                .unwrap()
                .unwrap()
                .first_ordinal,
            10
        );
        assert!(sstable_index.get_block_with_key(b"e").unwrap().is_none());

        assert_eq!(
            sstable_index.get_block_with_ord(0).unwrap().first_ordinal,
            0
        );
        assert_eq!(
            sstable_index.get_block_with_ord(1).unwrap().first_ordinal,
            0
        );
        assert_eq!(
            sstable_index.get_block_with_ord(4).unwrap().first_ordinal,
            0
        );
        assert_eq!(
            sstable_index.get_block_with_ord(5).unwrap().first_ordinal,
            5
        );
        assert_eq!(
            sstable_index.get_block_with_ord(6).unwrap().first_ordinal,
            5
        );
        assert_eq!(
            sstable_index.get_block_with_ord(100).unwrap().first_ordinal,
            15
        );
    }

    #[test]
    fn test_sstable_with_corrupted_data() {
        let mut sstable_builder = SSTableIndexBuilder::default();
        sstable_builder.add_block(b"aaa", 10..20, 0u64);
        sstable_builder.add_block(b"bbbbbbb", 20..30, 5u64);
        sstable_builder.add_block(b"ccc", 30..40, 10u64);
        sstable_builder.add_block(b"dddd", 40..50, 15u64);
        let mut buffer: Vec<u8> = Vec::new();
        sstable_builder
            .serialize(&mut buffer, crate::DEFAULT_MAX_ROOT_BLOCKS)
            .unwrap();
        buffer[2] = 9u8;
        let buffer = OwnedBytes::new(buffer);
        let data_corruption_err = SSTableIndex::load(buffer, 1, 0).err().unwrap();
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
