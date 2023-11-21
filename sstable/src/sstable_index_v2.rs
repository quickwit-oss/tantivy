use common::OwnedBytes;

use crate::{BlockAddr, SSTable, SSTableDataCorruption, TermOrdinal};

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

#[derive(Debug, Clone)]
pub(crate) struct BlockMeta {
    /// Any byte string that is lexicographically greater or equal to
    /// the last key in the block,
    /// and yet strictly smaller than the first key in the next block.
    pub last_key_or_greater: Vec<u8>,
    pub block_addr: BlockAddr,
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
