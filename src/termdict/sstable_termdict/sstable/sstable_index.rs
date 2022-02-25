use std::io;
use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::error::DataCorruption;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct SSTableIndex {
    blocks: Vec<BlockMeta>,
}

impl SSTableIndex {
    pub(crate) fn load(data: &[u8]) -> Result<SSTableIndex, DataCorruption> {
        serde_cbor::de::from_slice(data)
            .map_err(|_| DataCorruption::comment_only("SSTable index is corrupted"))
    }

    pub fn search(&self, key: &[u8]) -> Option<BlockAddr> {
        self.blocks
            .iter()
            .find(|block| &block.last_key[..] >= key)
            .map(|block| block.block_addr.clone())
    }
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct BlockAddr {
    pub byte_range: Range<usize>,
    pub first_ordinal: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct BlockMeta {
    pub last_key: Vec<u8>,
    pub block_addr: BlockAddr,
}

#[derive(Default)]
pub struct SSTableIndexBuilder {
    index: SSTableIndex,
}

impl SSTableIndexBuilder {
    pub fn add_block(&mut self, last_key: &[u8], byte_range: Range<usize>, first_ordinal: u64) {
        self.index.blocks.push(BlockMeta {
            last_key: last_key.to_vec(),
            block_addr: BlockAddr {
                byte_range,
                first_ordinal,
            },
        })
    }

    pub fn serialize(&self, wrt: &mut dyn io::Write) -> io::Result<()> {
        serde_cbor::ser::to_writer(wrt, &self.index).unwrap();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockAddr, SSTableIndex, SSTableIndexBuilder};

    #[test]
    fn test_sstable_index() {
        let mut sstable_builder = SSTableIndexBuilder::default();
        sstable_builder.add_block(b"aaa", 10..20, 0u64);
        sstable_builder.add_block(b"bbbbbbb", 20..30, 564);
        sstable_builder.add_block(b"ccc", 30..40, 10u64);
        sstable_builder.add_block(b"dddd", 40..50, 15u64);
        let mut buffer: Vec<u8> = Vec::new();
        sstable_builder.serialize(&mut buffer).unwrap();
        let sstable_index = SSTableIndex::load(&buffer[..]).unwrap();
        assert_eq!(
            sstable_index.search(b"bbbde"),
            Some(BlockAddr {
                first_ordinal: 10u64,
                byte_range: 30..40
            })
        );
    }

    #[test]
    fn test_sstable_with_corrupted_data() {
        let mut sstable_builder = SSTableIndexBuilder::default();
        sstable_builder.add_block(b"aaa", 10..20, 0u64);
        sstable_builder.add_block(b"bbbbbbb", 20..30, 564);
        sstable_builder.add_block(b"ccc", 30..40, 10u64);
        sstable_builder.add_block(b"dddd", 40..50, 15u64);
        let mut buffer: Vec<u8> = Vec::new();
        sstable_builder.serialize(&mut buffer).unwrap();
        buffer[1] = 9u8;
        let data_corruption_err = SSTableIndex::load(&buffer[..]).err().unwrap();
        assert_eq!(
            format!("{data_corruption_err:?}"),
            "Data corruption: SSTable index is corrupted."
        );
    }
}
