use serde::{Deserialize, Serialize};
use std::io;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct SSTableIndex {
    blocks: Vec<BlockMeta>,
}

impl SSTableIndex {
    pub fn load(data: &[u8]) -> SSTableIndex {
        // TODO
        serde_cbor::de::from_slice(data).unwrap()
    }

    pub fn search(&self, key: &[u8]) -> Option<BlockAddr> {
        self.blocks
            .iter()
            .find(|block| &block.last_key[..] >= key)
            .map(|block| block.block_addr)
    }
}

#[derive(Clone, Eq, PartialEq, Debug, Copy, Serialize, Deserialize)]
pub struct BlockAddr {
    pub start_offset: u64,
    pub end_offset: u64,
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
    pub fn add_block(
        &mut self,
        last_key: &[u8],
        start_offset: u64,
        stop_offset: u64,
        first_ordinal: u64,
    ) {
        self.index.blocks.push(BlockMeta {
            last_key: last_key.to_vec(),
            block_addr: BlockAddr {
                start_offset,
                end_offset: stop_offset,
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
        sstable_builder.add_block(b"aaa", 10u64, 20u64, 0u64);
        sstable_builder.add_block(b"bbbbbbb", 20u64, 30u64, 564);
        sstable_builder.add_block(b"ccc", 30u64, 40u64, 10u64);
        sstable_builder.add_block(b"dddd", 40u64, 50u64, 15u64);
        let mut buffer: Vec<u8> = Vec::new();
        sstable_builder.serialize(&mut buffer).unwrap();
        let sstable = SSTableIndex::load(&buffer[..]);
        assert_eq!(
            sstable.search(b"bbbde"),
            Some(BlockAddr {
                first_ordinal: 10u64,
                start_offset: 30u64,
                end_offset: 40u64
            })
        );
    }
}
