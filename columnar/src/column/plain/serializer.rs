// Only used by tests until the columnar writer supports the plain codec.
#![cfg_attr(not(test), allow(dead_code))]

use std::io;

use super::block::encode_block;
use super::block_index::PlainBlockIndexBuilder;
use super::{PLAIN_FOOTER_NUM_BYTES, PLAIN_FORMAT_VERSION};
use crate::column_index::{SerializableColumnIndex, serialize_column_index};

/// A block is closed once it holds this many values.
pub(crate) const PLAIN_BLOCK_MAX_NUM_VALUES: usize = 65_536;

/// A block is closed before its raw bytes exceed this size, unless it holds a single value.
pub(crate) const PLAIN_BLOCK_MAX_NUM_BYTES: usize = 1 << 20;

/// Serializes a plain-encoded column.
///
/// Values are streamed with `add_value`, in value order, and cut into blocks. Each block is
/// encoded as soon as it is full: memory is bounded by one block.
///
/// Layout: `[block 0] .. [block B-1] [block index] [column index] [footer]`, with
/// footer = `block_index_num_bytes: u32 | column_index_num_bytes: u32 | format_version: u8`.
pub(crate) struct PlainColumnSerializer<W: io::Write> {
    wrt: W,
    /// Raw bytes of the values of the current block, concatenated.
    block_bytes: Vec<u8>,
    /// Offsets of the values of the current block: value `i` is
    /// `block_bytes[block_offsets[i]..block_offsets[i + 1]]`. Invariant: `block_offsets[0] == 0`.
    block_offsets: Vec<u32>,
    encoded_block: Vec<u8>,
    scratch: Vec<u8>,
    block_index: PlainBlockIndexBuilder,
}

impl<W: io::Write> PlainColumnSerializer<W> {
    pub(crate) fn new(wrt: W) -> PlainColumnSerializer<W> {
        PlainColumnSerializer {
            wrt,
            block_bytes: Vec::new(),
            block_offsets: vec![0u32],
            encoded_block: Vec::new(),
            scratch: Vec::new(),
            block_index: PlainBlockIndexBuilder::default(),
        }
    }

    fn num_values_in_block(&self) -> usize {
        self.block_offsets.len() - 1
    }

    pub(crate) fn add_value(&mut self, value: &[u8]) -> io::Result<()> {
        self.add_value_with(value.len(), |block_bytes| {
            block_bytes.extend_from_slice(value)
        })
    }

    /// Adds a value of `len` bytes, appended to the block buffer by `append_value`.
    ///
    /// Contract: `append_value` appends exactly `len` bytes.
    pub(crate) fn add_value_with(
        &mut self,
        len: usize,
        append_value: impl FnOnce(&mut Vec<u8>),
    ) -> io::Result<()> {
        // A value larger than the block limit gets a block of its own.
        if self.num_values_in_block() > 0
            && self.block_bytes.len() + len > PLAIN_BLOCK_MAX_NUM_BYTES
        {
            self.flush_block()?;
        }
        let len_before = self.block_bytes.len();
        append_value(&mut self.block_bytes);
        assert_eq!(
            self.block_bytes.len() - len_before,
            len,
            "the value has an unexpected length"
        );
        let value_end = u32::try_from(self.block_bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "plain value too large"))?;
        self.block_offsets.push(value_end);
        if self.num_values_in_block() == PLAIN_BLOCK_MAX_NUM_VALUES {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> io::Result<()> {
        let num_values = self.num_values_in_block();
        if num_values == 0 {
            return Ok(());
        }
        encode_block(
            &self.block_bytes,
            &self.block_offsets,
            &mut self.encoded_block,
            &mut self.scratch,
        )?;
        self.wrt.write_all(&self.encoded_block)?;
        self.block_index
            .push_block(num_values as u32, self.encoded_block.len() as u64);
        self.block_bytes.clear();
        self.block_offsets.truncate(1);
        Ok(())
    }

    /// Writes the last block, the block index, the column index and the footer.
    ///
    /// Contract: `column_index` maps the docs to exactly the values added so far.
    pub(crate) fn finish(mut self, column_index: SerializableColumnIndex<'_>) -> io::Result<()> {
        self.flush_block()?;
        let mut block_index_bytes = Vec::new();
        self.block_index.serialize(&mut block_index_bytes);
        self.wrt.write_all(&block_index_bytes)?;
        let column_index_num_bytes = serialize_column_index(column_index, &mut self.wrt)?;
        let mut footer = [0u8; PLAIN_FOOTER_NUM_BYTES];
        footer[0..4].copy_from_slice(&(block_index_bytes.len() as u32).to_le_bytes());
        footer[4..8].copy_from_slice(&column_index_num_bytes.to_le_bytes());
        footer[8] = PLAIN_FORMAT_VERSION;
        self.wrt.write_all(&footer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_num_values(values: &[Vec<u8>]) -> Vec<u32> {
        let mut buffer = Vec::new();
        let mut serializer = PlainColumnSerializer::new(&mut buffer);
        for value in values {
            serializer.add_value(value).unwrap();
        }
        serializer.flush_block().unwrap();
        let block_index = serializer.block_index.build();
        (0..block_index.num_blocks())
            .map(|block_id| block_index.value_range(block_id).len() as u32)
            .collect()
    }

    #[test]
    fn test_blocks_cut_on_num_values() {
        let values: Vec<Vec<u8>> = vec![b"a".to_vec(); PLAIN_BLOCK_MAX_NUM_VALUES * 2 + 3];
        assert_eq!(
            block_num_values(&values),
            vec![
                PLAIN_BLOCK_MAX_NUM_VALUES as u32,
                PLAIN_BLOCK_MAX_NUM_VALUES as u32,
                3
            ]
        );
    }

    #[test]
    fn test_blocks_cut_on_num_bytes() {
        // 4 values of 300 KiB: 3 fit in 1 MiB.
        let values: Vec<Vec<u8>> = vec![vec![b'x'; 300 << 10]; 4];
        assert_eq!(block_num_values(&values), vec![3, 1]);
        // Exactly 1 MiB fits in a block.
        let values: Vec<Vec<u8>> = vec![vec![b'x'; 256 << 10]; 5];
        assert_eq!(block_num_values(&values), vec![4, 1]);
    }

    #[test]
    fn test_large_value_has_its_own_block() {
        let values: Vec<Vec<u8>> = vec![
            b"small".to_vec(),
            vec![b'x'; (1 << 20) + 1],
            b"small".to_vec(),
        ];
        assert_eq!(block_num_values(&values), vec![1, 1, 1]);
    }
}
