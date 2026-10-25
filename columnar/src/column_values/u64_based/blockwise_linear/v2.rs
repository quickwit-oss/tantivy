//! V2 layout: column stats, bitpacked residuals, fixed-size block records, three width bytes.
//!
//! Each record contains the slope, intercept, residual byte offset, and residual bit width.
//! The first three fields use little-endian integers with the minimum byte widths needed
//! across all blocks (zero bytes for an all-zero field). The residual bit width uses one
//! byte. Widths are stored at the end so the writer need not buffer the residuals.
//! The record size and row count locate the metadata without scanning it on open.

use std::io::{self, Write};

use common::{BinarySerializable, OwnedBytes};
use tantivy_bitpacker::BitUnpacker;

use super::{
    Block, BlockMetadata, BlockwiseLinearEstimator, BlockwiseLinearReader, compute_num_blocks,
};
use crate::column_values::u64_based::line::Line;
use crate::column_values::u64_based::{ColumnCodec, ColumnStats};

/// Byte widths of the line parameters and residual offset in each block record.
#[derive(Clone, Copy, Default)]
pub(super) struct BlockWidths {
    slope_bytes: u8,
    intercept_bytes: u8,
    offset_bytes: u8,
    masks: [u64; 3],
}

impl BlockWidths {
    fn new(slope_bytes: u8, intercept_bytes: u8, offset_bytes: u8) -> Self {
        Self {
            slope_bytes,
            intercept_bytes,
            offset_bytes,
            masks: [slope_bytes, intercept_bytes, offset_bytes]
                .map(|width| u64::MAX.checked_shr(64 - 8 * width as u32).unwrap_or(0)),
        }
    }

    pub(super) fn collect(&mut self, line: Line, data_start_offset: u64) {
        let num_bytes = |value: u64| (64 - value.leading_zeros()).div_ceil(8) as u8;
        *self = Self::new(
            self.slope_bytes.max(num_bytes(line.slope)),
            self.intercept_bytes.max(num_bytes(line.intercept)),
            self.offset_bytes.max(num_bytes(data_start_offset)),
        );
    }

    pub(super) fn record_size(self) -> usize {
        (self.slope_bytes + self.intercept_bytes + self.offset_bytes) as usize + 1
    }

    #[inline(always)]
    pub(super) fn read_block(self, bytes: &[u8]) -> Block {
        // Keep partial-word copies out of the lookup path. Seven extra bytes suffice
        // for an eight-byte load starting at any field in the record.
        if bytes.len() < self.record_size() + 7 {
            return self.read_block_tail(bytes);
        }
        let read_field = |offset: usize, mask: u64| {
            // Overlap subsequent fields/records instead of copying a variable-size field.
            u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) & mask
        };
        let slope = read_field(0, self.masks[0]);
        let intercept = read_field(self.slope_bytes as usize, self.masks[1]);
        let data_start_offset = read_field(
            (self.slope_bytes + self.intercept_bytes) as usize,
            self.masks[2],
        );
        Block {
            line: Line { slope, intercept },
            bit_unpacker: BitUnpacker::new(bytes[self.record_size() - 1]),
            data_start_offset: usize::try_from(data_start_offset)
                .expect("block offset exceeds usize"),
        }
    }

    #[inline(never)]
    fn read_block_tail(self, bytes: &[u8]) -> Block {
        assert!(bytes.len() >= self.record_size());
        // The largest record is 25 bytes; padding is local to this lookup, not the file.
        let mut padded = [0; 32];
        padded[..bytes.len()].copy_from_slice(bytes);
        self.read_block(&padded)
    }
}

pub(super) fn serialize_blocks(blocks: &[Block], wrt: &mut dyn Write) -> io::Result<()> {
    let mut widths = BlockWidths::default();
    for block in blocks {
        widths.collect(block.line, block.data_start_offset as u64);
    }
    for block in blocks {
        wrt.write_all(&block.line.slope.to_le_bytes()[..widths.slope_bytes as usize])?;
        wrt.write_all(&block.line.intercept.to_le_bytes()[..widths.intercept_bytes as usize])?;
        wrt.write_all(
            &(block.data_start_offset as u64).to_le_bytes()[..widths.offset_bytes as usize],
        )?;
        block.bit_unpacker.bit_width().serialize(wrt)?;
    }
    wrt.write_all(&[
        widths.slope_bytes,
        widths.intercept_bytes,
        widths.offset_bytes,
    ])
}

/// Fixed-size serialized block metadata for V2.
#[derive(Clone)]
pub struct BlockRecords {
    bytes: OwnedBytes,
    widths: BlockWidths,
}

impl BlockMetadata for BlockRecords {
    #[inline(always)]
    fn get_block(&self, block_id: usize) -> Block {
        self.widths
            .read_block(&self.bytes[block_id * self.widths.record_size()..])
    }
}

pub struct BlockwiseLinearCodec;

impl ColumnCodec for BlockwiseLinearCodec {
    type ColumnValues = BlockwiseLinearReader<BlockRecords>;
    type Estimator = BlockwiseLinearEstimator;

    fn load(mut bytes: OwnedBytes) -> io::Result<Self::ColumnValues> {
        let stats = ColumnStats::deserialize(&mut bytes)?;
        let invalid_footer = || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid blockwise linear V2 footer",
            )
        };
        let &[slope_bytes, intercept_bytes, offset_bytes] =
            bytes.last_chunk().ok_or_else(invalid_footer)?;
        if slope_bytes > 8 || intercept_bytes > 8 || offset_bytes > 8 {
            return Err(invalid_footer());
        }
        let widths = BlockWidths::new(slope_bytes, intercept_bytes, offset_bytes);
        let metadata_len = compute_num_blocks(stats.num_rows) as usize * widths.record_size();
        let metadata_offset = (bytes.len() - 3)
            .checked_sub(metadata_len)
            .ok_or_else(invalid_footer)?;
        let (data, footer) = bytes.split(metadata_offset);
        Ok(BlockwiseLinearReader {
            blocks: BlockRecords {
                bytes: footer.slice(0..metadata_len),
                widths,
            },
            data,
            stats,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_values::u64_based::tests::create_and_validate;
    use crate::column_values::{
        CodecType, ColumnValues, load_u64_based_column_values, serialize_u64_based_column_values,
    };

    fn serialize(vals: &[u64]) -> OwnedBytes {
        let mut bytes = Vec::new();
        serialize_u64_based_column_values(&vals, &[CodecType::BlockwiseLinearV2], &mut bytes)
            .unwrap();
        assert_eq!(bytes.remove(0), 3);
        OwnedBytes::new(bytes)
    }

    #[test]
    fn test_block_boundaries_and_shared_metadata() {
        for len in [0, 1, 511, 512, 513, 1024, 2049] {
            // Alternate linear, noisy, descending, and full-width blocks.
            let vals: Vec<u64> = (0..len)
                .map(|i| match i / 512 {
                    0 => i as u64,
                    1 => (i % 17) as u64,
                    2 => (2048 - i) as u64,
                    _ if i % 2 == 0 => u64::MAX,
                    _ => 0,
                })
                .collect();
            create_and_validate::<BlockwiseLinearCodec>(&vals, "block boundaries").unwrap();
            let bytes = serialize(&vals);
            let reader = BlockwiseLinearCodec::load(bytes.clone()).unwrap();
            let BlockRecords {
                bytes: metadata,
                widths,
            } = &reader.blocks;
            let metadata_offset = bytes.len() - 3 - metadata.len();
            assert_eq!(metadata.as_ptr(), bytes[metadata_offset..].as_ptr());
            assert_eq!(
                metadata.len(),
                compute_num_blocks(len) as usize * widths.record_size()
            );
            // Access backwards so reading a block cannot depend on preceding block state.
            for i in (0..len).rev() {
                assert_eq!(reader.get_val(i), vals[i as usize]);
            }
            for start in [0, 1, 510, 511, 512, 513] {
                if start <= len {
                    let mut output = vec![0; (len - start) as usize];
                    reader.get_range(start as u64, &mut output);
                    assert_eq!(output, vals[start as usize..]);
                }
            }
        }
    }

    #[test]
    fn test_minimum_field_widths() {
        // Exercise every byte width, including omitted fields and full-width u64s.
        for width in 0..=8usize {
            let value = match width {
                0 => 0,
                8 => u64::MAX,
                _ => 1 << ((width - 1) * 8),
            };
            let offset = value.min(usize::MAX as u64) as usize;
            let offset_width = width.min(std::mem::size_of::<usize>());
            let widths = [width as u8, width as u8, offset_width as u8];
            let blocks = [
                Block::default(),
                Block {
                    line: Line {
                        slope: value,
                        intercept: value,
                    },
                    data_start_offset: offset,
                    bit_unpacker: BitUnpacker::new(64),
                },
            ];
            let mut bytes = Vec::new();
            serialize_blocks(&blocks, &mut bytes).unwrap();
            let record_size = 2 * width + offset_width + 1;
            assert_eq!(bytes.len(), 2 * record_size + 3);
            assert_eq!(&bytes[2 * record_size..], &widths);
            let mut block_widths = BlockWidths::default();
            block_widths.collect(blocks[1].line, offset as u64);
            let decoded = block_widths.read_block(&bytes[record_size..]);
            assert_eq!(decoded.line.slope, value);
            assert_eq!(decoded.line.intercept, value);
            assert_eq!(decoded.data_start_offset, offset);
            assert_eq!(decoded.bit_unpacker.bit_width(), 64);
        }
        let bytes = serialize(&[10; 1024]);
        assert_eq!(&bytes[bytes.len() - 3..], &[0, 0, 0]);
        assert!(BlockwiseLinearCodec::load(bytes).unwrap().data.is_empty());
    }

    #[test]
    fn test_mixed_field_widths() {
        let value_for_width = |width: u8| match width {
            0 => 0,
            8 => u64::MAX,
            _ => 1 << (width * 8 - 1),
        };
        for slope_bytes in 0..=8 {
            for intercept_bytes in 0..=8 {
                for offset_bytes in 0..=std::mem::size_of::<usize>() as u8 {
                    let block = Block {
                        line: Line {
                            slope: value_for_width(slope_bytes),
                            intercept: value_for_width(intercept_bytes),
                        },
                        data_start_offset: value_for_width(offset_bytes) as usize,
                        bit_unpacker: BitUnpacker::new(64),
                    };
                    let blocks: Vec<Block> = (0..10)
                        .map(|i| if i % 2 == 0 { Block::default() } else { block })
                        .collect();
                    let mut bytes = Vec::new();
                    serialize_blocks(&blocks, &mut bytes).unwrap();
                    let widths = BlockWidths::new(slope_bytes, intercept_bytes, offset_bytes);
                    assert_eq!(
                        &bytes[bytes.len() - 3..],
                        &[slope_bytes, intercept_bytes, offset_bytes]
                    );
                    let records = &bytes[..bytes.len() - 3];
                    // Both overlapping word loads and the short metadata tail, including
                    // one-byte records with every integer field omitted.
                    for (i, expected) in blocks.iter().enumerate() {
                        let decoded = widths.read_block(&records[i * widths.record_size()..]);
                        assert_eq!(decoded.line.slope, expected.line.slope);
                        assert_eq!(decoded.line.intercept, expected.line.intercept);
                        assert_eq!(decoded.data_start_offset, expected.data_start_offset);
                        assert_eq!(
                            decoded.bit_unpacker.bit_width(),
                            expected.bit_unpacker.bit_width()
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_mapped_ranges() {
        fn check<T: crate::MonotonicallyMappableToU64>(values: &[T]) {
            for codec in [
                CodecType::Bitpacked,
                CodecType::Linear,
                CodecType::BlockwiseLinearV2,
            ] {
                let mut bytes = Vec::new();
                serialize_u64_based_column_values(&values, &[codec], &mut bytes).unwrap();
                let column = load_u64_based_column_values::<T>(OwnedBytes::new(bytes)).unwrap();
                for start in [0, 1, 63, 64, 510, 511, 512, 513, values.len()] {
                    for len in [0, 1, 63, 64, 65, 1024, values.len() - start] {
                        if len <= values.len() - start {
                            let mut output = vec![values[0]; len];
                            column.get_range(start as u64, &mut output);
                            assert_eq!(output, values[start..start + len]);
                        }
                    }
                }
            }
        }
        let signed: Vec<i64> = (0..2049).map(|i| i - 1024 + i % 17).collect();
        check(&signed);
        let floats: Vec<f64> = signed.iter().map(|&i| i as f64 / 10.0).collect();
        check(&floats);
        let dates: Vec<common::DateTime> = signed
            .iter()
            .map(|&i| common::DateTime::from_timestamp_nanos(1_700_000_000_000_000_000 + i * 1000))
            .collect();
        check(&dates);
    }

    #[test]
    fn test_invalid_footer() {
        let bytes = serialize(&[0; 513]);
        // Missing widths or records.
        for len in 4..bytes.len() {
            assert!(BlockwiseLinearCodec::load(bytes.slice(0..len)).is_err());
        }
        let mut bytes = bytes.to_vec();
        *bytes.last_mut().unwrap() = 9;
        assert!(BlockwiseLinearCodec::load(OwnedBytes::new(bytes)).is_err());
    }

    #[test]
    fn test_legacy_codec_request_writes_v2() {
        let vals = [10u64, 20, 40];
        let mut bytes = Vec::new();
        serialize_u64_based_column_values(&&vals[..], &[CodecType::BlockwiseLinear], &mut bytes)
            .unwrap();
        assert_eq!(bytes[0], 3);
        let reader = load_u64_based_column_values::<u64>(OwnedBytes::new(bytes)).unwrap();
        assert_eq!(reader.iter().collect::<Vec<_>>(), vals);
    }

    #[test]
    fn test_legacy_codec_fixture() {
        // V1: codec 2, stats for [10, 10], zero slope/intercept/bit width, footer length 3.
        let bytes = vec![2, 138, 129, 128, 130, 128, 128, 0, 3, 0, 0, 0];
        let reader = load_u64_based_column_values::<u64>(OwnedBytes::new(bytes)).unwrap();
        assert_eq!(reader.num_vals(), 2);
        assert_eq!(reader.get_val(0), 10);
        assert_eq!(reader.get_val(1), 10);
    }
}
