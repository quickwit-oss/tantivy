use std::sync::Arc;
use std::{io, iter};

use common::{BinarySerializable, CountingWriter, DeserializeFrom};
use ownedbytes::OwnedBytes;
use tantivy_bitpacker::{compute_num_bits, BitPacker, BitUnpacker};

use crate::line::Line;
use crate::optional_column::OptionalColumn;
use crate::serialize::NormalizedHeader;
use crate::{Column, FastFieldCodec, FastFieldCodecType, VecColumn};

const CHUNK_SIZE: usize = 512;

#[derive(Debug, Default)]
struct Block {
    line: Line,
    bit_unpacker: BitUnpacker,
    data_start_offset: usize,
}

impl BinarySerializable for Block {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        self.line.serialize(writer)?;
        self.bit_unpacker.bit_width().serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let line = Line::deserialize(reader)?;
        let bit_width = u8::deserialize(reader)?;
        Ok(Block {
            line,
            bit_unpacker: BitUnpacker::new(bit_width),
            data_start_offset: 0,
        })
    }
}

fn compute_num_blocks(num_vals: u32) -> usize {
    (num_vals as usize + CHUNK_SIZE - 1) / CHUNK_SIZE
}

pub struct BlockwiseLinearCodec;

impl FastFieldCodec for BlockwiseLinearCodec {
    const CODEC_TYPE: crate::FastFieldCodecType = FastFieldCodecType::BlockwiseLinear;
    type Reader = BlockwiseLinearReader;

    fn open_from_bytes(
        bytes: ownedbytes::OwnedBytes,
        normalized_header: NormalizedHeader,
    ) -> io::Result<Self::Reader> {
        let footer_len: u32 = (&bytes[bytes.len() - 4..]).deserialize()?;
        let footer_offset = bytes.len() - 4 - footer_len as usize;
        let (data, mut footer) = bytes.split(footer_offset);
        let num_blocks = compute_num_blocks(normalized_header.num_vals);
        let mut blocks: Vec<Block> = iter::repeat_with(|| Block::deserialize(&mut footer))
            .take(num_blocks)
            .collect::<io::Result<_>>()?;

        let mut start_offset = 0;
        for block in &mut blocks {
            block.data_start_offset = start_offset;
            start_offset += (block.bit_unpacker.bit_width() as usize) * CHUNK_SIZE / 8;
        }
        Ok(BlockwiseLinearReader {
            blocks: Arc::new(blocks),
            data,
            normalized_header,
        })
    }

    // Estimate first_chunk and extrapolate
    fn estimate(column: &dyn crate::Column) -> Option<f32> {
        if column.num_vals() < 10 * CHUNK_SIZE as u32 {
            return None;
        }
        let mut first_chunk: Vec<u64> = column.iter().take(CHUNK_SIZE as usize).collect();
        let line = Line::train(&VecColumn::from(&first_chunk));
        for (i, buffer_val) in first_chunk.iter_mut().enumerate() {
            let interpolated_val = line.eval(i as u32);
            *buffer_val = buffer_val.wrapping_sub(interpolated_val);
        }
        let estimated_bit_width = first_chunk
            .iter()
            .map(|el| ((el + 1) as f32 * 3.0) as u64)
            .map(compute_num_bits)
            .max()
            .unwrap();

        let metadata_per_block = {
            let mut out = vec![];
            Block::default().serialize(&mut out).unwrap();
            out.len()
        };
        let num_bits = estimated_bit_width as u64 * column.num_vals() as u64
            // function metadata per block
            + metadata_per_block as u64 * (column.num_vals() as u64 / CHUNK_SIZE as u64);
        let num_bits_uncompressed = 64 * column.num_vals();
        Some(num_bits as f32 / num_bits_uncompressed as f32)
    }

    fn serialize(column: &dyn Column, wrt: &mut impl io::Write) -> io::Result<()> {
        // The BitpackedReader assumes a normalized vector.
        assert_eq!(column.min_value(), 0);
        let mut buffer = Vec::with_capacity(CHUNK_SIZE);
        let num_vals = column.num_vals();

        let num_blocks = compute_num_blocks(num_vals);
        let mut blocks = Vec::with_capacity(num_blocks);

        let mut vals = column.iter();

        let mut bit_packer = BitPacker::new();

        for _ in 0..num_blocks {
            buffer.clear();
            buffer.extend((&mut vals).take(CHUNK_SIZE));
            let line = Line::train(&VecColumn::from(&buffer));

            assert!(!buffer.is_empty());

            for (i, buffer_val) in buffer.iter_mut().enumerate() {
                let interpolated_val = line.eval(i as u32);
                *buffer_val = buffer_val.wrapping_sub(interpolated_val);
            }
            let bit_width = buffer.iter().copied().map(compute_num_bits).max().unwrap();

            for &buffer_val in &buffer {
                bit_packer.write(buffer_val, bit_width, wrt)?;
            }

            blocks.push(Block {
                line,
                bit_unpacker: BitUnpacker::new(bit_width),
                data_start_offset: 0,
            });
        }

        bit_packer.close(wrt)?;

        assert_eq!(blocks.len(), compute_num_blocks(num_vals));

        let mut counting_wrt = CountingWriter::wrap(wrt);
        for block in &blocks {
            block.serialize(&mut counting_wrt)?;
        }
        let footer_len = counting_wrt.written_bytes();
        (footer_len as u32).serialize(&mut counting_wrt)?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct BlockwiseLinearReader {
    blocks: Arc<Vec<Block>>,
    normalized_header: NormalizedHeader,
    data: OwnedBytes,
}

impl Column for BlockwiseLinearReader {
    #[inline(always)]
    fn get_val(&self, idx: u32) -> u64 {
        let block_id = (idx / CHUNK_SIZE as u32) as usize;
        let idx_within_block = idx % (CHUNK_SIZE as u32);
        let block = &self.blocks[block_id];
        let interpoled_val: u64 = block.line.eval(idx_within_block);
        let block_bytes = &self.data[block.data_start_offset..];
        let bitpacked_diff = block.bit_unpacker.get(idx_within_block, block_bytes);
        interpoled_val.wrapping_add(bitpacked_diff)
    }

    fn min_value(&self) -> u64 {
        // The BlockwiseLinearReader assumes a normalized vector.
        0u64
    }

    fn max_value(&self) -> u64 {
        self.normalized_header.max_value
    }

    fn num_vals(&self) -> u32 {
        self.normalized_header.num_vals
    }
}

impl OptionalColumn for BlockwiseLinearReader {
    #[inline]
    fn get_val(&self, idx: u32) -> Option<u64> {
        let block_id = (idx / CHUNK_SIZE as u32) as usize;
        let idx_within_block = idx % (CHUNK_SIZE as u32);
        let block = &self.blocks[block_id];
        let interpoled_val: u64 = block.line.eval(idx_within_block);
        let block_bytes = &self.data[block.data_start_offset..];
        let bitpacked_diff = block.bit_unpacker.get(idx_within_block, block_bytes);
        Some(interpoled_val.wrapping_add(bitpacked_diff))
    }
    #[inline]
    fn min_value(&self) -> Option<u64> {
        // The BitpackedReader assumes a normalized vector.
        Some(0)
    }
    #[inline]
    fn max_value(&self) -> Option<u64> {
        Some(self.normalized_header.max_value)
    }
    #[inline]
    fn num_vals(&self) -> u32 {
        self.normalized_header.num_vals
    }
}
