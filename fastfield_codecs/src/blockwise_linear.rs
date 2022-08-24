use std::io;
use std::sync::Arc;

use common::{BinarySerializable, CountingWriter, DeserializeFrom};
use ownedbytes::OwnedBytes;
use tantivy_bitpacker::{compute_num_bits, BitPacker, BitUnpacker};

use crate::line::Line;
use crate::{Column, FastFieldCodec, FastFieldCodecType, VecColumn};

const CHUNK_SIZE: usize = 512;

#[derive(Debug)]
struct Block {
    line: Line,
    bit_unpacker: BitUnpacker,
    start_offset: usize,
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
            start_offset: 0,
        })
    }
}

#[derive(Debug)]
struct BlockwiseLinearParams {
    num_vals: u64,
    min_value: u64,
    max_value: u64,
    blocks: Vec<Block>,
}

impl BinarySerializable for BlockwiseLinearParams {
    fn serialize<W: io::Write>(&self, wrt: &mut W) -> io::Result<()> {
        self.num_vals.serialize(wrt)?;
        self.min_value.serialize(wrt)?;
        self.max_value.serialize(wrt)?;
        let expected_num_blocks = compute_num_blocks(self.num_vals);
        assert_eq!(expected_num_blocks, self.blocks.len());
        for block in &self.blocks {
            block.serialize(wrt)?;
        }
        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<BlockwiseLinearParams> {
        let num_vals = u64::deserialize(reader)?;
        let min_value = u64::deserialize(reader)?;
        let max_value = u64::deserialize(reader)?;
        let num_blocks = compute_num_blocks(num_vals);
        let mut blocks = Vec::with_capacity(num_blocks);
        for _ in 0..num_blocks {
            blocks.push(Block::deserialize(reader)?);
        }
        Ok(BlockwiseLinearParams {
            num_vals,
            min_value,
            max_value,
            blocks,
        })
    }
}

fn compute_num_blocks(num_vals: u64) -> usize {
    (num_vals as usize + CHUNK_SIZE - 1) / CHUNK_SIZE
}

pub struct BlockwiseLinearCodec;

impl FastFieldCodec for BlockwiseLinearCodec {
    const CODEC_TYPE: crate::FastFieldCodecType = FastFieldCodecType::BlockwiseLinear;
    type Reader = BlockwiseLinearReader;

    fn open_from_bytes(bytes: ownedbytes::OwnedBytes) -> io::Result<Self::Reader> {
        let footer_len: u32 = (&bytes[bytes.len() - 4..]).deserialize()?;
        let footer_offset = bytes.len() - 4 - footer_len as usize;
        let (data, mut footer) = bytes.split(footer_offset);
        let mut params = BlockwiseLinearParams::deserialize(&mut footer)?;
        let mut start_offset = 0;
        for block in params.blocks.iter_mut() {
            block.start_offset = start_offset;
            start_offset += (block.bit_unpacker.bit_width() as usize) * CHUNK_SIZE / 8;
        }
        Ok(BlockwiseLinearReader {
            params: Arc::new(params),
            data,
        })
    }

    fn estimate(_fastfield_accessor: &impl crate::Column) -> Option<f32> {
        Some(0.1f32)
    }

    fn serialize(
        wrt: &mut impl io::Write,
        fastfield_accessor: &dyn crate::Column,
    ) -> io::Result<()> {
        let mut buffer = Vec::with_capacity(CHUNK_SIZE);
        let num_vals = fastfield_accessor.num_vals();

        let num_blocks = compute_num_blocks(num_vals);
        let mut blocks = Vec::with_capacity(num_blocks);

        let mut vals = fastfield_accessor.iter();

        let mut bit_packer = BitPacker::new();

        for _ in 0..num_blocks {
            buffer.clear();
            buffer.extend((&mut vals).take(CHUNK_SIZE));
            let line = Line::train(&VecColumn(&buffer));

            assert!(!buffer.is_empty());

            for (i, buffer_val) in buffer.iter_mut().enumerate() {
                let interpolated_val = line.eval(i as u64);
                *buffer_val = buffer_val.wrapping_sub(interpolated_val);
            }
            let bit_width = buffer.iter().copied().map(compute_num_bits).max().unwrap();

            for &buffer_val in &buffer {
                bit_packer.write(buffer_val, bit_width, wrt)?;
            }

            blocks.push(Block {
                line,
                bit_unpacker: BitUnpacker::new(bit_width),
                start_offset: 0,
            });
        }

        let params = BlockwiseLinearParams {
            num_vals,
            min_value: fastfield_accessor.min_value(),
            max_value: fastfield_accessor.max_value(),
            blocks,
        };
        bit_packer.close(wrt)?;

        let mut counting_wrt = CountingWriter::wrap(wrt);

        params.serialize(&mut counting_wrt)?;

        let footer_len = counting_wrt.written_bytes();

        (footer_len as u32).serialize(&mut counting_wrt)?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct BlockwiseLinearReader {
    params: Arc<BlockwiseLinearParams>,
    data: OwnedBytes,
}

impl Column for BlockwiseLinearReader {
    #[inline(always)]
    fn get_val(&self, idx: u64) -> u64 {
        let block_id = (idx / CHUNK_SIZE as u64) as usize;
        let idx_within_block = idx % (CHUNK_SIZE as u64);
        let block = &self.params.blocks[block_id];
        let interpoled_val: u64 = block.line.eval(idx_within_block);
        let block_bytes = &self.data[block.start_offset..];
        let bitpacked_diff = block.bit_unpacker.get(idx_within_block, block_bytes);
        interpoled_val.wrapping_add(bitpacked_diff)
    }

    fn min_value(&self) -> u64 {
        self.params.min_value
    }

    fn max_value(&self) -> u64 {
        self.params.max_value
    }

    fn num_vals(&self) -> u64 {
        self.params.num_vals
    }
}
