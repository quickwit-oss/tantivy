use std::io::{self, Read, Write};
use std::ops::Range;
use std::sync::Arc;

use common::{BinarySerializable, FixedSize, OwnedBytes};
use tantivy_bitpacker::{compute_num_bits, BitPacker};
use tantivy_fst::raw::Fst;
use tantivy_fst::{IntoStreamer, Map, MapBuilder, Streamer};

use crate::{common_prefix_len, SSTableDataCorruption, TermOrdinal};

#[derive(Debug, Clone)]
pub enum SSTableIndex {
    V2,
    V3(SSTableIndexV3),
    V3Empty(SSTableIndexV3Empty),
}

impl SSTableIndex {
    /// Get the [`BlockAddr`] of the requested block.
    pub(crate) fn get_block(&self, block_id: u64) -> Option<BlockAddr> {
        match self {
            SSTableIndex::V2 => todo!(),
            SSTableIndex::V3(v3_index) => v3_index.get_block(block_id),
            SSTableIndex::V3Empty(v3_empty) => v3_empty.get_block(block_id),
        }
    }

    /// Get the block id of the block that would contain `key`.
    ///
    /// Returns None if `key` is lexicographically after the last key recorded.
    pub(crate) fn locate_with_key(&self, key: &[u8]) -> Option<u64> {
        match self {
            SSTableIndex::V2 => todo!(),
            SSTableIndex::V3(v3_index) => v3_index.locate_with_key(key),
            SSTableIndex::V3Empty(v3_empty) => v3_empty.locate_with_key(key),
        }
    }

    /// Get the [`BlockAddr`] of the block that would contain `key`.
    ///
    /// Returns None if `key` is lexicographically after the last key recorded.
    pub fn get_block_with_key(&self, key: &[u8]) -> Option<BlockAddr> {
        match self {
            SSTableIndex::V2 => todo!(),
            SSTableIndex::V3(v3_index) => v3_index.get_block_with_key(key),
            SSTableIndex::V3Empty(v3_empty) => v3_empty.get_block_with_key(key),
        }
    }

    pub(crate) fn locate_with_ord(&self, ord: TermOrdinal) -> u64 {
        match self {
            SSTableIndex::V2 => todo!(),
            SSTableIndex::V3(v3_index) => v3_index.locate_with_ord(ord),
            SSTableIndex::V3Empty(v3_empty) => v3_empty.locate_with_ord(ord),
        }
    }

    /// Get the [`BlockAddr`] of the block containing the `ord`-th term.
    pub(crate) fn get_block_with_ord(&self, ord: TermOrdinal) -> BlockAddr {
        match self {
            SSTableIndex::V2 => todo!(),
            SSTableIndex::V3(v3_index) => v3_index.get_block_with_ord(ord),
            SSTableIndex::V3Empty(v3_empty) => v3_empty.get_block_with_ord(ord),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SSTableIndexV3 {
    fst_index: Arc<Map<OwnedBytes>>,
    block_addr_store: BlockAddrStore,
}

impl SSTableIndexV3 {
    /// Load an index from its binary representation
    pub fn load(
        data: OwnedBytes,
        fst_length: u64,
    ) -> Result<SSTableIndexV3, SSTableDataCorruption> {
        let (fst_slice, block_addr_store_slice) = data.split(fst_length as usize);
        let fst_index = Fst::new(fst_slice)
            .map_err(|_| SSTableDataCorruption)?
            .into();
        let block_addr_store =
            BlockAddrStore::open(block_addr_store_slice).map_err(|_| SSTableDataCorruption)?;

        Ok(SSTableIndexV3 {
            fst_index: Arc::new(fst_index),
            block_addr_store,
        })
    }

    /// Get the [`BlockAddr`] of the requested block.
    pub(crate) fn get_block(&self, block_id: u64) -> Option<BlockAddr> {
        self.block_addr_store.get(block_id)
    }

    /// Get the block id of the block that would contain `key`.
    ///
    /// Returns None if `key` is lexicographically after the last key recorded.
    pub(crate) fn locate_with_key(&self, key: &[u8]) -> Option<u64> {
        self.fst_index
            .range()
            .ge(key)
            .into_stream()
            .next()
            .map(|(_key, id)| id)
    }

    /// Get the [`BlockAddr`] of the block that would contain `key`.
    ///
    /// Returns None if `key` is lexicographically after the last key recorded.
    pub fn get_block_with_key(&self, key: &[u8]) -> Option<BlockAddr> {
        self.locate_with_key(key).and_then(|id| self.get_block(id))
    }

    pub(crate) fn locate_with_ord(&self, ord: TermOrdinal) -> u64 {
        self.block_addr_store.binary_search_ord(ord).0
    }

    /// Get the [`BlockAddr`] of the block containing the `ord`-th term.
    pub(crate) fn get_block_with_ord(&self, ord: TermOrdinal) -> BlockAddr {
        self.block_addr_store.binary_search_ord(ord).1
    }
}

#[derive(Debug, Clone)]
pub struct SSTableIndexV3Empty {
    block_addr: BlockAddr,
}

impl SSTableIndexV3Empty {
    pub fn load(index_start_pos: usize) -> SSTableIndexV3Empty {
        SSTableIndexV3Empty {
            block_addr: BlockAddr {
                first_ordinal: 0,
                byte_range: 0..index_start_pos,
            },
        }
    }

    /// Get the [`BlockAddr`] of the requested block.
    pub(crate) fn get_block(&self, _block_id: u64) -> Option<BlockAddr> {
        Some(self.block_addr.clone())
    }

    /// Get the block id of the block that would contain `key`.
    ///
    /// Returns None if `key` is lexicographically after the last key recorded.
    pub(crate) fn locate_with_key(&self, _key: &[u8]) -> Option<u64> {
        Some(0)
    }

    /// Get the [`BlockAddr`] of the block that would contain `key`.
    ///
    /// Returns None if `key` is lexicographically after the last key recorded.
    pub fn get_block_with_key(&self, _key: &[u8]) -> Option<BlockAddr> {
        Some(self.block_addr.clone())
    }

    pub(crate) fn locate_with_ord(&self, _ord: TermOrdinal) -> u64 {
        0
    }

    /// Get the [`BlockAddr`] of the block containing the `ord`-th term.
    pub(crate) fn get_block_with_ord(&self, _ord: TermOrdinal) -> BlockAddr {
        self.block_addr.clone()
    }
}
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct BlockAddr {
    pub first_ordinal: u64,
    pub byte_range: Range<usize>,
}

impl BlockAddr {
    fn to_block_start(&self) -> BlockStartAddr {
        BlockStartAddr {
            first_ordinal: self.first_ordinal,
            byte_range_start: self.byte_range.start,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockStartAddr {
    first_ordinal: u64,
    byte_range_start: usize,
}

impl BlockStartAddr {
    fn to_block_addr(&self, byte_range_end: usize) -> BlockAddr {
        BlockAddr {
            first_ordinal: self.first_ordinal,
            byte_range: self.byte_range_start..byte_range_end,
        }
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

impl BinarySerializable for BlockStartAddr {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        let start = self.byte_range_start as u64;
        start.serialize(writer)?;
        self.first_ordinal.serialize(writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let byte_range_start = u64::deserialize(reader)? as usize;
        let first_ordinal = u64::deserialize(reader)?;
        Ok(BlockStartAddr {
            first_ordinal,
            byte_range_start,
        })
    }

    // Provided method
    fn num_bytes(&self) -> u64 {
        BlockStartAddr::SIZE_IN_BYTES as u64
    }
}

impl FixedSize for BlockStartAddr {
    const SIZE_IN_BYTES: usize = 2 * u64::SIZE_IN_BYTES;
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

#[derive(Default)]
pub struct SSTableIndexBuilder {
    blocks: Vec<BlockMeta>,
}

impl SSTableIndexBuilder {
    /// In order to make the index as light as possible, we
    /// try to find a shorter alternative to the last key of the last block
    /// that is still smaller than the next key.
    pub(crate) fn shorten_last_block_key_given_next_key(&mut self, next_key: &[u8]) {
        if let Some(last_block) = self.blocks.last_mut() {
            find_shorter_str_in_between(&mut last_block.last_key_or_greater, next_key);
        }
    }

    pub fn add_block(&mut self, last_key: &[u8], byte_range: Range<usize>, first_ordinal: u64) {
        self.blocks.push(BlockMeta {
            last_key_or_greater: last_key.to_vec(),
            block_addr: BlockAddr {
                byte_range,
                first_ordinal,
            },
        })
    }

    pub fn serialize<W: std::io::Write>(&self, wrt: W) -> io::Result<u64> {
        if self.blocks.len() <= 1 {
            return Ok(0);
        }
        // TODO handle errors
        let counting_writer = common::CountingWriter::wrap(wrt);
        let mut map_builder = MapBuilder::new(counting_writer).unwrap();
        for (i, block) in self.blocks.iter().enumerate() {
            map_builder
                .insert(&block.last_key_or_greater, i as u64)
                .unwrap();
        }
        let counting_writer = map_builder.into_inner().unwrap();
        let written_bytes = counting_writer.written_bytes();
        let mut wrt = counting_writer;

        let mut block_store_writer = BlockAddrStoreWriter::new();
        for block in &self.blocks {
            block_store_writer.write_block_meta(block.block_addr.clone())?;
        }
        block_store_writer.serialize(&mut wrt)?;
        eprintln!("fst len={written_bytes}");
        eprintln!("store len={}", wrt.written_bytes() - written_bytes);

        Ok(written_bytes)
    }
}

const STORE_BLOCK_LEN: usize = 256;

#[derive(Debug)]
struct BlockAddrBlockMetadata {
    offset: u64,
    ref_block_addr: BlockStartAddr,
    range_start_slop: u32,
    first_ordinal_slop: u32,
    range_start_nbits: u8,
    first_ordinal_nbits: u8,
    block_len: u16,
    // these fields are computed on deserialization, and not stored
    range_shift: i64,
    ordinal_shift: i64,
}

impl BlockAddrBlockMetadata {
    fn num_bits(&self) -> u8 {
        self.first_ordinal_nbits + self.range_start_nbits
    }

    fn deserialize_block_addr(&self, data: &[u8], inner_offset: usize) -> Option<BlockAddr> {
        if inner_offset == 0 {
            let range_end = self.ref_block_addr.byte_range_start
                + extract_bits(data, 0, self.range_start_nbits) as usize
                + self.range_start_slop as usize
                - self.range_shift as usize;
            return Some(self.ref_block_addr.to_block_addr(range_end));
        }
        let inner_offset = inner_offset - 1;
        if inner_offset >= self.block_len as usize {
            return None;
        }
        let num_bits = self.num_bits() as usize;

        let range_start_addr = num_bits * inner_offset;
        let ordinal_addr = range_start_addr + self.range_start_nbits as usize;
        let range_end_addr = range_start_addr + num_bits;

        if (range_end_addr + self.range_start_nbits as usize + 7) / 8 > data.len() {
            return None;
        }

        let range_start = self.ref_block_addr.byte_range_start
            + extract_bits(data, range_start_addr, self.range_start_nbits) as usize
            + self.range_start_slop as usize * (inner_offset + 1)
            - self.range_shift as usize;
        let first_ordinal = self.ref_block_addr.first_ordinal
            + extract_bits(data, ordinal_addr, self.first_ordinal_nbits)
            + self.first_ordinal_slop as u64 * (inner_offset + 1) as u64
            - self.ordinal_shift as u64;
        let range_end = self.ref_block_addr.byte_range_start
            + extract_bits(data, range_end_addr, self.range_start_nbits) as usize
            + self.range_start_slop as usize * (inner_offset + 2)
            - self.range_shift as usize;

        Some(BlockAddr {
            first_ordinal,
            byte_range: range_start..range_end,
        })
    }

    // /!\ countrary to deserialize_block_addr() for which inner_offset goes from 0 to
    // STORE_BLOCK_LEN - 2, this function goes from 1 to STORE_BLOCK_LEN - 1, and 0 marks
    // we should use ref_block_addr
    fn bissect_for_ord(&self, data: &[u8], target_ord: TermOrdinal) -> (u64, BlockAddr) {
        // TODO can panic if block has header only
        let inner_target_ord = target_ord - self.ref_block_addr.first_ordinal;
        let num_bits = self.num_bits() as usize;
        let range_start_nbits = self.range_start_nbits as usize;
        let get_ord = |index| {
            extract_bits(
                data,
                num_bits * index as usize + range_start_nbits,
                self.first_ordinal_nbits,
            ) + self.first_ordinal_slop as u64 * (index + 1)
                - self.ordinal_shift as u64
        };

        let inner_offset = match binary_search(self.block_len as u64, |index| {
            get_ord(index).cmp(&inner_target_ord)
        }) {
            Ok(inner_offset) => inner_offset + 1,
            Err(inner_offset) => inner_offset,
        };
        (
            inner_offset,
            self.deserialize_block_addr(data, inner_offset as usize)
                .unwrap(),
        )
    }
}

// TODO move this function to tantivy_common?
fn extract_bits(data: &[u8], addr_bits: usize, num_bits: u8) -> u64 {
    use byteorder::{ByteOrder, LittleEndian};
    assert!(num_bits <= 56);
    let addr_byte = addr_bits / 8;
    let bit_shift = (addr_bits % 8) as u64;
    let val_unshifted_unmasked: u64 = if data.len() >= addr_byte + 8 {
        LittleEndian::read_u64(&data[addr_byte..][..8])
    } else {
        // the buffer is not large enough.
        // Let's copy the few remaining bytes to a 8 byte buffer
        // padded with 0s.
        let mut buf = [0u8; 8];
        let data_to_copy = &data[addr_byte..];
        let nbytes = data_to_copy.len();
        buf[..nbytes].copy_from_slice(data_to_copy);
        LittleEndian::read_u64(&buf)
    };
    let val_shifted_unmasked = val_unshifted_unmasked >> bit_shift;
    let mask = (1u64 << u64::from(num_bits)) - 1;
    val_shifted_unmasked & mask
}

impl BinarySerializable for BlockAddrBlockMetadata {
    fn serialize<W: Write + ?Sized>(&self, write: &mut W) -> io::Result<()> {
        self.offset.serialize(write)?;
        self.ref_block_addr.serialize(write)?;
        self.range_start_slop.serialize(write)?;
        self.first_ordinal_slop.serialize(write)?;
        write.write_all(&[self.first_ordinal_nbits, self.range_start_nbits])?;
        self.block_len.serialize(write)?;
        self.num_bits();
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let offset = u64::deserialize(reader)?;
        let ref_block_addr = BlockStartAddr::deserialize(reader)?;
        let range_start_slop = u32::deserialize(reader)?;
        let first_ordinal_slop = u32::deserialize(reader)?;
        let mut buffer = [0u8; 2];
        reader.read_exact(&mut buffer)?;
        let first_ordinal_nbits = buffer[0];
        let range_start_nbits = buffer[1];
        let block_len = u16::deserialize(reader)?;
        Ok(BlockAddrBlockMetadata {
            offset,
            ref_block_addr,
            range_start_slop,
            first_ordinal_slop,
            range_start_nbits,
            first_ordinal_nbits,
            block_len,
            range_shift: 1 << (range_start_nbits - 1),
            ordinal_shift: 1 << (first_ordinal_nbits - 1),
        })
    }
}

impl FixedSize for BlockAddrBlockMetadata {
    const SIZE_IN_BYTES: usize = u64::SIZE_IN_BYTES
        + BlockStartAddr::SIZE_IN_BYTES
        + 2 * u32::SIZE_IN_BYTES
        + 2 * u8::SIZE_IN_BYTES
        + u16::SIZE_IN_BYTES;
}

#[derive(Debug, Clone)]
struct BlockAddrStore {
    block_meta_bytes: OwnedBytes,
    addr_bytes: OwnedBytes,
}

impl BlockAddrStore {
    fn open(term_info_store_file: OwnedBytes) -> io::Result<BlockAddrStore> {
        let (mut len_slice, main_slice) = term_info_store_file.split(8);
        let len = u64::deserialize(&mut len_slice)? as usize;
        let (block_meta_bytes, addr_bytes) = main_slice.split(len);
        Ok(BlockAddrStore {
            block_meta_bytes,
            addr_bytes,
        })
    }

    fn get_block_meta(&self, store_block_id: usize) -> Option<BlockAddrBlockMetadata> {
        let mut block_data: &[u8] = self
            .block_meta_bytes
            .get(store_block_id * BlockAddrBlockMetadata::SIZE_IN_BYTES..)?;
        BlockAddrBlockMetadata::deserialize(&mut block_data).ok()
    }

    fn get(&self, block_id: u64) -> Option<BlockAddr> {
        let store_block_id = (block_id as usize) / STORE_BLOCK_LEN;
        let inner_offset = (block_id as usize) % STORE_BLOCK_LEN;
        let block_addr_block_data = self.get_block_meta(store_block_id)?;
        block_addr_block_data.deserialize_block_addr(
            &self.addr_bytes[block_addr_block_data.offset as usize..],
            inner_offset,
        )
    }

    fn binary_search_ord(&self, ord: TermOrdinal) -> (u64, BlockAddr) {
        let max_block =
            (self.block_meta_bytes.len() / BlockAddrBlockMetadata::SIZE_IN_BYTES) as u64;
        let get_first_ordinal = |block_id| {
            self.get(block_id * STORE_BLOCK_LEN as u64)
                .unwrap()
                .first_ordinal
        };
        let store_block_id =
            binary_search(max_block, |block_id| get_first_ordinal(block_id).cmp(&ord));
        let store_block_id = match store_block_id {
            Ok(store_block_id) => {
                let block_id = store_block_id * STORE_BLOCK_LEN as u64;
                return (block_id, self.get(block_id).unwrap());
            }
            Err(store_block_id) => store_block_id - 1,
        };

        let block_addr_block_data = self.get_block_meta(store_block_id as usize).unwrap();
        let (inner_offset, block_addr) = block_addr_block_data.bissect_for_ord(
            &self.addr_bytes[block_addr_block_data.offset as usize..],
            ord,
        );
        (
            store_block_id * STORE_BLOCK_LEN as u64 + inner_offset,
            block_addr,
        )
    }
}

fn binary_search(max: u64, cmp_fn: impl Fn(u64) -> std::cmp::Ordering) -> Result<u64, u64> {
    use std::cmp::Ordering::*;
    let mut size = max;
    let mut left = 0;
    let mut right = size;
    while left < right {
        let mid = left + size / 2;

        let cmp = cmp_fn(mid);

        if cmp == Less {
            left = mid + 1;
        } else if cmp == Greater {
            right = mid;
        } else {
            return Ok(mid);
        }

        size = right - left;
    }
    Err(left)
}

struct BlockAddrStoreWriter {
    buffer_block_metas: Vec<u8>,
    buffer_addrs: Vec<u8>,
    block_addrs: Vec<BlockAddr>,
}

impl BlockAddrStoreWriter {
    fn new() -> Self {
        BlockAddrStoreWriter {
            buffer_block_metas: Vec::new(),
            buffer_addrs: Vec::new(),
            block_addrs: Vec::with_capacity(STORE_BLOCK_LEN),
        }
    }

    fn flush_block(&mut self) -> io::Result<()> {
        let ref_block_addr = self.block_addrs[0].clone();

        for block_addr in &mut self.block_addrs {
            block_addr.byte_range.start -= ref_block_addr.byte_range.start;
            block_addr.first_ordinal -= ref_block_addr.first_ordinal;
        }

        let mut last_block_addr = self.block_addrs.last().unwrap().clone();
        last_block_addr.byte_range.end -= ref_block_addr.byte_range.start;

        let (range_start_slop, range_start_nbits) = find_best_slop(
            self.block_addrs
                .iter()
                .map(|block| block.byte_range.start as u64)
                .chain(std::iter::once(last_block_addr.byte_range.end as u64))
                .enumerate()
                .skip(1),
        );

        let (first_ordinal_slop, first_ordinal_nbits) = find_best_slop(
            self.block_addrs
                .iter()
                .map(|block| block.first_ordinal)
                .enumerate()
                .skip(1),
        );

        let range_shift = 1 << (range_start_nbits - 1);
        let ordinal_shift = 1 << (first_ordinal_nbits - 1);

        let block_addr_block_meta = BlockAddrBlockMetadata {
            offset: self.buffer_addrs.len() as u64,
            ref_block_addr: ref_block_addr.to_block_start(),
            range_start_slop,
            first_ordinal_slop,
            range_start_nbits,
            first_ordinal_nbits,
            block_len: self.block_addrs.len() as u16 - 1,
            range_shift,
            ordinal_shift,
        };
        block_addr_block_meta.serialize(&mut self.buffer_block_metas)?;

        let mut bit_packer = BitPacker::new();

        for (i, block_addr) in self.block_addrs.iter().enumerate().skip(1) {
            let range_pred = (range_start_slop as usize * i) as i64;
            bit_packer.write(
                (block_addr.byte_range.start as i64 - range_pred + range_shift) as u64,
                range_start_nbits,
                &mut self.buffer_addrs,
            )?;
            let first_ordinal_pred = (first_ordinal_slop as u64 * i as u64) as i64;
            bit_packer.write(
                (block_addr.first_ordinal as i64 - first_ordinal_pred + ordinal_shift) as u64,
                first_ordinal_nbits,
                &mut self.buffer_addrs,
            )?;
        }

        let range_pred = (range_start_slop as usize * self.block_addrs.len()) as i64;
        bit_packer.write(
            (last_block_addr.byte_range.end as i64 - range_pred + range_shift) as u64,
            range_start_nbits,
            &mut self.buffer_addrs,
        )?;
        bit_packer.flush(&mut self.buffer_addrs)?;

        self.block_addrs.clear();
        Ok(())
    }

    fn write_block_meta(&mut self, block_addr: BlockAddr) -> io::Result<()> {
        self.block_addrs.push(block_addr);
        if self.block_addrs.len() >= STORE_BLOCK_LEN {
            self.flush_block()?;
        }
        Ok(())
    }

    fn serialize<W: std::io::Write>(&mut self, wrt: &mut W) -> io::Result<()> {
        if !self.block_addrs.is_empty() {
            self.flush_block()?;
        }
        let len = self.buffer_block_metas.len() as u64;
        len.serialize(wrt)?;
        wrt.write_all(&self.buffer_block_metas)?;
        wrt.write_all(&self.buffer_addrs)?;
        Ok(())
    }
}

fn find_best_slop(elements: impl Iterator<Item = (usize, u64)> + Clone) -> (u32, u8) {
    let slop_iterator = elements.clone();
    let derivation_iterator = elements;

    let mut min_slop_idx = 1;
    let mut min_slop_val = 0;
    let mut min_slop = u32::MAX;
    let mut max_slop_idx = 1;
    let mut max_slop_val = 0;
    let mut max_slop = 0;
    for (index, value) in slop_iterator {
        let slop = (value / index as u64) as u32;
        if slop <= min_slop {
            min_slop = slop;
            min_slop_idx = index;
            min_slop_val = value;
        }
        if slop >= max_slop {
            max_slop = slop;
            max_slop_idx = index;
            max_slop_val = value;
        }
    }

    // above is an heristic giving the "highest" and "lowest" point. It's imperfect in that in that
    // a point that appear earlier might have a high slop derivation, but a smaller absolute
    // derivation than a latter point.
    // The actual best values can be obtained by using the symplex method, but the improvement is
    // likely minimal, and computation is way more complexe.
    //
    // Assuming these point are the furthest up and down, we find the slop that would cause the same
    // positive derivation for the highest as negative derivation for the lowest.
    // A is the optimal slop. B is the derivation to the guess
    //
    // 0 = min_slop_val - min_slop_idx * A - B
    // 0 = max_slop_val - max_slop_idx * A + B
    //
    // 0 = min_slop_val + max_slop_val - (min_slop_idx + max_slop_idx) * A
    // (min_slop_val + max_slop_val) / (min_slop_idx + max_slop_idx) = A
    //
    // we actually add some correcting factor to have proper rounding, not truncation.

    let denominator = (min_slop_idx + max_slop_idx) as u64;
    let final_slop = ((min_slop_val + max_slop_val + denominator / 2) / denominator) as u32;

    // we don't solve for B because our choice of point is suboptimal, so it's actually a lower
    // bound and we need to iterate to find the actual worst value.

    let max_derivation = derivation_iterator.fold(0, |max_derivation, (index, value)| {
        let derivation = (value as i64 - final_slop as i64 * index as i64).unsigned_abs();

        max_derivation.max(derivation)
    });

    (final_slop, compute_num_bits(max_derivation) + 1)
}

#[cfg(test)]
mod tests {
    use common::OwnedBytes;

    use super::{BlockAddr, SSTableIndexBuilder, SSTableIndexV3};
    use crate::SSTableDataCorruption;

    #[test]
    fn test_sstable_index() {
        let mut sstable_builder = SSTableIndexBuilder::default();
        sstable_builder.add_block(b"aaa", 10..20, 0u64);
        sstable_builder.add_block(b"bbbbbbb", 20..30, 5u64);
        sstable_builder.add_block(b"ccc", 30..40, 10u64);
        sstable_builder.add_block(b"dddd", 40..50, 15u64);
        let mut buffer: Vec<u8> = Vec::new();
        let fst_len = sstable_builder.serialize(&mut buffer).unwrap();
        let buffer = OwnedBytes::new(buffer);
        let sstable_index = SSTableIndexV3::load(buffer, fst_len).unwrap();
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
        let fst_len = sstable_builder.serialize(&mut buffer).unwrap();
        buffer[2] = 9u8;
        let buffer = OwnedBytes::new(buffer);
        let data_corruption_err = SSTableIndexV3::load(buffer, fst_len).err().unwrap();
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
