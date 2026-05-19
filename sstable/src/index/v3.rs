use std::io::{self, Read, Write};
use std::sync::Arc;

use common::{BinarySerializable, FixedSize, OwnedBytes};
use tantivy_bitpacker::{BitPacker, compute_num_bits};
use tantivy_fst::raw::Fst;
use tantivy_fst::{Automaton, IntoStreamer, Map, Streamer};

use super::{BlockAddr, BlockStartAddr};
use crate::block_match_automaton::can_block_match_automaton;
use crate::{SSTableDataCorruption, TermOrdinal};

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

    pub(crate) fn get_and_locate_with_ord(&self, ord: TermOrdinal) -> (BlockAddr, u64) {
        let (location, block_addr) = self.block_addr_store.binary_search_ord(ord);
        (block_addr, location)
    }

    pub(crate) fn get_block_for_automaton<'a>(
        &'a self,
        automaton: &'a impl Automaton,
    ) -> impl Iterator<Item = (u64, BlockAddr)> + 'a {
        // this is more complicated than other index formats: we don't have a ready made list of
        // blocks, and instead need to stream-decode the sstable.

        GetBlockForAutomaton {
            streamer: self.fst_index.stream(),
            block_addr_store: &self.block_addr_store,
            prev_key: None,
            automaton,
        }
    }
}

// TODO we iterate over the entire Map to find matching blocks,
// we could manually iterate on the underlying Fst and skip whole branches if our Automaton says
// cannot match. this isn't as bad as it sounds given the fst is a lot smaller than the rest of the
// sstable.
// To do that, we can't use tantivy_fst's Stream with an automaton, as we need to know 2 consecutive
// fst keys to form a proper opinion on whether this is a match, which we want translate into a
// single automaton
struct GetBlockForAutomaton<'a, A: Automaton> {
    streamer: tantivy_fst::map::Stream<'a>,
    block_addr_store: &'a BlockAddrStore,
    prev_key: Option<Vec<u8>>,
    automaton: &'a A,
}

impl<A: Automaton> Iterator for GetBlockForAutomaton<'_, A> {
    type Item = (u64, BlockAddr);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some((new_key, block_id)) = self.streamer.next() {
            if let Some(prev_key) = self.prev_key.as_mut() {
                if can_block_match_automaton(Some(prev_key), new_key, self.automaton) {
                    prev_key.clear();
                    prev_key.extend_from_slice(new_key);
                    return Some((block_id, self.block_addr_store.get(block_id).unwrap()));
                }
                prev_key.clear();
                prev_key.extend_from_slice(new_key);
            } else {
                self.prev_key = Some(new_key.to_owned());
                if can_block_match_automaton(None, new_key, self.automaton) {
                    return Some((block_id, self.block_addr_store.get(block_id).unwrap()));
                }
            }
        }
        None
    }
}

#[derive(Debug, Clone)]
pub struct SSTableIndexV3Empty {
    pub block_addr: BlockAddr,
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
    pub(crate) fn get_block(&self, block_id: u64) -> Option<BlockAddr> {
        (block_id == 0).then(|| self.block_addr.clone())
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

    pub(crate) fn get_and_locate_with_ord(&self, _ord: TermOrdinal) -> (BlockAddr, u64) {
        (self.block_addr.clone(), 0)
    }
}

const STORE_BLOCK_LEN: usize = 128;

#[derive(Debug)]
struct BlockAddrBlockMetadata {
    offset: u64,
    ref_block_addr: BlockStartAddr,
    range_start_slope: u32,
    first_ordinal_slope: u32,
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
                + self.range_start_slope as usize
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

        if (range_end_addr + self.range_start_nbits as usize).div_ceil(8) > data.len() {
            return None;
        }

        let range_start = self.ref_block_addr.byte_range_start
            + extract_bits(data, range_start_addr, self.range_start_nbits) as usize
            + self.range_start_slope as usize * (inner_offset + 1)
            - self.range_shift as usize;
        let first_ordinal = self.ref_block_addr.first_ordinal
            + extract_bits(data, ordinal_addr, self.first_ordinal_nbits)
            + self.first_ordinal_slope as u64 * (inner_offset + 1) as u64
            - self.ordinal_shift as u64;
        let range_end = self.ref_block_addr.byte_range_start
            + extract_bits(data, range_end_addr, self.range_start_nbits) as usize
            + self.range_start_slope as usize * (inner_offset + 2)
            - self.range_shift as usize;

        Some(BlockAddr {
            first_ordinal,
            byte_range: range_start..range_end,
        })
    }

    fn bisect_for_ord(&self, data: &[u8], target_ord: TermOrdinal) -> (u64, BlockAddr) {
        let inner_target_ord = target_ord - self.ref_block_addr.first_ordinal;
        let num_bits = self.num_bits() as usize;
        let range_start_nbits = self.range_start_nbits as usize;
        let get_ord = |index| {
            extract_bits(
                data,
                num_bits * index as usize + range_start_nbits,
                self.first_ordinal_nbits,
            ) + self.first_ordinal_slope as u64 * (index + 1)
                - self.ordinal_shift as u64
        };

        let inner_offset = match binary_search(self.block_len as u64, |index| {
            get_ord(index).cmp(&inner_target_ord)
        }) {
            Ok(inner_offset) => inner_offset + 1,
            Err(inner_offset) => inner_offset,
        };
        // we can unwrap because inner_offset <= self.block_len
        (
            inner_offset,
            self.deserialize_block_addr(data, inner_offset as usize)
                .unwrap(),
        )
    }
}

// TODO move this function to tantivy_common?
#[inline(always)]
fn extract_bits(data: &[u8], addr_bits: usize, num_bits: u8) -> u64 {
    assert!(num_bits <= 56);
    let addr_byte = addr_bits / 8;
    let bit_shift = (addr_bits % 8) as u64;
    let val_unshifted_unmasked: u64 = if data.len() >= addr_byte + 8 {
        let b = data[addr_byte..addr_byte + 8].try_into().unwrap();
        u64::from_le_bytes(b)
    } else {
        // the buffer is not large enough.
        // Let's copy the few remaining bytes to a 8 byte buffer
        // padded with 0s.
        let mut buf = [0u8; 8];
        let data_to_copy = &data[addr_byte..];
        let nbytes = data_to_copy.len();
        buf[..nbytes].copy_from_slice(data_to_copy);
        u64::from_le_bytes(buf)
    };
    let val_shifted_unmasked = val_unshifted_unmasked >> bit_shift;
    let mask = (1u64 << u64::from(num_bits)) - 1;
    val_shifted_unmasked & mask
}

impl BinarySerializable for BlockAddrBlockMetadata {
    fn serialize<W: Write + ?Sized>(&self, write: &mut W) -> io::Result<()> {
        self.offset.serialize(write)?;
        self.ref_block_addr.serialize(write)?;
        self.range_start_slope.serialize(write)?;
        self.first_ordinal_slope.serialize(write)?;
        write.write_all(&[self.first_ordinal_nbits, self.range_start_nbits])?;
        self.block_len.serialize(write)?;
        self.num_bits();
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let offset = u64::deserialize(reader)?;
        let ref_block_addr = BlockStartAddr::deserialize(reader)?;
        let range_start_slope = u32::deserialize(reader)?;
        let first_ordinal_slope = u32::deserialize(reader)?;
        let mut buffer = [0u8; 2];
        reader.read_exact(&mut buffer)?;
        let first_ordinal_nbits = buffer[0];
        let range_start_nbits = buffer[1];
        let block_len = u16::deserialize(reader)?;
        Ok(BlockAddrBlockMetadata {
            offset,
            ref_block_addr,
            range_start_slope,
            first_ordinal_slope,
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
        + 2
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
            // we can unwrap because block_id < max_block
            self.get(block_id * STORE_BLOCK_LEN as u64)
                .unwrap()
                .first_ordinal
        };
        let store_block_id =
            binary_search(max_block, |block_id| get_first_ordinal(block_id).cmp(&ord));
        let store_block_id = match store_block_id {
            Ok(store_block_id) => {
                let block_id = store_block_id * STORE_BLOCK_LEN as u64;
                // we can unwrap because store_block_id < max_block
                return (block_id, self.get(block_id).unwrap());
            }
            Err(store_block_id) => store_block_id - 1,
        };

        // we can unwrap because store_block_id < max_block
        let block_addr_block_data = self.get_block_meta(store_block_id as usize).unwrap();
        let (inner_offset, block_addr) = block_addr_block_data.bisect_for_ord(
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

pub(crate) struct BlockAddrStoreWriter {
    buffer_block_metas: Vec<u8>,
    buffer_addrs: Vec<u8>,
    block_addrs: Vec<BlockAddr>,
}

impl BlockAddrStoreWriter {
    pub(crate) fn new() -> Self {
        BlockAddrStoreWriter {
            buffer_block_metas: Vec::new(),
            buffer_addrs: Vec::new(),
            block_addrs: Vec::with_capacity(STORE_BLOCK_LEN),
        }
    }

    pub(crate) fn flush_block(&mut self) -> io::Result<()> {
        if self.block_addrs.is_empty() {
            return Ok(());
        }
        let ref_block_addr = self.block_addrs[0].clone();

        for block_addr in &mut self.block_addrs {
            block_addr.byte_range.start -= ref_block_addr.byte_range.start;
            block_addr.first_ordinal -= ref_block_addr.first_ordinal;
        }

        // we are only called if block_addrs is not empty
        let mut last_block_addr = self.block_addrs.last().unwrap().clone();
        last_block_addr.byte_range.end -= ref_block_addr.byte_range.start;

        // we skip(1), so we never give an index of 0 to find_best_slope
        let (range_start_slope, range_start_nbits) = find_best_slope(
            self.block_addrs
                .iter()
                .map(|block| block.byte_range.start as u64)
                .chain(std::iter::once(last_block_addr.byte_range.end as u64))
                .enumerate()
                .skip(1),
        );

        // we skip(1), so we never give an index of 0 to find_best_slope
        let (first_ordinal_slope, first_ordinal_nbits) = find_best_slope(
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
            range_start_slope,
            first_ordinal_slope,
            range_start_nbits,
            first_ordinal_nbits,
            block_len: self.block_addrs.len() as u16 - 1,
            range_shift,
            ordinal_shift,
        };
        block_addr_block_meta.serialize(&mut self.buffer_block_metas)?;

        let mut bit_packer = BitPacker::new();

        for (i, block_addr) in self.block_addrs.iter().enumerate().skip(1) {
            let range_pred = (range_start_slope as usize * i) as i64;
            bit_packer.write(
                (block_addr.byte_range.start as i64 - range_pred + range_shift) as u64,
                range_start_nbits,
                &mut self.buffer_addrs,
            )?;
            let first_ordinal_pred = (first_ordinal_slope as u64 * i as u64) as i64;
            bit_packer.write(
                (block_addr.first_ordinal as i64 - first_ordinal_pred + ordinal_shift) as u64,
                first_ordinal_nbits,
                &mut self.buffer_addrs,
            )?;
        }

        let range_pred = (range_start_slope as usize * self.block_addrs.len()) as i64;
        bit_packer.write(
            (last_block_addr.byte_range.end as i64 - range_pred + range_shift) as u64,
            range_start_nbits,
            &mut self.buffer_addrs,
        )?;
        bit_packer.flush(&mut self.buffer_addrs)?;

        self.block_addrs.clear();
        Ok(())
    }

    pub(crate) fn write_block_meta(&mut self, block_addr: BlockAddr) -> io::Result<()> {
        self.block_addrs.push(block_addr);
        if self.block_addrs.len() >= STORE_BLOCK_LEN {
            self.flush_block()?;
        }
        Ok(())
    }

    pub(crate) fn serialize<W: std::io::Write>(&mut self, wrt: &mut W) -> io::Result<()> {
        self.flush_block()?;
        let len = self.buffer_block_metas.len() as u64;
        len.serialize(wrt)?;
        wrt.write_all(&self.buffer_block_metas)?;
        wrt.write_all(&self.buffer_addrs)?;
        Ok(())
    }
}

/// Given an iterator over (index, value), returns the slope, and number of bits needed to
/// represent the error to a prediction made by this slope.
///
/// The iterator may be empty, but all indexes in it must be non-zero.
fn find_best_slope(elements: impl Iterator<Item = (usize, u64)> + Clone) -> (u32, u8) {
    let slope_iterator = elements.clone();
    let derivation_iterator = elements;

    let mut min_slope_idx = 1;
    let mut min_slope_val = 0;
    let mut min_slope = u32::MAX;
    let mut max_slope_idx = 1;
    let mut max_slope_val = 0;
    let mut max_slope = 0;
    for (index, value) in slope_iterator {
        let slope = (value / index as u64) as u32;
        if slope <= min_slope {
            min_slope = slope;
            min_slope_idx = index;
            min_slope_val = value;
        }
        if slope >= max_slope {
            max_slope = slope;
            max_slope_idx = index;
            max_slope_val = value;
        }
    }

    // above is an heuristic giving the "highest" and "lowest" point. It's imperfect in that in that
    // a point that appear earlier might have a high slope derivation, but a smaller absolute
    // derivation than a latter point.
    // The actual best values can be obtained by using the symplex method, but the improvement is
    // likely minimal, and computation is way more complex.
    //
    // Assuming these point are the furthest up and down, we find the slope that would cause the
    // same positive derivation for the highest as negative derivation for the lowest.
    // A is the optimal slope. B is the derivation to the guess
    //
    // 0 = min_slope_val - min_slope_idx * A - B
    // 0 = max_slope_val - max_slope_idx * A + B
    //
    // 0 = min_slope_val + max_slope_val - (min_slope_idx + max_slope_idx) * A
    // (min_slope_val + max_slope_val) / (min_slope_idx + max_slope_idx) = A
    //
    // we actually add some correcting factor to have proper rounding, not truncation.

    let denominator = (min_slope_idx + max_slope_idx) as u64;
    let final_slope = ((min_slope_val + max_slope_val + denominator / 2) / denominator) as u32;

    // we don't solve for B because our choice of point is suboptimal, so it's actually a lower
    // bound and we need to iterate to find the actual worst value.

    let max_derivation: u64 = derivation_iterator
        .map(|(index, value)| (value as i64 - final_slope as i64 * index as i64).unsigned_abs())
        .max()
        .unwrap_or(0);

    (final_slope, compute_num_bits(max_derivation) + 1)
}

#[cfg(test)]
mod tests {
    use common::OwnedBytes;

    use super::*;
    use crate::block_match_automaton::tests::EqBuffer;
    use crate::index::BlockMeta;
    use crate::{SSTableDataCorruption, SSTableIndexBuilder};

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

    //    use proptest::prelude::*;

    #[test]
    fn test_find_best_slop() {
        assert_eq!(super::find_best_slope(std::iter::empty()), (0, 1));
        assert_eq!(
            super::find_best_slope(std::iter::once((1, 12345))),
            (12345, 1)
        );
    }

    #[test]
    fn test_get_block_for_automaton() {
        let sstable_index_builder = SSTableIndexBuilder {
            blocks: vec![
                BlockMeta {
                    last_key_or_greater: vec![0, 1, 2],
                    block_addr: BlockAddr {
                        first_ordinal: 0,
                        byte_range: 0..10,
                    },
                },
                BlockMeta {
                    last_key_or_greater: vec![0, 2, 2],
                    block_addr: BlockAddr {
                        first_ordinal: 5,
                        byte_range: 10..20,
                    },
                },
                BlockMeta {
                    last_key_or_greater: vec![0, 3, 2],
                    block_addr: BlockAddr {
                        first_ordinal: 10,
                        byte_range: 20..30,
                    },
                },
            ],
        };

        let mut sstable_index_bytes = Vec::new();
        let fst_len = sstable_index_builder
            .serialize(&mut sstable_index_bytes)
            .unwrap();

        let sstable = SSTableIndexV3::load(OwnedBytes::new(sstable_index_bytes), fst_len).unwrap();

        let res = sstable
            .get_block_for_automaton(&EqBuffer(vec![0, 1, 1]))
            .collect::<Vec<_>>();
        assert_eq!(
            res,
            vec![(
                0,
                BlockAddr {
                    first_ordinal: 0,
                    byte_range: 0..10
                }
            )]
        );
        let res = sstable
            .get_block_for_automaton(&EqBuffer(vec![0, 2, 1]))
            .collect::<Vec<_>>();
        assert_eq!(
            res,
            vec![(
                1,
                BlockAddr {
                    first_ordinal: 5,
                    byte_range: 10..20
                }
            )]
        );
        let res = sstable
            .get_block_for_automaton(&EqBuffer(vec![0, 3, 1]))
            .collect::<Vec<_>>();
        assert_eq!(
            res,
            vec![(
                2,
                BlockAddr {
                    first_ordinal: 10,
                    byte_range: 20..30
                }
            )]
        );
        let res = sstable
            .get_block_for_automaton(&EqBuffer(vec![0, 4, 1]))
            .collect::<Vec<_>>();
        assert!(res.is_empty());

        let complex_automaton = EqBuffer(vec![0, 1, 1]).union(EqBuffer(vec![0, 3, 1]));
        let res = sstable
            .get_block_for_automaton(&complex_automaton)
            .collect::<Vec<_>>();
        assert_eq!(
            res,
            vec![
                (
                    0,
                    BlockAddr {
                        first_ordinal: 0,
                        byte_range: 0..10
                    }
                ),
                (
                    2,
                    BlockAddr {
                        first_ordinal: 10,
                        byte_range: 20..30
                    }
                )
            ]
        );
    }
}
