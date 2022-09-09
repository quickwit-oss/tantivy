/// This codec takes a large number space (u128) and reduces it to a compact number space.
///
/// It will find spaces in the numer range. For example:
///
/// 100, 101, 102, 103, 104, 50000, 50001
/// could be mapped to
/// 100..104 -> 0..4
/// 50000..50001 -> 5..6
///
/// Compact space 0..=6 requires much less bits than 100..=50001
///
/// The codec is created to compress ip addresses, but may be employed in other use cases.
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    io::{self, Write},
    net::{IpAddr, Ipv6Addr},
    ops::RangeInclusive,
};

use common::{BinarySerializable, CountingWriter, VIntU128};
use ownedbytes::OwnedBytes;
use tantivy_bitpacker::{self, BitPacker, BitUnpacker};

use crate::column::{ColumnV2, ColumnV2Ext};

pub fn ip_to_u128(ip_addr: IpAddr) -> u128 {
    let ip_addr_v6: Ipv6Addr = match ip_addr {
        IpAddr::V4(v4) => v4.to_ipv6_mapped(),
        IpAddr::V6(v6) => v6,
    };
    u128::from_be_bytes(ip_addr_v6.octets())
}

const INTERVAL_COST_IN_BITS: usize = 64;

/// Store blank size and position. Order by blank size.
///
/// A blank is an unoccupied space in the data.
/// E.g. [100, 201] would have a `BlankAndPos{ pos: 0, blank_size: 100}`
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
struct BlankSizeAndPos {
    blank_size: u128,
    /// Position in the sorted data.
    pos: usize,
}

impl Ord for BlankSizeAndPos {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.blank_size.cmp(&other.blank_size)
    }
}
impl PartialOrd for BlankSizeAndPos {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.blank_size.partial_cmp(&other.blank_size)
    }
}

/// Put the deltas for the sorted values into a binary heap
fn get_deltas(values_sorted: &[u128]) -> BinaryHeap<BlankSizeAndPos> {
    let mut prev_opt = None;
    let mut deltas: BinaryHeap<BlankSizeAndPos> = BinaryHeap::new();
    for (pos, value) in values_sorted.iter().cloned().enumerate() {
        let blank_size = if let Some(prev) = prev_opt {
            value - prev
        } else {
            value + 1
        };
        // skip too small deltas
        if blank_size > 2 {
            deltas.push(BlankSizeAndPos { blank_size, pos });
        }
        prev_opt = Some(value);
    }
    deltas
}

struct BlankCollector {
    blanks: Vec<BlankSizeAndPos>,
    staged_blanks_sum: u128,
}
impl BlankCollector {
    fn new() -> Self {
        Self {
            blanks: vec![],
            staged_blanks_sum: 0,
        }
    }
    fn stage_blank(&mut self, blank: BlankSizeAndPos) {
        self.staged_blanks_sum += blank.blank_size - 1;
        self.blanks.push(blank);
    }
    fn drain(&mut self) -> std::vec::Drain<'_, BlankSizeAndPos> {
        self.staged_blanks_sum = 0;
        self.blanks.drain(..)
    }
    fn staged_blanks_sum(&self) -> u128 {
        self.staged_blanks_sum
    }
    fn num_blanks(&self) -> usize {
        self.blanks.len()
    }
}

fn num_bits(val: u128) -> u8 {
    (128u32 - val.leading_zeros()) as u8
}

/// Will collect blanks and add them to compact space if more bits are saved than cost from
/// metadata.
fn get_compact_space(values_sorted: &[u128], cost_per_blank: usize) -> CompactSpace {
    let max_val_incl_null = *values_sorted.last().unwrap_or(&0u128) + 1;
    let mut deltas = get_deltas(values_sorted);
    let mut amplitude_compact_space = max_val_incl_null;
    let mut amplitude_bits: u8 = num_bits(amplitude_compact_space);

    let mut compact_space = CompactSpaceBuilder::new();

    let mut blank_collector = BlankCollector::new();
    // We will stage blanks until they reduce the compact space by 1 bit.
    // Binary heap to process the gaps by their size
    while let Some(delta_and_pos) = deltas.pop() {
        blank_collector.stage_blank(delta_and_pos);

        let staged_spaces_sum: u128 = blank_collector.staged_blanks_sum();
        // +1 for later added null value
        let amplitude_new_compact_space = amplitude_compact_space - staged_spaces_sum + 1;
        let amplitude_new_bits = num_bits(amplitude_new_compact_space);
        if amplitude_bits == amplitude_new_bits {
            continue;
        }
        let saved_bits = (amplitude_bits - amplitude_new_bits) as usize * values_sorted.len();
        let cost = blank_collector.num_blanks() * cost_per_blank;
        if cost >= saved_bits {
            // Continue here, since although we walk over the deltas by size,
            // we can potentially save a lot at the last bits, which are smaller deltas
            //
            // E.g. if the first range reduces the compact space by 1000 from 2000 to 1000, which
            // saves 11-10=1 bit and the next range reduces the compact space by 950 to
            // 50, which saves 10-6=4 bit
            continue;
        }

        amplitude_compact_space = amplitude_new_compact_space;
        amplitude_bits = amplitude_new_bits;
        for pos in blank_collector
            .drain()
            .map(|blank_and_pos| blank_and_pos.pos)
        {
            let blank_end = values_sorted[pos] - 1;
            let blank_start = if pos == 0 {
                0
            } else {
                values_sorted[pos - 1] + 1
            };
            compact_space.add_blank(blank_start..=blank_end);
        }
    }
    if max_val_incl_null != u128::MAX {
        compact_space.add_blank(max_val_incl_null..=u128::MAX);
    }

    compact_space.finish()
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct CompactSpaceBuilder {
    covered_space: Vec<RangeInclusive<u128>>,
}

impl CompactSpaceBuilder {
    /// Creates a new compact space builder which will initially cover the whole space.
    fn new() -> Self {
        Self {
            covered_space: vec![0..=u128::MAX],
        }
    }

    /// Will extend the first range and assign the null value to it.
    fn assign_and_return_null(&mut self) -> u128 {
        self.covered_space[0] = *self.covered_space[0].start()..=*self.covered_space[0].end() + 1;
        *self.covered_space[0].end()
    }

    /// Assumes that repeated add_blank calls don't overlap, which will be the case on sorted
    /// values.
    fn add_blank(&mut self, blank: RangeInclusive<u128>) {
        let position = self
            .covered_space
            .iter()
            .position(|range| range.start() <= blank.start() && range.end() >= blank.end());
        if let Some(position) = position {
            let old_range = self.covered_space.remove(position);
            // Exact match, just remove
            if old_range == blank {
                return;
            }
            let new_range_end = blank.end().saturating_add(1)..=*old_range.end();
            if old_range.start() == blank.start() {
                self.covered_space.insert(position, new_range_end);
                return;
            }
            let new_range_start = *old_range.start()..=blank.start().saturating_sub(1);
            if old_range.end() == blank.end() {
                self.covered_space.insert(position, new_range_start);
                return;
            }
            self.covered_space.insert(position, new_range_end);
            self.covered_space.insert(position, new_range_start);
        }
    }
    fn finish(mut self) -> CompactSpace {
        let null_value = self.assign_and_return_null();

        let mut compact_start: u64 = 0;
        let mut ranges_and_compact_start = vec![];
        for cov in self.covered_space {
            let covered_range_len = cov.end() - cov.start();
            ranges_and_compact_start.push((cov, compact_start));
            compact_start += covered_range_len as u64 + 1;
        }
        CompactSpace {
            ranges_and_compact_start,
            null_value,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct CompactSpace {
    ranges_and_compact_start: Vec<(RangeInclusive<u128>, u64)>,
    pub null_value: u128,
}

impl BinarySerializable for CompactSpace {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        VIntU128(self.null_value).serialize(writer)?;
        VIntU128(self.ranges_and_compact_start.len() as u128).serialize(writer)?;

        let mut prev_value = 0;
        for (value_range, _compact) in &self.ranges_and_compact_start {
            let delta = value_range.start() - prev_value;
            VIntU128(delta).serialize(writer)?;
            prev_value = *value_range.start();

            let delta = value_range.end() - prev_value;
            VIntU128(delta).serialize(writer)?;
            prev_value = *value_range.end();
        }

        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let null_value = VIntU128::deserialize(reader)?.0;
        let num_values = VIntU128::deserialize(reader)?.0;
        let mut ranges_and_compact_start: Vec<(RangeInclusive<u128>, u64)> = vec![];
        let mut value = 0u128;
        let mut compact = 0u64;
        for _ in 0..num_values {
            let delta = VIntU128::deserialize(reader)?.0;
            value += delta;
            let value_start = value;

            let delta = VIntU128::deserialize(reader)?.0;
            value += delta;
            let value_end = value;

            let compact_delta = value_end - value_start + 1;

            ranges_and_compact_start.push((value_start..=value_end, compact));
            compact += compact_delta as u64;
        }

        Ok(Self {
            null_value,
            ranges_and_compact_start,
        })
    }
}

impl CompactSpace {
    fn amplitude_compact_space(&self) -> u128 {
        let last_range = &self.ranges_and_compact_start[self.ranges_and_compact_start.len() - 1];
        last_range.1 as u128 + (last_range.0.end() - last_range.0.start()) + 1
    }

    fn get_range_and_compact_start(&self, pos: usize) -> &(RangeInclusive<u128>, u64) {
        &self.ranges_and_compact_start[pos]
    }

    /// Returns either Ok(the value in the compact space) or if it is outside the compact space the
    /// Err(position on the next larger range above the value)
    fn to_compact(&self, value: u128) -> Result<u64, usize> {
        self.ranges_and_compact_start
            .binary_search_by(|probe| {
                let value_range = &probe.0;
                if *value_range.start() <= value && *value_range.end() >= value {
                    return Ordering::Equal;
                } else if value < *value_range.start() {
                    return Ordering::Greater;
                } else if value > *value_range.end() {
                    return Ordering::Less;
                }
                panic!("not covered all ranges in check");
            })
            .map(|pos| {
                let (range, compact_start) = &self.ranges_and_compact_start[pos];
                compact_start + (value - range.start()) as u64
            })
            .map_err(|pos| pos - 1)
    }

    /// Unpacks a value from compact space u64 to u128 space
    fn unpack(&self, compact: u64) -> u128 {
        let pos = self
            .ranges_and_compact_start
            .binary_search_by_key(&compact, |probe| probe.1)
            .map_or_else(|e| e - 1, |v| v);

        let range_and_compact_start = &self.ranges_and_compact_start[pos];
        let diff = compact - self.ranges_and_compact_start[pos].1;
        range_and_compact_start.0.start() + diff as u128
    }
}

pub struct CompactSpaceCompressor {
    params: IPCodecParams,
}
#[derive(Debug, Clone)]
pub struct IPCodecParams {
    compact_space: CompactSpace,
    bit_unpacker: BitUnpacker,
    null_value_compact_space: u64,
    null_value: u128,
    min_value: u128,
    max_value: u128,
    num_vals: u64,
    num_bits: u8,
}

impl CompactSpaceCompressor {
    pub fn null_value(&self) -> u128 {
        self.params.null_value
    }

    /// Taking the vals as Vec may cost a lot of memory.
    /// It is used to sort the vals.
    ///
    /// Less memory alternative: We could just store the index (u32), and use that as sorting.
    ///
    /// TODO: Should we take Option<u128> here? (better api, but 24bytes instead 16 per element)
    pub fn train_from(mut vals: Vec<u128>) -> Self {
        vals.sort();
        train(&vals)
    }

    fn to_compact(&self, value: u128) -> u64 {
        self.params.compact_space.to_compact(value).unwrap()
    }

    fn write_footer(mut self, writer: &mut impl Write, num_vals: u64) -> io::Result<()> {
        let writer = &mut CountingWriter::wrap(writer);
        self.params.num_vals = num_vals;
        self.params.serialize(writer)?;

        let footer_len = writer.written_bytes() as u32;
        footer_len.serialize(writer)?;

        Ok(())
    }

    pub fn compress(self, vals: impl Iterator<Item = u128>) -> io::Result<Vec<u8>> {
        let mut output = vec![];
        self.compress_into(vals, &mut output)?;
        Ok(output)
    }
    /// TODO: Should we take Option<u128> here? Other wise the caller has to replace None with
    /// `self.null_value()`
    pub fn compress_into(
        self,
        vals: impl Iterator<Item = u128>,
        write: &mut impl Write,
    ) -> io::Result<()> {
        let mut bitpacker = BitPacker::default();
        let mut num_vals = 0;
        for val in vals {
            let compact = self.to_compact(val);
            bitpacker
                .write(compact, self.params.num_bits, write)
                .unwrap();
            num_vals += 1;
        }
        bitpacker.close(write).unwrap();
        self.write_footer(write, num_vals)?;
        Ok(())
    }
}

fn train(values_sorted: &[u128]) -> CompactSpaceCompressor {
    let compact_space = get_compact_space(values_sorted, INTERVAL_COST_IN_BITS);
    let null_value = compact_space.null_value;
    let null_compact_space = compact_space
        .to_compact(null_value)
        .expect("could not convert null_value to compact space");
    let amplitude_compact_space = compact_space.amplitude_compact_space();

    assert!(
        amplitude_compact_space <= u64::MAX as u128,
        "case unsupported."
    );

    let num_bits = tantivy_bitpacker::compute_num_bits(amplitude_compact_space as u64);
    let min_value = *values_sorted.first().unwrap_or(&0);
    let max_value = *values_sorted.last().unwrap_or(&0);
    let compressor = CompactSpaceCompressor {
        params: IPCodecParams {
            compact_space,
            bit_unpacker: BitUnpacker::new(num_bits),
            null_value_compact_space: null_compact_space,
            null_value,
            min_value,
            max_value,
            num_vals: 0, // don't use values_sorted.len() here since they don't include null values
            num_bits,
        },
    };

    let max_value = *values_sorted.last().unwrap_or(&0u128).max(&null_value);
    assert_eq!(
        compressor.to_compact(max_value) + 1,
        amplitude_compact_space as u64
    );
    compressor
}

#[derive(Debug, Clone)]
pub struct CompactSpaceDecompressor {
    data: OwnedBytes,
    params: IPCodecParams,
}

impl BinarySerializable for IPCodecParams {
    fn serialize<W: io::Write>(&self, writer: &mut W) -> io::Result<()> {
        // header flags for future optional dictionary encoding
        let footer_flags = 0u64;
        footer_flags.serialize(writer)?;

        let null_value_compact_space = self
            .compact_space
            .to_compact(self.null_value)
            .expect("could not convert null to compact space");
        VIntU128(null_value_compact_space as u128).serialize(writer)?;
        VIntU128(self.min_value).serialize(writer)?;
        VIntU128(self.max_value).serialize(writer)?;
        VIntU128(self.num_vals as u128).serialize(writer)?;
        self.num_bits.serialize(writer)?;

        self.compact_space.serialize(writer)?;

        Ok(())
    }

    fn deserialize<R: io::Read>(reader: &mut R) -> io::Result<Self> {
        let _header_flags = u64::deserialize(reader)?;
        let null_value_compact_space = VIntU128::deserialize(reader)?.0 as u64;
        let min_value = VIntU128::deserialize(reader)?.0;
        let max_value = VIntU128::deserialize(reader)?.0;
        let num_vals = VIntU128::deserialize(reader)?.0 as u64;
        let num_bits = u8::deserialize(reader)?;
        let compact_space = CompactSpace::deserialize(reader)?;
        let null_value = compact_space.unpack(null_value_compact_space);

        Ok(Self {
            null_value,
            compact_space,
            bit_unpacker: BitUnpacker::new(num_bits),
            null_value_compact_space,
            min_value,
            max_value,
            num_vals,
            num_bits,
        })
    }
}

impl ColumnV2<u128> for CompactSpaceDecompressor {
    fn get_val(&self, doc: u64) -> Option<u128> {
        self.get(doc)
    }

    fn min_value(&self) -> u128 {
        self.min_value()
    }

    fn max_value(&self) -> u128 {
        self.max_value()
    }

    fn num_vals(&self) -> u64 {
        self.params.num_vals
    }

    fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = Option<u128>> + 'a> {
        Box::new(self.iter())
    }
}

impl ColumnV2Ext<u128> for CompactSpaceDecompressor {
    fn get_between_vals(&self, range: RangeInclusive<u128>) -> Vec<u64> {
        self.get_range(range)
    }
}

impl CompactSpaceDecompressor {
    pub fn open(data: OwnedBytes) -> io::Result<CompactSpaceDecompressor> {
        let (data_slice, footer_len_bytes) = data.split_at(data.len() - 4);
        let footer_len = u32::deserialize(&mut &footer_len_bytes[..])?;

        let data_footer = &data_slice[data_slice.len() - footer_len as usize..];
        let params = IPCodecParams::deserialize(&mut &data_footer[..])?;
        let decompressor = CompactSpaceDecompressor { data, params };

        Ok(decompressor)
    }

    /// Converting to compact space for the decompressor is more complex, since we may get values
    /// which are outside the compact space. e.g. if we map
    /// 1000 => 5
    /// 2000 => 6
    ///
    /// and we want a mapping for 1005, there is no equivalent compact space. We instead return an
    /// error with the index of the next range.
    fn to_compact(&self, value: u128) -> Result<u64, usize> {
        self.params.compact_space.to_compact(value)
    }

    fn compact_to_u128(&self, compact: u64) -> u128 {
        self.params.compact_space.unpack(compact)
    }

    /// Comparing on compact space: 1.2 GElements/s
    ///
    /// Comparing on original space: .06 GElements/s (not completely optimized)
    pub fn get_range(&self, range: RangeInclusive<u128>) -> Vec<u64> {
        let from_value = *range.start();
        let to_value = *range.end();
        assert!(to_value >= from_value);
        let compact_from = self.to_compact(from_value);
        let compact_to = self.to_compact(to_value);
        // Quick return, if both ranges fall into the same non-mapped space, the range can't cover
        // any values, so we can early exit
        match (compact_to, compact_from) {
            (Err(pos1), Err(pos2)) if pos1 == pos2 => return vec![],
            _ => {}
        }

        let compact_from = compact_from.unwrap_or_else(|pos| {
            let range_and_compact_start =
                self.params.compact_space.get_range_and_compact_start(pos);
            let compact_end = range_and_compact_start.1
                + (range_and_compact_start.0.end() - range_and_compact_start.0.start()) as u64;
            compact_end + 1
        });
        // If there is no compact space, we go to the closest upperbound compact space
        let compact_to = compact_to.unwrap_or_else(|pos| {
            let range_and_compact_start =
                self.params.compact_space.get_range_and_compact_start(pos);
            let compact_end = range_and_compact_start.1
                + (range_and_compact_start.0.end() - range_and_compact_start.0.start()) as u64;
            compact_end
        });

        let range = compact_from..=compact_to;
        let mut positions = vec![];

        for (pos, compact_value) in self
            .iter_compact()
            .enumerate()
            .filter(|(_pos, val)| *val != self.params.null_value_compact_space)
        {
            if range.contains(&compact_value) {
                positions.push(pos as u64);
            }
        }

        positions
    }

    #[inline]
    fn iter_compact(&self) -> impl Iterator<Item = u64> + '_ {
        (0..self.params.num_vals)
            .map(move |idx| self.params.bit_unpacker.get(idx as u64, &self.data) as u64)
    }

    #[inline]
    fn iter(&self) -> impl Iterator<Item = Option<u128>> + '_ {
        // TODO: Performance. It would be better to iterate on the ranges and check existence via
        // the bit_unpacker.
        self.iter_compact().map(|compact| {
            if compact == self.params.null_value_compact_space {
                None
            } else {
                Some(self.compact_to_u128(compact))
            }
        })
    }

    pub fn get(&self, idx: u64) -> Option<u128> {
        let compact = self.params.bit_unpacker.get(idx, &self.data);
        if compact == self.params.null_value_compact_space {
            None
        } else {
            Some(self.compact_to_u128(compact))
        }
    }

    pub fn min_value(&self) -> u128 {
        self.params.min_value
    }

    pub fn max_value(&self) -> u128 {
        self.params.max_value
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_binary_heap_pop_order() {
        let mut deltas: BinaryHeap<BlankSizeAndPos> = BinaryHeap::new();
        deltas.push(BlankSizeAndPos {
            blank_size: 10,
            pos: 1,
        });
        deltas.push(BlankSizeAndPos {
            blank_size: 100,
            pos: 10,
        });
        deltas.push(BlankSizeAndPos {
            blank_size: 1,
            pos: 10,
        });
        assert_eq!(deltas.pop().unwrap().blank_size, 100);
        assert_eq!(deltas.pop().unwrap().blank_size, 10);
    }

    #[test]
    fn compact_space_test() {
        let ips = vec![
            2u128, 4u128, 1000, 1001, 1002, 1003, 1004, 1005, 1008, 1010, 1012, 1260,
        ];
        let compact_space = get_compact_space(&ips, 11);
        assert_eq!(compact_space.null_value, 5);
        let amplitude = compact_space.amplitude_compact_space();
        assert_eq!(amplitude, 20);
        assert_eq!(2, compact_space.to_compact(2).unwrap());
        assert_eq!(compact_space.to_compact(100).unwrap_err(), 0);

        let mut output = vec![];
        compact_space.serialize(&mut output).unwrap();

        assert_eq!(
            compact_space,
            CompactSpace::deserialize(&mut &output[..]).unwrap()
        );

        for ip in &ips {
            let compact = compact_space.to_compact(*ip).unwrap();
            assert_eq!(compact_space.unpack(compact), *ip);
        }
    }

    fn decode_all(data: OwnedBytes) -> Vec<u128> {
        let decompressor = CompactSpaceDecompressor::open(data).unwrap();
        let mut u128_vals = Vec::new();
        for idx in 0..decompressor.params.num_vals as usize {
            let val = decompressor.get(idx as u64);
            if let Some(val) = val {
                u128_vals.push(val);
            }
        }
        u128_vals
    }

    fn test_aux_vals(u128_vals: &[u128]) -> OwnedBytes {
        let compressor = CompactSpaceCompressor::train_from(u128_vals.to_vec());
        let data = compressor.compress(u128_vals.iter().cloned()).unwrap();
        let data = OwnedBytes::new(data);
        let decoded_val = decode_all(data.clone());
        assert_eq!(&decoded_val, u128_vals);
        data
    }

    #[test]
    fn test_range_1() {
        let vals = &[
            1u128,
            100u128,
            3u128,
            99999u128,
            100000u128,
            100001u128,
            4_000_211_221u128,
            4_000_211_222u128,
            333u128,
        ];
        let data = test_aux_vals(vals);
        let decomp = CompactSpaceDecompressor::open(data).unwrap();
        let positions = decomp.get_range(0..=1);
        assert_eq!(positions, vec![0]);
        let positions = decomp.get_range(0..=2);
        assert_eq!(positions, vec![0]);
        let positions = decomp.get_range(0..=3);
        assert_eq!(positions, vec![0, 2]);
        assert_eq!(decomp.get_range(99999u128..=99999u128), vec![3]);
        assert_eq!(decomp.get_range(99998u128..=100000u128), vec![3, 4]);
        assert_eq!(decomp.get_range(99998u128..=99999u128), vec![3]);
        assert_eq!(decomp.get_range(99998u128..=99998u128), vec![]);
        assert_eq!(decomp.get_range(333u128..=333u128), vec![8]);
        assert_eq!(decomp.get_range(332u128..=333u128), vec![8]);
        assert_eq!(decomp.get_range(332u128..=334u128), vec![8]);
        assert_eq!(decomp.get_range(333u128..=334u128), vec![8]);

        assert_eq!(
            decomp.get_range(4_000_211_221u128..=5_000_000_000u128),
            vec![6, 7]
        );
    }

    #[test]
    fn test_empty() {
        let vals = &[];
        let data = test_aux_vals(vals);
        let _decomp = CompactSpaceDecompressor::open(data).unwrap();
    }

    #[test]
    fn test_range_2() {
        let vals = &[
            100u128,
            99999u128,
            100000u128,
            100001u128,
            4_000_211_221u128,
            4_000_211_222u128,
            333u128,
        ];
        let data = test_aux_vals(vals);
        let decomp = CompactSpaceDecompressor::open(data).unwrap();
        let positions = decomp.get_range(0..=5);
        assert_eq!(positions, vec![]);
        let positions = decomp.get_range(0..=100);
        assert_eq!(positions, vec![0]);
        let positions = decomp.get_range(0..=105);
        assert_eq!(positions, vec![0]);
    }

    #[test]
    fn test_null() {
        let vals = &[2u128];
        let compressor = CompactSpaceCompressor::train_from(vals.to_vec());
        let vals = vec![compressor.null_value(), 2u128];
        let data = compressor.compress(vals.iter().cloned()).unwrap();
        let decomp = CompactSpaceDecompressor::open(OwnedBytes::new(data)).unwrap();
        let positions = decomp.get_range(0..=1);
        assert_eq!(positions, vec![]);
        let positions = decomp.get_range(2..=2);
        assert_eq!(positions, vec![1]);
    }

    #[test]
    fn test_first_large_gaps() {
        let vals = &[1_000_000_000u128; 100];
        let _data = test_aux_vals(vals);
    }
    use proptest::prelude::*;

    proptest! {

            #[test]
            fn compress_decompress_random(vals in proptest::collection::vec(any::<u128>()
    , 1..1000)) {
                let _data = test_aux_vals(&vals);
            }
        }
}
