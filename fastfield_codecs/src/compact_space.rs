/// This codec takes a large number space (u128) and reduces it to a compact number space.
///
/// It will find spaces in the number range. For example:
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
    convert::{TryFrom, TryInto},
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

/// The cost per blank is quite hard actually, since blanks are delta encoded, the actual cost of
/// blanks depends on the number of blanks.
///
/// The number is taken by looking at a real dataset. It is optimized for larger datasets.
const COST_PER_BLANK_IN_BITS: usize = 36;

/// The range of a blank in value space.
///
/// A blank is an unoccupied space in the data.
/// Ordered by size
///
/// Use the try_into(), invalid ranges will be rejected.
///
/// TODO: move to own module to force try_into
#[derive(Debug, Eq, PartialEq, Clone)]
struct BlankRange {
    blank_range: RangeInclusive<u128>,
}
impl TryFrom<RangeInclusive<u128>> for BlankRange {
    type Error = &'static str;
    fn try_from(range: RangeInclusive<u128>) -> Result<Self, Self::Error> {
        let blank_size = range.end().saturating_sub(*range.start());
        if blank_size < 2 {
            Err("invalid range")
        } else {
            Ok(BlankRange { blank_range: range })
        }
    }
}
impl BlankRange {
    fn blank_size(&self) -> u128 {
        self.blank_range.end() - self.blank_range.start()
    }
}

impl Ord for BlankRange {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.blank_size().cmp(&other.blank_size())
    }
}
impl PartialOrd for BlankRange {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.blank_size().partial_cmp(&other.blank_size())
    }
}

/// Put the blanks for the sorted values into a binary heap
fn get_blanks(values_sorted: &[u128]) -> BinaryHeap<BlankRange> {
    let mut blanks: BinaryHeap<BlankRange> = BinaryHeap::new();
    let mut add_range = |blank_range: RangeInclusive<u128>| {
        let blank_range: Result<BlankRange, _> = blank_range.try_into();
        if let Ok(blank_range) = blank_range {
            blanks.push(blank_range);
        }
    };
    for values in values_sorted.windows(2) {
        let blank_range = values[0] + 1..=values[1] - 1;
        add_range(blank_range);
    }
    if let Some(first_val) = values_sorted.first().filter(|first_val| **first_val != 0) {
        let blank_range = 0..=first_val - 1;
        add_range(blank_range);
    }

    if let Some(last_val) = values_sorted
        .last()
        .filter(|last_val| **last_val != u128::MAX)
    {
        let blank_range = last_val + 1..=u128::MAX;
        add_range(blank_range);
    }
    blanks
}

struct BlankCollector {
    blanks: Vec<BlankRange>,
    staged_blanks_sum: u128,
}
impl BlankCollector {
    fn new() -> Self {
        Self {
            blanks: vec![],
            staged_blanks_sum: 0,
        }
    }
    fn stage_blank(&mut self, blank: BlankRange) {
        self.staged_blanks_sum += blank.blank_size();
        self.blanks.push(blank);
    }
    fn drain(&mut self) -> std::vec::Drain<'_, BlankRange> {
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
fn get_compact_space(
    values_deduped_sorted: &[u128],
    total_num_values: usize,
    cost_per_blank: usize,
) -> CompactSpace {
    let mut blanks = get_blanks(values_deduped_sorted);
    let mut amplitude_compact_space = u128::MAX;
    let mut amplitude_bits: u8 = num_bits(amplitude_compact_space);

    let mut compact_space = CompactSpaceBuilder::new();
    if values_deduped_sorted.is_empty() {
        return compact_space.finish();
    }

    let mut blank_collector = BlankCollector::new();
    // We will stage blanks until they reduce the compact space by 1 bit.
    // Binary heap to process the gaps by their size
    while let Some(blank_range) = blanks.pop() {
        blank_collector.stage_blank(blank_range);

        let staged_spaces_sum: u128 = blank_collector.staged_blanks_sum();
        // +1 for later added null value
        let amplitude_new_compact_space = amplitude_compact_space - staged_spaces_sum + 1;
        let amplitude_new_bits = num_bits(amplitude_new_compact_space);
        if amplitude_bits == amplitude_new_bits {
            continue;
        }
        let saved_bits = (amplitude_bits - amplitude_new_bits) as usize * total_num_values;
        // TODO: Maybe calculate exact cost of blanks and run this more expensive computation only,
        // when amplitude_new_bits changes
        let cost = blank_collector.num_blanks() * cost_per_blank;
        if cost >= saved_bits {
            // Continue here, since although we walk over the blanks by size,
            // we can potentially save a lot at the last bits, which are smaller blanks
            //
            // E.g. if the first range reduces the compact space by 1000 from 2000 to 1000, which
            // saves 11-10=1 bit and the next range reduces the compact space by 950 to
            // 50, which saves 10-6=4 bit
            continue;
        }

        amplitude_compact_space = amplitude_new_compact_space;
        amplitude_bits = amplitude_new_bits;
        compact_space.add_blanks(blank_collector.drain().map(|blank| blank.blank_range));
    }

    // special case, when we don't collected any blanks because:
    // * the data is empty
    // * the algorithm did decide it's not worth the cost, which can be the case for single values
    //
    // We drain one collected blank unconditionally, so the empty case is reserved for empty
    // data, and therefore empty compact_space means the data is empty and no data is covered
    // (conversely to all data) and we can assign null to it.
    if compact_space.is_empty() {
        compact_space.add_blanks(
            blank_collector
                .drain()
                .map(|blank| blank.blank_range)
                .take(1),
        );
    }

    compact_space.finish()
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct CompactSpaceBuilder {
    blanks: Vec<RangeInclusive<u128>>,
}

impl CompactSpaceBuilder {
    /// Creates a new compact space builder which will initially cover the whole space.
    fn new() -> Self {
        Self { blanks: vec![] }
    }

    /// Assumes that repeated add_blank calls don't overlap and are not adjacent,
    /// e.g. [3..=5, 5..=10] is not allowed
    ///
    /// Both of those assumptions are true when blanks are produced from sorted values.
    fn add_blanks(&mut self, blank: impl Iterator<Item = RangeInclusive<u128>>) {
        self.blanks.extend(blank);
    }

    fn is_empty(&self) -> bool {
        self.blanks.is_empty()
    }

    /// Convert blanks to covered space and assign null value
    fn finish(mut self) -> CompactSpace {
        // sort by start since ranges are not allowed to overlap
        self.blanks.sort_by_key(|blank| *blank.start());

        // Between the blanks
        let mut covered_space = self
            .blanks
            .windows(2)
            .map(|blanks| {
                assert!(
                    blanks[0].end() < blanks[1].start(),
                    "overlapping or adjacent ranges detected"
                );
                *blanks[0].end() + 1..=*blanks[1].start() - 1
            })
            .collect::<Vec<_>>();

        // Outside the blanks
        if let Some(first_blank_start) = self.blanks.first().map(RangeInclusive::start) {
            if *first_blank_start != 0 {
                covered_space.insert(0, 0..=first_blank_start - 1);
            }
        }

        if let Some(last_blank_end) = self.blanks.last().map(RangeInclusive::end) {
            if *last_blank_end != u128::MAX {
                covered_space.push(last_blank_end + 1..=u128::MAX);
            }
        }

        // Extend the first range and assign the null value to it.
        let null_value = if let Some(first_covered_space) = covered_space.first_mut() {
            // in case the first covered space ends at u128::MAX, assign null to the beginning
            if *first_covered_space.end() == u128::MAX {
                *first_covered_space = first_covered_space.start() - 1..=*first_covered_space.end();
                *first_covered_space.start()
            } else {
                *first_covered_space = *first_covered_space.start()..=first_covered_space.end() + 1;
                *first_covered_space.end()
            }
        } else {
            covered_space.push(0..=0); // empty data case
            0u128
        };

        let mut compact_start: u64 = 0;
        let mut ranges_and_compact_start = Vec::with_capacity(covered_space.len());
        for cov in covered_space {
            let covered_range_len = cov.end() - cov.start() + 1; // e.g. 0..=1 covered space 1-0+1= 2
            ranges_and_compact_start.push((cov, compact_start));
            compact_start += covered_range_len as u64;
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
            let blank_delta_start = value_range.start() - prev_value;
            VIntU128(blank_delta_start).serialize(writer)?;
            prev_value = *value_range.start();

            let blank_delta_end = value_range.end() - prev_value;
            VIntU128(blank_delta_end).serialize(writer)?;
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
            let blank_delta_start = VIntU128::deserialize(reader)?.0;
            value += blank_delta_start;
            let blank_start = value;

            let blank_delta_end = VIntU128::deserialize(reader)?.0;
            value += blank_delta_end;
            let blank_end = value;

            let compact_delta = blank_end - blank_start + 1;

            ranges_and_compact_start.push((blank_start..=blank_end, compact));
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
    /// Err(position where it would be inserted)
    fn to_compact(&self, value: u128) -> Result<u64, usize> {
        self.ranges_and_compact_start
            .binary_search_by(|probe| {
                let value_range = &probe.0;
                if value_range.contains(&value) {
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
    /// The lower memory alternative to just store the index (u32) and use that as sorting may be an
    /// issue for the merge case, where random access is more expensive.
    ///
    /// TODO: Should we take Option<u128> here? (better api, but 24bytes instead 16 per element)
    pub fn train_from(mut vals: Vec<u128>) -> Self {
        let total_num_values = vals.len(); // TODO: Null values should be here too
        vals.sort();
        // We don't care for duplicates
        vals.dedup();
        vals.shrink_to_fit();
        train(&vals, total_num_values)
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
    /// TODO: Should we take Option<u128> here? Otherwise the caller has to replace None with
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

fn train(values_sorted: &[u128], total_num_values: usize) -> CompactSpaceCompressor {
    let compact_space = get_compact_space(values_sorted, total_num_values, COST_PER_BLANK_IN_BITS);
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
    assert!(compressor.to_compact(max_value) < amplitude_compact_space as u64);
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
    pub fn get_range(&self, mut range: RangeInclusive<u128>) -> Vec<u64> {
        if range.start() > range.end() {
            range = *range.end()..=*range.start();
        }
        let from_value = *range.start();
        let to_value = *range.end();
        assert!(to_value >= from_value);
        let compact_from = self.to_compact(from_value);
        let compact_to = self.to_compact(to_value);

        // Quick return, if both ranges fall into the same non-mapped space, the range can't cover
        // any values, so we can early exit
        match (compact_to, compact_from) {
            (Err(pos1), Err(pos2)) if pos1 == pos2 => return Vec::new(),
            _ => {}
        }

        let compact_from = compact_from.unwrap_or_else(|pos| {
            // Correctness: Out of bounds, if this value is Err(last_index + 1), we early exit,
            // since the to_value also mapps into the same non-mapped space
            let range_and_compact_start =
                self.params.compact_space.get_range_and_compact_start(pos);
            range_and_compact_start.1
        });
        // If there is no compact space, we go to the closest upperbound compact space
        let compact_to = compact_to.unwrap_or_else(|pos| {
            // Correctness: Overflow, if this value is Err(0), we early exit,
            // since the from_value also mapps into the same non-mapped space

            // get end of previous range
            let pos = pos - 1;
            let range_and_compact_start =
                self.params.compact_space.get_range_and_compact_start(pos);
            let compact_end = range_and_compact_start.1
                + (range_and_compact_start.0.end() - range_and_compact_start.0.start()) as u64;
            compact_end
        });

        let range = compact_from..=compact_to;
        let mut positions = Vec::new();

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
        let mut blanks: BinaryHeap<BlankRange> = BinaryHeap::new();
        blanks.push(BlankRange {
            blank_range: 0..=10,
        });
        blanks.push(BlankRange {
            blank_range: 100..=200,
        });
        blanks.push(BlankRange {
            blank_range: 100..=110,
        });
        assert_eq!(blanks.pop().unwrap().blank_size(), 100);
        assert_eq!(blanks.pop().unwrap().blank_size(), 10);
    }

    #[test]
    fn compact_space_test() {
        let ips = &[
            2u128, 4u128, 1000, 1001, 1002, 1003, 1004, 1005, 1008, 1010, 1012, 1260,
        ];
        let compact_space = get_compact_space(ips, ips.len(), 11);
        assert_eq!(compact_space.null_value, 5);
        let amplitude = compact_space.amplitude_compact_space();
        assert_eq!(amplitude, 20);
        assert_eq!(2, compact_space.to_compact(2).unwrap());
        assert_eq!(3, compact_space.to_compact(3).unwrap());
        assert_eq!(compact_space.to_compact(100).unwrap_err(), 1);

        let mut output: Vec<u8> = Vec::new();
        compact_space.serialize(&mut output).unwrap();

        assert_eq!(
            compact_space,
            CompactSpace::deserialize(&mut &output[..]).unwrap()
        );

        for ip in ips {
            let compact = compact_space.to_compact(*ip).unwrap();
            assert_eq!(compact_space.unpack(compact), *ip);
        }
    }

    #[test]
    fn compact_space_amplitude_test() {
        let ips = &[100000u128, 1000000];
        let compact_space = get_compact_space(ips, ips.len(), 1);
        assert_eq!(compact_space.null_value, 100001);
        let amplitude = compact_space.amplitude_compact_space();
        assert_eq!(amplitude, 3);
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
        assert_eq!(decomp.get_range(99999u128..=100000u128), vec![3, 4]);
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

        let positions = decomp.get_range(2..=3);
        assert_eq!(positions, vec![1]);

        let positions = decomp.get_range(1..=3);
        assert_eq!(positions, vec![1]);

        let positions = decomp.get_range(2..=3);
        assert_eq!(positions, vec![1]);

        let positions = decomp.get_range(3..=3);
        assert_eq!(positions, vec![]);
    }

    #[test]
    fn test_range_3() {
        let vals = &[
            200u128,
            201,
            202,
            203,
            204,
            204,
            206,
            207,
            208,
            209,
            210,
            1_000_000,
            5_000_000_000,
        ];
        let compressor = CompactSpaceCompressor::train_from(vals.to_vec());
        // let vals = vec![compressor.null_value(), 2u128];
        let data = compressor.compress(vals.iter().cloned()).unwrap();
        let decomp = CompactSpaceDecompressor::open(OwnedBytes::new(data)).unwrap();

        assert_eq!(decomp.get_range(199..=200), vec![0]);
        assert_eq!(decomp.get_range(199..=201), vec![0, 1]);
        assert_eq!(decomp.get_range(200..=200), vec![0]);
        assert_eq!(decomp.get_range(1_000_000..=1_000_000), vec![11]);
    }

    #[test]
    fn test_bug1() {
        let vals = &[9223372036854775806];
        let _data = test_aux_vals(vals);
    }

    #[test]
    fn test_bug2() {
        let vals = &[340282366920938463463374607431768211455u128];
        let _data = test_aux_vals(vals);
    }

    #[test]
    fn test_bug3() {
        let vals = &[340282366920938463463374607431768211454];
        let _data = test_aux_vals(vals);
    }

    #[test]
    fn test_first_large_gaps() {
        let vals = &[1_000_000_000u128; 100];
        let _data = test_aux_vals(vals);
    }
    use proptest::prelude::*;

    fn num_strategy() -> impl Strategy<Value = u128> {
        prop_oneof![
            1 => prop::num::u128::ANY.prop_map(|num| u128::MAX - (num % 10) ),
            1 => prop::num::u128::ANY.prop_map(|num| i64::MAX as u128 + 5 - (num % 10) ),
            1 => prop::num::u128::ANY.prop_map(|num| i128::MAX as u128 + 5 - (num % 10) ),
            1 => prop::num::u128::ANY.prop_map(|num| num % 10 ),
            20 => prop::num::u128::ANY,
        ]
    }

    proptest! {

            #[test]
            fn compress_decompress_random(vals in proptest::collection::vec(num_strategy()
    , 1..1000)) {
                let _data = test_aux_vals(&vals);
            }
        }
}
