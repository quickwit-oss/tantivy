/// This codec takes a large number space (u128) and reduces it to a compact number space.
///
/// It will find spaces in the numer range. For example:
///
/// 100, 101, 102, 103, 104, 50000, 50001
/// could be mapped to
/// 100..104 -> 0..4
/// 50000..50001 -> 5..6
///
/// Compact space 0..6 requires much less bits than 100..50001
///
/// The codec is created to compress ip addresses, but may be employed in other use cases.
///
use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    io,
    net::{IpAddr, Ipv6Addr},
};

use tantivy_bitpacker::{self, BitPacker, BitUnpacker};

pub fn ip_to_u128(ip_addr: IpAddr) -> u128 {
    let ip_addr_v6: Ipv6Addr = match ip_addr {
        IpAddr::V4(v4) => v4.to_ipv6_mapped(),
        IpAddr::V6(v6) => v6,
    };
    u128::from_be_bytes(ip_addr_v6.octets())
}

const INTERVALL_COST_IN_BITS: usize = 64;

#[derive(Default, Debug)]
pub struct IntervalEncoding();

pub struct IntervalCompressor {
    ranges_and_compact_start: CompactSpace,
    pub num_bits: u8,
}

const STOP_BIT: u8 = 128u8;

fn serialize_vint(mut val: u128, output: &mut Vec<u8>) {
    loop {
        let next_byte: u8 = (val % 128u128) as u8;
        val /= 128u128;
        if val == 0 {
            output.push(next_byte | STOP_BIT);
            return;
        } else {
            output.push(next_byte);
        }
    }
}

fn deserialize_vint(data: &[u8]) -> io::Result<(u128, &[u8])> {
    let mut result = 0u128;
    let mut shift = 0u64;
    for i in 0..19 {
        let b = data[i];
        result |= u128::from(b % 128u8) << shift;
        if b >= STOP_BIT {
            return Ok((result, &data[i + 1..]));
        }
        shift += 7;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Failed to deserialize u128 vint",
    ))
}

#[derive(Debug, Eq, PartialEq)]
struct DeltaAndPos {
    delta: u128,
    pos: usize,
}
impl DeltaAndPos {
    fn new(ip: u128, pos: usize) -> Self {
        DeltaAndPos { delta: ip, pos }
    }
}

impl Ord for DeltaAndPos {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.delta.cmp(&other.delta)
    }
}
impl PartialOrd for DeltaAndPos {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.delta.partial_cmp(&other.delta)
    }
}

#[test]
fn test_delta_and_pos_sort() {
    let mut deltas: BinaryHeap<DeltaAndPos> = BinaryHeap::new();
    deltas.push(DeltaAndPos::new(10, 1));
    deltas.push(DeltaAndPos::new(100, 10));
    deltas.push(DeltaAndPos::new(1, 10));
    assert_eq!(deltas.pop().unwrap().delta, 100);
    assert_eq!(deltas.pop().unwrap().delta, 10);
}

/// Put the deltas for the sorted ip addresses into a binary heap
fn get_deltas(ip_addrs_sorted: &[u128]) -> BinaryHeap<DeltaAndPos> {
    let mut prev_opt = None;
    let mut deltas: BinaryHeap<DeltaAndPos> = BinaryHeap::new();
    for (pos, ip_addr) in ip_addrs_sorted.iter().cloned().enumerate() {
        let delta = if let Some(prev) = prev_opt {
            ip_addr - prev
        } else {
            ip_addr + 1
        };
        // skip too small deltas
        if delta > 2 {
            deltas.push(DeltaAndPos::new(delta, pos));
        }
        prev_opt = Some(ip_addr);
    }
    deltas
}

/// Will find blanks if it will affect the number of bits used on the compact space.
/// Returns the new amplitude and the positions of blanks
fn get_blanks(ip_addrs_sorted: &[u128], cost_per_interval: usize) -> (u128, CompactSpace) {
    let mut deltas = get_deltas(ip_addrs_sorted);
    let mut amplitude_compact_space = *ip_addrs_sorted.last().unwrap() + 1;
    let mut amplitude_bits: u8 = (amplitude_compact_space as f64).log2().ceil() as u8;
    let mut staged_blanks = vec![];

    let mut compact_space = CompactSpaceBuilder::new();

    // We will stage blanks until they reduce the compact space by 1 bit.
    // Binary heap to process the gaps by their size
    while let Some(ip_addr_and_pos) = deltas.pop() {
        let delta = ip_addr_and_pos.delta;
        let pos = ip_addr_and_pos.pos;
        staged_blanks.push((delta, pos));
        let staged_spaces_sum: u128 = staged_blanks.iter().map(|(delta, _)| delta - 1).sum();
        let amplitude_new_compact_space = amplitude_compact_space - staged_spaces_sum;
        let amplitude_new_bits = (amplitude_new_compact_space as f64).log2().ceil() as u8;
        if amplitude_bits == amplitude_new_bits {
            continue;
        }
        let saved_bits = (amplitude_bits - amplitude_new_bits) as usize * ip_addrs_sorted.len();
        let cost = staged_blanks.len() * cost_per_interval;
        if cost >= saved_bits {
            // Continue here, since although we walk over the deltas by size,
            // we can potentially save a lot at the last bits, which are smaller deltas
            //
            // E.g. if the first range reduces the compact space by 1000 from 2000 to 1000, which saves 11-10=1 bit
            // and the next range reduces the compact space by 950 to 50, which saves 10-6=4 bit
            continue;
        }

        amplitude_compact_space = amplitude_new_compact_space;
        amplitude_bits = amplitude_new_bits;
        for (_, pos) in staged_blanks.drain(..) {
            let ip_addr = ip_addrs_sorted[pos];
            if pos == 0 {
                compact_space.add_hole(0..=ip_addr - 1);
            } else {
                compact_space.add_hole(ip_addrs_sorted[pos - 1] + 1..=ip_addr - 1);
            }
        }
    }
    compact_space.add_hole(*ip_addrs_sorted.last().unwrap() + 1..=u128::MAX);

    (amplitude_compact_space, compact_space.finish())
}

#[test]
fn get_blanks_test() {
    // small ranges are ignored here
    let ips = vec![
        2u128, 4u128, 1000, 1001, 1002, 1003, 1004, 1005, 1008, 1010, 1012, 1260,
    ];
    let (amplitude, ranges_and_compact_start) = get_blanks(&ips, 11);
    assert_eq!(amplitude, 19);
    assert_eq!(2, ranges_and_compact_start.to_compact(2).unwrap());

    assert_eq!(ranges_and_compact_start.to_compact(100).unwrap_err(), 1);
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct CompactSpaceBuilder {
    covered_space: Vec<std::ops::RangeInclusive<u128>>,
}

impl CompactSpaceBuilder {
    fn new() -> Self {
        Self {
            covered_space: vec![0..=u128::MAX],
        }
    }

    // Assumes that repeated add_hole calls don't overlap.
    fn add_hole(&mut self, hole: std::ops::RangeInclusive<u128>) {
        let position = self
            .covered_space
            .iter()
            .position(|range| range.start() <= hole.start() && range.end() >= hole.end());
        if let Some(position) = position {
            let old_range = self.covered_space.remove(position);
            if old_range == hole {
                return;
            }
            let new_range_end = hole.end().saturating_add(1)..=*old_range.end();
            if old_range.start() == hole.start() {
                self.covered_space.insert(position, new_range_end);
                return;
            }
            let new_range_start = *old_range.start()..=hole.start().saturating_sub(1);
            if old_range.end() == hole.end() {
                self.covered_space.insert(position, new_range_start);
                return;
            }
            self.covered_space.insert(position, new_range_end);
            self.covered_space.insert(position, new_range_start);
        }
    }
    fn finish(self) -> CompactSpace {
        let mut compact_start: u64 = 0;
        let mut ranges_and_compact_start = vec![];
        for cov in self.covered_space {
            let covered_range_len = cov.end() - cov.start();
            ranges_and_compact_start.push((cov, compact_start));
            compact_start += covered_range_len as u64 + 1;
        }
        CompactSpace {
            ranges_and_compact_start,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct CompactSpace {
    ranges_and_compact_start: Vec<(std::ops::RangeInclusive<u128>, u64)>,
}
impl CompactSpace {
    fn get_range(&self, pos: usize) -> &(std::ops::RangeInclusive<u128>, u64) {
        &self.ranges_and_compact_start[pos]
    }
    fn serialize(&self, output: &mut Vec<u8>) {
        serialize_vint(self.ranges_and_compact_start.len() as u128, output);
        let mut prev_ip = 0;
        for (ip_range, _compact) in &self.ranges_and_compact_start {
            let delta_ip = ip_range.start() - prev_ip;
            serialize_vint(delta_ip as u128, output);
            prev_ip = *ip_range.start();

            let delta_ip = ip_range.end() - prev_ip;
            serialize_vint(delta_ip as u128, output);
            prev_ip = *ip_range.end();
        }
    }

    fn deserialize(mut data: &[u8]) -> io::Result<(&[u8], Self)> {
        let (num_ip_addrs, new_data) = deserialize_vint(data)?;
        data = new_data;
        let mut ip_addr = 0u128;
        let mut compact = 0u64;
        let mut ranges_and_compact_start: Vec<(std::ops::RangeInclusive<u128>, u64)> = vec![];
        for _ in 0..num_ip_addrs {
            let (ip_addr_delta, new_data) = deserialize_vint(data)?;
            data = new_data;
            ip_addr += ip_addr_delta;
            let ip_addr_start = ip_addr;

            let (ip_addr_delta, new_data) = deserialize_vint(data)?;
            data = new_data;
            ip_addr += ip_addr_delta;
            let ip_addr_end = ip_addr;

            let compact_delta = ip_addr_end - ip_addr_start + 1;

            ranges_and_compact_start.push((ip_addr_start..=ip_addr_end, compact));
            compact += compact_delta as u64;
        }
        Ok((
            data,
            Self {
                ranges_and_compact_start,
            },
        ))
    }

    /// Returns either Ok(the value in the compact space) or if it is outside the compact space the
    /// Err(position on the next larger range above the value)
    fn to_compact(&self, ip: u128) -> Result<u64, usize> {
        self.ranges_and_compact_start
            .binary_search_by(|probe| {
                let ip_range = &probe.0;
                if *ip_range.start() <= ip && *ip_range.end() >= ip {
                    return Ordering::Equal;
                } else if ip < *ip_range.start() {
                    return Ordering::Greater;
                } else if ip > *ip_range.end() {
                    return Ordering::Less;
                }
                panic!("not covered all ranges in check");
            })
            .map(|pos| {
                let (range, compact_start) = &self.ranges_and_compact_start[pos];
                compact_start + (ip - range.start()) as u64
            })
    }

    /// Unpacks a ip from compact space to u128 space
    fn unpack_ip(&self, compact: u64) -> u128 {
        let pos = self
            .ranges_and_compact_start
            .binary_search_by_key(&compact, |probe| probe.1)
            .map_or_else(|e| e - 1, |v| v);

        let range_and_compact_start = &self.ranges_and_compact_start[pos];
        let diff = compact - self.ranges_and_compact_start[pos].1;
        range_and_compact_start.0.start() + diff as u128
    }
}

#[test]
fn ranges_and_compact_start_test() {
    let ips = vec![
        2u128, 4u128, 1000, 1001, 1002, 1003, 1004, 1005, 1008, 1010, 1012, 1260,
    ];
    let (_amplitude, ranges_and_compact_start) = get_blanks(&ips, 11);

    let mut output = vec![];
    ranges_and_compact_start.serialize(&mut output);

    assert_eq!(
        ranges_and_compact_start,
        CompactSpace::deserialize(&output).unwrap().1
    );

    for ip in &ips {
        let compact = ranges_and_compact_start.to_compact(*ip).unwrap();
        assert_eq!(ranges_and_compact_start.unpack_ip(compact), *ip);
    }
}

pub fn train(ip_addrs_sorted: &[u128]) -> IntervalCompressor {
    let (amplitude, ranges_and_compact_start) = get_blanks(ip_addrs_sorted, INTERVALL_COST_IN_BITS);

    assert!(amplitude <= u64::MAX as u128, "case unsupported.");

    let num_bits = tantivy_bitpacker::compute_num_bits(amplitude as u64);
    let compressor = IntervalCompressor {
        ranges_and_compact_start,
        num_bits,
    };
    assert_eq!(
        compressor.to_compact(*ip_addrs_sorted.last().unwrap()) + 1,
        amplitude as u64
    );
    compressor
}

impl IntervalCompressor {
    fn to_compact(&self, ip_addr: u128) -> u64 {
        self.ranges_and_compact_start.to_compact(ip_addr).unwrap()
    }

    fn write_header(&self, output: &mut Vec<u8>) {
        assert!(output.is_empty());

        // header flags to for future optional dictionary encoding
        let header_flags = 0u64;
        output.extend_from_slice(&header_flags.to_le_bytes());

        self.ranges_and_compact_start.serialize(output);
        output.push(self.num_bits);
    }

    pub fn compress(&self, vals: &[u128]) -> Vec<u8> {
        let mut output = vec![];
        self.compress_into(vals, &mut output);
        output
    }
    pub fn compress_into(&self, vals: &[u128], output: &mut Vec<u8>) {
        self.write_header(output);
        serialize_vint(vals.len() as u128, output);
        let mut bitpacker = BitPacker::default();
        for &ip_addr in vals {
            let compact = self.to_compact(ip_addr);
            bitpacker.write(compact, self.num_bits, output).unwrap();
        }
        bitpacker.close(output).unwrap();
    }
}

#[derive(Debug)]
pub struct IntervallDecompressor {
    compact_space: CompactSpace,
    bit_unpacker: BitUnpacker,
    num_vals: usize,
}

impl IntervallDecompressor {
    pub fn open(data: &[u8]) -> io::Result<(IntervallDecompressor, &[u8])> {
        let (_header_flags, data) = data.split_at(8);
        let (mut data, compact_space) = CompactSpace::deserialize(data).unwrap();

        let num_bits = data[0];
        data = &data[1..];
        let (num_vals, data) = deserialize_vint(data)?;
        let decompressor = IntervallDecompressor {
            compact_space,
            num_vals: num_vals as usize,
            bit_unpacker: BitUnpacker::new(num_bits),
        };

        Ok((decompressor, data))
    }

    /// Converting to compact space for the decompressor is more complex, since we may get values which are
    /// outside the compact space. e.g. if we map
    /// 1000 => 5
    /// 2000 => 6
    ///
    /// and we want a mapping for 1005, there is no
    fn to_compact(&self, ip_addr: u128) -> Result<u64, usize> {
        self.compact_space.to_compact(ip_addr)
    }

    fn compact_to_ip_addr(&self, compact: u64) -> u128 {
        self.compact_space.unpack_ip(compact)
    }

    /// Comparing on compact space: 1.2 GElements/s
    ///
    /// Comparing on original space: .06 GElements/s
    pub fn get_range(&self, from_ip_addr: u128, to_ip_addr: u128, data: &[u8]) -> Vec<usize> {
        assert!(to_ip_addr >= from_ip_addr);
        let compact_from = self.to_compact(from_ip_addr);
        let compact_to = self.to_compact(to_ip_addr);
        // Quick return, if both ranges fall into the same non-mapped space, the range can't cover
        // any values, so we can early exit
        match (compact_to, compact_from) {
            (Err(pos1), Err(pos2)) if pos1 == pos2 => return vec![],
            _ => {}
        }

        let compact_from = compact_from.unwrap_or_else(|pos| self.compact_space.get_range(pos).1);
        // If there is no compact space, we go to the upper bound of the next lower compact space
        let compact_to = compact_to.unwrap_or_else(|pos| self.compact_space.get_range(pos).1 - 1);

        let range = compact_from..=compact_to;
        let mut positions = vec![];

        for (pos, compact_ip) in self.iter_compact(data).enumerate() {
            if range.contains(&compact_ip) {
                positions.push(pos);
            }
        }

        positions
    }

    #[inline]
    pub fn iter_compact<'a>(&'a self, data: &'a [u8]) -> impl Iterator<Item = u64> + 'a {
        (0..self.num_vals).map(move |idx| self.bit_unpacker.get(idx as u64, data) as u64)
    }

    #[inline]
    pub fn iter<'a>(&'a self, data: &'a [u8]) -> impl Iterator<Item = u128> + 'a {
        // TODO: Performance. It would be better to iterate on the ranges and check existence via the bit_unpacker.
        (0..self.num_vals)
            .map(move |idx| self.bit_unpacker.get(idx as u64, data) as u64)
            .map(|compact| self.compact_to_ip_addr(compact))
    }

    pub fn get(&self, idx: usize, data: &[u8]) -> u128 {
        let base = self.bit_unpacker.get(idx as u64, data);
        self.compact_to_ip_addr(base)
    }
}

impl IntervalEncoding {
    fn train(&self, mut vals: Vec<u128>) -> IntervalCompressor {
        vals.sort();
        train(&vals)
    }

    pub fn encode(&self, vals: &[u128]) -> Vec<u8> {
        if vals.is_empty() {
            return Vec::new();
        }
        let compressor = self.train(vals.to_vec());
        compressor.compress(&vals)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn decode_all(data: &[u8]) -> Vec<u128> {
        let (decompressor, data) = IntervallDecompressor::open(data).unwrap();
        let mut u128_vals = Vec::new();
        for idx in 0..decompressor.num_vals as usize {
            let val = decompressor.get(idx, data);
            u128_vals.push(val);
        }
        u128_vals
    }

    fn test_aux_vals(encoder: &IntervalEncoding, u128_vals: &[u128]) -> Vec<u8> {
        let data = encoder.encode(u128_vals);
        let decoded_val = decode_all(&data);
        assert_eq!(&decoded_val, u128_vals);
        data
    }

    #[test]
    fn test_compress() {
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
        let interval_encoding = IntervalEncoding::default();
        let data = test_aux_vals(&interval_encoding, vals);
        let (decomp, data) = IntervallDecompressor::open(&data).unwrap();
        let positions = decomp.get_range(0, 1, data);
        assert_eq!(positions, vec![0]);
        let positions = decomp.get_range(0, 2, data);
        assert_eq!(positions, vec![0]);
        let positions = decomp.get_range(0, 3, data);
        assert_eq!(positions, vec![0, 2]);
        assert_eq!(decomp.get_range(99999u128, 99999u128, data), vec![3]);
        assert_eq!(decomp.get_range(99998u128, 100000u128, data), vec![3, 4]);
        assert_eq!(decomp.get_range(99998u128, 99999u128, data), vec![3]);
        assert_eq!(decomp.get_range(99998u128, 99998u128, data), vec![]);
        assert_eq!(decomp.get_range(333u128, 333u128, data), vec![8]);
        assert_eq!(decomp.get_range(332u128, 333u128, data), vec![8]);
        assert_eq!(decomp.get_range(332u128, 334u128, data), vec![8]);
        assert_eq!(decomp.get_range(333u128, 334u128, data), vec![8]);
    }

    #[test]
    fn test_first_large_gaps() {
        let vals = &[1_000_000_000u128; 100];
        let interval_encoding = IntervalEncoding::default();
        let _data = test_aux_vals(&interval_encoding, vals);
    }
    use proptest::prelude::*;

    proptest! {

            #[test]
            fn compress_decompress_random(vals in proptest::collection::vec(any::<u128>()
    , 1..1000)) {
                let interval_encoding = IntervalEncoding::default();
                let _data = test_aux_vals(&interval_encoding, &vals);
            }
        }
}
