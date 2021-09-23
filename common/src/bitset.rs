use std::convert::TryInto;
use std::io::Write;
use std::u64;
use std::{fmt, io};

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct TinySet(u64);

impl fmt::Debug for TinySet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.into_iter().collect::<Vec<u32>>().fmt(f)
    }
}

pub struct TinySetIterator(TinySet);
impl Iterator for TinySetIterator {
    type Item = u32;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_lowest()
    }
}

impl IntoIterator for TinySet {
    type Item = u32;
    type IntoIter = TinySetIterator;
    fn into_iter(self) -> Self::IntoIter {
        TinySetIterator(self)
    }
}

impl TinySet {
    pub fn serialize<T: Write>(&self, writer: &mut T) -> io::Result<()> {
        writer.write_all(self.0.to_le_bytes().as_ref())
    }

    #[inline]
    pub fn deserialize(data: [u8; 8]) -> io::Result<Self> {
        let val: u64 = u64::from_le_bytes(data);
        Ok(TinySet(val))
    }

    /// Returns an empty `TinySet`.
    #[inline]
    pub fn empty() -> TinySet {
        TinySet(0u64)
    }

    /// Returns a full `TinySet`.
    #[inline]
    pub fn full() -> TinySet {
        TinySet::empty().complement()
    }

    pub fn clear(&mut self) {
        self.0 = 0u64;
    }

    #[inline]
    /// Returns the complement of the set in `[0, 64[`.
    pub fn complement(self) -> TinySet {
        TinySet(!self.0)
    }

    #[inline]
    /// Returns true iff the `TinySet` bit is set at position `pos`.
    pub fn contains(self, pos: u32) -> bool {
        !self.intersect(TinySet::singleton(pos)).is_empty()
    }

    #[inline]
    /// Returns the number of set bits in the TinySet.
    pub fn num_set(self) -> u32 {
        self.0.count_ones()
    }

    #[inline]
    /// Returns the number of unset bits in the TinySet.
    pub fn num_unset(self) -> u32 {
        self.0.count_zeros()
    }

    #[inline]
    /// Returns the intersection of `self` and `other`
    pub fn intersect(self, other: TinySet) -> TinySet {
        TinySet(self.0 & other.0)
    }

    /// Creates a new `TinySet` with only one bit set at `pos`.
    /// within `[0; 64[`
    #[inline]
    pub fn singleton(pos: u32) -> TinySet {
        TinySet(1u64 << u64::from(pos))
    }

    /// Insert a new element within [0..64)
    #[inline]
    pub fn insert(self, el: u32) -> TinySet {
        self.union(TinySet::singleton(el))
    }

    /// Removes an element within [0..64)
    #[inline]
    pub fn remove(self, el: u32) -> TinySet {
        self.intersect(TinySet::singleton(el).complement())
    }

    /// Insert a new element within [0..64)
    ///
    /// returns true if the bit changed
    #[inline]
    pub fn insert_mut(&mut self, el: u32) -> bool {
        let old = *self;
        *self = old.insert(el);
        old != *self
    }

    /// Remove a new element within [0..64)
    ///
    /// returns true if the bit changed
    #[inline]
    pub fn remove_mut(&mut self, el: u32) -> bool {
        let old = *self;
        *self = old.remove(el);
        old != *self
    }

    /// Returns the union of two tinysets
    #[inline]
    pub fn union(self, other: TinySet) -> TinySet {
        TinySet(self.0 | other.0)
    }

    /// Returns true iff the `TinySet` is empty.
    #[inline]
    pub fn is_empty(self) -> bool {
        self.0 == 0u64
    }

    /// Returns the lowest element in the `TinySet`
    /// and removes it.
    #[inline]
    pub fn pop_lowest(&mut self) -> Option<u32> {
        if self.is_empty() {
            None
        } else {
            let lowest = self.0.trailing_zeros() as u32;
            self.0 ^= TinySet::singleton(lowest).0;
            Some(lowest)
        }
    }

    /// Returns a `TinySet` than contains all values up
    /// to limit excluded.
    ///
    /// The limit is assumed to be strictly lower than 64.
    pub fn range_lower(upper_bound: u32) -> TinySet {
        TinySet((1u64 << u64::from(upper_bound % 64u32)) - 1u64)
    }

    /// Returns a `TinySet` that contains all values greater
    /// or equal to the given limit, included. (and up to 63)
    ///
    /// The limit is assumed to be strictly lower than 64.
    pub fn range_greater_or_equal(from_included: u32) -> TinySet {
        TinySet::range_lower(from_included).complement()
    }
}

#[derive(Clone)]
pub struct BitSet {
    tinysets: Box<[TinySet]>,
    len: u64,
    max_value: u32,
}

fn num_buckets(max_val: u32) -> u32 {
    (max_val + 63u32) / 64u32
}

impl BitSet {
    /// serialize a `BitSet`.
    ///
    pub fn serialize<T: Write>(&self, writer: &mut T) -> io::Result<()> {
        writer.write_all(self.max_value.to_le_bytes().as_ref())?;

        for tinyset in self.tinysets.iter() {
            tinyset.serialize(writer)?;
        }
        writer.flush()?;
        Ok(())
    }

    /// Deserialize a `BitSet`.
    ///
    #[cfg(test)]
    pub fn deserialize(mut data: &[u8]) -> io::Result<Self> {
        let max_value: u32 = u32::from_le_bytes(data[..4].try_into().unwrap());
        data = &data[4..];

        let mut len: u64 = 0;
        let mut tinysets = vec![];
        for chunk in data.chunks_exact(8) {
            let tinyset = TinySet::deserialize(chunk.try_into().unwrap())?;
            len += tinyset.num_set() as u64;
            tinysets.push(tinyset);
        }
        Ok(BitSet {
            tinysets: tinysets.into_boxed_slice(),
            len,
            max_value,
        })
    }

    /// Count the number of unset bits from serialized data.
    ///
    #[inline]
    pub fn count_unset_from_bytes<'a>(data: &'a [u8]) -> usize {
        BitSet::iter_tinysets_from_bytes(data)
            .map(|tinyset| tinyset.num_unset() as usize)
            .sum()
    }

    /// Iterate the tinyset on the fly from serialized data.
    ///
    #[inline]
    fn iter_tinysets_from_bytes<'a>(data: &'a [u8]) -> impl Iterator<Item = TinySet> + 'a {
        assert!((data.len() - 4) % 8 == 0);
        data[4..].chunks_exact(8).map(move |chunk| {
            let tinyset: TinySet = TinySet::deserialize(chunk.try_into().unwrap()).unwrap();
            tinyset
        })
    }

    /// Iterate over the positions of the unset elements.
    ///
    /// max_val needs to be provided, since the last 64 bits may
    #[inline]
    pub fn iter_unset_from_bytes<'a>(data: &'a [u8]) -> impl Iterator<Item = u32> + 'a {
        let max_val: u32 = u32::from_le_bytes(data[..4].try_into().unwrap());
        Self::iter_tinysets_from_bytes(data)
            .enumerate()
            .flat_map(move |(chunk_num, tinyset)| {
                let chunk_base_val = chunk_num as u32 * 64;
                tinyset
                    .into_iter()
                    .map(move |val| val + chunk_base_val)
                    .take_while(move |doc| *doc < max_val)
            })
    }

    /// Create a new `BitSet` that may contain elements
    /// within `[0, max_val)`.
    pub fn with_max_value(max_value: u32) -> BitSet {
        let num_buckets = num_buckets(max_value);
        let tinybisets = vec![TinySet::empty(); num_buckets as usize].into_boxed_slice();
        BitSet {
            tinysets: tinybisets,
            len: 0,
            max_value,
        }
    }

    /// Create a new `BitSet` that may contain elements
    /// within `[0, max_val)`.
    pub fn with_max_value_and_filled(max_value: u32) -> BitSet {
        let num_buckets = num_buckets(max_value);
        let tinybisets = vec![TinySet::full(); num_buckets as usize].into_boxed_slice();
        BitSet {
            tinysets: tinybisets,
            len: max_value as u64,
            max_value,
        }
    }

    /// Removes all elements from the `BitSet`.
    pub fn clear(&mut self) {
        for tinyset in self.tinysets.iter_mut() {
            *tinyset = TinySet::empty();
        }
    }

    /// Returns the number of elements in the `BitSet`.
    pub fn num_set_bits(&self) -> usize {
        self.len as usize
    }

    /// Inserts an element in the `BitSet`
    pub fn insert(&mut self, el: u32) {
        // we do not check saturated els.
        let higher = el / 64u32;
        let lower = el % 64u32;
        self.len += if self.tinysets[higher as usize].insert_mut(lower) {
            1
        } else {
            0
        };
    }

    /// Inserts an element in the `BitSet`
    pub fn remove(&mut self, el: u32) {
        // we do not check saturated els.
        let higher = el / 64u32;
        let lower = el % 64u32;
        self.len -= if self.tinysets[higher as usize].remove_mut(lower) {
            1
        } else {
            0
        };
    }

    /// Returns true iff the elements is in the `BitSet`.
    #[inline]
    pub fn contains_from_bytes(el: u32, data: &[u8]) -> bool {
        let byte_offset = 4 + el / 8u32;
        let b: u8 = data[byte_offset as usize];
        let shift = (el % 8) as u8;
        b & (1u8 << shift) != 0
    }

    /// Returns true iff the elements is in the `BitSet`.
    pub fn contains(&self, el: u32) -> bool {
        self.tinyset(el / 64u32).contains(el % 64)
    }

    /// Returns the first non-empty `TinySet` associated to a bucket lower
    /// or greater than bucket.
    ///
    /// Reminder: the tiny set with the bucket `bucket`, represents the
    /// elements from `bucket * 64` to `(bucket+1) * 64`.
    pub fn first_non_empty_bucket(&self, bucket: u32) -> Option<u32> {
        self.tinysets[bucket as usize..]
            .iter()
            .cloned()
            .position(|tinyset| !tinyset.is_empty())
            .map(|delta_bucket| bucket + delta_bucket as u32)
    }

    pub fn max_value(&self) -> u32 {
        self.max_value
    }

    /// Returns the tiny bitset representing the
    /// the set restricted to the number range from
    /// `bucket * 64` to `(bucket + 1) * 64`.
    pub fn tinyset(&self, bucket: u32) -> TinySet {
        self.tinysets[bucket as usize]
    }
}

#[cfg(test)]
mod tests {

    use super::BitSet;
    use super::TinySet;
    use rand::distributions::Bernoulli;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::HashSet;
    use std::convert::TryInto;

    #[test]
    fn test_tiny_set_remove() {
        {
            let mut u = TinySet::empty().insert(63u32).insert(5).remove(63u32);
            assert_eq!(u.pop_lowest(), Some(5u32));
            assert!(u.pop_lowest().is_none());
        }
        {
            let mut u = TinySet::empty()
                .insert(63u32)
                .insert(1)
                .insert(5)
                .remove(63u32);
            assert_eq!(u.pop_lowest(), Some(1u32));
            assert_eq!(u.pop_lowest(), Some(5u32));
            assert!(u.pop_lowest().is_none());
        }
        {
            let mut u = TinySet::empty().insert(1).remove(63u32);
            assert_eq!(u.pop_lowest(), Some(1u32));
            assert!(u.pop_lowest().is_none());
        }
        {
            let mut u = TinySet::empty().insert(1).remove(1u32);
            assert!(u.pop_lowest().is_none());
        }
    }
    #[test]
    fn test_tiny_set() {
        assert!(TinySet::empty().is_empty());
        {
            let mut u = TinySet::empty().insert(1u32);
            assert_eq!(u.pop_lowest(), Some(1u32));
            assert!(u.pop_lowest().is_none())
        }
        {
            let mut u = TinySet::empty().insert(1u32).insert(1u32);
            assert_eq!(u.pop_lowest(), Some(1u32));
            assert!(u.pop_lowest().is_none())
        }
        {
            let mut u = TinySet::empty().insert(2u32);
            assert_eq!(u.pop_lowest(), Some(2u32));
            u.insert_mut(1u32);
            assert_eq!(u.pop_lowest(), Some(1u32));
            assert!(u.pop_lowest().is_none());
        }
        {
            let mut u = TinySet::empty().insert(63u32);
            assert_eq!(u.pop_lowest(), Some(63u32));
            assert!(u.pop_lowest().is_none());
        }
        {
            let mut u = TinySet::empty().insert(63u32).insert(5);
            assert_eq!(u.pop_lowest(), Some(5u32));
            assert_eq!(u.pop_lowest(), Some(63u32));
            assert!(u.pop_lowest().is_none());
        }
        {
            let u = TinySet::empty().insert(63u32).insert(5);
            let mut data = vec![];
            u.serialize(&mut data).unwrap();
            let mut u = TinySet::deserialize(data[..8].try_into().unwrap()).unwrap();
            assert_eq!(u.pop_lowest(), Some(5u32));
            assert_eq!(u.pop_lowest(), Some(63u32));
            assert!(u.pop_lowest().is_none());
        }
    }

    #[test]
    fn test_bitset() {
        let test_against_hashset = |els: &[u32], max_value: u32| {
            let mut hashset: HashSet<u32> = HashSet::new();
            let mut bitset = BitSet::with_max_value(max_value);
            for &el in els {
                assert!(el < max_value);
                hashset.insert(el);
                bitset.insert(el);
            }
            for el in 0..max_value {
                assert_eq!(hashset.contains(&el), bitset.contains(el));
            }
            assert_eq!(bitset.max_value(), max_value);

            // test deser
            let mut data = vec![];
            bitset.serialize(&mut data).unwrap();
            let bitset = BitSet::deserialize(&data).unwrap();
            for el in 0..max_value {
                assert_eq!(hashset.contains(&el), bitset.contains(el));
            }
            assert_eq!(bitset.max_value(), max_value);
            assert_eq!(bitset.num_set_bits(), els.len());
        };

        test_against_hashset(&[], 0);
        test_against_hashset(&[], 1);
        test_against_hashset(&[0u32], 1);
        test_against_hashset(&[0u32], 100);
        test_against_hashset(&[1u32, 2u32], 4);
        test_against_hashset(&[99u32], 100);
        test_against_hashset(&[63u32], 64);
        test_against_hashset(&[62u32, 63u32], 64);
    }

    #[test]
    fn test_bitset_num_buckets() {
        use super::num_buckets;
        assert_eq!(num_buckets(0u32), 0);
        assert_eq!(num_buckets(1u32), 1);
        assert_eq!(num_buckets(64u32), 1);
        assert_eq!(num_buckets(65u32), 2);
        assert_eq!(num_buckets(128u32), 2);
        assert_eq!(num_buckets(129u32), 3);
    }

    #[test]
    fn test_tinyset_range() {
        assert_eq!(
            TinySet::range_lower(3).into_iter().collect::<Vec<u32>>(),
            [0, 1, 2]
        );
        assert!(TinySet::range_lower(0).is_empty());
        assert_eq!(
            TinySet::range_lower(63).into_iter().collect::<Vec<u32>>(),
            (0u32..63u32).collect::<Vec<_>>()
        );
        assert_eq!(
            TinySet::range_lower(1).into_iter().collect::<Vec<u32>>(),
            [0]
        );
        assert_eq!(
            TinySet::range_lower(2).into_iter().collect::<Vec<u32>>(),
            [0, 1]
        );
        assert_eq!(
            TinySet::range_greater_or_equal(3)
                .into_iter()
                .collect::<Vec<u32>>(),
            (3u32..64u32).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_bitset_len() {
        let mut bitset = BitSet::with_max_value(1_000);
        assert_eq!(bitset.num_set_bits(), 0);
        bitset.insert(3u32);
        assert_eq!(bitset.num_set_bits(), 1);
        bitset.insert(103u32);
        assert_eq!(bitset.num_set_bits(), 2);
        bitset.insert(3u32);
        assert_eq!(bitset.num_set_bits(), 2);
        bitset.insert(103u32);
        assert_eq!(bitset.num_set_bits(), 2);
        bitset.insert(104u32);
        assert_eq!(bitset.num_set_bits(), 3);
        bitset.remove(105u32);
        assert_eq!(bitset.num_set_bits(), 3);
        bitset.remove(104u32);
        assert_eq!(bitset.num_set_bits(), 2);
        bitset.remove(3u32);
        assert_eq!(bitset.num_set_bits(), 1);
        bitset.remove(103u32);
        assert_eq!(bitset.num_set_bits(), 0);
    }

    pub fn sample_with_seed(n: u32, ratio: f64, seed_val: u8) -> Vec<u32> {
        StdRng::from_seed([seed_val; 32])
            .sample_iter(&Bernoulli::new(ratio).unwrap())
            .take(n as usize)
            .enumerate()
            .filter_map(|(val, keep)| if keep { Some(val as u32) } else { None })
            .collect()
    }

    pub fn sample(n: u32, ratio: f64) -> Vec<u32> {
        sample_with_seed(n, ratio, 4)
    }

    #[test]
    fn test_bitset_clear() {
        let mut bitset = BitSet::with_max_value(1_000);
        let els = sample(1_000, 0.01f64);
        for &el in &els {
            bitset.insert(el);
        }
        assert!(els.iter().all(|el| bitset.contains(*el)));
        bitset.clear();
        for el in 0u32..1000u32 {
            assert!(!bitset.contains(el));
        }
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use super::BitSet;
    use super::TinySet;
    use test;

    #[bench]
    fn bench_tinyset_pop(b: &mut test::Bencher) {
        b.iter(|| {
            let mut tinyset = TinySet::singleton(test::black_box(31u32));
            tinyset.pop_lowest();
            tinyset.pop_lowest();
            tinyset.pop_lowest();
            tinyset.pop_lowest();
            tinyset.pop_lowest();
            tinyset.pop_lowest();
        });
    }

    #[bench]
    fn bench_tinyset_sum(b: &mut test::Bencher) {
        let tiny_set = TinySet::empty().insert(10u32).insert(14u32).insert(21u32);
        b.iter(|| {
            assert_eq!(test::black_box(tiny_set).into_iter().sum::<u32>(), 45u32);
        });
    }

    #[bench]
    fn bench_tinyarr_sum(b: &mut test::Bencher) {
        let v = [10u32, 14u32, 21u32];
        b.iter(|| test::black_box(v).iter().cloned().sum::<u32>());
    }

    #[bench]
    fn bench_bitset_initialize(b: &mut test::Bencher) {
        b.iter(|| BitSet::with_max_value(1_000_000));
    }
}
