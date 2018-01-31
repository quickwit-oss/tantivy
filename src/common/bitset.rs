use DocId;

pub trait TinySet {
    fn insert(&mut self, b: u32);
    fn is_empty(&self) -> bool;
    fn pop_lowest(&mut self) -> Option<u32>;
    fn remove(&mut self, b: u32);
    fn lowest(&mut self) -> Option<u32>;

    /// Update self to represent the
    /// intersection of its elements and the other
    /// set given in arguments.
    fn intersect(&mut self, other: Self);

    /// Returns a `TinySet` than contains all values up
    /// to limit excluded.
    ///
    /// The limit is assumed to be strictly lower than 64.
    fn range_lower(limit: u32) -> u64;

    /// Returns a `TinySet` that contains all values greater
    /// or equal to the given limit, included. (and up to 63)
    ///
    /// The limit is assumed to be strictly lower than 64.
    fn range_greater_or_equal(from_included: u32) -> u64 {
        assert!(from_included < 64);
        0 ^ Self::range_lower(from_included)
    }
}

impl TinySet for u64 {
    fn range_lower(from_included: u32) -> u64 {
        assert!(from_included < 64);
        (1u64 << (from_included as u64)) - 1u64
    }

    fn intersect(&mut self, filter_mask: u64) {
        *self &= filter_mask;
    }

    #[inline(always)]
    fn insert(&mut self, b: u32) {
        *self |= 1u64 << (b as u64);
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        *self == 0u64
    }

    #[inline(always)]
    fn pop_lowest(&mut self) -> Option<u32> {
        if let Some(lowest) = self.lowest() {
            self.remove(lowest);
            Some(lowest)
        } else {
            None
        }
    }

    #[inline(always)]
    fn remove(&mut self, b: u32) {
        *self ^= 1 << (b as u64);
    }

    #[inline(always)]
    fn lowest(&mut self) -> Option<u32> {
        if self.is_empty() {
            None
        } else {
            let least_significant_bit = self.trailing_zeros() as u32;
            Some(least_significant_bit)
        }
    }
}

pub struct DocBitSet {
    tinybitsets: Box<[u64]>,
    size_hint: usize, //< Technically it should be u32, but we
                      // count multiple inserts.
                      // `usize` guards us from overflow.
    max_doc: DocId
}

impl DocBitSet {
    pub fn with_maxdoc(max_doc: DocId) -> DocBitSet {
        let num_buckets = (max_doc + 63) / 64;
        DocBitSet {
            tinybitsets: vec![0u64; num_buckets as usize].into_boxed_slice(),
            size_hint: 0,
            max_doc
        }
    }

    pub fn size_hint(&self) -> u32 {
        if self.max_doc as usize > self.size_hint {
            self.size_hint as u32
        } else {
            self.max_doc
        }
    }

    pub fn insert(&mut self, doc: DocId) {
        // we do not check saturated els.
        self.size_hint += 1;
        let bucket = (doc / 64u32) as usize;
        self.tinybitsets[bucket].insert(doc % 64u32);
    }

    pub fn contains(&self, doc: DocId) -> bool {
        let tiny_bitset = self.tiny_bitset((doc / 64u32) as usize);
        let lower = doc % 64;
        let mask = 1u64 << (lower as u64);
        (tiny_bitset & mask) != 0u64
    }

    pub fn max_doc(&self) -> DocId {
        self.max_doc
    }

    pub fn num_tiny_bitsets(&self) -> usize {
        self.tinybitsets.len()
    }

    pub fn tiny_bitset(&self, bucket: usize) -> u64 {
        self.tinybitsets[bucket]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use DocId;
    use super::TinySet;
    use super::DocBitSet;

    #[test]
    fn test_tiny_set() {
        assert!(0u64.is_empty());
        {
            let mut u = 0u64;
            u.insert(1u32);
            assert_eq!(u.pop_lowest(), Some(1u32));
            assert!(u.pop_lowest().is_none())
        }
        {
            let mut u = 0u64;
            u.insert(1u32);
            u.insert(1u32);
            assert_eq!(u.pop_lowest(), Some(1u32));
            assert!(u.pop_lowest().is_none())
        }
        {
            let mut u = 0u64;
            u.insert(2u32);
            assert_eq!(u.pop_lowest(), Some(2u32));
            u.insert(1u32);
            assert_eq!(u.pop_lowest(), Some(1u32));
            assert!(u.pop_lowest().is_none());
        }
        {
            let mut u = 0u64;
            u.insert(63u32);
            assert_eq!(u.pop_lowest(), Some(63u32));
            assert!(u.pop_lowest().is_none());
        }
    }


    #[test]
    fn test_docbitset() {
        // docs are assumed to be lower than 100.
        let test_against_hashset = |docs: &[DocId], max_doc: u32| {
            let mut hashset: HashSet<DocId> = HashSet::new();
            let mut docbitset = DocBitSet::with_maxdoc(max_doc);
            for &doc in docs {
                assert!(doc < max_doc);
                hashset.insert(doc);
                docbitset.insert(doc);
            }
            for doc in 0..max_doc {
                assert_eq!(
                    hashset.contains(&doc),
                    docbitset.contains(doc)
                );
            }
            assert_eq!(docbitset.max_doc(), max_doc);
        };

        test_against_hashset(&[], 0);
        test_against_hashset(&[], 1);
        test_against_hashset(&[0u32], 1);
        test_against_hashset(&[0u32], 100);
        test_against_hashset(&[1u32, 2u32], 4);
        test_against_hashset(&[99u32], 100);
        test_against_hashset(&[63u32], 64);
        test_against_hashset(&[62u32,63u32], 64);
    }

    #[test]
    fn test_docbitset_num_buckets() {
        assert_eq!(DocBitSet::with_maxdoc(0u32).num_tiny_bitsets(), 0);
        assert_eq!(DocBitSet::with_maxdoc(1u32).num_tiny_bitsets(), 1);
        assert_eq!(DocBitSet::with_maxdoc(64u32).num_tiny_bitsets(), 1);
        assert_eq!(DocBitSet::with_maxdoc(65u32).num_tiny_bitsets(), 2);
        assert_eq!(DocBitSet::with_maxdoc(128u32).num_tiny_bitsets(), 2);
        assert_eq!(DocBitSet::with_maxdoc(129u32).num_tiny_bitsets(), 3);
    }
}

