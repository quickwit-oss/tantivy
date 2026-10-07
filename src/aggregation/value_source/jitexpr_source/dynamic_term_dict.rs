//! A term dictionary built on the fly, for computed text value sources.

use std::hash::BuildHasher;
use std::{fmt, io};

use hashbrown::HashTable;
use rustc_hash::FxBuildHasher;

use crate::aggregation::value_source::ValueSourceDictionary;

/// An append-only term dictionary, attributing term ords in first-seen order.
///
/// Term ords are dense (`0..num_terms()`) and stable: interning more terms never changes the ord
/// of a term. They are NOT sorted with the terms.
///
/// Each term is stored once, in `bytes`. The hash table stores the ord of each term along with a
/// 32-bit hash of the term, so that growing the table never reads the terms back. Equality
/// always compares the term bytes: a 32-bit hash collision only costs an extra comparison, and
/// never merges two terms.
///
/// Contract: at most `u32::MAX` terms (`intern` panics beyond that). Past ~2^25 buckets (~29M
/// terms), the bucket index and the tag of hashbrown start using the same bits of the 32-bit
/// hash: probing degrades, correctness does not.
pub(crate) struct DynamicTermDict {
    /// The concatenated terms, in ord order.
    bytes: Vec<u8>,
    /// `bytes[offsets[ord]..offsets[ord + 1]]` is the term of `ord`.
    ///
    /// Invariant: `offsets.len() == num_terms() + 1` and `offsets[0] == 0`.
    offsets: Vec<usize>,
    /// The entries of all of the terms, hashed by `table_hash(entry.hash)`.
    ords: HashTable<Entry>,
}

/// An entry of the hash table: a term ord and the 32-bit hash of its term.
#[derive(Clone, Copy)]
struct Entry {
    ord: u32,
    hash: u32,
}

/// Returns the 32-bit hash of `term` stored in the hash table.
fn term_hash(term: &[u8]) -> u32 {
    let hash: u64 = FxBuildHasher.hash_one(term);
    // Fold, so that both halves contribute.
    (hash ^ (hash >> 32)) as u32
}

/// Returns the hash used by the hash table for an entry of hash `hash`.
///
/// hashbrown takes the bucket index from the low bits and its 7-bit tag from the top 7 bits: the
/// multiplication spreads the 32 bits of `hash` to the top bits.
///
/// Contract: this must be a pure function of the stored `hash`, so that the table can recompute
/// it when growing.
fn table_hash(hash: u32) -> u64 {
    (hash as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

impl fmt::Debug for DynamicTermDict {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("DynamicTermDict")
            .field("num_terms", &self.num_terms())
            .finish()
    }
}

impl Default for DynamicTermDict {
    fn default() -> Self {
        DynamicTermDict {
            bytes: Vec::new(),
            offsets: vec![0],
            ords: HashTable::new(),
        }
    }
}

/// Returns the term of `ord`.
///
/// Precondition: `ord < offsets.len() - 1`.
fn term_of<'a>(bytes: &'a [u8], offsets: &[usize], ord: u64) -> &'a [u8] {
    let ord = ord as usize;
    &bytes[offsets[ord]..offsets[ord + 1]]
}

impl DynamicTermDict {
    /// Returns the term ord of `term`, attributing the next ord if the term is new.
    pub(crate) fn intern(&mut self, term: &[u8]) -> u64 {
        let DynamicTermDict {
            bytes,
            offsets,
            ords,
        } = self;
        let hash: u32 = term_hash(term);
        if let Some(entry) = ords.find(table_hash(hash), |entry| {
            entry.hash == hash && term_of(bytes, offsets, entry.ord as u64) == term
        }) {
            return entry.ord as u64;
        }
        let ord: u32 = u32::try_from(offsets.len() - 1)
            .expect("a dynamic term dictionary holds at most u32::MAX terms");
        bytes.extend_from_slice(term);
        offsets.push(bytes.len());
        ords.insert_unique(table_hash(hash), Entry { ord, hash }, |entry| {
            table_hash(entry.hash)
        });
        ord as u64
    }

    /// Returns the term of `ord`, or `None` if the ord is out of range.
    pub(crate) fn term(&self, ord: u64) -> Option<&[u8]> {
        if ord >= self.num_terms() {
            return None;
        }
        Some(term_of(&self.bytes, &self.offsets, ord))
    }

    pub(crate) fn num_terms(&self) -> u64 {
        (self.offsets.len() - 1) as u64
    }
}

impl ValueSourceDictionary for DynamicTermDict {
    fn ords_sorted_with_terms(&self) -> bool {
        false
    }

    fn num_terms(&self) -> u64 {
        DynamicTermDict::num_terms(self)
    }

    fn sorted_ords_to_term_cb(
        &self,
        sorted_ords: &[u64],
        callback: &mut dyn FnMut(&[u8]),
    ) -> io::Result<bool> {
        // Random access: the ords do not actually need to be sorted.
        for &ord in sorted_ords {
            let Some(term) = self.term(ord) else {
                return Ok(false);
            };
            callback(term);
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intern_attributes_ords_in_first_seen_order() {
        let mut dict = DynamicTermDict::default();
        assert_eq!(dict.intern(b"zebra"), 0);
        assert_eq!(dict.intern(b"apple"), 1);
        assert_eq!(dict.intern(b"zebra"), 0);
        assert_eq!(dict.intern(b""), 2);
        assert_eq!(dict.intern(b"apple"), 1);
        assert_eq!(dict.num_terms(), 3);
        assert_eq!(dict.term(0), Some(&b"zebra"[..]));
        assert_eq!(dict.term(1), Some(&b"apple"[..]));
        assert_eq!(dict.term(2), Some(&b""[..]));
        assert_eq!(dict.term(3), None);
        assert!(!dict.ords_sorted_with_terms());
    }

    #[test]
    fn test_sorted_ords_to_term_cb() {
        let mut dict = DynamicTermDict::default();
        for term in ["b", "a", "c"] {
            dict.intern(term.as_bytes());
        }
        let mut terms: Vec<String> = Vec::new();
        let all_found = dict
            .sorted_ords_to_term_cb(&[0, 2], &mut |term| {
                terms.push(String::from_utf8(term.to_vec()).unwrap())
            })
            .unwrap();
        assert!(all_found);
        assert_eq!(terms, vec!["b".to_string(), "c".to_string()]);
        let all_found = dict.sorted_ords_to_term_cb(&[3], &mut |_| {}).unwrap();
        assert!(!all_found);
    }

    #[test]
    fn test_many_terms() {
        let mut dict = DynamicTermDict::default();
        for i in 0..10_000u64 {
            assert_eq!(dict.intern(i.to_string().as_bytes()), i);
        }
        for i in 0..10_000u64 {
            assert_eq!(dict.intern(i.to_string().as_bytes()), i);
            assert_eq!(dict.term(i), Some(i.to_string().as_bytes()));
        }
    }

    #[test]
    fn test_colliding_hashes_get_distinct_ords() {
        // Brute-force two distinct terms sharing the same 32-bit hash. With 300k terms, ~10
        // colliding pairs are expected.
        let num_candidates = 300_000;
        let mut term_by_hash: std::collections::HashMap<u32, String> =
            std::collections::HashMap::with_capacity(num_candidates);
        let mut colliding_pair: Option<(String, String)> = None;
        for i in 0..num_candidates {
            let term = format!("t{i}");
            if let Some(other_term) = term_by_hash.insert(term_hash(term.as_bytes()), term.clone())
            {
                colliding_pair = Some((other_term, term));
                break;
            }
        }
        let (first_term, second_term) = colliding_pair.expect("no 32-bit hash collision found");
        assert_ne!(first_term, second_term);

        let mut dict = DynamicTermDict::default();
        assert_eq!(dict.intern(first_term.as_bytes()), 0);
        assert_eq!(dict.intern(second_term.as_bytes()), 1);
        assert_eq!(dict.intern(first_term.as_bytes()), 0);
        assert_eq!(dict.intern(second_term.as_bytes()), 1);
        assert_eq!(dict.term(0), Some(first_term.as_bytes()));
        assert_eq!(dict.term(1), Some(second_term.as_bytes()));
    }
}
