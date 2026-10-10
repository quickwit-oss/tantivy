//! A term dictionary built on the fly, for computed text value sources.

use std::hash::BuildHasher;
use std::{fmt, io};

use hashbrown::hash_table::Entry;
use hashbrown::HashTable;
use rustc_hash::FxBuildHasher;

use crate::aggregation::value_source::ValueSourceDictionary;

/// An append-only dictionary that interns terms and assigns them term ords.
///
/// Term ords are assigned in first-seen order. They are dense (`0..num_terms()`) and stable:
/// interning new terms never changes the ord of a known term. They are NOT sorted with the terms.
///
/// All of the terms are concatenated in a single buffer. A separate offset index gives the
/// boundaries of each term.
pub(crate) struct DynamicTermDictionary {
    /// Concatenation of all of the terms, in term ord order.
    buffer: Vec<u8>,
    /// `buffer[offsets[ord]..offsets[ord + 1]]` is the term of `ord`.
    ///
    /// Invariant: `offsets[0] == 0`, and `offsets.len() == num_terms() + 1`.
    offsets: Vec<usize>,
    /// One item per term, hashed with `table_hash(item.hash)`.
    term_ords: HashTable<TermOrdItem>,
}

/// An item of the hash table
///
/// We store the 32 bits term to avoid recomputing it upon table resize.
#[derive(Clone, Copy)]
struct TermOrdItem {
    ord: u32,
    hash: u32,
}

/// Returns the 32-bit hash of `term` stored in the hash table.
#[inline]
fn term_hash(term: &[u8]) -> u32 {
    let hash: u64 = FxBuildHasher.hash_one(term);
    // Both halves contribute to the folded hash.
    (hash ^ (hash >> 32)) as u32
}

/// Returns the hash used by the hash table for an item with the 32-bit hash `hash`.
///
/// hashbrown takes the bucket index from the low bits of the hash, and its 7-bit tag from the top
/// bits. The multiplication spreads the 32 bits of `hash` to the top bits: without it, all of the
/// tags would be 0.
///
/// Contract: this must only depend on the stored hash, so that the table can compute it again when
/// it grows without reading the term.
#[inline]
fn table_hash(hash: u32) -> u64 {
    (hash as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

impl Default for DynamicTermDictionary {
    fn default() -> Self {
        DynamicTermDictionary {
            buffer: Vec::new(),
            offsets: vec![0],
            term_ords: HashTable::new(),
        }
    }
}

impl fmt::Debug for DynamicTermDictionary {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("DynamicTermDictionary")
            .field("num_terms", &self.num_terms())
            .finish()
    }
}

/// Returns the term of `ord`.
///
/// Precondition: `ord < offsets.len() - 1`.
#[inline]
fn term_of<'a>(buffer: &'a [u8], offsets: &[usize], ord: usize) -> &'a [u8] {
    &buffer[offsets[ord]..offsets[ord + 1]]
}

impl DynamicTermDictionary {
    /// Returns the term ord of `term`. A new term gets the next term ord.
    pub(crate) fn intern(&mut self, term: &[u8]) -> u64 {
        let DynamicTermDictionary {
            buffer,
            offsets,
            term_ords,
        } = self;
        let hash: u32 = term_hash(term);
        let entry = term_ords.entry(
            table_hash(hash),
            |item| item.hash == hash && term_of(buffer, offsets, item.ord as usize) == term,
            |item| table_hash(item.hash),
        );
        match entry {
            Entry::Occupied(occupied_entry) => occupied_entry.get().ord as u64,
            Entry::Vacant(vacant_entry) => {
                let ord: u32 = u32::try_from(offsets.len() - 1)
                    .expect("a dynamic term dictionary holds at most u32::MAX terms");
                buffer.extend_from_slice(term);
                offsets.push(buffer.len());
                vacant_entry.insert(TermOrdItem { ord, hash });
                ord as u64
            }
        }
    }

    /// Returns the term of `ord`, or `None` if `ord` is out of range.
    pub(crate) fn term(&self, ord: u64) -> Option<&[u8]> {
        if ord >= self.num_terms() {
            return None;
        }
        Some(term_of(&self.buffer, &self.offsets, ord as usize))
    }

    pub(crate) fn num_terms(&self) -> u64 {
        (self.offsets.len() - 1) as u64
    }
}

impl ValueSourceDictionary for DynamicTermDictionary {
    fn ords_sorted_with_terms(&self) -> bool {
        false
    }

    fn num_terms(&self) -> u64 {
        DynamicTermDictionary::num_terms(self)
    }

    fn sorted_ords_to_term_cb(
        &self,
        sorted_ords: &[u64],
        callback: &mut dyn FnMut(&[u8]),
    ) -> io::Result<bool> {
        // Random access: the ords do not need to be sorted.
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
    fn test_intern_assigns_ords_in_first_seen_order() {
        let mut dictionary = DynamicTermDictionary::default();
        assert_eq!(dictionary.num_terms(), 0);
        assert_eq!(dictionary.intern(b"zebra"), 0);
        assert_eq!(dictionary.intern(b"apple"), 1);
        assert_eq!(dictionary.intern(b"zebra"), 0);
        assert_eq!(dictionary.intern(b""), 2);
        assert_eq!(dictionary.intern(b"apple"), 1);
        assert_eq!(dictionary.num_terms(), 3);
        assert_eq!(dictionary.term(0), Some(&b"zebra"[..]));
        assert_eq!(dictionary.term(1), Some(&b"apple"[..]));
        assert_eq!(dictionary.term(2), Some(&b""[..]));
        assert_eq!(dictionary.term(3), None);
        assert!(!dictionary.ords_sorted_with_terms());
    }

    #[test]
    fn test_offsets_layout() {
        let mut dictionary = DynamicTermDictionary::default();
        dictionary.intern(b"ab");
        dictionary.intern(b"");
        dictionary.intern(b"cde");
        assert_eq!(&dictionary.buffer, b"abcde");
        assert_eq!(&dictionary.offsets, &[0, 2, 2, 5]);
    }

    #[test]
    fn test_sorted_ords_to_term_cb() {
        let mut dictionary = DynamicTermDictionary::default();
        for term in ["b", "a", "c"] {
            dictionary.intern(term.as_bytes());
        }
        let mut terms: Vec<String> = Vec::new();
        let all_found = dictionary
            .sorted_ords_to_term_cb(&[0, 2], &mut |term| {
                terms.push(String::from_utf8(term.to_vec()).unwrap())
            })
            .unwrap();
        assert!(all_found);
        assert_eq!(terms, vec!["b".to_string(), "c".to_string()]);
        let all_found = dictionary
            .sorted_ords_to_term_cb(&[3], &mut |_| {})
            .unwrap();
        assert!(!all_found);
    }

    #[test]
    fn test_many_terms() {
        // Several table resizes.
        let mut dictionary = DynamicTermDictionary::default();
        for i in 0..10_000u64 {
            assert_eq!(dictionary.intern(i.to_string().as_bytes()), i);
        }
        for i in 0..10_000u64 {
            assert_eq!(dictionary.intern(i.to_string().as_bytes()), i);
            assert_eq!(dictionary.term(i), Some(i.to_string().as_bytes()));
        }
        assert_eq!(dictionary.num_terms(), 10_000);
    }

    #[test]
    fn test_colliding_hashes_get_distinct_ords() {
        // Brute-force two distinct terms with the same 32-bit hash. With 300k terms, about 10
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

        let mut dictionary = DynamicTermDictionary::default();
        assert_eq!(dictionary.intern(first_term.as_bytes()), 0);
        assert_eq!(dictionary.intern(second_term.as_bytes()), 1);
        assert_eq!(dictionary.intern(first_term.as_bytes()), 0);
        assert_eq!(dictionary.intern(second_term.as_bytes()), 1);
        assert_eq!(dictionary.term(0), Some(first_term.as_bytes()));
        assert_eq!(dictionary.term(1), Some(second_term.as_bytes()));
    }
}
