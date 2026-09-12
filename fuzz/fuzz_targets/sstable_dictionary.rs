#![no_main]

//! Fuzzes `Dictionary` deserialization for the SSTable format.
//!
//! The Dictionary is a critical data structure for term lookups in tantivy.
//! Deserialization of untrusted binary data must never panic or cause undefined behavior.

use libfuzzer_sys::fuzz_target;
use tantivy_common::OwnedBytes;
use tantivy_sstable::{Dictionary, MonotonicU64SSTable};

fuzz_target!(|data: &[u8]| {
    let owned = OwnedBytes::new(data.to_vec());

    // Attempt to deserialize the dictionary.
    // Malformed input should produce an Err, never a panic.
    if let Ok(dict) = Dictionary::<MonotonicU64SSTable>::from_bytes(owned) {
        // Exercise basic read methods on the successfully deserialized dictionary.
        // Ignore any Err results — they are legitimate for certain operations.
        let _num_terms = std::hint::black_box(dict.num_terms());

        // Try a lookup for an empty key and a common key.
        let _ = dict.get(b"");
        let _ = dict.term_ord(b"test");
    }
});
