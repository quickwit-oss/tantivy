#![no_main]

//! Fuzzes variable-length integer (VInt) deserialization in tantivy-common.
//!
//! VInt is used throughout tantivy for compact integer encoding.
//! Malformed input must produce errors gracefully without panicking.

use libfuzzer_sys::fuzz_target;
use tantivy_common::{BinarySerializable, VInt, VIntU128};

fuzz_target!(|data: &[u8]| {
    // Test VInt deserialization and round-trip property.
    // Use a mutable slice as the Read impl.
    let mut cursor: &[u8] = data;
    if let Ok(vint) = VInt::deserialize(&mut cursor) {
        // Round-trip check: re-serialize and verify we get the same value back.
        let mut buffer = Vec::new();
        let _ = vint.serialize(&mut buffer);
        let mut re_cursor: &[u8] = &buffer;
        if let Ok(re_deserialized) = VInt::deserialize(&mut re_cursor) {
            // A round-trip mismatch is a real bug, so assert.
            assert_eq!(vint, re_deserialized, "VInt round-trip failed");
        }
    }

    // Test VIntU128 deserialization and round-trip property.
    let mut cursor: &[u8] = data;
    if let Ok(vint_u128) = VIntU128::deserialize(&mut cursor) {
        // Round-trip check: re-serialize and verify we get the same value back.
        let mut buffer = Vec::new();
        let _ = vint_u128.serialize(&mut buffer);
        let mut re_cursor: &[u8] = &buffer;
        if let Ok(re_deserialized) = VIntU128::deserialize(&mut re_cursor) {
            // A round-trip mismatch is a real bug, so assert.
            assert_eq!(vint_u128, re_deserialized, "VIntU128 round-trip failed");
        }
    }

    // NOTE: We deliberately avoid low-level functions like read_u32_vint and
    // read_u32_vint_no_advance in common/src/vint.rs. Those functions have
    // documented preconditions (e.g. assuming well-formed input) and will panic
    // on precondition violations. Using them here would incorrectly report
    // valid fuzzer inputs as crashes.
});
