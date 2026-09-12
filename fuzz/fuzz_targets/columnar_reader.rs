#![no_main]

//! Fuzzes [`ColumnarReader::open`], which parses the columnar (fast field)
//! storage format out of a raw byte buffer.
//!
//! The format carries its own offsets and lengths in a footer, so a corrupted
//! or hostile buffer can point anywhere. Reading a split-brain index, a
//! truncated file or an attacker-supplied segment must surface as an `Err`
//! rather than a panic or an out-of-bounds read.

use libfuzzer_sys::fuzz_target;
use tantivy_columnar::ColumnarReader;

fuzz_target!(|data: &[u8]| {
    // `FileSlice: From<Vec<u8>>` via the blanket StableDeref impl.
    let Ok(reader) = ColumnarReader::open(data.to_vec()) else {
        // Rejecting malformed input is the correct outcome.
        return;
    };

    // Opening only parses the footer, so walk the column handles too: that is
    // where the per-column offsets recorded in the footer actually get used.
    let _ = std::hint::black_box(reader.num_columns());
    let _ = std::hint::black_box(reader.list_columns());
});
