//! Regenerates the seed corpora under `fuzz/seeds/`.
//!
//! The byte-oriented targets parse formats that carry their own footer, magic
//! and offsets. Blind mutation essentially never synthesises one, so without a
//! seed those targets spend their whole budget being rejected at the first
//! length check. A handful of small, valid files gives the fuzzer something to
//! corrupt, which is where the interesting states are.
//!
//! Run from the repository root:
//!
//! ```bash
//! cargo run --manifest-path fuzz/seeds/generator/Cargo.toml -- fuzz/seeds
//! ```

use std::io;
use std::path::Path;

use tantivy_columnar::ColumnarWriter;
use tantivy_sstable::{Dictionary, MonotonicU64SSTable};

/// Writes `bytes` to `dir/name`, creating `dir` if needed.
fn write_seed(dir: &Path, name: &str, bytes: &[u8]) -> io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(name);
    std::fs::write(&path, bytes)?;
    println!("{} ({} bytes)", path.display(), bytes.len());
    Ok(())
}

/// A columnar holding one column of each shape the reader dispatches on:
/// a dense numeric column, a string column (which brings its own dictionary),
/// an optional column and a multivalued one.
fn columnar_seed() -> io::Result<Vec<u8>> {
    let mut writer = ColumnarWriter::default();
    for row in 0..4u32 {
        writer.record_numerical(row, "count", i64::from(row) * 7 - 3);
        writer.record_str(row, "name", "hello world");
    }
    // Only some rows: an optional column index.
    writer.record_numerical(1u32, "sparse", 1.5f64);
    // Several values in one row: a multivalued column index.
    for value in 0..3i64 {
        writer.record_numerical(2u32, "multi", value);
    }
    writer.record_bool(0u32, "flag", true);
    let mut buffer = Vec::new();
    writer.serialize(4, None, &mut buffer)?;
    Ok(buffer)
}

/// An sstable dictionary in the same format the columnar footer embeds, with
/// enough terms to span more than one block's worth of shared prefixes.
fn sstable_seed() -> io::Result<Vec<u8>> {
    let mut builder = Dictionary::<MonotonicU64SSTable>::builder(Vec::new())?;
    for (ord, term) in [
        "alpha",
        "alphabet",
        "alphabetical",
        "beta",
        "betamax",
        "gamma",
        "zzz",
    ]
    .iter()
    .enumerate()
    {
        builder.insert(term.as_bytes(), &(ord as u64))?;
    }
    builder.finish()
}

fn main() -> io::Result<()> {
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "fuzz/seeds".to_string());
    let root = Path::new(&root);

    write_seed(
        &root.join("columnar_reader"),
        "columnar.bin",
        &columnar_seed()?,
    )?;
    write_seed(
        &root.join("sstable_dictionary"),
        "dictionary.bin",
        &sstable_seed()?,
    )?;
    Ok(())
}
