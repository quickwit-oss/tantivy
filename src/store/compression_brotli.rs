use std::io::{self, Cursor};

/// Name of the compression scheme used in the doc store.
///
/// This name is appended to the version string of tantivy.
pub const COMPRESSION: &'static str = "brotli";

pub fn compress(uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
    let mut params = brotli::enc::BrotliEncoderParams::default();
    params.quality = 5;

    compressed.clear();
    brotli::BrotliCompress(uncompressed, compressed, &params)?;
    Ok(())
}

pub fn decompress(compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()> {
    decompressed.clear();
    brotli::BrotliDecompress(compressed, decompressed)?;
    Ok(())
}
