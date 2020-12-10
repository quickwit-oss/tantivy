use std::io::{self, Read, Write};

/// Name of the compression scheme used in the doc store.
///
/// This name is appended to the version string of tantivy.
pub const COMPRESSION: &str = "lz4";

pub fn compress(uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
    compressed.clear();
    let mut encoder = lz4::EncoderBuilder::new().build(compressed)?;
    encoder.write_all(&uncompressed)?;
    let (_, encoder_result) = encoder.finish();
    encoder_result?;
    Ok(())
}

pub fn decompress(compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()> {
    decompressed.clear();
    let mut decoder = lz4::Decoder::new(compressed)?;
    decoder.read_to_end(decompressed)?;
    Ok(())
}
