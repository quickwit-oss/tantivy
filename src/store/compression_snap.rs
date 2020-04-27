use std::io::{self, Read, Write};

/// Name of the compression scheme used in the doc store.
///
/// This name is appended to the version string of tantivy.
pub const COMPRESSION: &str = "snappy";

pub fn compress(uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
    compressed.clear();
    let mut encoder = snap::write::FrameEncoder::new(compressed);
    encoder.write_all(&uncompressed)?;
    encoder.flush()?;
    Ok(())
}

pub fn decompress(compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()> {
    decompressed.clear();
    snap::read::FrameDecoder::new(compressed).read_to_end(decompressed)?;
    Ok(())
}
