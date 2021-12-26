use std::io::{self, Read, Write};

#[inline]
pub fn compress(uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
    compressed.clear();
    let mut encoder = snap::write::FrameEncoder::new(compressed);
    encoder.write_all(uncompressed)?;
    encoder.flush()?;
    Ok(())
}

#[inline]
pub fn decompress(compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()> {
    decompressed.clear();
    snap::read::FrameDecoder::new(compressed).read_to_end(decompressed)?;
    Ok(())
}
