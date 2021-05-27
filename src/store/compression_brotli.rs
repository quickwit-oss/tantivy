use std::io;

#[inline]
pub fn compress(mut uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
    let mut params = brotli::enc::BrotliEncoderParams::default();
    params.quality = 5;
    compressed.clear();
    brotli::BrotliCompress(&mut uncompressed, compressed, &params)?;
    Ok(())
}

#[inline]
pub fn decompress(mut compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()> {
    decompressed.clear();
    brotli::BrotliDecompress(&mut compressed, decompressed)?;
    Ok(())
}
