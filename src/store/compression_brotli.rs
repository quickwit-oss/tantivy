use std::io;

#[inline]
pub fn compress(mut uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
    let params = brotli::enc::BrotliEncoderParams {
        quality: 5,
        ..Default::default()
    };
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
