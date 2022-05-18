use std::io::{self, Read, Write};

use zstd::bulk::{compress_to_buffer, decompress_to_buffer};
use zstd::DEFAULT_COMPRESSION_LEVEL;

#[inline]
pub fn compress(uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
    compressed.clear();

    let count_size = std::mem::size_of::<u64>();

    let max_size: usize = zstd::compress_bound(uncompressed.len()) + count_size;

    compressed.resize(max_size, 0);

    let compressed_size = compress_to_buffer(
        uncompressed,
        &mut compressed[count_size..],
        DEFAULT_COMPRESSION_LEVEL,
    )
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

    compressed[0..count_size].copy_from_slice(&(uncompressed.len() as u64).to_le_bytes());
    compressed.resize(compressed_size + count_size, 0);

    Ok(())
}

#[inline]
pub fn decompress(compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()> {
    decompressed.clear();

    let count_size = std::mem::size_of::<u64>();

    let uncompressed_size_bytes: &[u8; count_size] = compressed
        .get(..count_size)
        .ok_or(io::ErrorKind::InvalidData)?
        .try_into()
        .unwrap();

    let uncompressed_size = u64::from_le_bytes(*uncompressed_size_bytes);

    decompressed.resize(uncompressed_size, 0);
    let decompressed_size = decompress_to_buffer(&compressed[count_size..], decompressed)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

    if decompressed_size != uncompressed_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "doc store block not completely decompressed, data corruption".to_string(),
        ));
    }

    Ok(())
}
