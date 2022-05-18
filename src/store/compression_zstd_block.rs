use std::io::{self, Read, Write};

use zstd::bulk::{compress_to_buffer, decompress_to_buffer};
use zstd::DEFAULT_COMPRESSION_LEVEL;

const USIZE_SIZE: usize = std::mem::size_of::<usize>();

#[inline]
pub fn compress(uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
    compressed.clear();

    let max_size: usize = uncompressed.len() + USIZE_SIZE;
    compressed.resize(max_size, 0);

    let compressed_size = compress_to_buffer(
        uncompressed,
        &mut compressed[USIZE_SIZE..],
        DEFAULT_COMPRESSION_LEVEL,
    )
    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

    compressed[0..USIZE_SIZE].copy_from_slice(&uncompressed.len().to_le_bytes());

    compressed.resize(compressed_size + USIZE_SIZE, 0);

    Ok(())
}

#[inline]
pub fn decompress(compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()> {
    decompressed.clear();

    let uncompressed_size_bytes: &[u8; USIZE_SIZE] = compressed
        .get(..USIZE_SIZE)
        .ok_or(io::ErrorKind::InvalidData)?
        .try_into()
        .unwrap();

    let uncompressed_size = usize::from_le_bytes(*uncompressed_size_bytes);

    decompressed.resize(uncompressed_size, 0);
    decompressed.resize(decompressed.capacity(), 0);
    let decompressed_size = decompress_to_buffer(&compressed[USIZE_SIZE..], decompressed)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

    if decompressed_size != uncompressed_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "doc store block not completely decompressed, data corruption".to_string(),
        ));
    }

    Ok(())
}
