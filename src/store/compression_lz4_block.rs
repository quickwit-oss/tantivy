use std::io::{self};

use core::convert::TryInto;
use lz4_flex::{compress_into, decompress_into};

#[inline]
pub fn compress(uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
    compressed.clear();
    let maximum_ouput_size = lz4_flex::block::get_maximum_output_size(uncompressed.len());
    compressed.reserve(maximum_ouput_size);

    unsafe {
        compressed.set_len(maximum_ouput_size + 4);
    }
    let bytes_written = compress_into(uncompressed, compressed, 4)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    let num_bytes = uncompressed.len() as u32;
    compressed[0..4].copy_from_slice(&num_bytes.to_le_bytes());
    unsafe {
        compressed.set_len(bytes_written + 4);
    }
    Ok(())
}

#[inline]
pub fn decompress(compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()> {
    decompressed.clear();
    let uncompressed_size_bytes: &[u8; 4] = compressed
        .get(..4)
        .ok_or(io::ErrorKind::InvalidData)?
        .try_into()
        .unwrap();
    let uncompressed_size = u32::from_le_bytes(*uncompressed_size_bytes) as usize;
    decompressed.reserve(uncompressed_size);
    unsafe {
        decompressed.set_len(uncompressed_size);
    }
    let bytes_written = decompress_into(&compressed[4..], decompressed, 0)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    if bytes_written != uncompressed_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "doc store block not completely decompressed, data corruption".to_string(),
        ));
    }
    Ok(())
}
