use std::io::{self};

use core::convert::TryInto;
use lz4_flex::{compress_into, decompress_into};
/// Name of the compression scheme used in the doc store.
///
/// This name is appended to the version string of tantivy.
pub const COMPRESSION: &str = "lz4_block";

pub fn compress(uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
    compressed.clear();

    compressed.extend_from_slice(&[0, 0, 0, 0]);
    compress_into(uncompressed, compressed);
    let size = uncompressed.len() as u32;
    compressed[0] = size as u8;
    compressed[1] = (size >> 8) as u8;
    compressed[2] = (size >> 16) as u8;
    compressed[3] = (size >> 24) as u8;
    Ok(())
}

pub fn decompress(compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()> {
    decompressed.clear();
    //next lz4_flex version will support slice as input parameter.
    //this will make the usage much less ugly
    let size = compressed.get(..4).ok_or(io::ErrorKind::InvalidData)?;
    let size: &[u8; 4] = size.try_into().unwrap();
    let uncompressed_size = u32::from_le_bytes(*size) as usize;
    // reserve more than required, because blocked writes may write out of bounds, will be improved
    // with lz4_flex 1.0
    decompressed.reserve(uncompressed_size + 4 + 24);
    unsafe {
        decompressed.set_len(uncompressed_size);
    }
    decompress_into(&compressed[4..], decompressed).map_err(|_err| io::ErrorKind::InvalidData)?;
    Ok(())
}
