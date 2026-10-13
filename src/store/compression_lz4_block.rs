use std::io::{self};
use std::mem;

use lz4_flex::{compress_into, decompress_into};

/// Compresses `uncompressed` into `compressed`.
///
/// Format: the uncompressed size as a little endian u32, followed by the LZ4 block.
/// On error or panic, `compressed` is left empty.
#[inline]
#[expect(clippy::uninit_vec)]
pub fn compress(uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
    // We work on a local `Vec` to keep `compressed` empty while the buffer has uninitialized
    // bytes. If lz4_flex panics, the local `Vec` is dropped without reading them.
    let mut buffer: Vec<u8> = mem::take(compressed);
    buffer.clear();
    let maximum_output_size =
        mem::size_of::<u32>() + lz4_flex::block::get_maximum_output_size(uncompressed.len());
    buffer.reserve(maximum_output_size);
    // SAFETY: the capacity is at least `maximum_output_size`. The bytes are uninitialized:
    // we accept this to avoid zeroing the buffer for every block. `compress_into` only writes
    // to the output slice. On success, the length is set back to the number of bytes written.
    // On error, the buffer is cleared.
    unsafe {
        buffer.set_len(maximum_output_size);
    }
    let result: io::Result<()> = match compress_into(uncompressed, &mut buffer[4..]) {
        Ok(bytes_written) => {
            let num_bytes = uncompressed.len() as u32;
            buffer[0..4].copy_from_slice(&num_bytes.to_le_bytes());
            // SAFETY: `bytes_written + 4 <= maximum_output_size`, and these bytes have been
            // written.
            unsafe {
                buffer.set_len(bytes_written + mem::size_of::<u32>());
            }
            Ok(())
        }
        Err(err) => Err(io::Error::new(io::ErrorKind::InvalidData, err.to_string())),
    };
    if result.is_err() {
        buffer.clear();
    }
    *compressed = buffer;
    result
}

/// Decompresses a block written by [`compress`] into `decompressed`.
///
/// On error or panic, `decompressed` is left empty.
#[inline]
#[expect(clippy::uninit_vec)]
pub fn decompress(compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()> {
    decompressed.clear();
    let uncompressed_size_bytes: &[u8; 4] = compressed
        .get(..4)
        .ok_or(io::ErrorKind::InvalidData)?
        .try_into()
        .unwrap();
    let uncompressed_size = u32::from_le_bytes(*uncompressed_size_bytes) as usize;
    // We work on a local `Vec` to keep `decompressed` empty while the buffer has uninitialized
    // bytes. If lz4_flex panics, the local `Vec` is dropped without reading them.
    let mut buffer: Vec<u8> = mem::take(decompressed);
    buffer.reserve(uncompressed_size);
    // SAFETY: the capacity is at least `uncompressed_size`. The bytes are uninitialized:
    // we accept this to avoid zeroing the buffer for every block. `decompress_into` only reads
    // output bytes it has already written (match copies are checked against the current
    // output position). The buffer is only handed back with this length if all
    // `uncompressed_size` bytes were written. Otherwise it is cleared.
    unsafe {
        buffer.set_len(uncompressed_size);
    }
    let result: io::Result<()> = match decompress_into(&compressed[4..], &mut buffer) {
        Ok(bytes_written) if bytes_written == uncompressed_size => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "doc store block not completely decompressed, data corruption".to_string(),
        )),
        Err(err) => Err(io::Error::new(io::ErrorKind::InvalidData, err.to_string())),
    };
    if result.is_err() {
        buffer.clear();
    }
    *decompressed = buffer;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip_edge_cases() {
        // Simple LCG: incompressible bytes, deterministic.
        let mut state: u64 = 42;
        let pseudo_random: Vec<u8> = (0..20_000)
            .map(|_| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (state >> 56) as u8
            })
            .collect();
        let mut compressed = Vec::new();
        let mut decompressed = Vec::new();
        for input in [Vec::new(), b"a".to_vec(), pseudo_random] {
            compress(&input, &mut compressed).unwrap();
            decompress(&compressed, &mut decompressed).unwrap();
            assert_eq!(decompressed, input);
        }
    }

    #[test]
    fn test_decompress_too_short() {
        let mut decompressed = b"previous".to_vec();
        assert!(decompress(&[1, 0, 0], &mut decompressed).is_err());
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_buffer_reused_after_error() {
        let input = b"lorem ipsum dolor sit amet ".repeat(100);
        let mut compressed = Vec::new();
        compress(&input, &mut compressed).unwrap();
        compressed[..4].copy_from_slice(&(input.len() as u32 + 1).to_le_bytes());
        let mut decompressed: Vec<u8> = Vec::with_capacity(10_000);
        let capacity_before = decompressed.capacity();
        assert!(decompress(&compressed, &mut decompressed).is_err());
        assert!(decompressed.is_empty());
        assert!(decompressed.capacity() >= capacity_before);
    }

    #[test]
    fn test_decompress_corrupted_size_header() {
        let input = b"lorem ipsum dolor sit amet ".repeat(100);
        let mut compressed = Vec::new();
        compress(&input, &mut compressed).unwrap();
        for corrupted_size in [input.len() as u32 - 1, input.len() as u32 + 1] {
            let mut corrupted = compressed.clone();
            corrupted[..4].copy_from_slice(&corrupted_size.to_le_bytes());
            let mut decompressed = b"previous".to_vec();
            let error = decompress(&corrupted, &mut decompressed).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(decompressed.is_empty());
        }
    }
}
