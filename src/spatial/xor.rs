//! XOR delta compression for f64 polygon coordinates.
//!
//! Lossless compression for floating-point coordinates using XOR delta encoding on IEEE
//! 754 bit patterns with bit packing. Designed for per-geometry random access in the document
//! store, where each geometry compresses independently without requiring sequential decompression.
//! Additionally used to store per-geometry edge and point storage in the index referenced by
//! quadtree cells.
//!
//! Spatially local coordinates share most high-order bits. A municipal boundary spanning 1km has
//! consecutive vertices typically within 100-500 meters, meaning their f64 bit patterns share
//! 30-40 bits. XOR reveals these common bits as zeros, which bit-pack efficiently with minimal
//! bit-width.
//!
//! Unlike delta.rs which uses arithmetic deltas for integers, this module operates on f64 bit
//! patterns directly to preserve exact floating-point values.

/// Compresses f64 coordinates using XOR delta encoding with automatic raw fallback.
///
/// First pass computes XOR deltas between consecutive coordinates to find the maximum, which
/// determines the bit width needed. Second pass writes a header (1 byte bit width + 8 bytes first
/// coordinate) followed by bit-packed XOR deltas. If the compressed output would be larger than
/// raw storage (8 bytes per coordinate) or if the delta is too large to allow for a 7 bit
/// remainder, it falls back to raw encoding.
///
/// Returns a byte vector that can be decompressed with `decompress_f64()` to recover exact
/// original values.
pub fn compress_f64(values: &[f64]) -> Vec<u8> {
    // Subsequent code assumes a first element.
    if values.is_empty() {
        return Vec::new();
    }

    // Compute max XOR delta
    let mut max_xor: u64 = 0;
    let mut previous = values[0].to_bits();
    for &value in &values[1..] {
        let bits = value.to_bits();
        let xor = bits ^ previous;
        max_xor = max_xor.max(xor);
        previous = bits;
    }

    // Bits needed for delta.
    let bits = if max_xor == 0 {
        0
    } else {
        64 - max_xor.leading_zeros() as u8
    };

    // Determine if compression will indeed compress.
    let count = values.len();
    let compressed_size = 9 + ((count - 1) * bits as usize).div_ceil(8);
    let raw_size = count * 8;

    // Return the uncompressed array if we do not have enough room to buffer seven bits. Return the
    // uncompressed array if compression buys us nothing.
    if bits > 57 || compressed_size >= raw_size {
        let mut output = Vec::with_capacity(raw_size);
        for &value in values {
            output.extend_from_slice(&value.to_le_bytes());
        }
        return output;
    }

    // Header: bit width (1 byte) + first value (8 bytes).
    let mut output = Vec::with_capacity(compressed_size);
    output.push(bits);
    output.extend_from_slice(&values[0].to_le_bytes());

    // Bit-packing of the XOR deltas.
    let mut buffer: u64 = 0;
    let mut buffer_bits: u32 = 0;
    let mut previous = values[0].to_bits();

    for &value in &values[1..] {
        let current = value.to_bits();
        let xor = current ^ previous;

        // Shift buffer left, add new bits.
        buffer = (buffer << bits) | xor;
        buffer_bits += bits as u32;

        // Flush complete bytes.
        while buffer_bits >= 8 {
            buffer_bits -= 8;
            output.push((buffer >> buffer_bits) as u8);
        }

        previous = current;
    }

    // Flush remaining bits left-aligned.
    if buffer_bits > 0 {
        output.push((buffer << (8 - buffer_bits)) as u8);
    }

    output
}

/// Decompresses f64 coordinates from XOR delta or raw encoding.
///
/// If the byte length is equal to the uncompressed size, then the data is raw and copied directly.
/// Otherwise, reads the delta size from the first byte, the first coordinate from next 8 bytes,
/// then unpacks bit-packed XOR deltas reconstructing the original sequence. A value in the
/// sequence is derived by applying the XOR delta to the value preceding it.
///
/// Returns exact f64 values that were passed to `compress_f64()`.
pub fn decompress_f64(bytes: &[u8], count: usize) -> Vec<f64> {
    // Check for raw encoding, read raw values.
    let mut values = Vec::with_capacity(count);
    if bytes.len() == count * 8 {
        for i in 0..count {
            values.push(f64::from_le_bytes(
                bytes[i * 8..(i + 1) * 8].try_into().unwrap(),
            ));
        }
        return values;
    }

    // Read header as unsigned integer for use with XOR.
    let bits = bytes[0] as u32;
    let first = u64::from_le_bytes(bytes[1..9].try_into().unwrap());

    // Convert unisgned integer bits to float.
    values.push(f64::from_bits(first));

    // Unpack bit-packed XOR deltas
    let mut buffer: u64 = 0;
    let mut buffer_bits: u32 = 0;
    let mut offset = 9;
    let mut previous = first;
    let mask = (1u64 << bits) - 1;

    for _ in 1..count {
        // Refill buffer.
        while buffer_bits < bits && offset < bytes.len() {
            buffer = (buffer << 8) | bytes[offset] as u64;
            buffer_bits += 8;
            offset += 1;
        }

        // Extract delta.
        buffer_bits -= bits;
        let xor = (buffer >> buffer_bits) & mask;

        let current = previous ^ xor;
        values.push(f64::from_bits(current));
        previous = current;
    }

    values
}

#[cfg(test)]
mod test {
    use super::*;

    // Compression ratio of 1.43 on small municipal polygons (12 vertices). Per FCBench (VLDB
    // 2024), this exceeds the median lossless floating-point compression ratio of 1.16 and matches
    // Gorilla/Chimp XOR-delta methods. Block-based methods (bitshuffle+zstd) achieve 1.4-1.5x on
    // comparable low-entropy spatial data, but require sequential decompression. Nothing
    // scientific about this observation, just a sanity check that we are, at first glance, on the
    // right track.
    #[test]
    fn test_compress_spatial_locality() {
        // Latitudes from a small town polygon. Hosmer, South Dakota.
        let latitudes = vec![
            45.577697, 45.571457, 45.571461, 45.571584, 45.571615, 45.57168, 45.57883, 45.586076,
            45.585926, 45.585953, 45.57873, 45.577697,
        ];
        let bytes = compress_f64(&latitudes);
        // Uncompressed is 96.
        assert_eq!(bytes.len(), 67);
        // Should decompress to exact original values
        let decompressed = decompress_f64(&bytes, latitudes.len());
        assert_eq!(latitudes, decompressed);
    }
    #[test]
    fn test_fallback_to_raw() {
        // Random, widely scattered values - poor compression
        let values = vec![
            12345.6789,
            -98765.4321,
            0.00001,
            999999.999,
            -0.0,
            std::f64::consts::PI,
            std::f64::consts::E,
            42.0,
        ];
        let bytes = compress_f64(&values);
        // Should fall back to raw storage
        assert_eq!(bytes.len(), values.len() * 8);
        // Should still decompress correctly
        let decompressed = decompress_f64(&bytes, values.len());
        assert_eq!(values, decompressed);
    }
}
