//! XOR delta compression for f64 polygon coordinates.
//!
//! Lossless compression for floating-point lat/lon coordinates using XOR delta encoding on IEEE
//! 754 bit patterns with variable-length integer encoding. Designed for per-polygon random access
//! in the document store, where each polygon compresses independently without requiring sequential
//! decompression.
//!
//! Spatially local coordinates share most high-order bits. A municipal boundary spanning 1km has
//! consecutive vertices typically within 100-500 meters, meaning their f64 bit patterns share
//! 30-40 bits. XOR reveals these common bits as zeros, which varint encoding then compresses
//! efficiently.
//!
//! The format stores the first coordinate as raw 8 bytes, then XOR deltas between consecutive
//! coordinates encoded as variable-length integers. When compression produces larger output than
//! the raw input (random data, compression-hostile patterns), the function automatically falls
//! back to storing coordinates as uncompressed 8-byte values.
//!
//! Unlike delta.rs which uses arithmetic deltas for i32 spatial coordinates in the block kd-tree,
//! this module operates on f64 bit patterns directly to preserve exact floating-point values for
//! returning to users.
use std::io::{Cursor, Read};

use common::VInt;

/// Compresses f64 coordinates using XOR delta encoding with automatic raw fallback.
///
/// Stores the first coordinate as raw bits, then computes XOR between consecutive coordinate bit
/// patterns and encodes as variable-length integers. If the compressed output would be larger than
/// raw storage (8 bytes per coordinate), automatically falls back to raw encoding.
///
/// Returns a byte vector that can be decompressed with `decompress_f64()` to recover exact
/// original values.
pub fn compress_f64(values: &[f64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }
    let mut output = Vec::new();
    let mut previous = values[0].to_bits();
    output.extend_from_slice(&previous.to_le_bytes());
    for &value in &values[1..] {
        let bits = value.to_bits();
        let xor = bits ^ previous;
        VInt(xor).serialize_into_vec(&mut output);
        previous = bits
    }
    if output.len() >= values.len() * 8 {
        let mut output = Vec::with_capacity(values.len() * 8);
        for &value in values {
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        return output;
    }
    output
}

/// Decompresses f64 coordinates from XOR delta or raw encoding.
///
/// Detects compression format by byte length - if `bytes.len() == count * 8`, data is raw and
/// copied directly. Otherwise, reads first coordinate from 8 bytes, then XOR deltas as varints,
/// reconstructing the original sequence.
///
/// Returns exact f64 values that were passed to `compress_f64()`.
pub fn decompress_f64(bytes: &[u8], count: usize) -> Vec<f64> {
    let mut values = Vec::with_capacity(count);
    if bytes.len() == count * 8 {
        for i in 0..count {
            let bits = u64::from_le_bytes(bytes[i * 8..(i + 1) * 8].try_into().unwrap());
            values.push(f64::from_bits(bits));
        }
        return values;
    }
    let mut cursor = Cursor::new(bytes);

    // Read first value (raw 8 bytes)
    let mut first_bytes = [0u8; 8];
    cursor.read_exact(&mut first_bytes).unwrap();
    let mut previous = u64::from_le_bytes(first_bytes);
    values.push(f64::from_bits(previous));

    // Read remaining values as VInt XORs
    while values.len() < count {
        let xor = VInt::deserialize_u64(&mut cursor).unwrap();
        let bits = previous ^ xor;
        values.push(f64::from_bits(bits));
        previous = bits;
    }

    values
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_compress_spatial_locality() {
        // Small town polygon - longitude only.
        let longitudes = vec![
            40.7580, 40.7581, 40.7582, 40.7583, 40.7584, 40.7585, 40.7586, 40.7587,
        ];
        let bytes = compress_f64(&longitudes);
        // Should compress well - XOR deltas will be small
        assert_eq!(bytes.len(), 46);
        // Should decompress to exact original values
        let decompressed = decompress_f64(&bytes, longitudes.len());
        assert_eq!(longitudes, decompressed);
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
