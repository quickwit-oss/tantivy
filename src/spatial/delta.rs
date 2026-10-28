//! Delta compression for block kd-tree leaves.
//!
//! Delta compression with dimension-major bit-packing for block kd-tree leaves. Each leaf contains
//! ≤512 triangles sorted by the split dimension (the dimension with maximum spread chosen during
//! tree construction). We store all 512 values for dimension 0, then all for dimension 1, etc.,
//! enabling tight bit-packing per dimension and better cache locality during decode.
//!
//! The split dimension is already optimal for compression. Since triangles in a leaf are spatially
//! clustered, sorting by the max-spread dimension naturally orders them by proximity in all
//! dimensions. Testing multiple sort orders would be wasted effort.
//!
//! Our encoding uses ~214 units/meter for latitude, ~107 units/meter for longitude (millimeter
//! precision). A quarter-acre lot (32m × 32m) spans ~6,850 units across 512 sorted triangles = avg
//! delta ~13 units = 4 bits. A baseball field (100m × 100m) is ~42 unit deltas = 6 bits. Even
//! Russia-sized polygons (1000 km) average ~418,000 unit deltas = 19 bits. Time will tell if these
//! numbers are anything to go by in practice.
//!
//! Our format for use with leaf-page triangles: First a count of triangles in the page, then the
//! delta encoded doc_ids followed by delta encoding of each series of the triangle dimensions,
//! followed by delta encoding of the flags. Creates eight parallel arrays from which triangles can
//! be reconstructed.
//!
//! Note: Tantivy also has delta encoding in `sstable/src/delta.rs`, but that's for string
//! dictionary compression (prefix sharing + vint deltas). This module uses bit-packing with zigzag
//! encoding, which is optimal for our signed i32 spatial coordinates with small deltas. It uses
//! the same basic algorithm to compress u32 doc_ids.
use std::io::{self, Write};

fn zigzag_encode(x: i32) -> u32 {
    ((x << 1) ^ (x >> 31)) as u32
}

fn zigzag_decode(x: u32) -> i32 {
    ((x >> 1) ^ (0u32.wrapping_sub(x & 1))) as i32
}

/// Trait for reading values by index during compression.
///
/// The `Compressible` trait allows `compress()` to work with two different data sources,
/// `Vec<Triangle>` when indexing and memory mapped `Triangle` when merging. The compress function
/// reads values on-demand via `get()`, computing deltas and bit-packing without intermediate
/// allocations.
pub trait Compressible {
    /// The type of the values being compressed.
    type Value: Copy;
    /// Returns the number of values in this source.
    fn len(&self) -> usize;
    /// Returns the value at the given index.
    fn get(&self, i: usize) -> Self::Value;
}

/// Operations for types that can be delta-encoded and bit-packed into four-byte words.
pub trait DeltaEncoder: Copy {
    /// Computes a zigzag-encoded delta between two values.
    fn compute_delta(current: Self, previous: Self) -> u32;
    /// Converts a value to little-endian bytes for storage.
    fn to_le_bytes(value: Self) -> [u8; 4];
}

impl DeltaEncoder for i32 {
    fn compute_delta(current: Self, previous: Self) -> u32 {
        zigzag_encode(current.wrapping_sub(previous))
    }
    fn to_le_bytes(value: Self) -> [u8; 4] {
        value.to_le_bytes()
    }
}

// Delta encoding for u32 values using wrapping arithmetic and zigzag encoding.
//
// This handles arbitrary u32 document IDs that may be non-sequential or widely spaced. The
// strategy uses wrapping subtraction followed by zigzag encoding:
//
// 1. wrapping_sub computes the difference modulo 2^32, producing a u32 result
// 2. Cast to i32 reinterprets the bit pattern as signed (two's complement)
// 3. zigzag_encode maps signed values to unsigned for efficient bit-packing:
//    - Positive deltas (0, 1, 2...) encode to even numbers (0, 2, 4...)
//    - Negative deltas (-1, -2, -3...) encode to odd numbers (1, 3, 5...)
//
// Example with large jump (doc_id 0 → 4,000,000,000):
//   delta = 4_000_000_000u32.wrapping_sub(0) = 4_000_000_000u32
//   as i32 = -294,967,296 (bit pattern preserved via two's complement)
//   zigzag_encode(-294,967,296) = some u32 value
//
// During decompression, zigzag_decode returns the signed i32 delta, which is cast back to u32 and
// added with wrapping_add. The bit pattern round-trips correctly because wrapping_add and
// wrapping_sub are mathematical inverses modulo 2^32, making this encoding symmetric for the full
// u32 range.
impl DeltaEncoder for u32 {
    fn compute_delta(current: Self, previous: Self) -> u32 {
        zigzag_encode(current.wrapping_sub(previous) as i32)
    }
    fn to_le_bytes(value: Self) -> [u8; 4] {
        value.to_le_bytes()
    }
}

/// Compresses values from a `Compressible` source using delta encoding and bit-packing.
///
/// Computes signed deltas between consecutive values, zigzag encodes them, and determines the
/// minimum bit width needed to represent all deltas. Writes a header (1 byte for bit width +
/// 4 bytes for first value in little-endian), then bit-packs the remaining deltas.
pub fn compress<T, W>(compressible: &T, write: &mut W) -> io::Result<()>
where
    T: Compressible,
    T::Value: DeltaEncoder,
    W: Write,
{
    let mut max_delta = 0u32;
    for i in 1..compressible.len() {
        let delta = T::Value::compute_delta(compressible.get(i), compressible.get(i - 1));
        max_delta = max_delta.max(delta);
    }
    let bits = if max_delta == 0 {
        0u32
    } else {
        32 - max_delta.leading_zeros() as u32
    };
    let mask = if bits == 32 {
        u32::MAX
    } else {
        (1u32 << bits) - 1
    };
    write.write_all(&[bits as u8])?;
    write.write_all(&T::Value::to_le_bytes(compressible.get(0)))?;
    let mut buffer = 0u64;
    let mut buffer_bits = 0u32;
    for i in 1..compressible.len() {
        let delta = T::Value::compute_delta(compressible.get(i), compressible.get(i - 1));
        let value = delta & mask;
        buffer = (buffer << bits) | (value as u64);
        buffer_bits += bits;
        while buffer_bits >= 8 {
            buffer_bits -= 8;
            write.write_all(&[(buffer >> buffer_bits) as u8])?;
        }
    }
    if buffer_bits > 0 {
        write.write_all(&[(buffer << (8 - buffer_bits)) as u8])?;
    }
    Ok(())
}

/// Operations needed to decompress delta-encoded values back to their original form.
pub trait DeltaDecoder: Copy + Sized {
    /// Converts from little-endian bytes to a value.
    fn from_le_bytes(bytes: [u8; 4]) -> Self;
    /// Applies a zigzag-decoded delta to reconstruct the next value.
    fn apply_delta(value: Self, delta: u32) -> Self;
}

impl DeltaDecoder for i32 {
    fn from_le_bytes(bytes: [u8; 4]) -> Self {
        i32::from_le_bytes(bytes)
    }
    fn apply_delta(value: Self, delta: u32) -> Self {
        value.wrapping_add(zigzag_decode(delta))
    }
}

impl DeltaDecoder for u32 {
    fn from_le_bytes(bytes: [u8; 4]) -> Self {
        u32::from_le_bytes(bytes)
    }
    fn apply_delta(value: Self, delta: u32) -> Self {
        value.wrapping_add(zigzag_decode(delta) as u32)
    }
}

/// Decompresses bit-packed delta-encoded values from a byte slice.
///
/// Reads the header to get bit width and first value, then unpacks the bit-packed deltas, applies
/// zigzag decoding, and reconstructs the original values by accumulating deltas.
///
/// Returns the count of bytes read from `data`.
pub fn decompress<T: DeltaDecoder, F>(
    data: &[u8],
    count: usize,
    mut process: F,
) -> io::Result<usize>
where
    F: FnMut(usize, T),
{
    if data.len() < 5 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated header",
        ));
    }
    let bits = data[0] as u32;
    let first = T::from_le_bytes([data[1], data[2], data[3], data[4]]);
    process(0, first);
    let mut offset = 5;
    if bits == 0 {
        // All deltas are zero - all values same as first
        for i in 1..count {
            process(i, first);
        }
        return Ok(offset);
    }
    let mut buffer = 0u64;
    let mut buffer_bits = 0u32;
    let mut prev = first;
    for i in 1..count {
        // Refill buffer with bytes
        while buffer_bits < bits {
            if offset >= data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("expected {} values but only decoded {}", count, i - 1),
                ));
            }
            buffer = (buffer << 8) | (data[offset] as u64);
            offset += 1;
            buffer_bits += 8;
        }
        if buffer_bits >= bits {
            // Extract packed value
            buffer_bits -= bits;
            let encoded = ((buffer >> buffer_bits) & ((1u64 << bits) - 1)) as u32;
            let value = T::apply_delta(prev, encoded);
            process(i, value);
            prev = value;
        } else {
            break;
        }
    }
    Ok(offset)
}

#[cfg(test)]
mod test {
    use super::*;

    pub struct CompressibleI32Vec {
        vec: Vec<i32>,
    }

    impl CompressibleI32Vec {
        fn new(vec: Vec<i32>) -> Self {
            CompressibleI32Vec { vec }
        }
    }

    impl Compressible for CompressibleI32Vec {
        type Value = i32;
        fn len(&self) -> usize {
            return self.vec.len();
        }
        fn get(&self, i: usize) -> i32 {
            return self.vec[i];
        }
    }

    #[test]
    fn test_spatial_delta_compress_decompress() {
        let values = vec![
            100000, 99975, 100050, 99980, 100100, 100025, 99950, 100150, 100075, 99925, 100200,
            100100,
        ];
        let compressible = CompressibleI32Vec::new(values.clone());
        let mut buffer = Vec::new();
        compress(&compressible, &mut buffer).unwrap();
        let mut vec = Vec::new();
        decompress::<i32, _>(&buffer, values.len(), |_, value| vec.push(value)).unwrap();
        assert_eq!(vec, values);
    }

    #[test]
    fn test_spatial_delta_bad_header() {
        let mut vec = Vec::new();
        let result = decompress::<i32, _>(&[1, 2], 1, |_, value| vec.push(value));
        assert!(result.is_err());
    }

    #[test]
    fn test_spatial_delta_insufficient_data() {
        let mut vec = Vec::new();
        let result = decompress::<i32, _>(&[5, 0, 0, 0, 1], 12, |_, value| vec.push(value));
        assert!(result.is_err());
    }

    #[test]
    fn test_spatial_delta_single_item() {
        let mut vec = Vec::new();
        decompress::<i32, _>(&[5, 1, 0, 0, 0], 1, |_, value| vec.push(value)).unwrap();
        assert_eq!(vec[0], 1);
    }

    #[test]
    fn test_spatial_delta_zero_length_delta() {
        let values = vec![1, 1, 1];
        let compressible = CompressibleI32Vec::new(values.clone());
        let mut buffer = Vec::new();
        compress(&compressible, &mut buffer).unwrap();
        let mut vec = Vec::new();
        decompress::<i32, _>(&buffer, values.len(), |_, value| vec.push(value)).unwrap();
        assert_eq!(vec, values);
    }
}
