//! Java-compatible binary encoding/decoding for DDSketch.
//!
//! This module implements the binary format used by the Java
//! `com.datadoghq.sketch.ddsketch.DDSketchWithExactSummaryStatistics` class
//! from the DataDog/sketches-java library. It enables cross-language
//! serialization so that sketches produced in Rust can be deserialized
//! and merged by Java consumers.

use std::convert::TryInto;
use std::fmt;

use crate::config::Config;
use crate::ddsketch::DDSketch;
use crate::store::Store;

// ---------------------------------------------------------------------------
// Flag byte layout: (subflag << 2) | type_ordinal
// ---------------------------------------------------------------------------

const FLAG_TYPE_SKETCH_FEATURES: u8 = 0b00;
const FLAG_TYPE_POSITIVE_STORE: u8 = 0b01;
const FLAG_TYPE_INDEX_MAPPING: u8 = 0b10;
const FLAG_TYPE_NEGATIVE_STORE: u8 = 0b11;

const FLAG_INDEX_MAPPING_LOG: u8 = (0 << 2) | FLAG_TYPE_INDEX_MAPPING; // 0x02
const FLAG_ZERO_COUNT: u8 = (1 << 2) | FLAG_TYPE_SKETCH_FEATURES; // 0x04
const FLAG_COUNT: u8 = (0x28 << 2) | FLAG_TYPE_SKETCH_FEATURES; // 0xA0
const FLAG_SUM: u8 = (0x21 << 2) | FLAG_TYPE_SKETCH_FEATURES; // 0x84
const FLAG_MIN: u8 = (0x22 << 2) | FLAG_TYPE_SKETCH_FEATURES; // 0x88
const FLAG_MAX: u8 = (0x23 << 2) | FLAG_TYPE_SKETCH_FEATURES; // 0x8C

// BinEncodingMode subflags
const BIN_MODE_INDEX_DELTAS_AND_COUNTS: u8 = 1;
const BIN_MODE_INDEX_DELTAS: u8 = 2;
const BIN_MODE_CONTIGUOUS_COUNTS: u8 = 3;

const VAR_DOUBLE_ROTATE_DISTANCE: u32 = 6;
const MAX_VAR_LEN_64: usize = 9;

const DEFAULT_MAX_BINS: u32 = 2048;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum DecodeError {
    UnexpectedEof,
    InvalidFlag(u8),
    InvalidData(String),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DecodeError::UnexpectedEof => write!(f, "unexpected end of input"),
            DecodeError::InvalidFlag(b) => write!(f, "invalid flag byte: 0x{:02X}", b),
            DecodeError::InvalidData(msg) => write!(f, "invalid data: {}", msg),
        }
    }
}

impl std::error::Error for DecodeError {}

// ---------------------------------------------------------------------------
// VarEncoding — bit-exact port of Java VarEncodingHelper
// ---------------------------------------------------------------------------

fn encode_unsigned_var_long(out: &mut Vec<u8>, mut value: u64) {
    let length = ((63 - value.leading_zeros() as i32) / 7).max(0).min(8);
    for _ in 0..length {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn decode_unsigned_var_long(input: &mut &[u8]) -> Result<u64, DecodeError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let next = read_byte(input)?;
        if next < 0x80 || shift == 56 {
            return Ok(value | ((next as u64) << shift));
        }
        value |= ((next as u64) & 0x7F) << shift;
        shift += 7;
    }
}

fn encode_signed_var_long(out: &mut Vec<u8>, value: i64) {
    let encoded = ((value >> 63) ^ (value << 1)) as u64;
    encode_unsigned_var_long(out, encoded);
}

fn decode_signed_var_long(input: &mut &[u8]) -> Result<i64, DecodeError> {
    let encoded = decode_unsigned_var_long(input)?;
    Ok(((encoded >> 1) as i64) ^ -((encoded & 1) as i64))
}

fn double_to_var_bits(value: f64) -> u64 {
    let bits = f64::to_bits(value + 1.0).wrapping_sub(f64::to_bits(1.0_f64));
    bits.rotate_left(VAR_DOUBLE_ROTATE_DISTANCE)
}

fn var_bits_to_double(bits: u64) -> f64 {
    f64::from_bits(
        bits.rotate_right(VAR_DOUBLE_ROTATE_DISTANCE)
            .wrapping_add(f64::to_bits(1.0_f64)),
    ) - 1.0
}

fn encode_var_double(out: &mut Vec<u8>, value: f64) {
    let mut bits = double_to_var_bits(value);
    for _ in 0..MAX_VAR_LEN_64 - 1 {
        let next = (bits >> 57) as u8;
        bits <<= 7;
        if bits == 0 {
            out.push(next);
            return;
        }
        out.push(next | 0x80);
    }
    out.push((bits >> 56) as u8);
}

fn decode_var_double(input: &mut &[u8]) -> Result<f64, DecodeError> {
    let mut bits: u64 = 0;
    let mut shift: i32 = 57; // 8*8 - 7
    loop {
        let next = read_byte(input)?;
        if shift == 1 {
            bits |= next as u64;
            break;
        }
        if next < 0x80 {
            bits |= (next as u64) << shift;
            break;
        }
        bits |= ((next as u64) & 0x7F) << shift;
        shift -= 7;
    }
    Ok(var_bits_to_double(bits))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn read_byte(input: &mut &[u8]) -> Result<u8, DecodeError> {
    if input.is_empty() {
        return Err(DecodeError::UnexpectedEof);
    }
    let b = input[0];
    *input = &input[1..];
    Ok(b)
}

fn write_f64_le(out: &mut Vec<u8>, value: f64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn read_f64_le(input: &mut &[u8]) -> Result<f64, DecodeError> {
    if input.len() < 8 {
        return Err(DecodeError::UnexpectedEof);
    }
    let bytes: [u8; 8] = input[..8].try_into().unwrap();
    *input = &input[8..];
    Ok(f64::from_le_bytes(bytes))
}

// ---------------------------------------------------------------------------
// Store encoding/decoding
// ---------------------------------------------------------------------------

/// Iterate the non-zero bins in the store as (absolute_index, count) pairs.
fn non_zero_bins(store: &Store) -> Vec<(i32, u64)> {
    if store.count == 0 {
        return Vec::new();
    }
    let start = (store.min_key - store.offset) as usize;
    let end = (store.max_key - store.offset + 1) as usize;
    let end = end.min(store.bins.len());
    let mut result = Vec::new();
    for i in start..end {
        let count = store.bins[i];
        if count > 0 {
            result.push((i as i32 + store.offset, count));
        }
    }
    result
}

fn encode_store(out: &mut Vec<u8>, store: &Store, flag_type: u8) {
    let bins = non_zero_bins(store);
    if bins.is_empty() {
        return;
    }

    // INDEX_DELTAS_AND_COUNTS mode
    out.push((BIN_MODE_INDEX_DELTAS_AND_COUNTS << 2) | flag_type);
    encode_unsigned_var_long(out, bins.len() as u64);

    let mut prev_index: i64 = 0;
    for &(index, count) in &bins {
        encode_signed_var_long(out, (index as i64) - prev_index);
        encode_var_double(out, count as f64);
        prev_index = index as i64;
    }
}

fn decode_store(input: &mut &[u8], subflag: u8, bin_limit: usize) -> Result<Store, DecodeError> {
    let mode = subflag;
    let num_bins = decode_unsigned_var_long(input)? as usize;
    let mut store = Store::new(bin_limit);

    match mode {
        BIN_MODE_INDEX_DELTAS_AND_COUNTS => {
            let mut index: i64 = 0;
            for _ in 0..num_bins {
                let delta = decode_signed_var_long(input)?;
                let count = decode_var_double(input)?;
                index += delta;
                store.add_count(index as i32, count as u64);
            }
        }
        BIN_MODE_INDEX_DELTAS => {
            let mut index: i64 = 0;
            for _ in 0..num_bins {
                let delta = decode_signed_var_long(input)?;
                index += delta;
                store.add_count(index as i32, 1);
            }
        }
        BIN_MODE_CONTIGUOUS_COUNTS => {
            let start_index = decode_signed_var_long(input)?;
            let index_delta = decode_signed_var_long(input)?;
            let mut index = start_index;
            for _ in 0..num_bins {
                let count = decode_var_double(input)?;
                store.add_count(index as i32, count as u64);
                index += index_delta;
            }
        }
        other => {
            return Err(DecodeError::InvalidData(format!(
                "unknown bin encoding mode subflag: {}",
                other
            )));
        }
    }

    Ok(store)
}

// ---------------------------------------------------------------------------
// Top-level encode / decode
// ---------------------------------------------------------------------------

/// Encode a DDSketch into the Java-compatible binary format.
///
/// The output follows the encoding order of
/// `DDSketchWithExactSummaryStatistics.encode()` then `DDSketch.encode()`:
///
/// 1. Summary statistics: COUNT, MIN, MAX (if count > 0)
/// 2. SUM (if sum != 0)
/// 3. Index mapping (LOG layout): gamma, indexOffset
/// 4. Zero count (if > 0)
/// 5. Positive store bins
/// 6. Negative store bins
pub fn encode_to_java_bytes(sketch: &DDSketch) -> Vec<u8> {
    let mut out = Vec::new();

    let count = sketch.count() as f64;

    // --- Summary statistics (DDSketchWithExactSummaryStatistics.encode) ---
    if count != 0.0 {
        out.push(FLAG_COUNT);
        encode_var_double(&mut out, count);
        out.push(FLAG_MIN);
        write_f64_le(&mut out, sketch.min);
        out.push(FLAG_MAX);
        write_f64_le(&mut out, sketch.max);
    }
    if sketch.sum != 0.0 {
        out.push(FLAG_SUM);
        write_f64_le(&mut out, sketch.sum);
    }

    // --- DDSketch.encode (index mapping + zero count + stores) ---

    // Index mapping (LOG layout, indexOffset = 0.0)
    out.push(FLAG_INDEX_MAPPING_LOG);
    write_f64_le(&mut out, sketch.config.gamma);
    write_f64_le(&mut out, 0.0_f64);

    // Zero count
    if sketch.zero_count != 0 {
        out.push(FLAG_ZERO_COUNT);
        encode_var_double(&mut out, sketch.zero_count as f64);
    }

    // Positive store
    encode_store(&mut out, &sketch.store, FLAG_TYPE_POSITIVE_STORE);

    // Negative store
    encode_store(&mut out, &sketch.negative_store, FLAG_TYPE_NEGATIVE_STORE);

    out
}

/// Decode a DDSketch from the Java-compatible binary format.
///
/// Accepts bytes with or without a `0x02` version prefix.
pub fn decode_from_java_bytes(bytes: &[u8]) -> Result<DDSketch, DecodeError> {
    if bytes.is_empty() {
        return Err(DecodeError::UnexpectedEof);
    }

    let mut input = bytes;

    // Skip optional version prefix (0x02 followed by a valid flag byte)
    if input.len() >= 2 && input[0] == 0x02 {
        let second = input[1];
        if is_valid_flag_byte(second) {
            input = &input[1..];
        }
    }

    let mut gamma: Option<f64> = None;
    let mut zero_count: f64 = 0.0;
    let mut sum: f64 = 0.0;
    let mut min: f64 = f64::INFINITY;
    let mut max: f64 = f64::NEG_INFINITY;
    let mut positive_store: Option<Store> = None;
    let mut negative_store: Option<Store> = None;

    while !input.is_empty() {
        let flag = read_byte(&mut input)?;
        let flag_type = flag & 0x03;
        let subflag = flag >> 2;

        match flag_type {
            FLAG_TYPE_INDEX_MAPPING => {
                gamma = Some(read_f64_le(&mut input)?);
                let _index_offset = read_f64_le(&mut input)?;
            }
            FLAG_TYPE_SKETCH_FEATURES => {
                if flag == FLAG_ZERO_COUNT {
                    zero_count += decode_var_double(&mut input)?;
                } else if flag == FLAG_COUNT {
                    let _count = decode_var_double(&mut input)?;
                } else if flag == FLAG_SUM {
                    sum = read_f64_le(&mut input)?;
                } else if flag == FLAG_MIN {
                    min = read_f64_le(&mut input)?;
                } else if flag == FLAG_MAX {
                    max = read_f64_le(&mut input)?;
                } else {
                    return Err(DecodeError::InvalidFlag(flag));
                }
            }
            FLAG_TYPE_POSITIVE_STORE => {
                positive_store = Some(decode_store(
                    &mut input,
                    subflag,
                    DEFAULT_MAX_BINS as usize,
                )?);
            }
            FLAG_TYPE_NEGATIVE_STORE => {
                negative_store = Some(decode_store(
                    &mut input,
                    subflag,
                    DEFAULT_MAX_BINS as usize,
                )?);
            }
            _ => {
                return Err(DecodeError::InvalidFlag(flag));
            }
        }
    }

    let g = gamma.unwrap_or_else(|| Config::defaults().gamma);
    let config = Config::from_gamma(g);
    let pos = positive_store.unwrap_or_else(|| Store::new(config.max_num_bins as usize));
    let neg = negative_store.unwrap_or_else(|| Store::new(config.max_num_bins as usize));

    Ok(DDSketch {
        config,
        store: pos,
        negative_store: neg,
        min,
        max,
        sum,
        zero_count: zero_count as u64,
    })
}

/// Check whether a byte is a valid flag byte for the DDSketch binary format.
/// Used to detect the optional version prefix.
fn is_valid_flag_byte(b: u8) -> bool {
    matches!(
        b,
        FLAG_ZERO_COUNT | FLAG_COUNT | FLAG_SUM | FLAG_MIN | FLAG_MAX | FLAG_INDEX_MAPPING_LOG
    ) || {
        let flag_type = b & 0x03;
        let subflag = b >> 2;
        (flag_type == FLAG_TYPE_POSITIVE_STORE || flag_type == FLAG_TYPE_NEGATIVE_STORE)
            && (1..=3).contains(&subflag)
    } || {
        // INDEX_MAPPING with other layouts (LOG_LINEAR=1..LOG_QUARTIC=4)
        let flag_type = b & 0x03;
        let subflag = b >> 2;
        flag_type == FLAG_TYPE_INDEX_MAPPING && subflag <= 4
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, DDSketch};

    // --- VarEncoding unit tests ---

    #[test]
    fn test_unsigned_var_long_zero() {
        let mut buf = Vec::new();
        encode_unsigned_var_long(&mut buf, 0);
        assert_eq!(buf, vec![0x00]);

        let mut input = buf.as_slice();
        assert_eq!(decode_unsigned_var_long(&mut input).unwrap(), 0);
        assert!(input.is_empty());
    }

    #[test]
    fn test_unsigned_var_long_small() {
        let mut buf = Vec::new();
        encode_unsigned_var_long(&mut buf, 1);
        assert_eq!(buf, vec![0x01]);

        let mut input = buf.as_slice();
        assert_eq!(decode_unsigned_var_long(&mut input).unwrap(), 1);
    }

    #[test]
    fn test_unsigned_var_long_128() {
        let mut buf = Vec::new();
        encode_unsigned_var_long(&mut buf, 128);
        assert_eq!(buf, vec![0x80, 0x01]);

        let mut input = buf.as_slice();
        assert_eq!(decode_unsigned_var_long(&mut input).unwrap(), 128);
    }

    #[test]
    fn test_unsigned_var_long_roundtrip() {
        for &v in &[0u64, 1, 127, 128, 255, 256, 16383, 16384, u64::MAX] {
            let mut buf = Vec::new();
            encode_unsigned_var_long(&mut buf, v);
            let mut input = buf.as_slice();
            let decoded = decode_unsigned_var_long(&mut input).unwrap();
            assert_eq!(decoded, v, "roundtrip failed for {}", v);
            assert!(input.is_empty());
        }
    }

    #[test]
    fn test_signed_var_long_roundtrip() {
        for &v in &[0i64, 1, -1, 63, -64, 64, -65, i64::MAX, i64::MIN] {
            let mut buf = Vec::new();
            encode_signed_var_long(&mut buf, v);
            let mut input = buf.as_slice();
            let decoded = decode_signed_var_long(&mut input).unwrap();
            assert_eq!(decoded, v, "roundtrip failed for {}", v);
            assert!(input.is_empty());
        }
    }

    #[test]
    fn test_var_double_roundtrip() {
        for &v in &[
            0.0, 1.0, 2.0, 5.0, 15.0, 42.0, 100.0, 1e-9, 1e15, 0.5, 3.14159,
        ] {
            let mut buf = Vec::new();
            encode_var_double(&mut buf, v);
            let mut input = buf.as_slice();
            let decoded = decode_var_double(&mut input).unwrap();
            assert!(
                (decoded - v).abs() < 1e-15 || decoded == v,
                "roundtrip failed for {}: got {}",
                v,
                decoded
            );
            assert!(input.is_empty());
        }
    }

    #[test]
    fn test_var_double_small_integers() {
        // Small non-negative integers should encode compactly
        let mut buf = Vec::new();
        encode_var_double(&mut buf, 1.0);
        assert_eq!(buf.len(), 1, "VarDouble(1.0) should be 1 byte");

        buf.clear();
        encode_var_double(&mut buf, 5.0);
        assert_eq!(buf.len(), 1, "VarDouble(5.0) should be 1 byte");
    }

    // --- DDSketch encode/decode roundtrip tests ---

    #[test]
    fn test_encode_empty_sketch() {
        let sketch = DDSketch::new(Config::defaults());
        let bytes = sketch.to_java_bytes();
        // Empty sketch: no summary stats, just index mapping
        assert!(!bytes.is_empty());

        let decoded = DDSketch::from_java_bytes(&bytes).unwrap();
        assert_eq!(decoded.count(), 0);
        assert_eq!(decoded.min(), None);
        assert_eq!(decoded.max(), None);
        assert_eq!(decoded.sum(), None);
    }

    #[test]
    fn test_encode_simple_sketch() {
        let mut sketch = DDSketch::new(Config::defaults());
        for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
            sketch.add(v);
        }

        let bytes = sketch.to_java_bytes();
        let decoded = DDSketch::from_java_bytes(&bytes).unwrap();

        assert_eq!(decoded.count(), 5);
        assert_eq!(decoded.min(), Some(1.0));
        assert_eq!(decoded.max(), Some(5.0));
        assert_eq!(decoded.sum(), Some(15.0));

        for q in [0.5, 0.9, 0.95, 0.99] {
            let orig = sketch.quantile(q).unwrap().unwrap();
            let dec = decoded.quantile(q).unwrap().unwrap();
            assert!(
                (orig - dec).abs() / orig.abs().max(1e-15) < 1e-12,
                "quantile({}) mismatch: {} vs {}",
                q,
                orig,
                dec
            );
        }
    }

    #[test]
    fn test_encode_single_value() {
        let mut sketch = DDSketch::new(Config::defaults());
        sketch.add(42.0);

        let bytes = sketch.to_java_bytes();
        let decoded = DDSketch::from_java_bytes(&bytes).unwrap();

        assert_eq!(decoded.count(), 1);
        assert_eq!(decoded.min(), Some(42.0));
        assert_eq!(decoded.max(), Some(42.0));
        assert_eq!(decoded.sum(), Some(42.0));
    }

    #[test]
    fn test_encode_negative_values() {
        let mut sketch = DDSketch::new(Config::defaults());
        for v in [-3.0, -1.0, 2.0, 5.0] {
            sketch.add(v);
        }

        let bytes = sketch.to_java_bytes();
        let decoded = DDSketch::from_java_bytes(&bytes).unwrap();

        assert_eq!(decoded.count(), 4);
        assert_eq!(decoded.min(), Some(-3.0));
        assert_eq!(decoded.max(), Some(5.0));
        assert_eq!(decoded.sum(), Some(3.0));

        for q in [0.0, 0.25, 0.5, 0.75, 1.0] {
            let orig = sketch.quantile(q).unwrap().unwrap();
            let dec = decoded.quantile(q).unwrap().unwrap();
            assert!(
                (orig - dec).abs() / orig.abs().max(1e-15) < 1e-12,
                "quantile({}) mismatch: {} vs {}",
                q,
                orig,
                dec
            );
        }
    }

    #[test]
    fn test_encode_with_zero_value() {
        let mut sketch = DDSketch::new(Config::defaults());
        for v in [0.0, 1.0, 2.0] {
            sketch.add(v);
        }

        let bytes = sketch.to_java_bytes();
        let decoded = DDSketch::from_java_bytes(&bytes).unwrap();

        assert_eq!(decoded.count(), 3);
        assert_eq!(decoded.min(), Some(0.0));
        assert_eq!(decoded.max(), Some(2.0));
        assert_eq!(decoded.sum(), Some(3.0));
        assert_eq!(decoded.zero_count, 1);
    }

    #[test]
    fn test_encode_large_range() {
        let mut sketch = DDSketch::new(Config::defaults());
        sketch.add(0.001);
        sketch.add(1_000_000.0);

        let bytes = sketch.to_java_bytes();
        let decoded = DDSketch::from_java_bytes(&bytes).unwrap();

        assert_eq!(decoded.count(), 2);
        assert_eq!(decoded.min(), Some(0.001));
        assert_eq!(decoded.max(), Some(1_000_000.0));
    }

    #[test]
    fn test_encode_with_version_prefix() {
        let mut sketch = DDSketch::new(Config::defaults());
        for v in [1.0, 2.0, 3.0] {
            sketch.add(v);
        }

        let bytes = sketch.to_java_bytes();

        // Simulate Java's toByteArrayV2: prepend 0x02
        let mut v2_bytes = vec![0x02];
        v2_bytes.extend_from_slice(&bytes);

        let decoded = DDSketch::from_java_bytes(&v2_bytes).unwrap();
        assert_eq!(decoded.count(), 3);
        assert_eq!(decoded.min(), Some(1.0));
        assert_eq!(decoded.max(), Some(3.0));
    }

    #[test]
    fn test_byte_level_encoding() {
        let mut sketch = DDSketch::new(Config::defaults());
        sketch.add(1.0);

        let bytes = sketch.to_java_bytes();

        // First byte should be FLAG_COUNT (0xA0) since count > 0
        assert_eq!(bytes[0], FLAG_COUNT, "first byte should be COUNT flag");

        // After count + min + max + sum blocks, we should see FLAG_INDEX_MAPPING_LOG (0x02)
        let has_mapping = bytes.contains(&FLAG_INDEX_MAPPING_LOG);
        assert!(has_mapping, "should contain index mapping flag");
    }

    fn hex_to_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    // Golden bytes generated by Java's DDSketchWithExactSummaryStatistics.encode()
    // using LogarithmicMapping(0.01) + CollapsingLowestDenseStore(2048)
    const GOLDEN_SIMPLE: &str = "a00588000000000000f03f8c0000000000001440840000000000002e4002fd4a815abf52f03f000000000000000005050002440228021e021602";
    const GOLDEN_SINGLE: &str = "a0028800000000000045408c000000000000454084000000000000454002fd4a815abf52f03f00000000000000000501f40202";
    const GOLDEN_NEGATIVE: &str = "a084408800000000000008c08c000000000000144084000000000000084002fd4a815abf52f03f0000000000000000050244025c02070200026c02";
    const GOLDEN_ZERO: &str = "a0048800000000000000008c000000000000004084000000000000084002fd4a815abf52f03f00000000000000000402050200024402";
    const GOLDEN_EMPTY: &str = "02fd4a815abf52f03f0000000000000000";
    const GOLDEN_MANY: &str = "a08d1488000000000000f03f8c0000000000005940840000000000bab34002fd4a815abf52f03f000000000000000005550002440228021e021602120210020c020c020c0208020a020802060208020602060206020602040206020402040204020402040204020402040204020202040202020402020204020202020204020202020202020402020202020202020202020202020202020202020202020202020202020203020202020202020302020202020302020202020302020203020202030202020302030202020302030203020202030203020302030202";

    #[test]
    fn test_cross_language_simple() {
        let mut sketch = DDSketch::new(Config::defaults());
        for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
            sketch.add(v);
        }
        let bytes = sketch.to_java_bytes();
        let expected = hex_to_bytes(GOLDEN_SIMPLE);
        assert_eq!(
            bytes,
            expected,
            "Rust encoding doesn't match Java golden bytes for SIMPLE.\nRust:   {}\nJava:   {}",
            bytes
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>(),
            GOLDEN_SIMPLE
        );
    }

    #[test]
    fn test_cross_language_single() {
        let mut sketch = DDSketch::new(Config::defaults());
        sketch.add(42.0);
        let bytes = sketch.to_java_bytes();
        let expected = hex_to_bytes(GOLDEN_SINGLE);
        assert_eq!(
            bytes,
            expected,
            "Rust encoding doesn't match Java golden bytes for SINGLE.\nRust:   {}\nJava:   {}",
            bytes
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>(),
            GOLDEN_SINGLE
        );
    }

    #[test]
    fn test_cross_language_negative() {
        let mut sketch = DDSketch::new(Config::defaults());
        for v in [-3.0, -1.0, 2.0, 5.0] {
            sketch.add(v);
        }
        let bytes = sketch.to_java_bytes();
        let expected = hex_to_bytes(GOLDEN_NEGATIVE);
        assert_eq!(
            bytes,
            expected,
            "Rust encoding doesn't match Java golden bytes for NEGATIVE.\nRust:   {}\nJava:   {}",
            bytes
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>(),
            GOLDEN_NEGATIVE
        );
    }

    #[test]
    fn test_cross_language_zero() {
        let mut sketch = DDSketch::new(Config::defaults());
        for v in [0.0, 1.0, 2.0] {
            sketch.add(v);
        }
        let bytes = sketch.to_java_bytes();
        let expected = hex_to_bytes(GOLDEN_ZERO);
        assert_eq!(
            bytes,
            expected,
            "Rust encoding doesn't match Java golden bytes for ZERO.\nRust:   {}\nJava:   {}",
            bytes
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>(),
            GOLDEN_ZERO
        );
    }

    #[test]
    fn test_cross_language_empty() {
        let sketch = DDSketch::new(Config::defaults());
        let bytes = sketch.to_java_bytes();
        let expected = hex_to_bytes(GOLDEN_EMPTY);
        assert_eq!(
            bytes,
            expected,
            "Rust encoding doesn't match Java golden bytes for EMPTY.\nRust:   {}\nJava:   {}",
            bytes
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>(),
            GOLDEN_EMPTY
        );
    }

    #[test]
    fn test_cross_language_many() {
        let mut sketch = DDSketch::new(Config::defaults());
        for i in 1..=100 {
            sketch.add(i as f64);
        }
        let bytes = sketch.to_java_bytes();
        let expected = hex_to_bytes(GOLDEN_MANY);
        assert_eq!(
            bytes,
            expected,
            "Rust encoding doesn't match Java golden bytes for MANY.\nRust:   {}\nJava:   {}",
            bytes
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>(),
            GOLDEN_MANY
        );
    }

    #[test]
    fn test_decode_java_golden_bytes() {
        // Verify we can decode all Java golden bytes
        for (name, hex) in [
            ("SIMPLE", GOLDEN_SIMPLE),
            ("SINGLE", GOLDEN_SINGLE),
            ("NEGATIVE", GOLDEN_NEGATIVE),
            ("ZERO", GOLDEN_ZERO),
            ("EMPTY", GOLDEN_EMPTY),
            ("MANY", GOLDEN_MANY),
        ] {
            let bytes = hex_to_bytes(hex);
            let result = DDSketch::from_java_bytes(&bytes);
            assert!(
                result.is_ok(),
                "failed to decode {}: {:?}",
                name,
                result.err()
            );
        }
    }

    #[test]
    fn test_encode_decode_many_values() {
        let mut sketch = DDSketch::new(Config::defaults());
        for i in 1..=100 {
            sketch.add(i as f64);
        }

        let bytes = sketch.to_java_bytes();
        let decoded = DDSketch::from_java_bytes(&bytes).unwrap();

        assert_eq!(decoded.count(), 100);
        assert_eq!(decoded.min(), Some(1.0));
        assert_eq!(decoded.max(), Some(100.0));
        assert_eq!(decoded.sum(), Some(5050.0));

        let alpha = 0.01;
        let orig_p95 = sketch.quantile(0.95).unwrap().unwrap();
        let dec_p95 = decoded.quantile(0.95).unwrap().unwrap();
        assert!(
            (orig_p95 - dec_p95).abs() / orig_p95 < alpha,
            "p95 mismatch: {} vs {}",
            orig_p95,
            dec_p95
        );
    }
}
