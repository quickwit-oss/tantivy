use std::fmt;
use std::num::NonZeroU8;

use ordered_float::NotNan;
use thiserror::Error;

use crate::dictionary::UnorderedId;
use crate::value::NumericalValue;
use crate::DocId;

/// When we build a columnar dataframe, we first just group
/// all mutations per column, and append them in append-only object.
///
/// We represents all of these operations as `ColumnOperation`.
#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub(crate) enum ColumnOperation<T> {
    NewDoc(DocId),
    Value(T),
}

impl<T> From<T> for ColumnOperation<T> {
    fn from(value: T) -> Self {
        ColumnOperation::Value(value)
    }
}

#[allow(clippy::from_over_into)]
pub(crate) trait SymbolValue: Into<MiniBuffer> + Clone + Copy + fmt::Debug {
    fn deserialize(header: NonZeroU8, bytes: &mut &[u8]) -> Result<Self, ParseError>;
}

pub(crate) struct MiniBuffer {
    pub bytes: [u8; 9],
    pub len: usize,
}

impl MiniBuffer {
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

fn compute_header_byte(typ: SymbolType, len: usize) -> u8 {
    assert!(len <= 9);
    (len << 4) as u8 | typ as u8
}

impl SymbolValue for NumericalValue {
    fn deserialize(header_byte: NonZeroU8, bytes: &mut &[u8]) -> Result<Self, ParseError> {
        let (typ, len) = parse_header_byte(header_byte)?;
        let value_bytes: &[u8];
        (value_bytes, *bytes) = bytes.split_at(len);
        let symbol: NumericalValue = match typ {
            SymbolType::U64 => {
                let mut octet: [u8; 8] = [0u8; 8];
                octet[..value_bytes.len()].copy_from_slice(value_bytes);
                let val: u64 = u64::from_le_bytes(octet);
                NumericalValue::U64(val)
            }
            SymbolType::I64 => {
                let mut octet: [u8; 8] = [0u8; 8];
                octet[..value_bytes.len()].copy_from_slice(value_bytes);
                let encoded: u64 = u64::from_le_bytes(octet);
                let val: i64 = decode_zig_zag(encoded);
                NumericalValue::I64(val)
            }
            SymbolType::Float => {
                let octet: [u8; 8] =
                    value_bytes.try_into().map_err(|_| ParseError::InvalidLen {
                        typ: SymbolType::Float,
                        len,
                    })?;
                let val_possibly_nan = f64::from_le_bytes(octet);
                let val_not_nan = NotNan::new(val_possibly_nan)
                    .map_err(|_| ParseError::NaN)?;
                NumericalValue::F64(val_not_nan)
            }
        };
        Ok(symbol)
    }
}

#[allow(clippy::from_over_into)]
impl Into<MiniBuffer> for NumericalValue {
    fn into(self) -> MiniBuffer {
        let mut bytes = [0u8; 9];
        match self {
            NumericalValue::F64(val) => {
                let len = 8;
                let header_byte = compute_header_byte(SymbolType::Float, len);
                bytes[0] = header_byte;
                bytes[1..].copy_from_slice(&val.to_le_bytes());
                MiniBuffer {
                    bytes,
                    len: len + 1,
                }
            }
            NumericalValue::U64(val) => {
                let len = compute_num_bytes_for_u64(val);
                let header_byte = compute_header_byte(SymbolType::U64, len);
                bytes[0] = header_byte;
                bytes[1..].copy_from_slice(&val.to_le_bytes());
                MiniBuffer {
                    bytes,
                    len: len + 1,
                }
            }
            NumericalValue::I64(val) => {
                let encoded = encode_zig_zag(val);
                let len = compute_num_bytes_for_u64(encoded);
                let header_byte = compute_header_byte(SymbolType::I64, len);
                bytes[0] = header_byte;
                bytes[1..].copy_from_slice(&encoded.to_le_bytes());
                MiniBuffer {
                    bytes,
                    len: len + 1,
                }
            }
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<MiniBuffer> for UnorderedId {
    fn into(self) -> MiniBuffer {
        let mut bytes = [0u8; 9];
        let val = self.0 as u64;
        let len = compute_num_bytes_for_u64(val) + 1;
        bytes[0] = len as u8;
        bytes[1..].copy_from_slice(&val.to_le_bytes());
        MiniBuffer { bytes, len }
    }
}

impl SymbolValue for UnorderedId {
    fn deserialize(header: NonZeroU8, bytes: &mut &[u8]) -> Result<UnorderedId, ParseError> {
        let len = header.get() as usize;
        let symbol_bytes: &[u8];
        (symbol_bytes, *bytes) = bytes.split_at(len);
        let mut value_bytes = [0u8; 4];
        value_bytes[..len - 1].copy_from_slice(&symbol_bytes[1..]);
        let value = u32::from_le_bytes(value_bytes);
        Ok(UnorderedId(value))
    }
}

const HEADER_MASK: u8 = (1u8 << 4) - 1u8;

fn compute_num_bytes_for_u64(val: u64) -> usize {
    let msb = (64u32 - val.leading_zeros()) as usize;
    (msb + 7) / 8
}

fn parse_header_byte(byte: NonZeroU8) -> Result<(SymbolType, usize), ParseError> {
    let len = (byte.get() as usize) >> 4;
    let typ_code = byte.get() & HEADER_MASK;
    let typ = SymbolType::try_from(typ_code)?;
    Ok((typ, len))
}

#[derive(Error, Debug)]
pub enum ParseError {
    #[error("Type byte unknown `{0}`")]
    UnknownType(u8),
    #[error("Invalid len for type `{len}` for type `{typ:?}`.")]
    InvalidLen { typ: SymbolType, len: usize },
    #[error("Missing bytes.")]
    MissingBytes,
    #[error("Not a number value.")]
    NaN,
}

impl<V: SymbolValue> ColumnOperation<V> {
    pub fn serialize(self) -> MiniBuffer {
        match self {
            ColumnOperation::NewDoc(doc) => {
                let mut minibuf: [u8; 9] = [0u8; 9];
                minibuf[0] = 0u8;
                minibuf[1..5].copy_from_slice(&doc.to_le_bytes());
                MiniBuffer {
                    bytes: minibuf,
                    len: 5,
                }
            }
            ColumnOperation::Value(val) => val.into(),
        }
    }

    pub fn deserialize(bytes: &mut &[u8]) -> Result<Self, ParseError> {
        if bytes.is_empty() {
            return Err(ParseError::MissingBytes);
        }
        let header_byte = bytes[0];
        *bytes = &bytes[1..];
        if let Some(header_byte) = NonZeroU8::new(header_byte) {
            let value = V::deserialize(header_byte, bytes)?;
            Ok(ColumnOperation::Value(value))
        } else {
            let doc_bytes: &[u8];
            (doc_bytes, *bytes) = bytes.split_at(4);
            let doc: u32 =
                u32::from_le_bytes(doc_bytes.try_into().map_err(|_| ParseError::MissingBytes)?);
            Ok(ColumnOperation::NewDoc(doc))
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum SymbolType {
    U64 = 1u8,
    I64 = 2u8,
    Float = 3u8,
}

impl TryFrom<u8> for SymbolType {
    type Error = ParseError;

    fn try_from(byte: u8) -> Result<Self, ParseError> {
        match byte {
            1u8 => Ok(SymbolType::U64),
            2u8 => Ok(SymbolType::I64),
            3u8 => Ok(SymbolType::Float),
            _ => Err(ParseError::UnknownType(byte)),
        }
    }
}

fn encode_zig_zag(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

fn decode_zig_zag(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

#[cfg(test)]
mod tests {
    use super::{SymbolType, *};

    #[track_caller]
    fn test_zig_zag_aux(val: i64) {
        let encoded = super::encode_zig_zag(val);
        assert_eq!(decode_zig_zag(encoded), val);
        if let Some(abs_val) = val.checked_abs() {
            let abs_val = abs_val as u64;
            assert!(encoded <= abs_val * 2);
        }
    }

    #[test]
    fn test_zig_zag() {
        assert_eq!(encode_zig_zag(0i64), 0u64);
        assert_eq!(encode_zig_zag(-1i64), 1u64);
        assert_eq!(encode_zig_zag(1i64), 2u64);
        test_zig_zag_aux(0i64);
        test_zig_zag_aux(i64::MIN);
        test_zig_zag_aux(i64::MAX);
    }

    use proptest::prelude::any;
    use proptest::proptest;

    proptest! {
        #[test]
        fn test_proptest_zig_zag(val in any::<i64>()) {
            test_zig_zag_aux(val);
        }
    }

    #[track_caller]
    fn ser_deser_header_byte_aux(symbol_type: SymbolType, len: usize) {
        let header_byte = compute_header_byte(symbol_type, len);
        let (serdeser_numerical_type, serdeser_len) =
            parse_header_byte(NonZeroU8::new(header_byte).unwrap()).unwrap();
        assert_eq!(symbol_type, serdeser_numerical_type);
        assert_eq!(len, serdeser_len);
    }

    #[test]
    fn test_header_byte_serialization() {
        for len in 1..9 {
            ser_deser_header_byte_aux(SymbolType::Float, len);
            ser_deser_header_byte_aux(SymbolType::I64, len);
            ser_deser_header_byte_aux(SymbolType::U64, len);
        }
    }

    #[track_caller]
    fn ser_deser_symbol(symbol: ColumnOperation<NumericalValue>) {
        let buf = symbol.serialize();
        let mut bytes = &buf.bytes[..];
        let serdeser_symbol = ColumnOperation::deserialize(&mut bytes).unwrap();
        assert_eq!(bytes.len() + buf.len, buf.bytes.len());
        assert_eq!(symbol, serdeser_symbol);
    }

    #[test]
    fn test_compute_num_bytes_for_u64() {
        assert_eq!(compute_num_bytes_for_u64(0), 0);
        assert_eq!(compute_num_bytes_for_u64(1), 1);
        assert_eq!(compute_num_bytes_for_u64(255), 1);
        assert_eq!(compute_num_bytes_for_u64(256), 2);
        assert_eq!(compute_num_bytes_for_u64((1 << 16) - 1), 2);
        assert_eq!(compute_num_bytes_for_u64(1 << 16), 3);
    }

    #[test]
    fn test_symbol_serialization() {
        ser_deser_symbol(ColumnOperation::NewDoc(0));
        ser_deser_symbol(ColumnOperation::NewDoc(3));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::I64(0i64)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::I64(1i64)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::U64(257u64)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::I64(-257i64)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::I64(i64::MIN)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::U64(0u64)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::U64(u64::MIN)));
        ser_deser_symbol(ColumnOperation::Value(NumericalValue::U64(u64::MAX)));
    }
}
