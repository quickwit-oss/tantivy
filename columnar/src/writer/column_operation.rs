use crate::dictionary::UnorderedId;
use crate::utils::{place_bits, pop_first_byte, select_bits};
use crate::value::NumericalValue;
use crate::{DocId, NumericalType};

/// When we build a columnar dataframe, we first just group
/// all mutations per column, and append them in append-only object.
///
/// We represents all of these operations as `ColumnOperation`.
#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub(crate) enum ColumnOperation<T> {
    NewDoc(DocId),
    Value(T),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct ColumnOperationHeader {
    typ_code: u8,
    len: u8,
}

impl ColumnOperationHeader {
    fn to_code(self) -> u8 {
        place_bits::<0, 4>(self.len) | place_bits::<4, 8>(self.typ_code)
    }

    fn from_code(code: u8) -> Self {
        let len = select_bits::<0, 4>(code);
        let typ_code = select_bits::<4, 8>(code);
        ColumnOperationHeader { typ_code, len }
    }
}

const NEW_DOC_CODE: u8 = 0u8;
const NEW_VALUE_CODE: u8 = 1u8;

impl<V: SymbolValue> ColumnOperation<V> {
    pub fn serialize(self) -> impl AsRef<[u8]> {
        let mut minibuf = MiniBuffer::default();
        let header = match self {
            ColumnOperation::NewDoc(new_doc) => {
                let symbol_len = new_doc.serialize(&mut minibuf.bytes[1..]);
                ColumnOperationHeader {
                    typ_code: NEW_DOC_CODE,
                    len: symbol_len,
                }
            }
            ColumnOperation::Value(val) => {
                let symbol_len = val.serialize(&mut minibuf.bytes[1..]);
                ColumnOperationHeader {
                    typ_code: NEW_VALUE_CODE,
                    len: symbol_len,
                }
            }
        };
        minibuf.bytes[0] = header.to_code();
        minibuf.len = 1 + header.len;
        minibuf
    }

    /// Deserialize a colummn operation.
    /// Returns None if the buffer is empty.
    ///
    /// Panics if the payload is invalid.
    pub fn deserialize(bytes: &mut &[u8]) -> Option<Self> {
        let header_byte = pop_first_byte(bytes)?;
        let column_op_header = ColumnOperationHeader::from_code(header_byte);
        let symbol_bytes: &[u8];
        (symbol_bytes, *bytes) = bytes.split_at(column_op_header.len as usize);
        match column_op_header.typ_code {
            NEW_DOC_CODE => {
                let new_doc = u32::deserialize(symbol_bytes);
                Some(ColumnOperation::NewDoc(new_doc))
            }
            NEW_VALUE_CODE => {
                let value = V::deserialize(symbol_bytes);
                Some(ColumnOperation::Value(value))
            }
            _ => {
                panic!("Unknown code {}", column_op_header.typ_code);
            }
        }
    }
}

impl<T> From<T> for ColumnOperation<T> {
    fn from(value: T) -> Self {
        ColumnOperation::Value(value)
    }
}

#[allow(clippy::from_over_into)]
pub(crate) trait SymbolValue: Clone + Copy {
    fn serialize(self, buffer: &mut [u8]) -> u8;

    // Reads the header type and the given bytes.
    //
    // `bytes` does not contain the header byte.
    // This method should advance bytes by the number of bytes that were consumed.
    fn deserialize(bytes: &[u8]) -> Self;
}

impl SymbolValue for bool {
    fn serialize(self, buffer: &mut [u8]) -> u8 {
        buffer[0] = if self { 1u8 } else { 0u8 };
        1u8
    }

    fn deserialize(bytes: &[u8]) -> Self {
        bytes[0] == 1u8
    }
}

#[derive(Default)]
struct MiniBuffer {
    pub bytes: [u8; 10],
    pub len: u8,
}

impl AsRef<[u8]> for MiniBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }
}

impl SymbolValue for NumericalValue {
    fn deserialize(mut bytes: &[u8]) -> Self {
        let type_code = pop_first_byte(&mut bytes).unwrap();
        let symbol_type = NumericalType::try_from_code(type_code).unwrap();
        let mut octet: [u8; 8] = [0u8; 8];
        octet[..bytes.len()].copy_from_slice(bytes);
        match symbol_type {
            NumericalType::U64 => {
                let val: u64 = u64::from_le_bytes(octet);
                NumericalValue::U64(val)
            }
            NumericalType::I64 => {
                let encoded: u64 = u64::from_le_bytes(octet);
                let val: i64 = decode_zig_zag(encoded);
                NumericalValue::I64(val)
            }
            NumericalType::F64 => {
                debug_assert_eq!(bytes.len(), 8);
                let val: f64 = f64::from_le_bytes(octet);
                NumericalValue::F64(val)
            }
        }
    }

    fn serialize(self, output: &mut [u8]) -> u8 {
        match self {
            NumericalValue::F64(val) => {
                output[0] = NumericalType::F64 as u8;
                output[1..9].copy_from_slice(&val.to_le_bytes());
                9u8
            }
            NumericalValue::U64(val) => {
                let len = compute_num_bytes_for_u64(val) as u8;
                output[0] = NumericalType::U64 as u8;
                output[1..9].copy_from_slice(&val.to_le_bytes());
                len + 1u8
            }
            NumericalValue::I64(val) => {
                let zig_zag_encoded = encode_zig_zag(val);
                let len = compute_num_bytes_for_u64(zig_zag_encoded) as u8;
                output[0] = NumericalType::I64 as u8;
                output[1..9].copy_from_slice(&zig_zag_encoded.to_le_bytes());
                len + 1u8
            }
        }
    }
}

impl SymbolValue for u32 {
    fn serialize(self, output: &mut [u8]) -> u8 {
        let len = compute_num_bytes_for_u64(self as u64);
        output[0..4].copy_from_slice(&self.to_le_bytes());
        len as u8
    }

    fn deserialize(bytes: &[u8]) -> Self {
        let mut quartet: [u8; 4] = [0u8; 4];
        quartet[..bytes.len()].copy_from_slice(bytes);
        u32::from_le_bytes(quartet)
    }
}

impl SymbolValue for UnorderedId {
    fn serialize(self, output: &mut [u8]) -> u8 {
        self.0.serialize(output)
    }

    fn deserialize(bytes: &[u8]) -> Self {
        UnorderedId(u32::deserialize(bytes))
    }
}

fn compute_num_bytes_for_u64(val: u64) -> usize {
    let msb = (64u32 - val.leading_zeros()) as usize;
    (msb + 7) / 8
}

fn encode_zig_zag(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

fn decode_zig_zag(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_header_byte_serialization() {
        for len in 0..=15 {
            for typ_code in 0..=15 {
                let header = ColumnOperationHeader { typ_code, len };
                let header_code = header.to_code();
                let serdeser_header = ColumnOperationHeader::from_code(header_code);
                assert_eq!(header, serdeser_header);
            }
        }
    }

    #[track_caller]
    fn ser_deser_symbol(column_op: ColumnOperation<NumericalValue>) {
        let buf = column_op.serialize();
        let mut buffer = buf.as_ref().to_vec();
        buffer.extend_from_slice(b"234234");
        let mut bytes = &buffer[..];
        let serdeser_symbol = ColumnOperation::deserialize(&mut bytes).unwrap();
        assert_eq!(bytes.len() + buf.as_ref().len() as usize, buffer.len());
        assert_eq!(column_op, serdeser_symbol);
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

    fn test_column_operation_unordered_aux(val: u32, expected_len: usize) {
        let column_op = ColumnOperation::Value(UnorderedId(val));
        let minibuf = column_op.serialize();
        assert_eq!(minibuf.as_ref().len() as usize, expected_len);
        let mut buf = minibuf.as_ref().to_vec();
        buf.extend_from_slice(&[2, 2, 2, 2, 2, 2]);
        let mut cursor = &buf[..];
        let column_op_serdeser: ColumnOperation<UnorderedId> =
            ColumnOperation::deserialize(&mut cursor).unwrap();
        assert_eq!(column_op_serdeser, ColumnOperation::Value(UnorderedId(val)));
        assert_eq!(cursor.len() + expected_len, buf.len());
    }

    #[test]
    fn test_column_operation_unordered() {
        test_column_operation_unordered_aux(300u32, 3);
        test_column_operation_unordered_aux(1u32, 2);
        test_column_operation_unordered_aux(0u32, 1);
    }
}
