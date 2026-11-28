use std::io;
use std::io::{Read, Write};

use super::BinarySerializable;

#[cold]
#[inline(never)]
fn invalid_vint_u32() -> ! {
    panic!("Corrupted data. Invalid VInt 32");
}

#[cold]
#[inline(never)]
fn vint_eof_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "Reach end of buffer while reading VInt",
    )
}

#[inline(always)]
fn read_vint_byte<R: Read>(reader: &mut R) -> io::Result<u8> {
    let mut buf = [0u8; 1];
    if reader.read_exact(&mut buf).is_err() {
        return Err(vint_eof_error());
    }
    Ok(buf[0])
}

#[inline]
fn serialize_vint_u128_into(val: u128, buffer: &mut [u8; 19]) -> usize {
    for (i, b) in buffer[..18].iter_mut().enumerate() {
        let remaining = val >> (i * 7);
        if remaining < 128 {
            *b = (remaining as u8) | STOP_BIT;
            return i + 1;
        }
        *b = (remaining as u8) & 0x7f;
    }
    buffer[18] = ((val >> 126) as u8) | STOP_BIT;
    19
}

///   Wrapper over a `u128` that serializes as a variable int.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VIntU128(pub u128);

impl BinarySerializable for VIntU128 {
    #[inline]
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        if let Ok(val) = u64::try_from(self.0) {
            return VInt(val).serialize(writer);
        }
        let mut buffer = [0u8; 19];
        let num_bytes = serialize_vint_u128_into(self.0, &mut buffer);
        writer.write_all(&buffer[..num_bytes])
    }

    #[inline]
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut result = 0u128;
        for i in 0..19u32 {
            let b = read_vint_byte(reader)?;
            let shift = i * 7;
            if b >= STOP_BIT {
                return Ok(VIntU128(result | (u128::from(b & 0x7f) << shift)));
            }
            result |= u128::from(b) << shift;
        }
        Err(vint_eof_error())
    }
}

///   Wrapper over a `u64` that serializes as a variable int.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VInt(pub u64);

const STOP_BIT: u8 = 128;

#[inline]
pub fn serialize_vint_u32(val: u32, buf: &mut [u8; 8]) -> &[u8] {
    const START_2: u64 = 1 << 7;
    const START_3: u64 = 1 << 14;
    const START_4: u64 = 1 << 21;
    const START_5: u64 = 1 << 28;

    const MASK_1: u64 = 127;
    const MASK_2: u64 = MASK_1 << 7;
    const MASK_3: u64 = MASK_2 << 7;
    const MASK_4: u64 = MASK_3 << 7;
    const MASK_5: u64 = MASK_4 << 7;

    let val = u64::from(val);
    const STOP_BIT: u64 = 128u64;
    let (res, num_bytes) = if val < START_2 {
        (val | STOP_BIT, 1)
    } else if val < START_3 {
        (
            (val & MASK_1) | ((val & MASK_2) << 1) | (STOP_BIT << (8)),
            2,
        )
    } else if val < START_4 {
        (
            (val & MASK_1) | ((val & MASK_2) << 1) | ((val & MASK_3) << 2) | (STOP_BIT << (8 * 2)),
            3,
        )
    } else if val < START_5 {
        (
            (val & MASK_1)
                | ((val & MASK_2) << 1)
                | ((val & MASK_3) << 2)
                | ((val & MASK_4) << 3)
                | (STOP_BIT << (8 * 3)),
            4,
        )
    } else {
        (
            (val & MASK_1)
                | ((val & MASK_2) << 1)
                | ((val & MASK_3) << 2)
                | ((val & MASK_4) << 3)
                | ((val & MASK_5) << 4)
                | (STOP_BIT << (8 * 4)),
            5,
        )
    };
    *buf = res.to_le_bytes();
    &buf[0..num_bytes]
}

/// Reads a vint `u32` from a buffer, and
/// consumes its payload data.
///
/// # Panics
///
/// If the buffer does not start by a valid
/// vint payload
#[inline]
pub fn read_u32_vint(data: &mut &[u8]) -> u32 {
    let mut result = 0u32;
    let mut cursor = *data;
    for shift in [0u32, 7, 14, 21, 28] {
        let Some((&b, rest)) = cursor.split_first() else {
            invalid_vint_u32();
        };
        cursor = rest;
        if b >= STOP_BIT {
            *data = cursor;
            return result | (u32::from(b & 0x7f) << shift);
        }
        result |= u32::from(b) << shift;
    }
    invalid_vint_u32()
}

#[inline]
pub fn read_u32_vint_no_advance(data: &[u8]) -> (u32, usize) {
    let mut cursor = data;
    let val = read_u32_vint(&mut cursor);
    (val, data.len() - cursor.len())
}

/// Write a `u32` as a vint payload.
#[inline]
pub fn write_u32_vint<W: io::Write + ?Sized>(val: u32, writer: &mut W) -> io::Result<()> {
    let mut buf = [0u8; 8];
    let data = serialize_vint_u32(val, &mut buf);
    writer.write_all(data)
}

impl VInt {
    #[inline]
    pub fn val(&self) -> u64 {
        self.0
    }

    #[inline]
    pub fn deserialize_u64<R: Read>(reader: &mut R) -> io::Result<u64> {
        VInt::deserialize(reader).map(|vint| vint.0)
    }

    #[inline]
    pub fn serialize_into_vec(&self, output: &mut Vec<u8>) {
        if self.0 < 128 {
            output.push((self.0 as u8) | STOP_BIT);
            return;
        }
        let mut buffer = [0u8; 10];
        let num_bytes = self.serialize_into(&mut buffer);
        output.extend_from_slice(&buffer[0..num_bytes]);
    }

    #[inline]
    pub fn serialize_into(&self, buffer: &mut [u8; 10]) -> usize {
        let val = self.0;
        if val < (1 << 7) {
            buffer[0] = (val as u8) | STOP_BIT;
            return 1;
        }
        buffer[0] = (val as u8) & 0x7f;
        if val < (1 << 14) {
            buffer[1] = ((val >> 7) as u8) | STOP_BIT;
            return 2;
        }
        buffer[1] = ((val >> 7) as u8) & 0x7f;
        if val < (1 << 21) {
            buffer[2] = ((val >> 14) as u8) | STOP_BIT;
            return 3;
        }
        buffer[2] = ((val >> 14) as u8) & 0x7f;
        let mut remaining = val >> 21;
        for (i, b) in buffer[3..9].iter_mut().enumerate() {
            if remaining < 128 {
                *b = (remaining as u8) | STOP_BIT;
                return i + 4;
            }
            *b = (remaining as u8) & 0x7f;
            remaining >>= 7;
        }
        buffer[9] = (remaining as u8) | STOP_BIT;
        10
    }
}

impl BinarySerializable for VInt {
    #[inline]
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        if let Ok(val) = u32::try_from(self.0) {
            return write_u32_vint(val, writer);
        }
        let mut buffer = [0u8; 10];
        let num_bytes = self.serialize_into(&mut buffer);
        writer.write_all(&buffer[0..num_bytes])
    }

    #[inline]
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut result = 0u64;
        for shift in [0u32, 7, 14, 21, 28, 35, 42, 49, 56, 63] {
            let b = read_vint_byte(reader)?;
            if b >= STOP_BIT {
                return Ok(VInt(result | (u64::from(b & 0x7f) << shift)));
            }
            result |= u64::from(b) << shift;
        }
        Err(vint_eof_error())
    }
}

#[cfg(test)]
mod tests {

    use super::{
        BinarySerializable, VInt, VIntU128, read_u32_vint, read_u32_vint_no_advance,
        serialize_vint_u32, write_u32_vint,
    };

    fn aux_test_vint(val: u64) {
        let mut v = [14u8; 10];
        let num_bytes = VInt(val).serialize_into(&mut v);
        for el in &v[num_bytes..10] {
            assert_eq!(el, &14u8);
        }
        assert!(num_bytes > 0);
        if num_bytes < 10 {
            assert!(1u64 << (7 * num_bytes) > val);
        }
        if num_bytes > 1 {
            assert!(1u64 << (7 * (num_bytes - 1)) <= val);
        }
        let serdeser_val = VInt::deserialize(&mut &v[..]).unwrap();
        assert_eq!(val, serdeser_val.val());

        let mut vec_buf = Vec::new();
        VInt(val).serialize_into_vec(&mut vec_buf);
        assert_eq!(&vec_buf[..], &v[..num_bytes]);

        let mut trait_buf = Vec::new();
        VInt(val).serialize(&mut trait_buf).unwrap();
        assert_eq!(trait_buf, vec_buf);

        let mut cursor: &[u8] = &vec_buf;
        assert_eq!(VInt::deserialize_u64(&mut cursor).unwrap(), val);
        assert!(cursor.is_empty());
    }

    #[test]
    fn test_vint() {
        aux_test_vint(0);
        aux_test_vint(1);
        aux_test_vint(5);
        aux_test_vint(u64::MAX - 1);
        aux_test_vint(u64::MAX);
        for i in 1..=9 {
            let power_of_128 = 1u64 << (7 * i);
            aux_test_vint(power_of_128 - 1u64);
            aux_test_vint(power_of_128);
            aux_test_vint(power_of_128 + 1u64);
        }
        aux_test_vint(10);
    }

    fn aux_test_serialize_vint_u32(val: u32) {
        let mut buffer = [0u8; 10];
        let mut buffer2 = [0u8; 8];
        let len_vint = VInt(val as u64).serialize_into(&mut buffer);
        let res2 = serialize_vint_u32(val, &mut buffer2);
        assert_eq!(&buffer[..len_vint], res2, "array wrong for {val}");

        let mut written = Vec::new();
        write_u32_vint(val, &mut written).unwrap();
        assert_eq!(&written[..], res2);

        // Exact-length slice and padded slice with trailing bytes
        assert_eq!(read_u32_vint_no_advance(res2), (val, len_vint));
        let mut exact = res2;
        assert_eq!(read_u32_vint(&mut exact), val);
        assert!(exact.is_empty());

        let mut padded = res2.to_vec();
        padded.extend_from_slice(&[0x55, 0xAA, 0xFF, 0x00]);
        assert_eq!(read_u32_vint_no_advance(&padded), (val, len_vint));
        let mut cursor: &[u8] = &padded;
        assert_eq!(read_u32_vint(&mut cursor), val);
        assert_eq!(cursor, &[0x55, 0xAA, 0xFF, 0x00]);
    }

    #[test]
    fn test_vint_u32() {
        aux_test_serialize_vint_u32(0);
        aux_test_serialize_vint_u32(1);
        aux_test_serialize_vint_u32(5);
        for i in 1..=4 {
            let power_of_128 = 1u32 << (7 * i);
            aux_test_serialize_vint_u32(power_of_128 - 1u32);
            aux_test_serialize_vint_u32(power_of_128);
            aux_test_serialize_vint_u32(power_of_128 + 1u32);
        }
        aux_test_serialize_vint_u32(u32::MAX);
    }

    #[test]
    fn test_read_u32_vint_5th_byte_high_bits_truncation() {
        let exact = [0x00, 0x00, 0x00, 0x00, 0xFF];
        assert_eq!(read_u32_vint_no_advance(&exact), (0xF000_0000, 5));
    }

    #[test]
    #[should_panic(expected = "Corrupted data")]
    fn test_read_u32_vint_panics_on_empty() {
        let _ = read_u32_vint_no_advance(&[]);
    }

    #[test]
    #[should_panic(expected = "Corrupted data")]
    fn test_read_u32_vint_panics_on_short_unterminated() {
        let _ = read_u32_vint_no_advance(&[1, 2, 3, 4]);
    }

    #[test]
    #[should_panic(expected = "Corrupted data")]
    fn test_read_u32_vint_panics_on_long_unterminated() {
        let _ = read_u32_vint_no_advance(&[1, 2, 3, 4, 5, 0x80]);
    }

    #[test]
    fn test_vint_u128_boundaries() {
        let mut vals = vec![0u128, 1, 127, 128, u128::MAX - 1, u128::MAX];
        for i in 1..=18 {
            let p = 1u128 << (7 * i);
            vals.push(p - 1);
            vals.push(p);
            vals.push(p + 1);
        }
        for val in vals {
            let mut buf = Vec::new();
            VIntU128(val).serialize(&mut buf).unwrap();
            let expected_len = if val == 0 {
                1
            } else {
                ((128 - val.leading_zeros()) as usize).div_ceil(7)
            };
            assert_eq!(buf.len(), expected_len);

            let mut cursor: &[u8] = &buf;
            assert_eq!(VIntU128::deserialize(&mut cursor).unwrap().0, val);
            assert!(cursor.is_empty());

            let mut padded = buf;
            padded.extend_from_slice(&[0xAA, 0x55]);
            let mut cursor: &[u8] = &padded;
            assert_eq!(VIntU128::deserialize(&mut cursor).unwrap().0, val);
            assert_eq!(cursor, &[0xAA, 0x55]);
        }
    }

    #[test]
    fn test_vint_truncated_and_overlong_errors() {
        for len in 0..=10 {
            let buf = vec![0x01u8; len];
            let err = VInt::deserialize(&mut &buf[..]).unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        }
        for len in 0..=19 {
            let buf = vec![0x01u8; len];
            let err = VIntU128::deserialize(&mut &buf[..]).unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn test_vint_interrupted_reader() {
        struct InterruptedReader<'a> {
            data: &'a [u8],
            interrupt_next: bool,
        }
        impl std::io::Read for InterruptedReader<'_> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.interrupt_next {
                    self.interrupt_next = false;
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "interrupted",
                    ));
                }
                self.interrupt_next = true;
                self.data.read(buf)
            }
        }

        let mut encoded = Vec::new();
        VInt(0x1234_5678_9ABC_DEF0).serialize(&mut encoded).unwrap();
        let mut reader = InterruptedReader {
            data: &encoded,
            interrupt_next: true,
        };
        assert_eq!(
            VInt::deserialize_u64(&mut reader).unwrap(),
            0x1234_5678_9ABC_DEF0
        );

        let mut encoded128 = Vec::new();
        VIntU128(u128::MAX - 42).serialize(&mut encoded128).unwrap();
        let mut reader128 = InterruptedReader {
            data: &encoded128,
            interrupt_next: true,
        };
        assert_eq!(
            VIntU128::deserialize(&mut reader128).unwrap(),
            VIntU128(u128::MAX - 42)
        );
    }
}
