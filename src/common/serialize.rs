use crate::common::Endianness;
use crate::common::VInt;
use byteorder::{ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::io;
use std::io::Read;
use std::io::Write;

/// Trait for a simple binary serialization.
pub trait BinarySerializable: fmt::Debug + Sized {
    /// Serialize
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()>;
    /// Deserialize
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self>;
}

/// `FixedSize` marks a `BinarySerializable` as
/// always serializing to the same size.
pub trait FixedSize: BinarySerializable {
    const SIZE_IN_BYTES: usize;
}

impl BinarySerializable for () {
    fn serialize<W: Write>(&self, _: &mut W) -> io::Result<()> {
        Ok(())
    }
    fn deserialize<R: Read>(_: &mut R) -> io::Result<Self> {
        Ok(())
    }
}

impl FixedSize for () {
    const SIZE_IN_BYTES: usize = 0;
}

impl<T: BinarySerializable> BinarySerializable for Vec<T> {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        VInt(self.len() as u64).serialize(writer)?;
        for it in self {
            it.serialize(writer)?;
        }
        Ok(())
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Vec<T>> {
        let num_items = VInt::deserialize(reader)?.val();
        let mut items: Vec<T> = Vec::with_capacity(num_items as usize);
        for _ in 0..num_items {
            let item = T::deserialize(reader)?;
            items.push(item);
        }
        Ok(items)
    }
}

impl<Left: BinarySerializable, Right: BinarySerializable> BinarySerializable for (Left, Right) {
    fn serialize<W: Write>(&self, write: &mut W) -> io::Result<()> {
        self.0.serialize(write)?;
        self.1.serialize(write)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        Ok((Left::deserialize(reader)?, Right::deserialize(reader)?))
    }
}

impl BinarySerializable for u32 {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u32::<Endianness>(*self)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<u32> {
        reader.read_u32::<Endianness>()
    }
}

impl FixedSize for u32 {
    const SIZE_IN_BYTES: usize = 4;
}

impl BinarySerializable for u64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u64::<Endianness>(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        reader.read_u64::<Endianness>()
    }
}

impl FixedSize for u64 {
    const SIZE_IN_BYTES: usize = 8;
}

impl BinarySerializable for f32 {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_f32::<Endianness>(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        reader.read_f32::<Endianness>()
    }
}

impl FixedSize for f32 {
    const SIZE_IN_BYTES: usize = 4;
}

impl BinarySerializable for i64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_i64::<Endianness>(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        reader.read_i64::<Endianness>()
    }
}

impl FixedSize for i64 {
    const SIZE_IN_BYTES: usize = 8;
}

impl BinarySerializable for f64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_f64::<Endianness>(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        reader.read_f64::<Endianness>()
    }
}

impl FixedSize for f64 {
    const SIZE_IN_BYTES: usize = 8;
}

impl BinarySerializable for u8 {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<u8> {
        reader.read_u8()
    }
}

impl FixedSize for u8 {
    const SIZE_IN_BYTES: usize = 1;
}

impl BinarySerializable for String {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let data: &[u8] = self.as_bytes();
        VInt(data.len() as u64).serialize(writer)?;
        writer.write_all(data)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<String> {
        let string_length = VInt::deserialize(reader)?.val() as usize;
        let mut result = String::with_capacity(string_length);
        reader
            .take(string_length as u64)
            .read_to_string(&mut result)?;
        Ok(result)
    }
}

#[cfg(test)]
pub mod test {

    use super::*;
    use crate::common::VInt;

    pub fn fixed_size_test<O: BinarySerializable + FixedSize + Default>() {
        let mut buffer = Vec::new();
        O::default().serialize(&mut buffer).unwrap();
        assert_eq!(buffer.len(), O::SIZE_IN_BYTES);
    }

    fn serialize_test<T: BinarySerializable + Eq>(v: T) -> usize {
        let mut buffer: Vec<u8> = Vec::new();
        v.serialize(&mut buffer).unwrap();
        let num_bytes = buffer.len();
        let mut cursor = &buffer[..];
        let deser = T::deserialize(&mut cursor).unwrap();
        assert_eq!(deser, v);
        num_bytes
    }

    #[test]
    fn test_serialize_u8() {
        fixed_size_test::<u8>();
    }

    #[test]
    fn test_serialize_u32() {
        fixed_size_test::<u32>();
        assert_eq!(4, serialize_test(3u32));
        assert_eq!(4, serialize_test(5u32));
        assert_eq!(4, serialize_test(u32::max_value()));
    }

    #[test]
    fn test_serialize_i64() {
        fixed_size_test::<i64>();
    }

    #[test]
    fn test_serialize_f64() {
        fixed_size_test::<f64>();
    }

    #[test]
    fn test_serialize_u64() {
        fixed_size_test::<u64>();
    }

    #[test]
    fn test_serialize_string() {
        assert_eq!(serialize_test(String::from("")), 1);
        assert_eq!(serialize_test(String::from("ぽよぽよ")), 1 + 3 * 4);
        assert_eq!(serialize_test(String::from("富士さん見える。")), 1 + 3 * 8);
    }

    #[test]
    fn test_serialize_vec() {
        assert_eq!(serialize_test(Vec::<u8>::new()), 1);
        assert_eq!(serialize_test(vec![1u32, 3u32]), 1 + 4 * 2);
    }

    #[test]
    fn test_serialize_vint() {
        for i in 0..10_000 {
            serialize_test(VInt(i as u64));
        }
        assert_eq!(serialize_test(VInt(7u64)), 1);
        assert_eq!(serialize_test(VInt(127u64)), 1);
        assert_eq!(serialize_test(VInt(128u64)), 2);
        assert_eq!(serialize_test(VInt(129u64)), 2);
        assert_eq!(serialize_test(VInt(1234u64)), 2);
        assert_eq!(serialize_test(VInt(16_383u64)), 2);
        assert_eq!(serialize_test(VInt(16_384u64)), 3);
        assert_eq!(serialize_test(VInt(u64::max_value())), 10);
    }
}
