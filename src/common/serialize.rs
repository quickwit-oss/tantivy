use byteorder::{ReadBytesExt, WriteBytesExt};
use byteorder::LittleEndian as Endianness;
use std::fmt;
use std::io::Write;
use std::io::Read;
use std::io;
use common::VInt;



pub trait BinarySerializable: fmt::Debug + Sized {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()>;
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self>;
}

impl BinarySerializable for () {
    fn serialize<W: Write>(&self, _: &mut W) -> io::Result<()> {
        Ok(())
    }
    fn deserialize<R: Read>(_: &mut R) -> io::Result<Self> {
        Ok(())
    }
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


impl BinarySerializable for u64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u64::<Endianness>(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        reader.read_u64::<Endianness>()
    }
}

impl BinarySerializable for i64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_i64::<Endianness>(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        reader.read_i64::<Endianness>()
    }
}


impl BinarySerializable for u8 {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(*self)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<u8> {
        reader.read_u8()
    }
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
        reader.take(string_length as u64).read_to_string(
            &mut result,
        )?;
        Ok(result)
    }
}


#[cfg(test)]
mod test {

    use common::VInt;
    use super::*;

    fn serialize_test<T: BinarySerializable + Eq>(v: T, num_bytes: usize) {
        let mut buffer: Vec<u8> = Vec::new();
        if num_bytes != 0 {
            v.serialize(&mut buffer).unwrap();
            assert_eq!(buffer.len(), num_bytes);
        } else {
            v.serialize(&mut buffer).unwrap();
        }
        let mut cursor = &buffer[..];
        let deser = T::deserialize(&mut cursor).unwrap();
        assert_eq!(deser, v);
    }

    #[test]
    fn test_serialize_u8() {
        serialize_test(3u8, 1);
        serialize_test(5u8, 1);
    }

    #[test]
    fn test_serialize_u32() {
        serialize_test(3u32, 4);
        serialize_test(5u32, 4);
        serialize_test(u32::max_value(), 4);
    }

    #[test]
    fn test_serialize_string() {
        serialize_test(String::from(""), 1);
        serialize_test(String::from("ぽよぽよ"), 1 + 3 * 4);
        serialize_test(String::from("富士さん見える。"), 1 + 3 * 8);
    }

    #[test]
    fn test_serialize_vec() {
        let v: Vec<u8> = Vec::new();
        serialize_test(v, 1);
        serialize_test(vec![1u32, 3u32], 1 + 4 * 2);
    }

    #[test]
    fn test_serialize_vint() {
        for i in 0..10_000 {
            serialize_test(VInt(i as u64), 0);
        }
        serialize_test(VInt(7u64), 1);
        serialize_test(VInt(127u64), 1);
        serialize_test(VInt(128u64), 2);
        serialize_test(VInt(129u64), 2);
        serialize_test(VInt(1234u64), 2);
        serialize_test(VInt(16_383), 2);
        serialize_test(VInt(16_384), 3);
        serialize_test(VInt(u64::max_value()), 10);
    }
}
