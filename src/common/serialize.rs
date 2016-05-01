
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::io::Write;
use std::io::Read;
use std::io;
use common::VInt;
use byteorder;

pub trait BinarySerializable : fmt::Debug + Sized {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize>;
    fn deserialize(reader: &mut Read) -> io::Result<Self>;
}

fn convert_byte_order_error(byteorder_error: byteorder::Error) -> io::Error {
    match byteorder_error {
        byteorder::Error::UnexpectedEOF => io::Error::new(io::ErrorKind::InvalidData, "Reached EOF unexpectedly"),
        byteorder::Error::Io(e) => e,
    }
}

impl BinarySerializable for () {
    fn serialize(&self, _: &mut Write) -> io::Result<usize> {
        Ok(0)
    }
    fn deserialize(_: &mut Read) -> io::Result<Self> {
        Ok(())
    }
}

impl<T: BinarySerializable> BinarySerializable for Vec<T> {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        let mut total_size = try!(VInt(self.len() as u64).serialize(writer));
        for it in self.iter() {
            total_size += try!(it.serialize(writer));
        }
        Ok(total_size)
    }
    fn deserialize(reader: &mut Read) -> io::Result<Vec<T>> {
        let num_items = try!(VInt::deserialize(reader)).val();
        let mut items: Vec<T> = Vec::with_capacity(num_items as usize);
        for _ in 0..num_items {
            let item = try!(T::deserialize(reader));
            items.push(item);
        }
        Ok(items)
    }
}


impl<Left: BinarySerializable, Right: BinarySerializable> BinarySerializable for (Left, Right) {
    fn serialize(&self, write: &mut Write) -> io::Result<usize> {
        Ok(try!(self.0.serialize(write)) + try!(self.1.serialize(write)))
    }
    fn deserialize(reader: &mut Read) -> io::Result<Self> {
        Ok( (try!(Left::deserialize(reader)), try!(Right::deserialize(reader))) )
    }
}

impl BinarySerializable for u32 {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        writer.write_u32::<NativeEndian>(self.clone())
              .map(|_| 4)
              .map_err(convert_byte_order_error)
    }

    fn deserialize(reader: &mut Read) -> io::Result<u32> {
        reader.read_u32::<NativeEndian>()
              .map_err(convert_byte_order_error)
    }
}


impl BinarySerializable for u64 {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        writer.write_u64::<NativeEndian>(self.clone())
              .map(|_| 8)
              .map_err(convert_byte_order_error)
    }
    fn deserialize(reader: &mut Read) -> io::Result<u64> {
        reader.read_u64::<NativeEndian>()
              .map_err(convert_byte_order_error)
    }
}


impl BinarySerializable for u8 {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        // TODO error
        try!(writer.write_u8(self.clone()).map_err(convert_byte_order_error));
        Ok(1)
    }
    fn deserialize(reader: &mut Read) -> io::Result<u8> {
        reader.read_u8()
              .map_err(convert_byte_order_error)
    }
}

impl BinarySerializable for String {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        let data: &[u8] = self.as_bytes();
        let mut size = try!(VInt(data.len() as u64).serialize(writer));
        size += data.len();
        try!(writer.write_all(data));
        Ok(size)
    }

    fn deserialize(reader: &mut Read) -> io::Result<String> {
        let string_length = try!(VInt::deserialize(reader)).val() as usize;
        let mut result = String::with_capacity(string_length);
        try!(reader.take(string_length as u64).read_to_string(&mut result));
        Ok(result)
    }
}


#[cfg(test)]
mod test {


    use std::io::Cursor;
    use common::VInt;
    use super::*;

    fn serialize_test<T: BinarySerializable + Eq>(v: T, num_bytes: usize) {
        let mut buffer: Vec<u8> = Vec::new();
        assert_eq!(v.serialize(&mut buffer).unwrap(), num_bytes);
        assert_eq!(buffer.len(), num_bytes);
        let mut cursor = Cursor::new(&buffer[..]);
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
        serialize_test(String::from("ぽよぽよ"), 1 + 3*4);
        serialize_test(String::from("富士さん見える。"), 1 + 3*8);
    }

    #[test]
    fn test_serialize_vec() {
        let v: Vec<u8> = Vec::new();
        serialize_test(v, 1);
        serialize_test(vec!(1u32, 3u32), 1 + 4*2);
    }

    #[test]
    fn test_serialize_vint() {
        serialize_test(VInt(7u64), 1);
        serialize_test(VInt(127u64), 1);
        serialize_test(VInt(128u64), 2);
        serialize_test(VInt(1234u64), 2);
        serialize_test(VInt(16_383), 2);
        serialize_test(VInt(16_384), 3);
        serialize_test(VInt(u64::max_value()), 10);
    }
}
