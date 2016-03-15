
use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::io::Write;
use std::io::Read;
use std::io;
use byteorder;



fn convert_byte_order_error(byteorder_error: byteorder::Error) -> io::Error {
    match byteorder_error {
        byteorder::Error::UnexpectedEOF => io::Error::new(io::ErrorKind::InvalidData, "Reached EOF unexpectedly"),
        byteorder::Error::Io(e) => e,
    }
}

pub trait BinarySerializable : fmt::Debug + Sized {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize>;
    fn deserialize(reader: &mut Read) -> io::Result<Self>;
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
        let mut total_size = try!((self.len() as u32).serialize(writer));
        for it in self.iter() {
            total_size += try!(it.serialize(writer));
        }
        Ok(total_size)
    }
    fn deserialize(reader: &mut Read) -> io::Result<Vec<T>> {
        let num_items = try!(u32::deserialize(reader));
        let mut items: Vec<T> = Vec::with_capacity(num_items as usize);
        for _ in 0..num_items {
            let item = try!(T::deserialize(reader));
            items.push(item);
        }
        Ok(items)
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
        // TODO error
        let data: &[u8] = self.as_bytes();
        let mut size = try!((data.len() as u32).serialize(writer));
        size += data.len();
        try!(writer.write_all(data));
        Ok(size)
    }

    fn deserialize(reader: &mut Read) -> io::Result<String> {
        // TODO error
        let string_length = try!(u32::deserialize(reader)) as usize;
        let mut result = String::with_capacity(string_length);
        try!(reader.take(string_length as u64).read_to_string(&mut result));
        Ok(result)
    }
}


#[cfg(test)]
mod test {

    use core::serialize::BinarySerializable;
    use std::io::Cursor;

    #[test]
    fn test_serialize_u8() {
        let mut buffer: Vec<u8> = Vec::new();
        {
            let x: u8 = 3;
            x.serialize(&mut buffer).unwrap();
            assert_eq!(buffer.len(), 1);
        }
        {
            let x: u8 = 5;
            x.serialize(&mut buffer).unwrap();
            assert_eq!(buffer.len(), 2);
        }
        let mut cursor = Cursor::new(&buffer[..]);
        assert_eq!(3, u8::deserialize(&mut cursor).unwrap());
        assert_eq!(5, u8::deserialize(&mut cursor).unwrap());
        assert!(u8::deserialize(&mut cursor).is_err());
    }


    #[test]
    fn test_serialize_u32() {
        let mut buffer: Vec<u8> = Vec::new();
        {
            let x: u32 = 3;
            x.serialize(&mut buffer).unwrap();
            assert_eq!(buffer.len(), 4);
        }
        {
            let x: u32 = 5;
            x.serialize(&mut buffer).unwrap();
            assert_eq!(buffer.len(), 8);
        }
        let mut cursor = Cursor::new(&buffer[..]);
        assert_eq!(3, u32::deserialize(&mut cursor).unwrap());
        assert_eq!(5, u32::deserialize(&mut cursor).unwrap());
        assert!(u32::deserialize(&mut cursor).is_err());
    }

    #[test]
    fn test_serialize_string() {
        let mut buffer: Vec<u8> = Vec::new();
        let first_length = 4 + 3 * 4;
        let second_length = 4 + 3 * 8;
        {
            let x: String = String::from("ぽよぽよ");
            assert_eq!(x.serialize(&mut buffer).unwrap(), first_length);
            assert_eq!(buffer.len(), first_length);
        }
        {
            let x: String = String::from("富士さん見える。");
            assert_eq!(x.serialize(&mut buffer).unwrap(), second_length);
            assert_eq!(buffer.len(), first_length + second_length);
        }
        let mut cursor = Cursor::new(&buffer[..]);
        assert_eq!("ぽよぽよ", String::deserialize(&mut cursor).unwrap());
        assert_eq!("富士さん見える。", String::deserialize(&mut cursor).unwrap());
        assert!(u32::deserialize(&mut cursor).is_err());
    }

    #[test]
    fn test_serialize_vec() {
        let mut buffer: Vec<u8> = Vec::new();
        let first_length = 4 + 3 * 4;
        let second_length = 4 + 3 * 8;
        let vec = vec!(String::from("ぽよぽよ"), String::from("富士さん見える。"));
        assert_eq!(vec.serialize(&mut buffer).unwrap(), first_length + second_length + 4);
        let mut cursor = Cursor::new(&buffer[..]);
        {
            let deser: Vec<String> = Vec::deserialize(&mut cursor).unwrap();
            assert_eq!(deser.len(), 2);
            assert_eq!("ぽよぽよ", deser[0]);
            assert_eq!("富士さん見える。", deser[1]);
        }
    }


}
