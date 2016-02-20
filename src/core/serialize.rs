use byteorder;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::io::Write;
use core::error;
use core::error::Error;
use std::io::Cursor;
use std::io::Read;

pub trait BinarySerializable : fmt::Debug + Sized {
    // TODO move Result from Error.
    fn serialize(&self, writer: &mut Write) -> error::Result<usize>;
    fn deserialize(reader: &mut Read) -> error::Result<Self>;
}

impl BinarySerializable for () {
    fn serialize(&self, writer: &mut Write) -> error::Result<usize> {
        Ok(0)
    }
    fn deserialize(reader: &mut Read) -> error::Result<Self> {
        Ok(())
    }
}

impl<T: BinarySerializable> BinarySerializable for Vec<T> {
    fn serialize(&self, writer: &mut Write) -> error::Result<usize> {
        let num_elements = self.len() as u32;
        let mut total_size = try!(num_elements.serialize(writer));
        for it in self.iter() {
            total_size += try!(it.serialize(writer));
        }
        Ok(total_size)
    }
    fn deserialize(reader: &mut Read) -> error::Result<Vec<T>> {
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
    fn serialize(&self, writer: &mut Write) -> error::Result<usize> {
        writer.write_u32::<BigEndian>(self.clone())
              .map(|x| 4)
              .map_err(Error::BinaryReadError)
    }
    fn deserialize(reader: &mut Read) -> error::Result<u32> {
        reader.read_u32::<BigEndian>()
              .map_err(Error::BinaryReadError)
    }
}

impl BinarySerializable for u64 {
    fn serialize(&self, writer: &mut Write) -> error::Result<usize> {
        writer.write_u64::<BigEndian>(self.clone())
              .map(|x| 4)
              .map_err(Error::BinaryReadError)
    }
    fn deserialize(reader: &mut Read) -> error::Result<u64> {
        reader.read_u64::<BigEndian>()
              .map_err(Error::BinaryReadError)
    }
}


impl BinarySerializable for u8 {
    fn serialize(&self, writer: &mut Write) -> error::Result<usize> {
        // TODO error
        writer.write_u8(self.clone());
        Ok(1)
    }
    fn deserialize(reader: &mut Read) -> error::Result<u8> {
        reader.read_u8()
              .map_err(Error::BinaryReadError)
    }
}

impl BinarySerializable for String {
    fn serialize(&self, writer: &mut Write) -> error::Result<usize> {
        // TODO error
        let data: &[u8] = self.as_bytes();
        let mut size = try!((data.len() as u32).serialize(writer));
        size += data.len();
        writer.write_all(data);
        Ok(size)
    }

    fn deserialize(reader: &mut Read) -> error::Result<String> {
        // TODO error
        let string_length = try!(u32::deserialize(reader)) as usize;
        let mut result = String::with_capacity(string_length);
        reader.take(string_length as u64).read_to_string(&mut result);
        Ok(result)
    }
}


#[test]
fn test_serialize_u8() {
    let mut buffer: Vec<u8> = Vec::new();
    {
        let x: u8 = 3;
        x.serialize(&mut buffer);
        assert_eq!(buffer.len(), 1);
    }
    {
        let x: u8 = 5;
        x.serialize(&mut buffer);
        assert_eq!(buffer.len(), 2);
    }
    let mut cursor = Cursor::new(&buffer);
    assert_eq!(3, u8::deserialize(&mut cursor).unwrap());
    assert_eq!(5, u8::deserialize(&mut cursor).unwrap());
    assert!(u8::deserialize(&mut cursor).is_err());
}


#[test]
fn test_serialize_u32() {
    let mut buffer: Vec<u8> = Vec::new();
    {
        let x: u32 = 3;
        x.serialize(&mut buffer);
        assert_eq!(buffer.len(), 4);
    }
    {
        let x: u32 = 5;
        x.serialize(&mut buffer);
        assert_eq!(buffer.len(), 8);
    }
    let mut cursor = Cursor::new(&buffer);
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
    let mut cursor = Cursor::new(&buffer);
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
    let mut cursor = Cursor::new(&buffer);
    {
        let deser: Vec<String> = Vec::deserialize(&mut cursor).unwrap();
        assert_eq!(deser.len(), 2);
        assert_eq!("ぽよぽよ", deser[0]);
        assert_eq!("富士さん見える。", deser[1]);
    }
}
