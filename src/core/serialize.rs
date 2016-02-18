use byteorder;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;
use std::io::Write;
use core::error;
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
        let mut total_size = 0;
        writer.write_u32::<BigEndian>(self.len() as u32);
        total_size += 4;
        for it in self.iter() {
            let item_size = try!(it.serialize(writer));
            total_size += item_size;
        }
        Ok(total_size)
    }
    fn deserialize(reader: &mut Read) -> error::Result<Vec<T>> {
        // TODO error
        let num_items = reader.read_u32::<BigEndian>().unwrap();
        let mut items: Vec<T> = Vec::with_capacity(num_items as usize);
        for i in 0..num_items {
            let item = try!(T::deserialize(reader));
            items.push(item);
        }
        Ok(items)
    }
}
