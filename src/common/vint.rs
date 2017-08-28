use super::BinarySerializable;
use std::io;
use std::io::Write;
use std::io::Read;



///   Wrapper over a `u64` that serializes as a variable int.
#[derive(Debug, Eq, PartialEq)]
pub struct VInt(pub u64);

impl VInt {
    pub fn val(&self) -> u64 {
        self.0
    }
}

impl BinarySerializable for VInt {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut remaining = self.0;
        let mut buffer = [0u8; 10];
        let mut i = 0;
        loop {
            let next_byte: u8 = (remaining % 128u64) as u8;
            remaining /= 128u64;
            if remaining == 0u64 {
                buffer[i] = next_byte | 128u8;
                return writer.write_all(&buffer[0..i + 1]);
            } else {
                buffer[i] = next_byte;
            }
            i += 1;
        }

    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut bytes = reader.bytes();
        let mut result = 0u64;
        let mut shift = 0u64;
        loop {
            match bytes.next() {
                Some(Ok(b)) => {
                    result += ((b % 128u8) as u64) << shift;
                    if b & 128u8 != 0u8 {
                        break;
                    }
                    shift += 7;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Reach end of buffer",
                    ))
                }
            }
        }
        Ok(VInt(result))
    }
}
