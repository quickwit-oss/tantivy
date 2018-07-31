use super::BinarySerializable;
use std::io;
use std::io::Read;
use std::io::Write;

///   Wrapper over a `u64` that serializes as a variable int.
#[derive(Debug, Eq, PartialEq)]
pub struct VInt(pub u64);

const STOP_BIT: u8 = 128;

impl VInt {


    pub fn val(&self) -> u64 {
        self.0
    }

    pub fn deserialize_u64<R: Read>(reader: &mut R) -> io::Result<u64> {
        VInt::deserialize(reader).map(|vint| vint.0)
    }

    pub fn serialize_into_vec(&self, output: &mut Vec<u8>){
        let mut buffer = [0u8; 10];
        let num_bytes = self.serialize_into(&mut buffer);
        output.extend(&buffer[0..num_bytes]);
    }

    fn serialize_into(&self, buffer: &mut [u8; 10]) -> usize {

        let mut remaining = self.0;
        for (i, b) in buffer.iter_mut().enumerate() {
            let next_byte: u8 = (remaining % 128u64) as u8;
            remaining /= 128u64;
            if remaining == 0u64 {
                *b = next_byte | STOP_BIT;
                return i + 1;
            } else {
                *b = next_byte;
            }
        }
        unreachable!();
    }
}

impl BinarySerializable for VInt {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let mut buffer = [0u8; 10];
        let num_bytes = self.serialize_into(&mut buffer);
        writer.write_all(&buffer[0..num_bytes])
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut bytes = reader.bytes();
        let mut result = 0u64;
        let mut shift = 0u64;
        loop {
            match bytes.next() {
                Some(Ok(b)) => {
                    result |= u64::from(b % 128u8) << shift;
                    if b >= STOP_BIT {
                        return Ok(VInt(result));
                    }
                    shift += 7;
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Reach end of buffer while reading VInt",
                    ))
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {

    use super::VInt;
    use common::BinarySerializable;

    fn aux_test_vint(val: u64) {
        let mut v = [14u8; 10];
        let num_bytes = VInt(val).serialize_into(&mut v);
        for i in num_bytes..10 {
            assert_eq!(v[i], 14u8);
        }
        assert!(num_bytes > 0);
        if num_bytes < 10 {
            assert!(1u64 << (7*num_bytes) > val);
        }
        if num_bytes > 1 {
            assert!(1u64 << (7*(num_bytes-1)) <= val);
        }
        let serdeser_val = VInt::deserialize(&mut &v[..]).unwrap();
        assert_eq!(val, serdeser_val.0);
    }

    #[test]
    fn test_vint() {
        aux_test_vint(0);
        aux_test_vint(1);
        aux_test_vint(5);
        aux_test_vint(u64::max_value());
        for i in 1..9 {
            let power_of_128 = 1u64 << (7*i);
            aux_test_vint(power_of_128 - 1u64);
            aux_test_vint(power_of_128 );
            aux_test_vint(power_of_128 + 1u64);
        }
        aux_test_vint(10);
    }
}