use std::io::Write;
use std::io;
use core::serialize::BinarySerializable;

struct IntFastFieldWriter {
    vals: Vec<u64>,
}

impl IntFastFieldWriter {

    pub fn new() -> IntFastFieldWriter {
        IntFastFieldWriter {
            vals: Vec::new()
        }
    }

    pub fn add(&mut self, val: u64) {
        self.vals.push(val);
    }

    pub fn compute_num_bits(&self, amplitude: u64) -> u8 {
        if amplitude == 0 {
            0
        }
        else {
            1 + self.compute_num_bits(amplitude / 2)
        }
    }

    pub fn close(&self, write: &mut Write) -> io::Result<()> {
        try!((self.vals.len() as u32).serialize(write));
        if self.vals.is_empty() {
            return Ok(())
        }
        let min = self.vals.iter().min().unwrap();
        let max = self.vals.iter().max().unwrap();
        let amplitude: u64 = max - min;
        let num_bits = self.compute_num_bits(amplitude);
        for val in self.vals.iter() {
            try!(val.serialize(write));
        }
        Ok(())
    }
}


#[cfg(test)]
mod tests {

    use super::IntFastFieldWriter;

    #[test]
    fn test_intfastfieldwriter() {
        let mut write: Vec<u8> = Vec::new();
        let mut int_fast_field_writer = IntFastFieldWriter::new();
        int_fast_field_writer.add(4u64);
        int_fast_field_writer.add(14u64);
        int_fast_field_writer.add(2u64);
        int_fast_field_writer.close(&mut write).unwrap();
        assert_eq!(write.len(), 8 * 3 + 4);
    }
}
