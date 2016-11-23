mod serialize;
mod timer;
mod vint;

pub use self::serialize::BinarySerializable;
pub use self::timer::Timing;
pub use self::timer::TimerTree;
pub use self::timer::OpenTimer;
pub use self::vint::VInt;
use std::io;

pub fn make_io_err(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Other, msg)
}


/// Has length trait
pub trait HasLen {
    /// Return length
    fn len(&self,) -> usize;
    
    /// Returns true iff empty.
    fn is_empty(&self,) -> bool {
        self.len() == 0
    }
}


fn count_leading_zeros(mut val: u32) -> u8 {
    if val == 0 {
        return 32;
    }
    let mut result = 0u8;
    while (val & (1u32 << 31)) == 0 {
        val <<= 1;
        result += 1;
    }
    result
}


pub fn compute_num_bits(amplitude: u32) -> u8 {
    32u8 - count_leading_zeros(amplitude)
}



#[cfg(test)]
mod test {
    use super::compute_num_bits;
    
    fn test_compute_num_bits() {
        assert_eq!(compute_num_bits(1), 1u8);
        assert_eq!(compute_num_bits(0), 0u8);
        assert_eq!(compute_num_bits(2), 2u8);
        assert_eq!(compute_num_bits(3), 2u8);
        assert_eq!(compute_num_bits(4), 3u8);
        assert_eq!(compute_num_bits(255), 8u8);
        assert_eq!(compute_num_bits(256), 9u8);
    }
}