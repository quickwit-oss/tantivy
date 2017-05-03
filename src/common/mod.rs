mod serialize;
mod timer;
mod vint;
pub mod bitpacker;

pub use self::serialize::BinarySerializable;
pub use self::timer::Timing;
pub use self::timer::TimerTree;
pub use self::timer::OpenTimer;
pub use self::vint::VInt;
use std::io;

/// Create a default io error given a string.
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


/// Creates an uninitialized Vec of a given usize
///
/// `allocate_vec` does an unsafe call to `set_len`
/// as other solution are extremely slow in debug mode.
pub fn allocate_vec<T>(capacity: usize) -> Vec<T> {
    let mut v = Vec::with_capacity(capacity);
    unsafe {
        v.set_len(capacity);
    }
    v
}


const HIGHEST_BIT: u64 = 1 << 63;

#[inline(always)]
pub fn i64_to_u64(val: i64) -> u64 {
    (val as u64) ^ HIGHEST_BIT
}

#[inline(always)]
pub fn u64_to_i64(val: u64) -> i64 {
    (val ^ HIGHEST_BIT) as i64
}



#[cfg(test)]
mod test {

    use super::{i64_to_u64, u64_to_i64};

    fn test_i64_converter_helper(val: i64) {
        assert_eq!(u64_to_i64(i64_to_u64(val)), val);
    }

    #[test]
    fn test_i64_converter() {
        assert_eq!(i64_to_u64(i64::min_value()), u64::min_value());
        assert_eq!(i64_to_u64(i64::max_value()), u64::max_value());
        test_i64_converter_helper(0i64);
        test_i64_converter_helper(i64::min_value());
        test_i64_converter_helper(i64::max_value());
        for i in -1000i64..1000i64 {
            test_i64_converter_helper(i);
        }
    }
}