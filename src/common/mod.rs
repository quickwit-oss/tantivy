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