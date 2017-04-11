mod serialize;
mod timer;
mod vint;
pub mod bitpacker;
mod counting_writer;

pub use self::serialize::BinarySerializable;
pub use self::timer::Timing;
pub use self::timer::TimerTree;
pub use self::timer::OpenTimer;
pub use self::vint::VInt;
pub use self::counting_writer::CountingWriter;

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


