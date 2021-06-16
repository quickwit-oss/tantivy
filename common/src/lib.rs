pub use byteorder::LittleEndian as Endianness;

mod serialize;
mod vint;
mod writer;

pub use serialize::{BinarySerializable, DeserializeFrom, FixedSize};
pub use vint::{read_u32_vint, read_u32_vint_no_advance, serialize_vint_u32, write_u32_vint, VInt};
pub use writer::{AntiCallToken, CountingWriter, TerminatingWrite};
