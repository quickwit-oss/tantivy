use std::io;
use core::codec::SegmentSerializer;

pub trait SerializableSegment {
    fn write(&self, serializer: SegmentSerializer) -> io::Result<()>;
}
