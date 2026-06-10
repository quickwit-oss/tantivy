use std::io;

use common::OwnedBytes;

/// Codec for the positions file.
pub trait PositionsCodec: Send + Sync + 'static {
    /// The serializer type created by this codec.
    type Serializer<W: io::Write>: PositionsSerializer<W>;
    /// The reader type created by this codec.
    type Reader: PositionsReader;

    /// Creates a new positions serializer writing into `writer`.
    fn new_serializer<W: io::Write>(&self, writer: W) -> Self::Serializer<W>;

    /// Opens a positions reader from the given raw byte slice.
    fn open_reader(&self, data: OwnedBytes) -> io::Result<Self::Reader>;
}

/// Serializes delta-encoded positions for all terms in a field.
///
/// A single serializer is reused across all terms. Clients must call
/// `close_term` after each term, then `close` once when the field is done.
pub trait PositionsSerializer<W: io::Write> {
    /// Returns the total number of bytes written since this serializer was created.
    fn written_bytes(&self) -> u64;

    /// Appends delta-encoded positions for the current document.
    fn write_positions_delta(&mut self, positions_delta: &[u32]);

    /// Finalizes and flushes positions data for the current term.
    fn close_term(&mut self) -> io::Result<()>;

    /// Flushes the underlying writer. Must be called once after all terms are done.
    fn close(self) -> io::Result<()>;
}

/// Reads delta-encoded positions from a byte slice.
pub trait PositionsReader: Send + 'static {
    /// Fills `output` with delta-encoded positions starting at `offset`.
    ///
    /// Hidden contract: offset values should be non-decreasing for best performance;
    /// passing a lower offset resets internal state and incurs extra work.
    fn read(&mut self, offset: u64, output: &mut [u32]);

    /// Returns a heap-allocated clone of this reader.
    ///
    /// Needed to clone `SegmentPostings`, which owns a boxed reader.
    fn clone_box(&self) -> Box<dyn PositionsReader>;
}
