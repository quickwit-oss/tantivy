
/// Positions are stored in three parts and over two files.
//
/// The `SegmentComponent::POSITIONS` file contains all of the bitpacked positions delta,
/// for all terms of a given field, one term after the other.
///
/// If the last block is incomplete, it is simply padded with zeros.
/// It cannot be read alone, as it actually does not contain the number of bits used for
/// each blocks.
/// .
/// Each block is serialized one after the other.
/// If the last block is incomplete, it is simply padded with zeros.
///
///
/// The `SegmentComponent::POSITIONSSKIP` file contains the number of bits used in each block in `u8`
/// stream.
///
/// This makes it possible to rapidly skip over `n positions`.
///
/// For every block #n where n = k * `LONG_SKIP_INTERVAL` blocks (k>=1), we also store
/// in this file the sum of number of bits used for all of the previous block (blocks `[0, n[`).
/// That is useful to start reading the positions for a given term: The TermInfo contains
/// an address in the positions stream, expressed in "number of positions".
/// The long skip structure makes it possible to skip rapidly to the a checkpoint close to this
/// value, and then skip normally.
///

mod reader;
mod serializer;

pub use self::reader::PositionReader;
pub use self::serializer::PositionSerializer;
use bitpacking::{BitPacker4x, BitPacker};

const COMPRESSION_BLOCK_SIZE: usize = BitPacker4x::BLOCK_LEN;
const LONG_SKIP_IN_BLOCKS: usize = 1_024;
const LONG_SKIP_INTERVAL: u64 = (LONG_SKIP_IN_BLOCKS * COMPRESSION_BLOCK_SIZE) as u64;

lazy_static! {
    static ref BIT_PACKER: BitPacker4x = BitPacker4x::new();
}

#[cfg(test)]
pub mod tests {

    use std::iter;
    use super::{PositionSerializer, PositionReader};
    use directory::ReadOnlySource;
    use positions::COMPRESSION_BLOCK_SIZE;

    fn create_stream_buffer(vals: &[u32]) -> (ReadOnlySource, ReadOnlySource) {
        let mut skip_buffer = vec![];
        let mut stream_buffer = vec![];
        {
            let mut serializer = PositionSerializer::new(&mut stream_buffer, &mut skip_buffer);
            for (i, &val) in vals.iter().enumerate() {
                assert_eq!(serializer.positions_idx(), i as u64);
                serializer.write_all(&[val]).unwrap();
            }
            serializer.close().unwrap();
        }
        (ReadOnlySource::from(stream_buffer), ReadOnlySource::from(skip_buffer))
    }

    #[test]
    fn test_position_read() {
        let v: Vec<u32> = (0..1000).collect();
        let (stream, skip) = create_stream_buffer(&v[..]);
        assert_eq!(skip.len(), 12);
        assert_eq!(stream.len(), 1168);
        let mut position_reader = PositionReader::new(stream, skip, 0u64);
        for &n in &[1, 10, 127, 128, 130, 312] {
            let mut v = vec![0u32; n];
            position_reader.read(&mut v[..n]);
            for i in 0..n {
                assert_eq!(v[i], i as u32);
            }
        }
    }

    #[test]
    fn test_position_skip() {
        let v: Vec<u32> = (0..1_000).collect();
        let (stream, skip) = create_stream_buffer(&v[..]);
        assert_eq!(skip.len(), 12);
        assert_eq!(stream.len(), 1168);

        let mut position_reader = PositionReader::new(stream, skip, 0u64);
        position_reader.skip(10);
        for &n in &[10, 127, COMPRESSION_BLOCK_SIZE, 130, 312] {
            let mut v = vec![0u32; n];
            position_reader.read(&mut v[..n]);
            for i in 0..n {
                assert_eq!(v[i], 10u32 + i as u32);
            }
        }
    }

    #[test]
    fn test_position_read_after_skip() {
        let v: Vec<u32> = (0..1_000).collect();
        let (stream, skip) = create_stream_buffer(&v[..]);
        assert_eq!(skip.len(), 12);
        assert_eq!(stream.len(), 1168);

        let mut position_reader = PositionReader::new(stream,skip, 0u64);
        let mut buf = [0u32; 7];
        let mut c = 0;
        for _ in 0..100 {
            position_reader.read(&mut buf);
            position_reader.read(&mut buf);
            position_reader.skip(4);
            position_reader.skip(3);
            for &el in &buf {
                assert_eq!(c, el);
                c += 1;
            }
        }
    }

    #[test]
    fn test_position_long_skip_const() {
        const CONST_VAL: u32 = 9u32;
        let v: Vec<u32> = iter::repeat(CONST_VAL).take(2_000_000).collect();
        let (stream, skip) = create_stream_buffer(&v[..]);
        assert_eq!(skip.len(), 15_749);
        assert_eq!(stream.len(), 1_000_000);
        let mut position_reader = PositionReader::new(stream,skip, 128 * 1024);
        let mut buf = [0u32; 1];
        position_reader.read(&mut buf);
        assert_eq!(buf[0], CONST_VAL);
    }

    #[test]
    fn test_position_long_skip_2() {
        let v: Vec<u32> = (0..2_000_000).collect();
        let (stream, skip) = create_stream_buffer(&v[..]);
        assert_eq!(skip.len(), 15_749);
        assert_eq!(stream.len(), 4_987_872);
        for &offset in &[10, 128 * 1024, 128 * 1024 - 1, 128 * 1024 + 7, 128 * 10 * 1024 + 10] {
            let mut position_reader = PositionReader::new(stream.clone(),skip.clone(), offset);
            let mut buf = [0u32; 1];
            position_reader.read(&mut buf);
            assert_eq!(buf[0], offset as u32);
        }
    }
}

