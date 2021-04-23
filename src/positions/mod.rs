/// Positions are stored in three parts and over two files.
//
/// The `SegmentComponent::Positions` file contains all of the bitpacked positions delta,
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
/// The `SegmentComponent::PositionsSKIP` file contains the number of bits used in each block in `u8`
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
use bitpacking::{BitPacker, BitPacker4x};

const COMPRESSION_BLOCK_SIZE: usize = BitPacker4x::BLOCK_LEN;

#[cfg(test)]
pub mod tests {

    use super::PositionSerializer;
    use crate::directory::OwnedBytes;
    use crate::positions::reader::PositionReader;
    use std::iter;

    fn create_positions_data(vals: &[u32]) -> OwnedBytes {
        let mut positions_buffer = vec![];
        {
            let mut serializer = PositionSerializer::new(&mut positions_buffer);
            for &val in vals {
                serializer.write_all(&[val]).unwrap();
            }
            serializer.close_term().unwrap();
            serializer.close().unwrap();
        }
        OwnedBytes::new(positions_buffer)
    }

    #[test]
    fn test_position_read() {
        let v: Vec<u32> = (0..1000).collect();
        let positions_data = create_positions_data(&v[..]);
        assert_eq!(positions_data.len(), 1224);
        let mut position_reader = PositionReader::new(positions_data).unwrap();
        for &n in &[1, 10, 127, 128, 130, 312] {
            let mut v = vec![0u32; n];
            position_reader.read(0, &mut v[..]);
            for i in 0..n {
                assert_eq!(v[i], i as u32);
            }
        }
    }

    #[test]
    fn test_position_read_with_offset() {
        let v: Vec<u32> = (0..1000).collect();
        let positions_data = create_positions_data(&v[..]);
        assert_eq!(positions_data.len(), 1224);
        let mut position_reader = PositionReader::new(positions_data).unwrap();
        for &offset in &[1u64, 10u64, 127u64, 128u64, 130u64, 312u64] {
            for &len in &[1, 10, 130, 500] {
                let mut v = vec![0u32; len];
                position_reader.read(offset, &mut v[..]);
                for i in 0..len {
                    assert_eq!(v[i], i as u32 + offset as u32);
                }
            }
        }
    }

    #[test]
    fn test_position_read_after_skip() {
        let v: Vec<u32> = (0..1_000).collect();
        let positions_data = create_positions_data(&v[..]);
        assert_eq!(positions_data.len(), 1224);

        let mut position_reader = PositionReader::new(positions_data).unwrap();
        let mut buf = [0u32; 7];
        let mut c = 0;

        let mut offset = 0;
        for _ in 0..100 {
            position_reader.read(offset, &mut buf);
            position_reader.read(offset, &mut buf);
            offset += 7;
            for &el in &buf {
                assert_eq!(c, el);
                c += 1;
            }
        }
    }

    #[test]
    fn test_position_reread_anchor_different_than_block() {
        let positions_delta: Vec<u32> = (0..2_000_000).collect();
        let positions_data = create_positions_data(&positions_delta[..]);
        assert_eq!(positions_data.len(), 5003499);
        let mut position_reader = PositionReader::new(positions_data.clone()).unwrap();
        let mut buf = [0u32; 256];
        position_reader.read(128, &mut buf);
        for i in 0..256 {
            assert_eq!(buf[i], (128 + i) as u32);
        }
        position_reader.read(128, &mut buf);
        for i in 0..256 {
            assert_eq!(buf[i], (128 + i) as u32);
        }
    }

    #[test]
    #[should_panic(expected = "offset arguments should be increasing.")]
    fn test_position_panic_if_called_previous_anchor() {
        let positions_delta: Vec<u32> = (0..2_000_000).collect();
        let positions_data = create_positions_data(&positions_delta[..]);
        assert_eq!(positions_data.len(), 5_003_499);
        let mut buf = [0u32; 1];
        let mut position_reader = PositionReader::new(positions_data).unwrap();
        position_reader.read(230, &mut buf);
        position_reader.read(9, &mut buf);
    }

    #[test]
    fn test_positions_bug() {
        let mut positions_delta: Vec<u32> = vec![];
        for i in 1..200 {
            for j in 0..i {
                positions_delta.push(j);
            }
        }
        let positions_data = create_positions_data(&positions_delta[..]);
        let mut buf = Vec::new();
        let mut position_reader = PositionReader::new(positions_data).unwrap();
        let mut offset = 0;
        for i in 1..24 {
            buf.resize(i, 0);
            offset += i as u64;
            position_reader.read(offset, &mut buf[..]);
            let expected_positions_delta: Vec<u32> = (0..i as u32).collect();
            assert_eq!(buf, &expected_positions_delta[..], "Failed for offset={},i={}", offset, i);
        }
    }

    #[test]
    fn test_position() {
        const CONST_VAL: u32 = 9u32;
        let positions_delta: Vec<u32> = iter::repeat(CONST_VAL).take(2_000_000).collect();
        let positions_data = create_positions_data(&positions_delta[..]);
        assert_eq!(positions_data.len(), 1_015_627);
        let mut position_reader = PositionReader::new(positions_data).unwrap();
        let mut buf = [0u32; 1];
        position_reader.read(0, &mut buf);
        assert_eq!(buf[0], CONST_VAL);
    }

    #[test]
    fn test_position_advance() {
        let positions_delta: Vec<u32> = (0..2_000_000).collect();
        let positions_data = create_positions_data(&positions_delta[..]);
        assert_eq!(positions_data.len(), 5_003_499);
        for &offset in &[
            10,
            128 * 1024,
            128 * 1024 - 1,
            128 * 1024 + 7,
            128 * 10 * 1024 + 10,
        ] {
            let mut position_reader = PositionReader::new(positions_data.clone()).unwrap();
            let mut buf = [0u32; 1];
            position_reader.read(offset, &mut buf);
            assert_eq!(buf[0], offset as u32);
        }
    }
}
