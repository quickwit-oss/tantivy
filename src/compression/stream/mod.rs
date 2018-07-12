mod reader;
mod serializer;

pub use self::reader::CompressedIntStream;
pub use self::serializer::{PositionReader, PositionSerializer};

#[cfg(test)]
pub mod tests {

    use super::CompressedIntStream;
    use compression::compressed_block_size;
    use bitpacking::{BitPacker4x, BitPacker};
    use compression::COMPRESSION_BLOCK_SIZE;
    use directory::ReadOnlySource;



    fn create_stream_buffer() -> ReadOnlySource {
        let bitpacker = BitPacker4x::new();
        let mut buffer: Vec<u8> = vec![];
        let vals: Vec<u32> = (0u32..1152u32).collect();
        for chunk in vals.chunks(COMPRESSION_BLOCK_SIZE) {
            let num_bits = bitpacker.num_bits(chunk);
            buffer.push(num_bits);
            let mut compressed_block: Vec<u8> = vec![0u8; compressed_block_size(num_bits)];
            let compressed_block_len = bitpacker.compress(chunk, &mut compressed_block[..], num_bits);
            assert_eq!(compressed_block.len(), compressed_block_len);
            buffer.extend_from_slice(&compressed_block[..]);
        }
        ReadOnlySource::from(buffer)
    }

    #[test]
    fn test_compressed_int_stream() {
        let buffer = create_stream_buffer();
        let mut stream = CompressedIntStream::wrap(buffer);
        let mut block: [u32; COMPRESSION_BLOCK_SIZE] = [0u32; COMPRESSION_BLOCK_SIZE];

        stream.read(&mut block[0..2]);
        assert_eq!(block[0], 0);
        assert_eq!(block[1], 1);
        // reading does not consume the stream
        stream.read(&mut block[0..2]);
        assert_eq!(block[0], 0);
        assert_eq!(block[1], 1);
        stream.skip(2);

        stream.skip(5);
        stream.read(&mut block[0..3]);
        stream.skip(3);

        assert_eq!(block[0], 7);
        assert_eq!(block[1], 8);
        assert_eq!(block[2], 9);
        stream.skip(500);
        stream.read(&mut block[0..3]);
        stream.skip(3);

        assert_eq!(block[0], 510);
        assert_eq!(block[1], 511);
        assert_eq!(block[2], 512);
        stream.skip(511);
        stream.read(&mut block[..1]);
        assert_eq!(block[0], 1024);
    }
}
