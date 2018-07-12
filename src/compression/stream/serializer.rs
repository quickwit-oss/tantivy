use std::io;
use bitpacking::{BitPacker4x, BitPacker};
use compression::{compressed_block_size, COMPRESSION_BLOCK_SIZE};
use owned_read::OwnedRead;

lazy_static! {
    static ref BIT_PACKER: BitPacker4x = BitPacker4x::new();
}

pub struct PositionSerializer<W: io::Write> {
    write_stream: W,
    write_skiplist: W,
    block: Vec<u32>,
    buffer: Vec<u8>,
    bitpacker: BitPacker4x,
    num_ints: u64,
}

impl<W: io::Write> PositionSerializer<W> {
    pub fn new(write_stream: W, write_skiplist: W) -> PositionSerializer<W> {
        PositionSerializer {
            write_stream,
            write_skiplist,
            block: Vec::with_capacity(128),
            buffer: vec![0u8; 128 * 4],
            bitpacker: BitPacker4x::new(),
            num_ints: 0u64,
        }
    }

    pub fn positions_idx(&self) -> u64 {
        self.num_ints
    }

    pub fn write(&mut self, val: u32) -> io::Result<()> {
        self.block.push(val);
        self.num_ints += 1;
        if self.block.len() == COMPRESSION_BLOCK_SIZE {
            self.flush_block()?;
        }
        Ok(())
    }

    pub fn write_all(&mut self, vals: &[u32]) -> io::Result<()> {
        // TODO optimize
        for &val in vals {
            self.write(val)?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> io::Result<()> {
        let num_bits = BIT_PACKER.num_bits(&self.block[..]);
        self.write_skiplist.write(&[num_bits])?;
        let written_len = BIT_PACKER.compress(&self.block[..], &mut self.buffer, num_bits);
        self.write_stream.write_all(&self.buffer[..written_len])?;
        self.block.clear();
        Ok(())
    }

    pub fn close(mut self) -> io::Result<()> {
        if !self.block.is_empty() {
            self.block.resize(COMPRESSION_BLOCK_SIZE, 0u32);
            self.flush_block()?;
        }
        self.write_skiplist.flush()?;
        self.write_stream.flush()?;
        Ok(())
    }
}


pub struct PositionReader {
    skip_read: OwnedRead,
    position_read: OwnedRead,
    inner_offset: usize,
    buffer: Box<[u32; 128]>,
    ahead: usize,
}

fn read_impl(
    mut position: &[u8],
    buffer: &mut [u32; 128],
    mut inner_offset: usize,
    num_bits: &[u8],
    output: &mut [u32]) -> usize {
    let mut output_start = 0;
    let mut output_len = output.len();
    let mut ahead = 0;
    loop {
        let available_len = 128 - inner_offset;
        if output_len <= available_len {
            output[output_start..].copy_from_slice(&buffer[inner_offset..][..output_len]);
            return ahead;
        } else {
            output[output_start..][..available_len].copy_from_slice(&buffer[inner_offset..]);
            output_len -= available_len;
            output_start += available_len;
            inner_offset = 0;
            let num_bits = num_bits[ahead];
            BitPacker4x::new()
                .decompress(position, &mut buffer[..], num_bits);
            let block_len = compressed_block_size(num_bits);
            position = &position[block_len..];
            ahead += 1;
        }
    }
}


impl PositionReader {
    pub fn new(position_read: OwnedRead, skip_read: OwnedRead) -> PositionReader {
        PositionReader {
            skip_read,
            position_read,
            inner_offset: 0,
            buffer: Box::new([0u32; 128]),
            ahead: usize::max_value(),
        }
    }

    /// Fills a buffer with the next `output.len()` integers.
    /// This does not consume / advance the stream.
    pub fn read(&mut self, output: &mut [u32]) {
        let skip_data = self.skip_read.as_ref();
        let position_data = self.position_read.as_ref();
        let num_bits = self.skip_read.get(0);
        if self.ahead != 0 {
            // the block currently available is not the block
            // for the current position
            BIT_PACKER.decompress(position_data, self.buffer.as_mut(), num_bits);
        }
        let block_len = compressed_block_size(num_bits);
        self.ahead = read_impl(
            &position_data[block_len..],
                self.buffer.as_mut(),
                self.inner_offset,
            &skip_data[1..],
            output);
    }

    /// Skip the next `skip_len` integer.
    ///
    /// If a full block is skipped, calling
    /// `.skip(...)` will avoid decompressing it.
    ///
    /// May panic if the end of the stream is reached.
    pub fn skip(&mut self, skip_len: usize) {
        // let residual_skip_len = skip_len - self.inner_offset;
        let num_blocks_to_advance = (skip_len + self.inner_offset) / COMPRESSION_BLOCK_SIZE;
        self.inner_offset = (self.inner_offset + skip_len) % COMPRESSION_BLOCK_SIZE;

        // TODO use an Option?
        if self.ahead < num_blocks_to_advance {
            self.ahead = usize::max_value();
        } else {
            self.ahead -= num_blocks_to_advance;
        }

        let skip_len = self.skip_read
            .as_ref()[..num_blocks_to_advance]
            .iter()
            .cloned()
            .map(|num_bit| num_bit as usize)
            .sum::<usize>() * (COMPRESSION_BLOCK_SIZE / 8);

        self.skip_read.advance(num_blocks_to_advance);
        self.position_read.advance(skip_len);
    }
}

#[cfg(test)]
pub mod tests {

    use super::{PositionSerializer, PositionReader};
    use directory::ReadOnlySource;
    use owned_read::OwnedRead;

    fn create_stream_buffer(vals: &[u32]) -> (ReadOnlySource, ReadOnlySource) {
        let mut skip_buffer = vec![];
        let mut stream_buffer = vec![];
        {
            let mut serializer = PositionSerializer::new(&mut stream_buffer, &mut skip_buffer);
            for &i in vals {
                assert_eq!(serializer.positions_idx(), i as u64);
                serializer.write(i).unwrap();
            }
            serializer.close().unwrap();
        }
        (ReadOnlySource::from(stream_buffer), ReadOnlySource::from(skip_buffer))
    }

    #[test]
    fn test_position_read() {
        let v: Vec<u32> = (0..1000).collect();
        let (stream, skip) = create_stream_buffer(&v[..]);
        assert_eq!(skip.len(), 8);
        assert_eq!(stream.len(), 1168);
        let mut position_reader = PositionReader::new(OwnedRead::new(stream), OwnedRead::new(skip));
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
        let v: Vec<u32> = (0..1000).collect();
        let (stream, skip) = create_stream_buffer(&v[..]);
        assert_eq!(skip.len(), 8);
        assert_eq!(stream.len(), 1168);

        let mut position_reader = PositionReader::new(OwnedRead::new(stream), OwnedRead::new(skip));
        position_reader.skip(10);
        for &n in &[127] { //, 10, 127, 128, 130, 312] {
            let mut v = vec![0u32; n];
            position_reader.read(&mut v[..n]);
            for i in 0..n {
                assert_eq!(v[i], 10u32 + i as u32);
            }
        }
    }

    #[test]
    fn test_position_read_skip() {
        let v: Vec<u32> = (0..1000).collect();
        let (stream, skip) = create_stream_buffer(&v[..]);
        assert_eq!(skip.len(), 8);
        assert_eq!(stream.len(), 1168);

        let mut position_reader = PositionReader::new(OwnedRead::new(stream), OwnedRead::new(skip));
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
}