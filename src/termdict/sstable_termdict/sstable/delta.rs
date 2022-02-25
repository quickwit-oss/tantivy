use std::io::{self, BufWriter, Write};
use std::ops::Range;

use common::CountingWriter;

use super::value::ValueWriter;
use super::{value, vint, BlockReader};

const FOUR_BIT_LIMITS: usize = 1 << 4;
const VINT_MODE: u8 = 1u8;
const BLOCK_LEN: usize = 32_000;

pub struct DeltaWriter<W, TValueWriter>
where W: io::Write
{
    block: Vec<u8>,
    write: CountingWriter<BufWriter<W>>,
    value_writer: TValueWriter,
}

impl<W, TValueWriter> DeltaWriter<W, TValueWriter>
where
    W: io::Write,
    TValueWriter: ValueWriter,
{
    pub fn new(wrt: W) -> Self {
        DeltaWriter {
            block: Vec::with_capacity(BLOCK_LEN * 2),
            write: CountingWriter::wrap(BufWriter::new(wrt)),
            value_writer: TValueWriter::default(),
        }
    }
}

impl<W, TValueWriter> DeltaWriter<W, TValueWriter>
where
    W: io::Write,
    TValueWriter: value::ValueWriter,
{
    pub fn flush_block(&mut self) -> io::Result<Option<Range<usize>>> {
        if self.block.is_empty() {
            return Ok(None);
        }
        let start_offset = self.write.written_bytes() as usize;
        // TODO avoid buffer allocation
        let mut buffer = Vec::new();
        self.value_writer.write_block(&mut buffer);
        let block_len = buffer.len() + self.block.len();
        self.write.write_all(&(block_len as u32).to_le_bytes())?;
        self.write.write_all(&buffer[..])?;
        self.write.write_all(&self.block[..])?;
        let end_offset = self.write.written_bytes() as usize;
        self.block.clear();
        Ok(Some(start_offset..end_offset))
    }

    fn encode_keep_add(&mut self, keep_len: usize, add_len: usize) {
        if keep_len < FOUR_BIT_LIMITS && add_len < FOUR_BIT_LIMITS {
            let b = (keep_len | add_len << 4) as u8;
            self.block.extend_from_slice(&[b])
        } else {
            let mut buf = [VINT_MODE; 20];
            let mut len = 1 + vint::serialize(keep_len as u64, &mut buf[1..]);
            len += vint::serialize(add_len as u64, &mut buf[len..]);
            self.block.extend_from_slice(&buf[..len])
        }
    }

    pub(crate) fn write_suffix(&mut self, common_prefix_len: usize, suffix: &[u8]) {
        let keep_len = common_prefix_len;
        let add_len = suffix.len();
        self.encode_keep_add(keep_len, add_len);
        self.block.extend_from_slice(suffix);
    }

    pub(crate) fn write_value(&mut self, value: &TValueWriter::Value) {
        self.value_writer.write(value);
    }

    pub fn flush_block_if_required(&mut self) -> io::Result<Option<Range<usize>>> {
        if self.block.len() > BLOCK_LEN {
            return self.flush_block();
        }
        Ok(None)
    }

    pub fn finalize(self) -> CountingWriter<BufWriter<W>> {
        self.write
    }
}

pub struct DeltaReader<'a, TValueReader> {
    common_prefix_len: usize,
    suffix_start: usize,
    suffix_end: usize,
    value_reader: TValueReader,
    block_reader: BlockReader<'a>,
    idx: usize,
}

impl<'a, TValueReader> DeltaReader<'a, TValueReader>
where TValueReader: value::ValueReader
{
    pub fn new<R: io::Read + 'a>(reader: R) -> Self {
        DeltaReader {
            idx: 0,
            common_prefix_len: 0,
            suffix_start: 0,
            suffix_end: 0,
            value_reader: TValueReader::default(),
            block_reader: BlockReader::new(Box::new(reader)),
        }
    }

    fn deserialize_vint(&mut self) -> u64 {
        self.block_reader.deserialize_u64()
    }

    fn read_keep_add(&mut self) -> Option<(usize, usize)> {
        let b = {
            let buf = &self.block_reader.buffer();
            if buf.is_empty() {
                return None;
            }
            buf[0]
        };
        self.block_reader.advance(1);
        match b {
            VINT_MODE => {
                let keep = self.deserialize_vint() as usize;
                let add = self.deserialize_vint() as usize;
                Some((keep, add))
            }
            b => {
                let keep = (b & 0b1111) as usize;
                let add = (b >> 4) as usize;
                Some((keep, add))
            }
        }
    }

    fn read_delta_key(&mut self) -> bool {
        if let Some((keep, add)) = self.read_keep_add() {
            self.common_prefix_len = keep;
            self.suffix_start = self.block_reader.offset();
            self.suffix_end = self.suffix_start + add;
            self.block_reader.advance(add);
            true
        } else {
            false
        }
    }

    pub fn advance(&mut self) -> io::Result<bool> {
        if self.block_reader.buffer().is_empty() {
            if !self.block_reader.read_block()? {
                return Ok(false);
            }
            self.value_reader.read(&mut self.block_reader)?;
            self.idx = 0;
        } else {
            self.idx += 1;
        }
        if !self.read_delta_key() {
            return Ok(false);
        }
        Ok(true)
    }

    pub fn common_prefix_len(&self) -> usize {
        self.common_prefix_len
    }

    pub fn suffix(&self) -> &[u8] {
        &self
            .block_reader
            .buffer_from_to(self.suffix_start, self.suffix_end)
    }

    pub fn value(&self) -> &TValueReader::Value {
        self.value_reader.value(self.idx)
    }
}
