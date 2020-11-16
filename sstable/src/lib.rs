extern crate jemallocator;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

extern crate slice_deque;
extern crate core;
extern crate byteorder;

use std::io::{self, Write, BufWriter};
use merge::ValueMerger;
use byteorder::{ByteOrder, LittleEndian};
use std::usize;

pub(crate) mod vint;
pub mod value;
pub mod merge;
mod block_reader;

pub use self::block_reader::BlockReader;

pub use self::merge::VoidMerge;

const BLOCK_LEN: usize = 256_000;
const END_CODE: u8 = 0u8;
const VINT_MODE: u8 = 1u8;

const DEFAULT_KEY_CAPACITY: usize = 50;
const FOUR_BIT_LIMITS: usize = 1 << 4;

pub(crate) fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter().cloned()
        .zip(right.iter().cloned())
        .take_while(|(left, right)| left==right)
        .count()
}

pub trait SSTable: Sized {

    type Value;
    type Reader: value::ValueReader<Value=Self::Value>;
    type Writer: value::ValueWriter<Value=Self::Value>;

    fn delta_writer<W: io::Write>(write: W) -> DeltaWriter<W, Self::Writer> {
        DeltaWriter {
            block: vec![0u8; 4],
            write: BufWriter::new(write),
            value_writer: Self::Writer::default()
        }
    }

    fn writer<W: io::Write>(write: W) -> Writer<W, Self::Writer> {
        Writer {
            previous_key: Vec::with_capacity(DEFAULT_KEY_CAPACITY),
            delta_writer: Self::delta_writer(write)
        }
    }

    fn delta_reader<'a, R: io::Read + 'a>(reader: R) -> DeltaReader<'a, Self::Reader> {
        DeltaReader {
            common_prefix_len: 0,
            suffix_start: 0,
            suffix_end: 0,
            offset: 0,
            value_reader: Self::Reader::default(),
            block_reader: BlockReader::new(Box::new(reader)),
        }
    }

    fn reader<'a, R: io::Read + 'a>(reader: R) -> Reader<'a, Self::Reader> {
        Reader {
            key: Vec::with_capacity(DEFAULT_KEY_CAPACITY),
            delta_reader: Self::delta_reader(reader)
        }
    }

    fn merge<R: io::Read, W: io::Write, M: ValueMerger<Self::Value>>(io_readers: Vec<R>, w: W, merger: M) -> io::Result<()> {
        let mut readers = vec![];
        for io_reader in io_readers.into_iter() {
            let reader = Self::reader(io_reader);
            readers.push(reader)
        }
        let writer = Self::writer(w);
        merge::merge_sstable::<Self, _, _>(readers, writer, merger)
    }
}

pub struct VoidSSTable;

impl SSTable for VoidSSTable {
    type Value = ();
    type Reader = value::VoidReader;
    type Writer = value::VoidWriter;
}


pub struct Reader<'a, TValueReader> {
    key: Vec<u8>,
    delta_reader: DeltaReader<'a, TValueReader>,
}

impl<'a, TValueReader> Reader<'a, TValueReader>
    where TValueReader: value::ValueReader {

    pub fn advance(&mut self) -> io::Result<bool> {
        if self.delta_reader.advance()? {
            let common_prefix_len = self.delta_reader.common_prefix_len();
            let suffix = self.delta_reader.suffix();
            let new_len = self.delta_reader.common_prefix_len() + suffix.len();
            self.key.resize(new_len, 0u8);
            self.key[common_prefix_len..].copy_from_slice(suffix);
            Ok(true)
        } else {
            Ok(false)
        }

    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &TValueReader::Value {
        self.delta_reader.value()
    }

    pub(crate) fn into_delta_reader(self) -> DeltaReader<'a, TValueReader> {
        assert!(self.key.is_empty());
        self.delta_reader
    }
}

impl<'a, TValueReader> AsRef<[u8]> for Reader<'a, TValueReader> {
    fn as_ref(&self) -> &[u8] {
        &self.key
    }
}


pub struct Writer<W, TValueWriter>
    where W: io::Write {
    previous_key: Vec<u8>,
    delta_writer: DeltaWriter<W, TValueWriter>,
}

impl<W, TValueWriter> Writer<W, TValueWriter>
    where W: io::Write, TValueWriter: value::ValueWriter {

    pub(crate) fn current_key(&self) -> &[u8] {
        &self.previous_key[..]
    }

    pub(crate) fn write_key(&mut self, key: &[u8]) {
        let keep_len = common_prefix_len(&self.previous_key, key);
        let add_len = key.len() - keep_len;
        let increasing_keys =
            add_len > 0 &&
                (self.previous_key.len() == keep_len ||
                    self.previous_key[keep_len] < key[keep_len]);
        assert!(increasing_keys, "Keys should be increasing. ({:?} > {:?})", self.previous_key, key);
        self.previous_key.resize(key.len(), 0u8);
        self.previous_key[keep_len..].copy_from_slice(&key[keep_len..]);
        self.delta_writer.write_suffix(
            keep_len,
            &key[keep_len..]);
    }

    pub(crate) fn into_delta_writer(self) -> DeltaWriter<W, TValueWriter> {
        self.delta_writer
    }

    pub fn write(&mut self, key: &[u8], value: &TValueWriter::Value) -> io::Result<()> {
        self.write_key(key);
        self.write_value(value);
        self.delta_writer.flush_block_if_required()?;
        Ok(())
    }

    pub(crate) fn write_value(&mut self, value: &TValueWriter::Value) {
        self.delta_writer.write_value(value)
    }

    pub fn finalize(self) -> io::Result<()> {
        self.delta_writer.finalize()
    }
}


pub struct DeltaWriter<W, TValueWriter>
    where W: io::Write {
    block: Vec<u8>,
    write: BufWriter<W>,
    value_writer: TValueWriter,
}

impl<W, TValueWriter> DeltaWriter<W, TValueWriter>
    where W: io::Write, TValueWriter: value::ValueWriter {

    fn flush_block(&mut self) -> io::Result<()> {
        let block_len = self.block.len() as u32;
        LittleEndian::write_u32(&mut self.block[..4], block_len - 4u32);
        self.write.write_all(&mut self.block[..])?;
        self.block.resize(4, 0u8);
        Ok(())
    }

    fn encode_keep_add(&mut self, keep_len: usize, add_len: usize) {
        if keep_len < FOUR_BIT_LIMITS && add_len < FOUR_BIT_LIMITS {
            let b = (keep_len | add_len << 4) as u8;
            self.block.extend_from_slice(&[b])
        } else {
            let mut buf = [1u8; 20];
            let mut len = 1 + vint::serialize(keep_len as u64, &mut buf[1..]);
            len += vint::serialize(add_len as u64, &mut buf[len..]);
            self.block.extend_from_slice(&mut buf[..len])
        }
    }

    pub(crate) fn write_suffix(&mut self, common_prefix_len: usize, suffix: &[u8]) {
        let keep_len = common_prefix_len;
        let add_len = suffix.len();
        self.encode_keep_add(keep_len, add_len);
        self.block.extend_from_slice(suffix);
    }

    pub(crate) fn write_value(&mut self, value: &TValueWriter::Value) {
        self.value_writer.write(value, &mut self.block);
    }

    pub fn write_delta(&mut self, common_prefix_len: usize, suffix: &[u8], value: &TValueWriter::Value) -> io::Result<()> {
        self.write_suffix(common_prefix_len, suffix);
        self.write_value(value);
        self.flush_block_if_required()
    }

    pub fn flush_block_if_required(&mut self) -> io::Result<()> {
        if self.block.len() > BLOCK_LEN {
            self.flush_block()?;
        }
        Ok(())
    }

    pub fn finalize(mut self) -> io::Result<()> {
        if self.block.len() > 4 {
            self.flush_block()?;
        }
        self.flush_block()?;
        Ok(())
    }
}


pub struct DeltaReader<'a, TValueReader> {
    common_prefix_len: usize,
    suffix_start: usize,
    suffix_end: usize,
    offset: usize,
    value_reader: TValueReader,
    block_reader: BlockReader<'a>,
}

impl<'a, TValueReader> DeltaReader<'a, TValueReader>
    where TValueReader: value::ValueReader {

    fn deserialize_vint(&mut self) -> u64 {
        let (consumed, result) =
            vint::deserialize_read(&self.block_reader.buffer()[self.offset..]);
        self.offset += consumed;
        result
    }

    fn read_keep_add(&mut self) -> Option<(usize, usize)> {
        let b = {
            let buf = &self.block_reader.buffer()[self.offset..];
            if buf.is_empty() {
                return None;
            }
            buf[0]
        };
        self.offset += 1;
        match b {
            END_CODE => {
                None
            }
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
            self.suffix_start = self.offset;
            self.suffix_end = self.suffix_start + add;
            self.offset += add;
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
        }
        if !self.read_delta_key() {
            return Ok(false);
        }
        self.value_reader.read(&mut self.block_reader)?;
        Ok(true)
    }

    pub fn common_prefix_len(&self) -> usize {
        self.common_prefix_len
    }

    pub fn suffix(&self) -> &[u8] {
        &self.block_reader.buffer()[self.suffix_start..self.suffix_end]
    }

    pub fn suffix_from(&self, offset: usize) -> &[u8] {
        &self.block_reader.buffer()[self.suffix_start.wrapping_add(offset).wrapping_sub(self.common_prefix_len)..self.suffix_end]
    }

    pub fn value(&self) -> &TValueReader::Value {
        self.value_reader.value()
    }
}


#[cfg(test)]
mod test {
    use common_prefix_len;
    use super::VoidSSTable;
    use super::SSTable;
    use VoidMerge;

    fn aux_test_common_prefix_len(left: &str, right: &str, expect_len: usize) {
        assert_eq!(common_prefix_len(left.as_bytes(), right.as_bytes()), expect_len);
        assert_eq!(common_prefix_len(right.as_bytes(), left.as_bytes()), expect_len);
    }

    #[test]
    fn test_common_prefix_len() {
        aux_test_common_prefix_len("a", "ab", 1);
        aux_test_common_prefix_len("", "ab", 0);
        aux_test_common_prefix_len("ab", "abc", 2);
        aux_test_common_prefix_len("abde", "abce", 2);
    }


    #[test]
    fn test_long_key_diff() {
        let long_key = (0..1_024).map(|x| (x % 255) as u8).collect::<Vec<_>>();
        let long_key2 = (1..300).map(|x| (x % 255) as u8).collect::<Vec<_>>();
        let mut buffer = vec![];
        {
            let mut sstable_writer = VoidSSTable::writer(&mut buffer);
            assert!(sstable_writer.write(&long_key[..], &()).is_ok());
            assert!(sstable_writer.write(&[0,3,4], &()).is_ok());
            assert!(sstable_writer.write(&long_key2[..], &()).is_ok());
            assert!(sstable_writer.finalize().is_ok());
        }
        let mut sstable_reader = VoidSSTable::reader(&buffer[..]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &long_key[..]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &[0,3,4]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &long_key2[..]);
        assert!(!sstable_reader.advance().unwrap());
    }

    #[test]
    fn test_simple_sstable() {
        let mut buffer = vec![];
        {
            let mut sstable_writer = VoidSSTable::writer(&mut buffer);
            assert!(sstable_writer.write(&[17u8], &()).is_ok());
            assert!(sstable_writer.write(&[17u8, 18u8, 19u8], &()).is_ok());
            assert!(sstable_writer.write(&[17u8, 20u8], &()).is_ok());
            assert!(sstable_writer.finalize().is_ok());
        }
        assert_eq!(&buffer, &[
            7,0,0,0,
            16u8, 17u8,
            33u8, 18u8, 19u8,
            17u8, 20u8,
            0u8, 0u8, 0u8, 0u8]);
        let mut sstable_reader = VoidSSTable::reader(&buffer[..]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &[17u8]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &[17u8, 18u8, 19u8]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &[17u8, 20u8]);
        assert!(!sstable_reader.advance().unwrap());
    }


    #[test]
    #[should_panic]
    fn test_simple_sstable_non_increasing_key() {
        let mut buffer = vec![];
        let mut sstable_writer = VoidSSTable::writer(&mut buffer);
        assert!(sstable_writer.write(&[17u8], &()).is_ok());
        assert!(sstable_writer.write(&[16u8], &()).is_ok());
    }

    #[test]
    fn test_merge_abcd_abe() {
        let mut buffer = Vec::new();
        {
            let mut writer = VoidSSTable::writer(&mut buffer);
            writer.write(b"abcd", &()).unwrap();
            writer.write(b"abe", &()).unwrap();
            writer.finalize().unwrap();
        }
        let mut output = Vec::new();
        assert!(VoidSSTable::merge(vec![&buffer[..], &buffer[..]], &mut output, VoidMerge).is_ok());
        assert_eq!(&output[..], &buffer[..]);
    }

}