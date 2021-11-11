use std::io::{self, Write};
use std::usize;

use merge::ValueMerger;

mod delta;
pub mod merge;
pub mod value;

pub(crate) mod sstable_index;

pub(crate) use self::sstable_index::{SSTableIndex, SSTableIndexBuilder};
pub(crate) mod vint;

mod block_reader;
pub use self::block_reader::BlockReader;
pub use self::delta::DeltaReader;
use self::delta::DeltaWriter;
pub use self::merge::VoidMerge;
use self::value::{U64MonotonicReader, U64MonotonicWriter, ValueReader, ValueWriter};

const DEFAULT_KEY_CAPACITY: usize = 50;

pub(crate) fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .cloned()
        .zip(right.iter().cloned())
        .take_while(|(left, right)| left == right)
        .count()
}

pub trait SSTable: Sized {
    type Value;
    type Reader: ValueReader<Value = Self::Value>;
    type Writer: ValueWriter<Value = Self::Value>;

    fn delta_writer<W: io::Write>(write: W) -> DeltaWriter<W, Self::Writer> {
        DeltaWriter::new(write)
    }

    fn writer<W: io::Write>(write: W) -> Writer<W, Self::Writer> {
        Writer {
            previous_key: Vec::with_capacity(DEFAULT_KEY_CAPACITY),
            num_terms: 0u64,
            index_builder: SSTableIndexBuilder::default(),
            delta_writer: Self::delta_writer(write),
            first_ordinal_of_the_block: 0u64,
        }
    }

    fn delta_reader<'a, R: io::Read + 'a>(reader: R) -> DeltaReader<'a, Self::Reader> {
        DeltaReader::new(reader)
    }

    fn reader<'a, R: io::Read + 'a>(reader: R) -> Reader<'a, Self::Reader> {
        Reader {
            key: Vec::with_capacity(DEFAULT_KEY_CAPACITY),
            delta_reader: Self::delta_reader(reader),
        }
    }

    fn merge<R: io::Read, W: io::Write, M: ValueMerger<Self::Value>>(
        io_readers: Vec<R>,
        w: W,
        merger: M,
    ) -> io::Result<()> {
        let readers: Vec<_> = io_readers.into_iter().map(Self::reader).collect();
        let writer = Self::writer(w);
        merge::merge_sstable::<Self, _, _>(readers, writer, merger)
    }
}

#[allow(dead_code)]
pub struct VoidSSTable;

impl SSTable for VoidSSTable {
    type Value = ();
    type Reader = value::VoidReader;
    type Writer = value::VoidWriter;
}

#[allow(dead_code)]
pub struct SSTableMonotonicU64;

impl SSTable for SSTableMonotonicU64 {
    type Value = u64;

    type Reader = U64MonotonicReader;

    type Writer = U64MonotonicWriter;
}

pub struct Reader<'a, TValueReader> {
    key: Vec<u8>,
    delta_reader: DeltaReader<'a, TValueReader>,
}

impl<'a, TValueReader> Reader<'a, TValueReader>
where TValueReader: ValueReader
{
    pub fn advance(&mut self) -> io::Result<bool> {
        if !self.delta_reader.advance()? {
            return Ok(false);
        }
        let common_prefix_len = self.delta_reader.common_prefix_len();
        let suffix = self.delta_reader.suffix();
        let new_len = self.delta_reader.common_prefix_len() + suffix.len();
        self.key.resize(new_len, 0u8);
        self.key[common_prefix_len..].copy_from_slice(suffix);
        Ok(true)
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn value(&self) -> &TValueReader::Value {
        self.delta_reader.value()
    }
}

impl<'a, TValueReader> AsRef<[u8]> for Reader<'a, TValueReader> {
    fn as_ref(&self) -> &[u8] {
        &self.key
    }
}

pub struct Writer<W, TValueWriter>
where W: io::Write
{
    previous_key: Vec<u8>,
    index_builder: SSTableIndexBuilder,
    delta_writer: DeltaWriter<W, TValueWriter>,
    num_terms: u64,
    first_ordinal_of_the_block: u64,
}

impl<W, TValueWriter> Writer<W, TValueWriter>
where
    W: io::Write,
    TValueWriter: value::ValueWriter,
{
    pub(crate) fn current_key(&self) -> &[u8] {
        &self.previous_key[..]
    }

    pub fn write_key(&mut self, key: &[u8]) {
        let keep_len = common_prefix_len(&self.previous_key, key);
        let add_len = key.len() - keep_len;
        let increasing_keys = add_len > 0 && (self.previous_key.len() == keep_len)
            || self.previous_key.is_empty()
            || self.previous_key[keep_len] < key[keep_len];
        assert!(
            increasing_keys,
            "Keys should be increasing. ({:?} > {:?})",
            self.previous_key, key
        );
        self.previous_key.resize(key.len(), 0u8);
        self.previous_key[keep_len..].copy_from_slice(&key[keep_len..]);
        self.delta_writer.write_suffix(keep_len, &key[keep_len..]);
    }

    #[allow(dead_code)]
    pub fn write(&mut self, key: &[u8], value: &TValueWriter::Value) -> io::Result<()> {
        self.write_key(key);
        self.write_value(value)?;
        Ok(())
    }

    pub fn write_value(&mut self, value: &TValueWriter::Value) -> io::Result<()> {
        self.delta_writer.write_value(value);
        self.num_terms += 1u64;
        self.flush_block_if_required()
    }

    pub fn flush_block_if_required(&mut self) -> io::Result<()> {
        if let Some(byte_range) = self.delta_writer.flush_block_if_required()? {
            self.index_builder.add_block(
                &self.previous_key[..],
                byte_range,
                self.first_ordinal_of_the_block,
            );
            self.first_ordinal_of_the_block = self.num_terms;
            self.previous_key.clear();
        }
        Ok(())
    }

    pub fn finalize(mut self) -> io::Result<W> {
        if let Some(byte_range) = self.delta_writer.flush_block()? {
            self.index_builder.add_block(
                &self.previous_key[..],
                byte_range,
                self.first_ordinal_of_the_block,
            );
            self.first_ordinal_of_the_block = self.num_terms;
        }
        let mut wrt = self.delta_writer.finalize();
        wrt.write_all(&0u32.to_le_bytes())?;

        let offset = wrt.written_bytes();

        self.index_builder.serialize(&mut wrt)?;
        wrt.write_all(&offset.to_le_bytes())?;
        wrt.write_all(&self.num_terms.to_le_bytes())?;
        let wrt = wrt.finish();
        Ok(wrt.into_inner()?)
    }
}
#[cfg(test)]
mod test {
    use std::io;

    use super::{common_prefix_len, SSTable, SSTableMonotonicU64, VoidMerge, VoidSSTable};

    fn aux_test_common_prefix_len(left: &str, right: &str, expect_len: usize) {
        assert_eq!(
            common_prefix_len(left.as_bytes(), right.as_bytes()),
            expect_len
        );
        assert_eq!(
            common_prefix_len(right.as_bytes(), left.as_bytes()),
            expect_len
        );
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
            assert!(sstable_writer.write(&[0, 3, 4], &()).is_ok());
            assert!(sstable_writer.write(&long_key2[..], &()).is_ok());
            assert!(sstable_writer.finalize().is_ok());
        }
        let mut sstable_reader = VoidSSTable::reader(&buffer[..]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &long_key[..]);
        assert!(sstable_reader.advance().unwrap());
        assert_eq!(sstable_reader.key(), &[0, 3, 4]);
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
        assert_eq!(
            &buffer,
            &[
                // block len
                7u8, 0u8, 0u8, 0u8, // keep 0 push 1 |  ""
                16u8, 17u8, // keep 1 push 2 | 18 19
                33u8, 18u8, 19u8, // keep 1 push 1 | 20
                17u8, 20u8, 0u8, 0u8, 0u8, 0u8, // no more blocks
                // index
                161, 102, 98, 108, 111, 99, 107, 115, 129, 162, 104, 108, 97, 115, 116, 95, 107,
                101, 121, 130, 17, 20, 106, 98, 108, 111, 99, 107, 95, 97, 100, 100, 114, 162, 106,
                98, 121, 116, 101, 95, 114, 97, 110, 103, 101, 162, 101, 115, 116, 97, 114, 116, 0,
                99, 101, 110, 100, 11, 109, 102, 105, 114, 115, 116, 95, 111, 114, 100, 105, 110,
                97, 108, 0, 15, 0, 0, 0, 0, 0, 0, 0, // offset for the index
                3u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8 // num terms
            ]
        );
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

    #[test]
    fn test_sstable() {
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

    #[test]
    fn test_sstable_u64() -> io::Result<()> {
        let mut buffer = Vec::new();
        let mut writer = SSTableMonotonicU64::writer(&mut buffer);
        writer.write(b"abcd", &1u64)?;
        writer.write(b"abe", &4u64)?;
        writer.write(b"gogo", &4324234234234234u64)?;
        writer.finalize()?;
        let mut reader = SSTableMonotonicU64::reader(&buffer[..]);
        assert!(reader.advance()?);
        assert_eq!(reader.key(), b"abcd");
        assert_eq!(reader.value(), &1u64);
        assert!(reader.advance()?);
        assert_eq!(reader.key(), b"abe");
        assert_eq!(reader.value(), &4u64);
        assert!(reader.advance()?);
        assert_eq!(reader.key(), b"gogo");
        assert_eq!(reader.value(), &4324234234234234u64);
        assert!(!reader.advance()?);
        Ok(())
    }
}
