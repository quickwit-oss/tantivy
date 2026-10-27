//! Plain-encoded `Str` and `Bytes` columns.
//!
//! Unlike dictionary encoded columns, plain columns store the values themselves, without
//! term ordinals. This is better suited to columns with a high cardinality.
//!
//! Values are stored in self-contained blocks of at most 100,000 values and at most 10 MiB of raw
//! bytes. Each block is compressed with OnPair16 and has its own dictionary, so reading a value
//! only requires reading its block.
//!
//! Lookup of a value:
//! - doc -> value idx: the `ColumnIndex`, as for any other column.
//! - value idx -> block: the block index, loaded when the column is opened.
//! - block, slot -> codes: the code offsets of the block.
//! - codes -> bytes: OnPair decoding with the dictionary of the block.
//!
//! The format is described in `LAYOUT.md`, next to this file.

mod block;
mod block_index;
mod serializer;

use std::ops::{Deref, Range};
use std::sync::Arc;
use std::{fmt, io};

use common::OwnedBytes;
use common::file_slice::FileSlice;
pub(crate) use serializer::PlainColumnSerializer;

use self::block::PlainBlock;
use self::block_index::PlainBlockIndex;
use crate::column_index::{ColumnIndex, open_column_index};
use crate::{Cardinality, DocId, RowId, Version};

/// Version of the layout of the plain column payload.
const PLAIN_FORMAT_VERSION: u8 = 1;

/// `block_index_num_bytes: u32 | column_index_num_bytes: u32 | format_version: u8`
const PLAIN_FOOTER_NUM_BYTES: usize = 9;

fn invalid_data(msg: impl ToString) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

/// A plain-encoded `Bytes` column.
///
/// Cheap to clone. Reading values requires a [`PlainValueReader`], see
/// [`PlainBytesColumn::reader`].
#[derive(Clone)]
pub struct PlainBytesColumn {
    inner: Arc<PlainBytesColumnInner>,
}

struct PlainBytesColumnInner {
    column_index: ColumnIndex,
    block_index: PlainBlockIndex,
    /// The blocks, read lazily, one at a time.
    blocks: FileSlice,
}

impl fmt::Debug for PlainBytesColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlainBytesColumn")
            .field("cardinality", &self.get_cardinality())
            .field("num_values", &self.num_values())
            .field("num_blocks", &self.num_blocks())
            .finish()
    }
}

impl PlainBytesColumn {
    /// Returns a column without any value.
    pub fn empty(num_docs: u32) -> PlainBytesColumn {
        PlainBytesColumn::from_inner(PlainBytesColumnInner {
            column_index: ColumnIndex::Empty { num_docs },
            block_index: PlainBlockIndex::empty(),
            blocks: FileSlice::empty(),
        })
    }

    /// Opens a plain column. Only the footer, the block index and the column index are read.
    pub(crate) fn open(
        file_slice: FileSlice,
        format_version: Version,
    ) -> io::Result<PlainBytesColumn> {
        let num_bytes = file_slice.num_bytes().get_bytes() as usize;
        if num_bytes < PLAIN_FOOTER_NUM_BYTES {
            return Err(invalid_data("truncated plain column"));
        }
        let footer_start = num_bytes - PLAIN_FOOTER_NUM_BYTES;
        let footer_data = file_slice.read_bytes_slice(footer_start..num_bytes)?;
        let (footer_4_bytes, _) = footer_data.as_slice()[..8].as_chunks::<4>();
        let block_index_num_bytes = u32::from_le_bytes(footer_4_bytes[0]) as usize;
        let column_index_num_bytes = u32::from_le_bytes(footer_4_bytes[1]) as usize;
        let plain_format_version = footer_data.as_slice()[8];
        if plain_format_version != PLAIN_FORMAT_VERSION {
            return Err(invalid_data(format!(
                "unknown plain column format version {plain_format_version}"
            )));
        }
        let blocks_num_bytes = footer_start
            .checked_sub(block_index_num_bytes + column_index_num_bytes)
            .ok_or_else(|| invalid_data("truncated plain column"))?;
        let metadata: OwnedBytes = file_slice.read_bytes_slice(blocks_num_bytes..footer_start)?;
        let (block_index_bytes, column_index_bytes) = metadata.split(block_index_num_bytes);
        let block_index = PlainBlockIndex::open(block_index_bytes)?;
        let column_index = open_column_index(column_index_bytes, format_version)?;
        if block_index.blocks_num_bytes() != blocks_num_bytes as u64 {
            return Err(invalid_data(
                "the plain block index does not match the blocks",
            ));
        }
        Ok(PlainBytesColumn::from_inner(PlainBytesColumnInner {
            column_index,
            block_index,
            blocks: file_slice.slice(0..blocks_num_bytes),
        }))
    }

    fn from_inner(inner: PlainBytesColumnInner) -> PlainBytesColumn {
        PlainBytesColumn {
            inner: Arc::new(inner),
        }
    }

    /// Returns the index mapping docs to value idxs.
    pub fn column_index(&self) -> &ColumnIndex {
        &self.inner.column_index
    }

    pub fn into_column_index(self) -> ColumnIndex {
        // Avoids the clone when this is the last reference to the column.
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner.column_index,
            Err(inner) => inner.column_index.clone(),
        }
    }

    pub fn get_cardinality(&self) -> Cardinality {
        self.inner.column_index.get_cardinality()
    }

    /// Returns the number of values in the column.
    pub fn num_values(&self) -> u32 {
        self.inner.block_index.num_values()
    }

    /// Returns the number of docs in the column.
    pub fn num_docs(&self) -> u32 {
        // A full column has one value per doc.
        self.inner
            .column_index
            .num_docs()
            .unwrap_or_else(|| self.num_values())
    }

    /// Returns the number of blocks in the column.
    pub fn num_blocks(&self) -> u32 {
        self.inner.block_index.num_blocks()
    }

    /// Returns the range of the blocks holding the values of `doc`.
    ///
    /// The range is empty if `doc` has no value. The values of a doc can span several blocks.
    pub fn block_range_for_doc(&self, doc: DocId) -> Range<u32> {
        let value_range = self.inner.column_index.value_row_ids(doc);
        if value_range.is_empty() {
            return 0..0;
        }
        let first_block = self.inner.block_index.block_containing(value_range.start);
        if value_range.len() == 1 {
            return first_block..first_block + 1;
        }
        let last_block = self.inner.block_index.block_containing(value_range.end - 1);
        first_block..last_block + 1
    }

    /// Returns a reader for the values of the column.
    pub fn reader(&self) -> PlainValueReader {
        PlainValueReader {
            column: self.clone(),
            current_block: None,
            codes_buffer: Vec::new(),
            value_buffer: Vec::new(),
        }
    }

    fn load_block(&self, block_id: u32) -> io::Result<PlainBlock> {
        let byte_range = self.inner.block_index.byte_range(block_id);
        let block_bytes = self
            .inner
            .blocks
            .read_bytes_slice(byte_range.start as usize..byte_range.end as usize)?;
        let num_values = self.inner.block_index.value_range(block_id).len() as u32;
        Ok(PlainBlock::open(block_bytes, num_values))
    }
}

/// A plain-encoded `Str` column. Values are checked to be valid UTF-8 when they are read as
/// strings.
#[derive(Clone)]
pub struct PlainStrColumn(PlainBytesColumn);

impl fmt::Debug for PlainStrColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlainStrColumn")
            .field("cardinality", &self.get_cardinality())
            .field("num_values", &self.num_values())
            .field("num_blocks", &self.num_blocks())
            .finish()
    }
}

impl PlainStrColumn {
    pub fn wrap(plain_bytes_column: PlainBytesColumn) -> PlainStrColumn {
        PlainStrColumn(plain_bytes_column)
    }

    pub fn empty(num_docs: u32) -> PlainStrColumn {
        PlainStrColumn(PlainBytesColumn::empty(num_docs))
    }
}

impl Deref for PlainStrColumn {
    type Target = PlainBytesColumn;

    fn deref(&self) -> &PlainBytesColumn {
        &self.0
    }
}

impl From<PlainStrColumn> for PlainBytesColumn {
    fn from(plain_str_column: PlainStrColumn) -> PlainBytesColumn {
        plain_str_column.0
    }
}

/// Reads the values of a plain column.
///
/// Keeps the last block it read: reading the values in increasing order loads each block once.
/// A reader is meant to be owned by a single consumer.
pub struct PlainValueReader {
    column: PlainBytesColumn,
    /// The id of the last block read, and the block.
    current_block: Option<(u32, PlainBlock)>,
    codes_buffer: Vec<u16>,
    value_buffer: Vec<u8>,
}

impl PlainValueReader {
    /// Returns the column read by this reader.
    pub fn column(&self) -> &PlainBytesColumn {
        &self.column
    }

    /// Makes `current_block` the block containing `value_idx`, loading it only if needed.
    ///
    /// Precondition: `value_idx < self.column.num_values()`.
    fn load_block_for_value(&mut self, value_idx: RowId) -> io::Result<()> {
        let block_index = &self.column.inner.block_index;
        // Fast path: values are usually read in increasing order.
        if let Some(current_block_id) = self.current_block_id() {
            if block_index
                .value_range(current_block_id)
                .contains(&value_idx)
            {
                return Ok(());
            }
            let next_block_id = current_block_id + 1;
            if next_block_id < block_index.num_blocks()
                && block_index.value_range(next_block_id).contains(&value_idx)
            {
                let block = self.column.load_block(next_block_id)?;
                self.current_block = Some((next_block_id, block));
                return Ok(());
            }
        }
        let block_id = block_index.block_containing(value_idx);
        let block = self.column.load_block(block_id)?;
        self.current_block = Some((block_id, block));
        Ok(())
    }

    #[inline(always)]
    fn current_block_id(&self) -> Option<u32> {
        let (current_block_id, _) = self.current_block.as_ref()?;
        Some(*current_block_id)
    }

    /// Read bytes of the value for value_idx and append them to `output`.
    ///
    /// Output is NOT cleared beforehand.
    pub fn append_value(&mut self, value_idx: RowId, output: &mut Vec<u8>) -> io::Result<()> {
        assert!(
            value_idx < self.column.num_values(),
            "value idx {value_idx} out of range (num values: {})",
            self.column.num_values()
        );
        self.load_block_for_value(value_idx)?;
        let (current_block_id, current_block) = self
            .current_block
            .as_ref()
            .expect("load_block_for_value sets the current block");
        let block_index = &self.column.inner.block_index;
        let slot = value_idx - block_index.value_range(*current_block_id).start;
        current_block.append_value(slot, &mut self.codes_buffer, output);
        Ok(())
    }

    /// Appends the value `value_idx` to `output`.
    ///
    /// Returns an `InvalidData` error if the value is not valid UTF-8. `output` is then left
    /// unchanged.
    pub fn append_str_value(&mut self, value_idx: RowId, output: &mut String) -> io::Result<()> {
        let mut value_buffer = std::mem::take(&mut self.value_buffer);
        value_buffer.clear();
        let append_result = self.append_value(value_idx, &mut value_buffer);
        let str_result = append_result.and_then(|_| {
            let value_str = std::str::from_utf8(&value_buffer)
                .map_err(|_| invalid_data("plain str value is not valid utf-8"))?;
            output.push_str(value_str);
            Ok(())
        });
        self.value_buffer = value_buffer;
        str_result
    }

    /// Appends the first value of `doc` to `output`.
    ///
    /// Returns false if `doc` has no value.
    pub fn append_first_value(&mut self, doc: DocId, output: &mut Vec<u8>) -> io::Result<bool> {
        let value_range = self.column.inner.column_index.value_row_ids(doc);
        if value_range.is_empty() {
            return Ok(false);
        }
        self.append_value(value_range.start, output)?;
        Ok(true)
    }

    /// Calls `callback` with each value of `doc`, in order.
    pub fn for_each_value(
        &mut self,
        doc: DocId,
        mut callback: impl FnMut(&[u8]),
    ) -> io::Result<()> {
        let value_range = self.column.inner.column_index.value_row_ids(doc);
        let mut value_buffer = std::mem::take(&mut self.value_buffer);
        for value_idx in value_range {
            value_buffer.clear();
            self.append_value(value_idx, &mut value_buffer)?;
            callback(&value_buffer);
        }
        self.value_buffer = value_buffer;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_index::{SerializableColumnIndex, SerializableOptionalIndex};

    fn serialize_full_column(values: &[Vec<u8>]) -> Vec<u8> {
        let mut buffer = Vec::new();
        let mut serializer = PlainColumnSerializer::new(&mut buffer);
        for value in values {
            serializer.add_value(value).unwrap();
        }
        serializer.finish(SerializableColumnIndex::Full).unwrap();
        buffer
    }

    fn open(buffer: Vec<u8>) -> PlainBytesColumn {
        PlainBytesColumn::open(FileSlice::from(buffer), Version::V3).unwrap()
    }

    fn values_for_test(num_values: usize) -> Vec<Vec<u8>> {
        (0..num_values)
            .map(|i| format!("value-{i}-{}", "abc".repeat(i % 7)).into_bytes())
            .collect()
    }

    #[test]
    fn test_full_column_roundtrip() {
        // Several blocks.
        let values = values_for_test(150_000);
        let column = open(serialize_full_column(&values));
        assert_eq!(column.num_values(), 150_000);
        assert_eq!(column.num_docs(), 150_000);
        // About 21 bytes per value: blocks are cut by the number of values.
        assert_eq!(column.num_blocks(), 2);
        assert_eq!(column.get_cardinality(), Cardinality::Full);
        let mut reader = column.reader();
        let mut value = Vec::new();
        for (value_idx, expected_value) in values.iter().enumerate() {
            value.clear();
            reader.append_value(value_idx as u32, &mut value).unwrap();
            assert_eq!(&value, expected_value);
        }
        // Random access, going backward.
        for value_idx in [149_999u32, 70_000, 3, 100_000, 99_999, 0] {
            value.clear();
            reader.append_value(value_idx, &mut value).unwrap();
            assert_eq!(value, values[value_idx as usize]);
        }
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn test_append_value_out_of_range_panics() {
        let values = values_for_test(3);
        let column = open(serialize_full_column(&values));
        let mut value = Vec::new();
        let _ = column.reader().append_value(3, &mut value);
    }

    #[test]
    fn test_optional_column_roundtrip() {
        let values = values_for_test(3);
        let docs_with_value: Vec<RowId> = vec![1, 4, 5];
        let mut buffer = Vec::new();
        let mut serializer = PlainColumnSerializer::new(&mut buffer);
        for value in &values {
            serializer.add_value(value).unwrap();
        }
        serializer
            .finish(SerializableColumnIndex::Optional(
                SerializableOptionalIndex {
                    non_null_row_ids: Box::new(&docs_with_value[..]),
                    num_rows: 7,
                },
            ))
            .unwrap();
        let column = open(buffer);
        assert_eq!(column.num_docs(), 7);
        assert_eq!(column.get_cardinality(), Cardinality::Optional);
        let mut reader = column.reader();
        let mut value = Vec::new();
        for doc in 0..7u32 {
            value.clear();
            let has_value = reader.append_first_value(doc, &mut value).unwrap();
            match docs_with_value
                .iter()
                .position(|&doc_with_value| doc_with_value == doc)
            {
                Some(value_idx) => {
                    assert!(has_value);
                    assert_eq!(value, values[value_idx]);
                }
                None => assert!(!has_value),
            }
        }
        assert_eq!(column.block_range_for_doc(4), 0..1);
        assert_eq!(column.block_range_for_doc(0), 0..0);
    }

    #[test]
    fn test_empty_column() {
        let mut buffer = Vec::new();
        let serializer = PlainColumnSerializer::new(&mut buffer);
        let no_docs: Vec<RowId> = Vec::new();
        serializer
            .finish(SerializableColumnIndex::Optional(
                SerializableOptionalIndex {
                    non_null_row_ids: Box::new(&no_docs[..]),
                    num_rows: 3,
                },
            ))
            .unwrap();
        let column = open(buffer);
        assert_eq!(column.num_values(), 0);
        assert_eq!(column.num_blocks(), 0);
        let mut value = Vec::new();
        assert!(!column.reader().append_first_value(1, &mut value).unwrap());
    }

    #[test]
    fn test_str_value() {
        let values = vec!["hello".as_bytes().to_vec(), vec![0xff, 0xfe]];
        let column = open(serialize_full_column(&values));
        let mut reader = column.reader();
        let mut value = String::from("prefix:");
        reader.append_str_value(0, &mut value).unwrap();
        assert_eq!(value, "prefix:hello");
        assert!(reader.append_str_value(1, &mut value).is_err());
        assert_eq!(value, "prefix:hello");
    }

    #[test]
    fn test_for_each_value() {
        let values = values_for_test(10);
        let column = open(serialize_full_column(&values));
        let mut reader = column.reader();
        let mut doc_values: Vec<Vec<u8>> = Vec::new();
        reader
            .for_each_value(3, |value| doc_values.push(value.to_vec()))
            .unwrap();
        assert_eq!(doc_values, vec![values[3].clone()]);
    }

    #[test]
    fn test_corrupted_footer() {
        let values = values_for_test(10);
        let mut buffer = serialize_full_column(&values);
        let last = buffer.len() - 1;
        buffer[last] = 2;
        let file_slice = FileSlice::from(buffer);
        assert!(PlainBytesColumn::open(file_slice, Version::V3).is_err());
        let file_slice = FileSlice::from(vec![1u8, 2, 3]);
        assert!(PlainBytesColumn::open(file_slice, Version::V3).is_err());
    }
}
