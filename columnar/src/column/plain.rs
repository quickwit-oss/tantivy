use std::ops::Range;
use std::sync::Arc;
use std::{fmt, io};

use common::OwnedBytes;
use common::file_slice::FileSlice;
use onpair::{CompactDictionary, OwnedDictionaryStorage};

use crate::{Cardinality, ColumnIndex, RowId};

const ONPAIR_BLOCK_FOOTER_NUM_BYTES: usize = 12;

/// Target amount of uncompressed value bytes in one OnPair block.
///
/// A lower threshold reduces the amount fetched for a point lookup and can improve compression
/// when sorted values make each block locally homogeneous. It also creates more blocks, which
/// means more OnPair dictionaries to store and load, a larger block directory, and more block
/// boundaries to seek through. A higher threshold amortizes those dictionary, directory, and seek
/// costs over more values, but downloads more data for a point lookup and can dilute the locality
/// benefit of sorting, potentially reducing the compression ratio.
#[cfg(test)]
pub(crate) const PLAIN_BLOCK_RAW_NUM_BYTES_THRESHOLD: usize = 10 * 1024 * 1024;

/// Bounds block-local offset storage when values are very short or empty.
#[cfg(test)]
pub(crate) const PLAIN_BLOCK_MAX_NUM_VALUES: usize = 262_144;

#[derive(Clone, Debug)]
pub(crate) struct PlainBlockMeta {
    end_bytes: Box<[usize]>,
    end_values: Box<[u32]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PlainBlockRange {
    byte_range: Range<usize>,
    value_range: Range<u32>,
}

impl PlainBlockMeta {
    pub(crate) fn try_new(
        end_bytes: Box<[usize]>,
        end_values: Box<[u32]>,
        block_data_len: usize,
    ) -> io::Result<Self> {
        if end_bytes.len() != end_values.len() {
            return Err(invalid_data(
                "plain column block endpoint arrays have different lengths",
            ));
        }
        if end_bytes.last().copied().unwrap_or(0) != block_data_len {
            return Err(invalid_data(
                "plain column block directory does not cover the block payload",
            ));
        }
        Ok(Self {
            end_bytes,
            end_values,
        })
    }

    fn len(&self) -> usize {
        self.end_values.len()
    }

    fn num_values(&self) -> u32 {
        self.end_values.last().copied().unwrap_or(0)
    }

    fn find_block(&self, value_ord: u32) -> Option<(usize, PlainBlockRange)> {
        if value_ord >= self.num_values() {
            return None;
        }
        let block_ord = self
            .end_values
            .partition_point(|&end_value| end_value <= value_ord);
        Some((block_ord, self.block(block_ord)?))
    }

    fn block(&self, block_ord: usize) -> Option<PlainBlockRange> {
        let &end_byte = self.end_bytes.get(block_ord)?;
        let &end_value = self.end_values.get(block_ord)?;
        let (start_byte, start_value) = block_ord
            .checked_sub(1)
            .map(|previous_block_ord| {
                (
                    self.end_bytes[previous_block_ord],
                    self.end_values[previous_block_ord],
                )
            })
            .unwrap_or((0, 0));
        Some(PlainBlockRange {
            byte_range: start_byte..end_byte,
            value_range: start_value..end_value,
        })
    }
}

struct PlainBytesColumnData {
    block_data: FileSlice,
    blocks: PlainBlockMeta,
}

/// A byte column whose values are stored directly rather than as dictionary ordinals.
///
/// The payload is split into independently loadable OnPair blocks. Decoding reads only the block
/// containing the requested value through a [`PlainBytesColumnAccessor`].
#[derive(Clone)]
pub struct PlainBytesColumn {
    pub(crate) column_index: ColumnIndex,
    data: Arc<PlainBytesColumnData>,
}

impl fmt::Debug for PlainBytesColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PlainBytesColumn")
            .field("column_index", &self.column_index)
            .field("num_values", &self.num_values())
            .field("num_blocks", &self.data.blocks.len())
            .finish()
    }
}

impl PlainBytesColumn {
    pub(crate) fn open(
        column_index: ColumnIndex,
        block_data: FileSlice,
        blocks: PlainBlockMeta,
    ) -> io::Result<Self> {
        let num_values = blocks.num_values();
        validate_index_num_values(&column_index, num_values)?;
        Ok(Self {
            column_index,
            data: Arc::new(PlainBytesColumnData { block_data, blocks }),
        })
    }

    /// Returns the number of rows in the column.
    pub fn num_rows(&self) -> RowId {
        match &self.column_index {
            ColumnIndex::Empty { num_docs } => *num_docs,
            ColumnIndex::Full => self.data.blocks.num_values(),
            ColumnIndex::Optional(optional_index) => optional_index.num_docs(),
            ColumnIndex::Multivalued(multivalued_index) => multivalued_index.num_docs(),
        }
    }

    /// Returns the number of values in the column.
    pub fn num_values(&self) -> u32 {
        self.data.blocks.num_values()
    }

    /// Returns the column index mapping rows to physical values.
    pub fn column_index(&self) -> &ColumnIndex {
        &self.column_index
    }

    /// Returns the cardinality of the column.
    pub fn get_cardinality(&self) -> Cardinality {
        self.column_index.get_cardinality()
    }

    /// Creates an accessor with its own parsed-block cache and decode buffer.
    pub fn accessor(&self) -> PlainBytesColumnAccessor {
        PlainBytesColumnAccessor {
            column: self.clone(),
            cached_block: None,
            output: Vec::new(),
        }
    }

    fn value_ords(&self, row_id: RowId) -> io::Result<Range<u32>> {
        if row_id >= self.num_rows() {
            return Err(invalid_input("plain column row id is out of bounds"));
        }
        Ok(self.column_index.value_row_ids(row_id))
    }

    #[cfg(test)]
    pub(crate) fn for_test(column_index: ColumnIndex, values: &[&[u8]]) -> Self {
        if values.is_empty() {
            let blocks = PlainBlockMeta::try_new(Box::new([]), Box::new([]), 0).unwrap();
            return Self::open(column_index, FileSlice::empty(), blocks).unwrap();
        }
        let block_bytes = serialize_test_block(values);
        let block_len = block_bytes.len();
        let blocks = PlainBlockMeta::try_new(
            Box::new([block_len]),
            Box::new([values.len() as u32]),
            block_len,
        )
        .unwrap();
        Self::open(column_index, FileSlice::from(block_bytes), blocks).unwrap()
    }
}

/// Mutable decoding state for a [`PlainBytesColumn`].
///
/// The accessor owns the most recently parsed block and reuses its output buffer. A value returned
/// by [`Self::get_val`] or [`Self::first`] is valid until the next mutable accessor call.
pub struct PlainBytesColumnAccessor {
    column: PlainBytesColumn,
    cached_block: Option<(usize, onpair::Column<u32>)>,
    output: Vec<u8>,
}

impl fmt::Debug for PlainBytesColumnAccessor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PlainBytesColumnAccessor")
            .field("column", &self.column)
            .field(
                "cached_block_ord",
                &self.cached_block.as_ref().map(|(block_ord, _)| block_ord),
            )
            .finish()
    }
}

impl PlainBytesColumnAccessor {
    /// Decodes the physical value at `value_ord` into the accessor's reusable buffer.
    pub fn get_val(&mut self, value_ord: u32) -> io::Result<&[u8]> {
        let Some((block_ord, block_range)) = self.column.data.blocks.find_block(value_ord) else {
            return Err(invalid_input("plain column value ordinal is out of bounds"));
        };
        let is_cached = self
            .cached_block
            .as_ref()
            .is_some_and(|(cached_block_ord, _)| *cached_block_ord == block_ord);
        if !is_cached {
            let block_bytes = self
                .column
                .data
                .block_data
                .read_bytes_slice(block_range.byte_range.clone())?;
            let num_values = block_range.value_range.end - block_range.value_range.start;
            let block = open_onpair_block(block_bytes, num_values)?;
            self.cached_block = Some((block_ord, block));
        }

        let local_value_ord = value_ord - block_range.value_range.start;
        let block = &self.cached_block.as_ref().unwrap().1;
        decode_value(block, local_value_ord, &mut self.output)?;
        Ok(self.output.as_slice())
    }

    /// Decodes the first value associated with `row_id`.
    pub fn first(&mut self, row_id: RowId) -> io::Result<Option<&[u8]>> {
        let Some(value_ord) = self.column.value_ords(row_id)?.next() else {
            return Ok(None);
        };
        self.get_val(value_ord).map(Some)
    }

    /// Decodes each value associated with `row_id`, reusing the accessor's output buffer.
    pub fn for_each_value(
        &mut self,
        row_id: RowId,
        mut callback: impl FnMut(&[u8]),
    ) -> io::Result<()> {
        for value_ord in self.column.value_ords(row_id)? {
            callback(self.get_val(value_ord)?);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn num_cached_blocks(&self) -> usize {
        usize::from(self.cached_block.is_some())
    }
}

/// UTF-8 view over a [`PlainBytesColumn`].
#[derive(Clone, Debug)]
pub struct PlainStrColumn(PlainBytesColumn);

impl PlainStrColumn {
    /// Wraps a plain byte column as a string column.
    pub fn wrap(bytes_column: PlainBytesColumn) -> Self {
        PlainStrColumn(bytes_column)
    }

    /// Returns the underlying byte column.
    pub fn as_bytes(&self) -> &PlainBytesColumn {
        &self.0
    }

    /// Returns the number of rows in the column.
    pub fn num_rows(&self) -> RowId {
        self.0.num_rows()
    }

    /// Returns the number of values in the column.
    pub fn num_values(&self) -> u32 {
        self.0.num_values()
    }

    /// Returns the column index mapping rows to physical values.
    pub fn column_index(&self) -> &ColumnIndex {
        self.0.column_index()
    }

    /// Returns the cardinality of the column.
    pub fn get_cardinality(&self) -> Cardinality {
        self.0.get_cardinality()
    }

    /// Creates an accessor with its own parsed-block cache and decode buffer.
    pub fn accessor(&self) -> PlainStrColumnAccessor {
        PlainStrColumnAccessor(self.0.accessor())
    }
}

/// Mutable decoding state for a [`PlainStrColumn`].
///
/// The accessor reuses one byte buffer and validates its contents as UTF-8. A value returned by
/// [`Self::get_val`] or [`Self::first`] is valid until the next mutable accessor call.
#[derive(Debug)]
pub struct PlainStrColumnAccessor(PlainBytesColumnAccessor);

impl PlainStrColumnAccessor {
    /// Decodes the physical value at `value_ord` and validates its UTF-8.
    pub fn get_val(&mut self, value_ord: u32) -> io::Result<&str> {
        let bytes = self.0.get_val(value_ord)?;
        std::str::from_utf8(bytes).map_err(invalid_utf8)
    }

    /// Decodes the first value associated with `row_id` and validates its UTF-8.
    pub fn first(&mut self, row_id: RowId) -> io::Result<Option<&str>> {
        let Some(bytes) = self.0.first(row_id)? else {
            return Ok(None);
        };
        std::str::from_utf8(bytes).map(Some).map_err(invalid_utf8)
    }

    /// Decodes each value associated with `row_id`, validates its UTF-8, and invokes `callback`.
    pub fn for_each_value(
        &mut self,
        row_id: RowId,
        mut callback: impl FnMut(&str),
    ) -> io::Result<()> {
        for value_ord in self.0.column.value_ords(row_id)? {
            callback(self.get_val(value_ord)?);
        }
        Ok(())
    }
}

impl From<PlainStrColumn> for PlainBytesColumn {
    fn from(str_column: PlainStrColumn) -> Self {
        str_column.0
    }
}

fn decode_value(
    block: &onpair::Column<u32>,
    value_ord: u32,
    output: &mut Vec<u8>,
) -> io::Result<()> {
    let view = block.view();
    let codes = view.row_codes(value_ord as usize);
    let decoded_len = onpair::decoded_len(codes, view.dict);
    let output_len = decoded_len
        .checked_add(onpair::DECODE_PADDING)
        .ok_or_else(|| invalid_data("plain value decoded length overflows address space"))?;

    output.clear();
    output.reserve(output_len);
    let spare = output.spare_capacity_mut();
    // SAFETY: the OnPair dictionary was validated on block open, every code was checked against
    // it, and `spare[..output_len]` includes the padding required by `decode_into`.
    let written = unsafe { onpair::decode_into(codes, view.dict, &mut spare[..output_len]) };
    // SAFETY: `decode_into` initialized exactly `written` bytes at the beginning of `spare`.
    unsafe {
        output.set_len(written);
    }
    Ok(())
}

pub(crate) fn open_onpair_block(
    data: OwnedBytes,
    num_values: u32,
) -> io::Result<onpair::Column<u32>> {
    if data.len() < ONPAIR_BLOCK_FOOTER_NUM_BYTES {
        return Err(invalid_data("truncated OnPair block footer"));
    }
    let body_len = data.len() - ONPAIR_BLOCK_FOOTER_NUM_BYTES;
    let (body, footer) = data.split(body_len);
    let footer = footer.as_slice();
    let dictionary_bytes_num_bytes = read_u32(&footer[0..4]) as usize;
    let dictionary_offsets_num_bytes = read_u32(&footer[4..8]) as usize;
    let codes_num_bytes = read_u32(&footer[8..12]) as usize;
    let value_offsets_num_bytes = usize::try_from(num_values)
        .ok()
        .and_then(|num_values| num_values.checked_add(1))
        .and_then(|num_offsets| num_offsets.checked_mul(4))
        .ok_or_else(|| invalid_data("OnPair block value-offset length overflows"))?;

    let expected_body_len = dictionary_bytes_num_bytes
        .checked_add(dictionary_offsets_num_bytes)
        .and_then(|len| len.checked_add(codes_num_bytes))
        .and_then(|len| len.checked_add(value_offsets_num_bytes))
        .ok_or_else(|| invalid_data("OnPair block region lengths overflow"))?;
    if expected_body_len != body.len() {
        return Err(invalid_data(
            "OnPair block region lengths do not match the block payload",
        ));
    }
    if dictionary_offsets_num_bytes == 0 || !dictionary_offsets_num_bytes.is_multiple_of(4) {
        return Err(invalid_data(
            "OnPair dictionary offsets are missing or misaligned",
        ));
    }
    if !codes_num_bytes.is_multiple_of(2) {
        return Err(invalid_data("OnPair codes are misaligned"));
    }

    let (dictionary_bytes, body) = body.split(dictionary_bytes_num_bytes);
    let (dictionary_offsets_bytes, body) = body.split(dictionary_offsets_num_bytes);
    let (codes_bytes, value_offsets_bytes) = body.split(codes_num_bytes);
    let dictionary_offsets = dictionary_offsets_bytes
        .as_slice()
        .chunks_exact(4)
        .map(read_u32)
        .collect::<Vec<_>>();
    let codes = codes_bytes
        .as_slice()
        .chunks_exact(2)
        .map(read_u16)
        .collect::<Vec<_>>();
    let value_offsets = value_offsets_bytes
        .as_slice()
        .chunks_exact(4)
        .map(read_u32)
        .collect::<Vec<_>>();

    let dictionary = CompactDictionary::validate(OwnedDictionaryStorage::new(
        dictionary_bytes.as_slice().to_vec(),
        dictionary_offsets,
    ))
    .map_err(|error| invalid_data_owned(format!("invalid OnPair dictionary: {error}")))?;
    let num_tokens = dictionary.num_tokens();
    if codes.iter().any(|&code| code as usize >= num_tokens) {
        return Err(invalid_data(
            "OnPair block code references an unknown dictionary token",
        ));
    }
    if value_offsets.first().copied() != Some(0)
        || value_offsets
            .windows(2)
            .any(|offsets| offsets[0] > offsets[1])
        || value_offsets.last().copied().map(|offset| offset as usize) != Some(codes.len())
    {
        return Err(invalid_data(
            "OnPair block value offsets do not delimit the code stream",
        ));
    }
    let max_decoded_len = (codes.len() as u64)
        .checked_mul(onpair::MAX_TOKEN_SIZE as u64)
        .ok_or_else(|| invalid_data("OnPair block decoded length overflows"))?;
    usize::try_from(max_decoded_len)
        .map_err(|_| invalid_data("OnPair block decoded length does not fit in memory"))?;

    Ok(onpair::Column {
        dict: dictionary,
        codes,
        row_offsets: value_offsets,
    })
}

fn validate_index_num_values(column_index: &ColumnIndex, num_values: u32) -> io::Result<()> {
    let expected_num_values = match column_index {
        ColumnIndex::Empty { .. } => Some(0),
        ColumnIndex::Full => None,
        ColumnIndex::Optional(optional_index) => Some(optional_index.num_non_nulls()),
        ColumnIndex::Multivalued(multivalued_index) => {
            let start_offsets = multivalued_index.get_start_index_column();
            Some(start_offsets.get_val(start_offsets.num_vals() - 1))
        }
    };
    if expected_num_values.is_some_and(|expected| expected != num_values) {
        return Err(invalid_data(
            "plain column index value count does not match its block directory",
        ));
    }
    Ok(())
}

fn read_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().unwrap())
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().unwrap())
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn invalid_data_owned(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_utf8(error: std::str::Utf8Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

#[cfg(test)]
pub(crate) fn serialize_test_block(values: &[&[u8]]) -> Vec<u8> {
    let mut raw_bytes = Vec::new();
    let mut raw_offsets = vec![0u32];
    for value in values {
        raw_bytes.extend_from_slice(value);
        raw_offsets.push(raw_bytes.len() as u32);
    }
    let column = onpair::compress(&raw_bytes, &raw_offsets, onpair::DEFAULT_CONFIG).unwrap();
    let (dictionary, codes, value_offsets) = column.into_raw();
    let (dictionary_bytes, dictionary_offsets) = dictionary.into_raw();

    let mut output = Vec::new();
    output.extend_from_slice(&dictionary_bytes);
    for offset in &dictionary_offsets {
        output.extend_from_slice(&offset.to_le_bytes());
    }
    for code in &codes {
        output.extend_from_slice(&code.to_le_bytes());
    }
    for offset in &value_offsets {
        output.extend_from_slice(&offset.to_le_bytes());
    }
    output.extend_from_slice(&(dictionary_bytes.len() as u32).to_le_bytes());
    output.extend_from_slice(&((dictionary_offsets.len() * 4) as u32).to_le_bytes());
    output.extend_from_slice(&((codes.len() * 2) as u32).to_le_bytes());
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_index::{MultiValueIndex, OptionalIndex};

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn test_plain_columns_are_send_and_sync() {
        assert_send_sync::<PlainBytesColumn>();
        assert_send_sync::<PlainStrColumn>();
    }

    #[test]
    fn test_plain_block_meta_empty() {
        let blocks = PlainBlockMeta::try_new(Box::new([]), Box::new([]), 0).unwrap();
        assert_eq!(blocks.len(), 0);
        assert_eq!(blocks.num_values(), 0);
        assert_eq!(blocks.find_block(0), None);
        assert_eq!(blocks.block(0), None);
    }

    #[test]
    fn test_plain_block_meta_lookup_and_ranges() {
        let blocks =
            PlainBlockMeta::try_new(Box::new([10, 25, 40]), Box::new([2, 5, 9]), 40).unwrap();

        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks.num_values(), 9);
        assert_eq!(
            blocks.find_block(0),
            Some((
                0,
                PlainBlockRange {
                    byte_range: 0..10,
                    value_range: 0..2,
                }
            ))
        );
        assert_eq!(blocks.find_block(1), blocks.find_block(0));
        assert_eq!(
            blocks.find_block(2),
            Some((
                1,
                PlainBlockRange {
                    byte_range: 10..25,
                    value_range: 2..5,
                }
            ))
        );
        assert_eq!(blocks.find_block(4), blocks.find_block(2));
        assert_eq!(
            blocks.find_block(5),
            Some((
                2,
                PlainBlockRange {
                    byte_range: 25..40,
                    value_range: 5..9,
                }
            ))
        );
        assert_eq!(blocks.find_block(8), blocks.find_block(5));
        assert_eq!(blocks.find_block(9), None);
        assert_eq!(blocks.block(3), None);
    }

    #[test]
    fn test_plain_block_meta_validation() {
        let invalid_directories = [
            (vec![1], vec![], 1),
            (vec![0], vec![1], 0),
            (vec![1], vec![0], 1),
            (vec![1, 1], vec![1, 2], 1),
            (vec![1, 2], vec![2, 2], 2),
            (vec![1], vec![1], 2),
        ];
        for (end_bytes, end_values, block_data_len) in invalid_directories {
            assert_eq!(
                PlainBlockMeta::try_new(
                    end_bytes.into_boxed_slice(),
                    end_values.into_boxed_slice(),
                    block_data_len,
                )
                .unwrap_err()
                .kind(),
                io::ErrorKind::InvalidData
            );
        }
    }

    #[test]
    fn test_plain_bytes_full_access() {
        let values: &[&[u8]] = &[b"alpha", b"", &[0, 255]];
        let column = PlainBytesColumn::for_test(ColumnIndex::Full, values);
        assert_eq!(column.num_rows(), 3);
        assert_eq!(column.num_values(), 3);
        assert_eq!(column.get_cardinality(), Cardinality::Full);

        let mut accessor = column.accessor();
        assert_eq!(accessor.get_val(0).unwrap(), b"alpha");
        assert_eq!(accessor.get_val(1).unwrap(), b"");
        assert_eq!(accessor.first(2).unwrap(), Some(&[0, 255][..]));
        assert_eq!(accessor.num_cached_blocks(), 1);
        assert_eq!(
            accessor.get_val(3).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(
            accessor.first(3).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn test_plain_bytes_optional_access() {
        let index = ColumnIndex::Optional(OptionalIndex::for_test(5, &[1, 4]));
        let column = PlainBytesColumn::for_test(index, &[b"one", b"four"]);
        assert_eq!(column.num_rows(), 5);
        assert_eq!(column.get_cardinality(), Cardinality::Optional);
        let mut accessor = column.accessor();
        assert_eq!(accessor.first(0).unwrap(), None);
        assert_eq!(accessor.first(1).unwrap(), Some(&b"one"[..]));
        assert_eq!(accessor.first(4).unwrap(), Some(&b"four"[..]));

        let empty = PlainBytesColumn::for_test(ColumnIndex::Empty { num_docs: 3 }, &[]);
        assert_eq!(empty.num_rows(), 3);
        assert_eq!(empty.num_values(), 0);
        assert_eq!(empty.accessor().first(0).unwrap(), None);
    }

    #[test]
    fn test_plain_bytes_multivalued_access() {
        let index = ColumnIndex::Multivalued(MultiValueIndex::for_test(&[0, 2, 2, 3]));
        let column = PlainBytesColumn::for_test(index, &[b"first", b"second", b"third"]);
        let mut accessor = column.accessor();
        let mut values = Vec::new();
        accessor
            .for_each_value(0, |value| values.push(value.to_vec()))
            .unwrap();
        assert_eq!(values, [b"first".to_vec(), b"second".to_vec()]);

        values.clear();
        accessor
            .for_each_value(1, |value| values.push(value.to_vec()))
            .unwrap();
        assert!(values.is_empty());
        assert_eq!(accessor.first(2).unwrap(), Some(&b"third"[..]));
    }

    #[test]
    fn test_plain_str_access_and_utf8_validation() {
        let bytes = PlainBytesColumn::for_test(ColumnIndex::Full, &["café".as_bytes(), b"tea"]);
        let column = PlainStrColumn::wrap(bytes);
        assert_eq!(column.num_rows(), 2);
        assert_eq!(column.num_values(), 2);
        assert_eq!(column.get_cardinality(), Cardinality::Full);
        let mut accessor = column.accessor();
        assert_eq!(accessor.get_val(0).unwrap(), "café");
        assert_eq!(accessor.first(1).unwrap(), Some("tea"));
        let mut values = Vec::new();
        accessor
            .for_each_value(0, |value| values.push(value.to_owned()))
            .unwrap();
        assert_eq!(values, ["café"]);

        let invalid =
            PlainStrColumn::wrap(PlainBytesColumn::for_test(ColumnIndex::Full, &[&[0xff]]));
        assert_eq!(
            invalid.accessor().get_val(0).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn test_open_onpair_block_validation() {
        let block = serialize_test_block(&[b"a"]);
        open_onpair_block(OwnedBytes::new(block.clone()), 1).unwrap();

        let mut invalid_codes = block.clone();
        let footer = invalid_codes.len() - ONPAIR_BLOCK_FOOTER_NUM_BYTES;
        let dict_bytes_len = read_u32(&invalid_codes[footer..footer + 4]) as usize;
        let dict_offsets_len = read_u32(&invalid_codes[footer + 4..footer + 8]) as usize;
        let codes_start = dict_bytes_len + dict_offsets_len;
        invalid_codes[codes_start..codes_start + 2].copy_from_slice(&u16::MAX.to_le_bytes());
        assert_eq!(
            open_onpair_block(OwnedBytes::new(invalid_codes), 1)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );

        assert_eq!(
            open_onpair_block(OwnedBytes::new(block), 2)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
}
