use std::io;
use std::io::Write;
use std::sync::Arc;

use common::file_slice::FileSlice;
use common::{HasLen, OwnedBytes};
use sstable::Dictionary;

use crate::column::{
    BytesColumn, Column, DictionaryEncodedBytesColumn, DictionaryEncodedStrColumn, PlainBlockMeta,
    PlainBytesColumn, PlainStrColumn,
};
use crate::column_index::{SerializableColumnIndex, serialize_column_index};
use crate::column_values::{
    CodecType, MonotonicallyMappableToU64, MonotonicallyMappableToU128,
    load_u64_based_column_values, serialize_column_values_u128, serialize_u64_based_column_values,
};
use crate::iterable::Iterable;
use crate::{PayloadEncoding, StrColumn, Version};

pub fn serialize_column_mappable_to_u128<T: MonotonicallyMappableToU128>(
    column_index: SerializableColumnIndex<'_>,
    iterable: &dyn Iterable<T>,
    output: &mut impl Write,
) -> io::Result<()> {
    let column_index_num_bytes = serialize_column_index(column_index, output)?;
    serialize_column_values_u128(iterable, output)?;
    output.write_all(&column_index_num_bytes.to_le_bytes())?;
    Ok(())
}

pub fn serialize_column_mappable_to_u64<T: MonotonicallyMappableToU64>(
    column_index: SerializableColumnIndex<'_>,
    column_values: &impl Iterable<T>,
    output: &mut impl Write,
) -> io::Result<()> {
    let column_index_num_bytes = serialize_column_index(column_index, output)?;
    serialize_u64_based_column_values(
        column_values,
        &[CodecType::Bitpacked, CodecType::BlockwiseLinear],
        output,
    )?;
    output.write_all(&column_index_num_bytes.to_le_bytes())?;
    Ok(())
}

pub fn open_column_u64<T: MonotonicallyMappableToU64>(
    bytes: OwnedBytes,
    format_version: Version,
) -> io::Result<Column<T>> {
    let (body, column_index_num_bytes_payload) = bytes.rsplit(4);
    let column_index_num_bytes = u32::from_le_bytes(
        column_index_num_bytes_payload
            .as_slice()
            .try_into()
            .unwrap(),
    );
    let (column_index_data, column_values_data) = body.split(column_index_num_bytes as usize);
    let column_index = crate::column_index::open_column_index(column_index_data, format_version)?;
    let column_values = load_u64_based_column_values(column_values_data)?;
    Ok(Column {
        index: column_index,
        values: column_values,
    })
}

pub fn open_column_u128<T: MonotonicallyMappableToU128>(
    bytes: OwnedBytes,
    format_version: Version,
) -> io::Result<Column<T>> {
    let (body, column_index_num_bytes_payload) = bytes.rsplit(4);
    let column_index_num_bytes = u32::from_le_bytes(
        column_index_num_bytes_payload
            .as_slice()
            .try_into()
            .unwrap(),
    );
    let (column_index_data, column_values_data) = body.split(column_index_num_bytes as usize);
    let column_index = crate::column_index::open_column_index(column_index_data, format_version)?;
    let column_values = crate::column_values::open_u128_mapped(column_values_data)?;
    Ok(Column {
        index: column_index,
        values: column_values,
    })
}

/// Open the column as u64.
///
/// See [`open_u128_as_compact_u64`] for more details.
pub fn open_column_u128_as_compact_u64(
    bytes: OwnedBytes,
    format_version: Version,
) -> io::Result<Column<u64>> {
    let (body, column_index_num_bytes_payload) = bytes.rsplit(4);
    let column_index_num_bytes = u32::from_le_bytes(
        column_index_num_bytes_payload
            .as_slice()
            .try_into()
            .unwrap(),
    );
    let (column_index_data, column_values_data) = body.split(column_index_num_bytes as usize);
    let column_index = crate::column_index::open_column_index(column_index_data, format_version)?;
    let column_values = crate::column_values::open_u128_as_compact_u64(column_values_data)?;
    Ok(Column {
        index: column_index,
        values: column_values,
    })
}

pub fn open_column_bytes(data: OwnedBytes, format_version: Version) -> io::Result<BytesColumn> {
    open_column_bytes_from_file_slice(FileSlice::new(Arc::new(data)), format_version)
}

pub(crate) fn open_column_bytes_from_file_slice(
    data: FileSlice,
    format_version: Version,
) -> io::Result<BytesColumn> {
    match format_version {
        Version::V1 | Version::V2 => {
            open_dictionary_bytes_column(data.read_bytes()?, format_version)
        }
        Version::V3 => {
            if data.len() < 1 {
                return Err(invalid_data("missing string/byte payload encoding tag"));
            }
            let (encoding_slice, payload) = data.split(1);
            let encoding_bytes = encoding_slice.read_bytes()?;
            let encoding =
                PayloadEncoding::try_from_code(encoding_bytes[0]).map_err(io::Error::from)?;
            match encoding {
                PayloadEncoding::Dictionary => {
                    open_dictionary_bytes_column(payload.read_bytes()?, format_version)
                }
                PayloadEncoding::Plain => open_plain_bytes_column(payload).map(Into::into),
            }
        }
    }
}

fn open_dictionary_bytes_column(
    data: OwnedBytes,
    format_version: Version,
) -> io::Result<BytesColumn> {
    if data.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated dictionary string/byte column payload",
        ));
    }
    let (body, dictionary_len_bytes) = data.rsplit(4);
    let dictionary_len = u32::from_le_bytes(dictionary_len_bytes.as_slice().try_into().unwrap());
    if dictionary_len as usize > body.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "dictionary length exceeds string/byte column payload",
        ));
    }
    let (dictionary_bytes, column_bytes) = body.split(dictionary_len as usize);
    let dictionary = Arc::new(Dictionary::from_bytes(dictionary_bytes)?);
    let term_ord_column = crate::column::open_column_u64::<u64>(column_bytes, format_version)?;
    Ok(DictionaryEncodedBytesColumn {
        dictionary,
        term_ord_column,
    }
    .into())
}

pub fn open_column_str(data: OwnedBytes, format_version: Version) -> io::Result<StrColumn> {
    open_column_str_from_file_slice(FileSlice::new(Arc::new(data)), format_version)
}

pub(crate) fn open_column_str_from_file_slice(
    data: FileSlice,
    format_version: Version,
) -> io::Result<StrColumn> {
    match open_column_bytes_from_file_slice(data, format_version)? {
        BytesColumn::DictionaryEncoded(bytes_column) => {
            Ok(DictionaryEncodedStrColumn::wrap(bytes_column).into())
        }
        BytesColumn::Plain(bytes_column) => Ok(PlainStrColumn::wrap(bytes_column).into()),
    }
}

const PLAIN_FOOTER_NUM_BYTES: usize = 8;
const PLAIN_BLOCK_DIRECTORY_ENTRY_NUM_BYTES: usize = 8;

fn open_plain_bytes_column(data: FileSlice) -> io::Result<PlainBytesColumn> {
    if data.len() < PLAIN_FOOTER_NUM_BYTES {
        return Err(invalid_data("truncated plain string/byte column footer"));
    }
    let (body, footer_slice) = data.split_from_end(PLAIN_FOOTER_NUM_BYTES);
    let footer = footer_slice.read_bytes()?;
    let column_index_num_bytes = read_u32(&footer[0..4]) as usize;
    let num_blocks = read_u32(&footer[4..8]) as usize;
    let directory_num_bytes = num_blocks
        .checked_mul(PLAIN_BLOCK_DIRECTORY_ENTRY_NUM_BYTES)
        .ok_or_else(|| invalid_data("plain column block directory length overflows"))?;
    if directory_num_bytes > body.len() {
        return Err(invalid_data(
            "plain column block directory exceeds the payload",
        ));
    }
    let (column_and_blocks, directory_slice) = body.split_from_end(directory_num_bytes);
    if column_index_num_bytes > column_and_blocks.len() {
        return Err(invalid_data("plain column index exceeds the payload"));
    }
    let (column_index_slice, block_data) = column_and_blocks.split(column_index_num_bytes);
    let column_index =
        crate::column_index::open_column_index(column_index_slice.read_bytes()?, Version::V3)?;
    let directory = directory_slice.read_bytes()?;
    let mut end_bytes = Vec::with_capacity(num_blocks);
    let mut end_values = Vec::with_capacity(num_blocks);
    let mut byte_start = 0usize;
    for entry in directory
        .as_slice()
        .chunks_exact(PLAIN_BLOCK_DIRECTORY_ENTRY_NUM_BYTES)
    {
        let block_num_bytes = read_u32(&entry[0..4]) as usize;
        let value_end = read_u32(&entry[4..8]);
        let byte_end = byte_start
            .checked_add(block_num_bytes)
            .ok_or_else(|| invalid_data("plain column block address overflows"))?;
        end_bytes.push(byte_end);
        end_values.push(value_end);
        byte_start = byte_end;
    }
    let blocks = PlainBlockMeta::try_new(
        end_bytes.into_boxed_slice(),
        end_values.into_boxed_slice(),
        block_data.len(),
    )?;
    PlainBytesColumn::open(column_index, block_data, blocks)
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().unwrap())
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Mutex;

    use common::file_slice::FileHandle;

    use super::*;
    use crate::column::{
        PLAIN_BLOCK_MAX_NUM_VALUES, PLAIN_BLOCK_RAW_NUM_BYTES_THRESHOLD, serialize_test_block,
    };
    use crate::column_index::SerializableColumnIndex;

    #[derive(Debug)]
    struct RecordingFileHandle {
        data: Arc<[u8]>,
        reads: Mutex<Vec<Range<usize>>>,
    }

    impl RecordingFileHandle {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data: data.into(),
                reads: Mutex::new(Vec::new()),
            }
        }

        fn reads(&self) -> Vec<Range<usize>> {
            self.reads.lock().unwrap().clone()
        }

        fn clear_reads(&self) {
            self.reads.lock().unwrap().clear();
        }
    }

    impl HasLen for RecordingFileHandle {
        fn len(&self) -> usize {
            self.data.len()
        }
    }

    impl FileHandle for RecordingFileHandle {
        fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
            self.reads.lock().unwrap().push(range.clone());
            Ok(OwnedBytes::new(self.data[range].to_vec()))
        }
    }

    struct SerializedPlainColumn {
        bytes: Vec<u8>,
        block_ranges: Vec<Range<usize>>,
    }

    #[test]
    fn test_v3_payload_encoding_tag_errors() {
        let error = open_column_bytes(OwnedBytes::new(Vec::new()), Version::V3).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let error = open_column_bytes(OwnedBytes::new(vec![u8::MAX]), Version::V3).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let error = open_column_bytes(
            OwnedBytes::new(vec![PayloadEncoding::Plain.to_code()]),
            Version::V3,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);

        let error = open_column_bytes(
            OwnedBytes::new(vec![PayloadEncoding::Dictionary.to_code()]),
            Version::V3,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_open_v3_plain_bytes_column() {
        let data = serialize_plain_column(&[b"alpha", b"", &[0, 255]]).bytes;
        let BytesColumn::Plain(column) =
            open_column_bytes(OwnedBytes::new(data), Version::V3).unwrap()
        else {
            panic!("expected a plain byte column")
        };
        assert_eq!(column.num_values(), 3);
        let mut accessor = column.accessor();
        assert_eq!(accessor.get_val(0).unwrap(), b"alpha");
        assert_eq!(accessor.get_val(1).unwrap(), b"");
        assert_eq!(accessor.first(2).unwrap(), Some(&[0, 255][..]));
    }

    #[test]
    fn test_open_v3_plain_str_column() {
        let data = serialize_plain_column(&["café".as_bytes(), b"tea"]).bytes;
        let StrColumn::Plain(column) = open_column_str(OwnedBytes::new(data), Version::V3).unwrap()
        else {
            panic!("expected a plain string column")
        };
        let mut accessor = column.accessor();
        assert_eq!(accessor.get_val(0).unwrap(), "café");
        assert_eq!(accessor.first(1).unwrap(), Some("tea"));

        let invalid_utf8 = serialize_plain_column(&[&[0xff]]).bytes;
        let StrColumn::Plain(column) =
            open_column_str(OwnedBytes::new(invalid_utf8), Version::V3).unwrap()
        else {
            panic!("expected a plain string column")
        };
        assert_eq!(
            column.accessor().get_val(0).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn test_open_v3_plain_column_rejects_inconsistent_directory() {
        let mut data = serialize_plain_column(&[b"value"]).bytes;
        let directory_start = data.len() - PLAIN_FOOTER_NUM_BYTES - 8;
        let block_num_bytes = read_u32(&data[directory_start..directory_start + 4]);
        data[directory_start..directory_start + 4]
            .copy_from_slice(&(block_num_bytes + 1).to_le_bytes());
        let error = open_column_bytes(OwnedBytes::new(data), Version::V3).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_plain_column_loads_and_caches_only_selected_blocks() {
        let values: &[&[u8]] = &[b"zero", b"one", b"two", b"three", b"four"];
        let serialized = serialize_plain_column_with_limits(values, usize::MAX, 1);
        assert_eq!(serialized.block_ranges.len(), 5);
        let handle = Arc::new(RecordingFileHandle::new(serialized.bytes));
        let BytesColumn::Plain(column) =
            open_column_bytes_from_file_slice(FileSlice::new(handle.clone()), Version::V3).unwrap()
        else {
            panic!("expected a plain byte column")
        };

        let open_reads = handle.reads();
        assert!(open_reads.iter().all(|read| {
            serialized
                .block_ranges
                .iter()
                .all(|block| !ranges_overlap(read, block))
        }));

        handle.clear_reads();
        let mut accessor = column.accessor();
        assert_eq!(accessor.get_val(0).unwrap(), b"zero");
        assert_eq!(handle.reads(), [serialized.block_ranges[0].clone()]);

        assert_eq!(accessor.get_val(0).unwrap(), b"zero");
        assert_eq!(handle.reads(), [serialized.block_ranges[0].clone()]);

        for (value_ord, value) in values.iter().enumerate().skip(1) {
            assert_eq!(accessor.get_val(value_ord as u32).unwrap(), *value);
        }
        assert_eq!(accessor.num_cached_blocks(), 1);
        assert_eq!(handle.reads().len(), 5);

        assert_eq!(accessor.get_val(0).unwrap(), b"zero");
        assert_eq!(accessor.num_cached_blocks(), 1);
        assert_eq!(handle.reads().len(), 6);
        assert_eq!(handle.reads().last(), Some(&serialized.block_ranges[0]));

        let mut second_accessor = column.accessor();
        assert_eq!(second_accessor.get_val(0).unwrap(), b"zero");
        assert_eq!(handle.reads().len(), 7);
        assert_eq!(handle.reads().last(), Some(&serialized.block_ranges[0]));
    }

    #[test]
    fn test_plain_column_block_limits() {
        assert_eq!(PLAIN_BLOCK_RAW_NUM_BYTES_THRESHOLD, 10 * 1024 * 1024);
        let values: &[&[u8]] = &[b"aa", b"bb", b"", b"cc"];
        let by_bytes = serialize_plain_column_with_limits(values, 4, usize::MAX);
        assert_eq!(by_bytes.block_ranges.len(), 2);
        let by_values = serialize_plain_column_with_limits(values, usize::MAX, 2);
        assert_eq!(by_values.block_ranges.len(), 2);
    }

    #[test]
    fn test_open_empty_v3_plain_column() {
        let data = serialize_plain_column(&[]).bytes;
        let BytesColumn::Plain(column) =
            open_column_bytes(OwnedBytes::new(data), Version::V3).unwrap()
        else {
            panic!("expected a plain byte column")
        };
        assert_eq!(column.num_values(), 0);
        assert_eq!(column.num_rows(), 0);
    }

    fn ranges_overlap(left: &Range<usize>, right: &Range<usize>) -> bool {
        left.start < right.end && right.start < left.end
    }

    fn serialize_plain_column(values: &[&[u8]]) -> SerializedPlainColumn {
        serialize_plain_column_with_limits(
            values,
            PLAIN_BLOCK_RAW_NUM_BYTES_THRESHOLD,
            PLAIN_BLOCK_MAX_NUM_VALUES,
        )
    }

    fn serialize_plain_column_with_limits(
        values: &[&[u8]],
        raw_num_bytes_threshold: usize,
        max_num_values: usize,
    ) -> SerializedPlainColumn {
        assert!(raw_num_bytes_threshold > 0);
        assert!(max_num_values > 0);

        let mut output = vec![PayloadEncoding::Plain.to_code()];
        let column_index_num_bytes =
            serialize_column_index(SerializableColumnIndex::Full, &mut output).unwrap();
        let mut directory = Vec::new();
        let mut block_ranges = Vec::new();
        let mut block_value_start = 0usize;
        let mut raw_num_bytes = 0usize;
        for (value_ord, value) in values.iter().enumerate() {
            raw_num_bytes = raw_num_bytes.checked_add(value.len()).unwrap();
            let block_num_values = value_ord + 1 - block_value_start;
            if raw_num_bytes >= raw_num_bytes_threshold || block_num_values >= max_num_values {
                serialize_block(
                    &values[block_value_start..=value_ord],
                    value_ord + 1,
                    &mut output,
                    &mut directory,
                    &mut block_ranges,
                );
                block_value_start = value_ord + 1;
                raw_num_bytes = 0;
            }
        }
        if block_value_start < values.len() {
            serialize_block(
                &values[block_value_start..],
                values.len(),
                &mut output,
                &mut directory,
                &mut block_ranges,
            );
        }
        let num_blocks = block_ranges.len() as u32;
        output.extend_from_slice(&directory);
        output.extend_from_slice(&column_index_num_bytes.to_le_bytes());
        output.extend_from_slice(&num_blocks.to_le_bytes());
        SerializedPlainColumn {
            bytes: output,
            block_ranges,
        }
    }

    fn serialize_block(
        values: &[&[u8]],
        value_end: usize,
        output: &mut Vec<u8>,
        directory: &mut Vec<u8>,
        block_ranges: &mut Vec<Range<usize>>,
    ) {
        let block = serialize_test_block(values);
        let block_start = output.len();
        output.extend_from_slice(&block);
        let block_end = output.len();
        block_ranges.push(block_start..block_end);
        directory.extend_from_slice(&(block.len() as u32).to_le_bytes());
        directory.extend_from_slice(&(value_end as u32).to_le_bytes());
    }
}
