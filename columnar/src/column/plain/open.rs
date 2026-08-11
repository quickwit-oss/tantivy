use std::io;

use common::HasLen;
use common::file_slice::FileSlice;

use super::{PlainBlockIndex, PlainBytesColumn};
use crate::Version;

const PLAIN_FOOTER_NUM_BYTES: usize = 8;
const PLAIN_BLOCK_INDEX_ENDPOINT_NUM_BYTES: usize = 4;

pub(crate) fn open_plain_bytes_column(data: FileSlice) -> io::Result<PlainBytesColumn> {
    if data.len() < PLAIN_FOOTER_NUM_BYTES {
        return Err(invalid_data("truncated plain string/byte column footer"));
    }
    let (body, footer_slice) = data.split_from_end(PLAIN_FOOTER_NUM_BYTES);
    let footer = footer_slice.read_bytes()?;
    let column_index_num_bytes = read_u32(&footer[0..4]) as usize;
    let num_blocks = read_u32(&footer[4..8]) as usize;
    let endpoint_array_num_bytes = num_blocks
        .checked_mul(PLAIN_BLOCK_INDEX_ENDPOINT_NUM_BYTES)
        .ok_or_else(|| invalid_data("plain column block directory length overflows"))?;
    let directory_num_bytes = endpoint_array_num_bytes
        .checked_mul(2)
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
    let (end_bytes_data, end_values_data) = directory.as_slice().split_at(endpoint_array_num_bytes);
    let end_bytes: Box<[usize]> = end_bytes_data
        .chunks_exact(PLAIN_BLOCK_INDEX_ENDPOINT_NUM_BYTES)
        .map(|bytes| read_u32(bytes) as usize)
        .collect();
    let end_values: Box<[u32]> = end_values_data
        .chunks_exact(PLAIN_BLOCK_INDEX_ENDPOINT_NUM_BYTES)
        .map(read_u32)
        .collect();
    let blocks = PlainBlockIndex::try_new(end_bytes, end_values, block_data.len())?;
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
    use std::sync::{Arc, Mutex};

    use common::file_slice::{FileHandle, FileSlice};
    use common::{HasLen, OwnedBytes};

    use super::*;
    use crate::column::{
        BytesColumn, StrColumn, open_column_bytes, open_column_bytes_from_file_slice,
        open_column_str,
    };
    use crate::column_index::{SerializableColumnIndex, serialize_column_index};
    use crate::{PayloadEncoding, Version};

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
    fn test_open_v3_plain_bytes_column() {
        let data = serialize_plain_column(&[b"alpha", b"", &[0, 255]]).bytes;
        let BytesColumn::Plain(column) =
            open_column_bytes(OwnedBytes::new(data), Version::V3).unwrap()
        else {
            panic!("expected a plain byte column")
        };
        assert_eq!(column.num_values(), 3);
        let mut accessor = column.accessor();
        assert_eq!(accessor.get_val(0), b"alpha");
        assert_eq!(accessor.get_val(1), b"");
        assert_eq!(accessor.first(2), Some(&[0, 255][..]));
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
        assert_eq!(accessor.first(1), Some("tea"));

        let invalid_utf8 = serialize_plain_column(&[&[0xff]]).bytes;
        let StrColumn::Plain(column) =
            open_column_str(OwnedBytes::new(invalid_utf8), Version::V3).unwrap()
        else {
            panic!("expected a plain string column")
        };
        assert!(column.accessor().get_val(0).is_err());
    }

    #[test]
    fn test_open_v3_plain_column_rejects_inconsistent_block_index() {
        let mut data = serialize_plain_column(&[b"value"]).bytes;
        let directory_start = data.len() - PLAIN_FOOTER_NUM_BYTES - 8;
        let end_byte = read_u32(&data[directory_start..directory_start + 4]);
        data[directory_start..directory_start + 4].copy_from_slice(&(end_byte + 1).to_le_bytes());
        let error = open_column_bytes(OwnedBytes::new(data), Version::V3).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_plain_column_block_index_serializes_endpoint_arrays() {
        let serialized =
            serialize_plain_column_with_limits(&[b"zero", b"one", b"two"], usize::MAX, 1);
        let num_blocks = serialized.block_ranges.len();
        let endpoint_array_num_bytes = num_blocks * PLAIN_BLOCK_INDEX_ENDPOINT_NUM_BYTES;
        let directory_start =
            serialized.bytes.len() - PLAIN_FOOTER_NUM_BYTES - endpoint_array_num_bytes * 2;
        let directory =
            &serialized.bytes[directory_start..directory_start + endpoint_array_num_bytes * 2];
        let (end_bytes_data, end_values_data) = directory.split_at(endpoint_array_num_bytes);

        let end_bytes: Vec<u32> = end_bytes_data
            .chunks_exact(PLAIN_BLOCK_INDEX_ENDPOINT_NUM_BYTES)
            .map(read_u32)
            .collect();
        let end_values: Vec<u32> = end_values_data
            .chunks_exact(PLAIN_BLOCK_INDEX_ENDPOINT_NUM_BYTES)
            .map(read_u32)
            .collect();
        let block_data_start = serialized.block_ranges[0].start;
        let expected_end_bytes: Vec<u32> = serialized
            .block_ranges
            .iter()
            .map(|block_range| (block_range.end - block_data_start) as u32)
            .collect();

        assert_eq!(end_bytes, expected_end_bytes);
        assert_eq!(end_values, [1, 2, 3]);
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
        assert_eq!(accessor.get_val(0), b"zero");
        assert_eq!(handle.reads(), [serialized.block_ranges[0].clone()]);

        assert_eq!(accessor.get_val(0), b"zero");
        assert_eq!(handle.reads(), [serialized.block_ranges[0].clone()]);

        for (value_ord, value) in values.iter().enumerate().skip(1) {
            assert_eq!(accessor.get_val(value_ord as u32), *value);
        }
        assert_eq!(accessor.num_cached_blocks(), 1);
        assert_eq!(handle.reads().len(), 5);

        assert_eq!(accessor.get_val(0), b"zero");
        assert_eq!(accessor.num_cached_blocks(), 1);
        assert_eq!(handle.reads().len(), 6);
        assert_eq!(handle.reads().last(), Some(&serialized.block_ranges[0]));

        let mut second_accessor = column.accessor();
        assert_eq!(second_accessor.get_val(0), b"zero");
        assert_eq!(handle.reads().len(), 7);
        assert_eq!(handle.reads().last(), Some(&serialized.block_ranges[0]));
    }

    #[test]
    fn test_plain_column_block_limits() {
        assert_eq!(
            super::super::PLAIN_BLOCK_RAW_NUM_BYTES_THRESHOLD,
            10 * 1024 * 1024
        );
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
            super::super::PLAIN_BLOCK_RAW_NUM_BYTES_THRESHOLD,
            super::super::PLAIN_BLOCK_MAX_NUM_VALUES,
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
        let mut end_bytes = Vec::new();
        let mut end_values = Vec::new();
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
                    &mut end_bytes,
                    &mut end_values,
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
                &mut end_bytes,
                &mut end_values,
                &mut block_ranges,
            );
        }
        let num_blocks = block_ranges.len() as u32;
        for end_byte in end_bytes {
            output.extend_from_slice(&end_byte.to_le_bytes());
        }
        for end_value in end_values {
            output.extend_from_slice(&end_value.to_le_bytes());
        }
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
        end_bytes: &mut Vec<u32>,
        end_values: &mut Vec<u32>,
        block_ranges: &mut Vec<Range<usize>>,
    ) {
        let block = super::super::serialize_test_block(values);
        let block_start = output.len();
        output.extend_from_slice(&block);
        let block_end = output.len();
        block_ranges.push(block_start..block_end);
        let previous_end_byte = end_bytes.last().copied().unwrap_or(0);
        end_bytes.push(previous_end_byte + block.len() as u32);
        end_values.push(value_end as u32);
    }
}
