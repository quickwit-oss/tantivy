//! Merge of `Str` and `Bytes` columns into a plain-encoded column.

use std::io;

use crate::column::PlainColumnSerializer;
use crate::column_index::SerializableColumnIndex;
use crate::{BytesColumn, ColumnIndex, MergeRowOrder, PlainBytesColumn, PlainValueReader, RowId};

/// A `Str` or `Bytes` column merged into a plain column.
///
/// Dictionary encoded inputs are converted: their values are read from their dictionary.
pub(crate) enum PlainMergeInput {
    Dictionary(BytesColumn),
    Plain(PlainBytesColumn),
}

impl PlainMergeInput {
    pub(crate) fn column_index(&self) -> &ColumnIndex {
        match self {
            PlainMergeInput::Dictionary(bytes_column) => &bytes_column.term_ord_column.index,
            PlainMergeInput::Plain(plain_bytes_column) => plain_bytes_column.column_index(),
        }
    }

    fn value_reader(self) -> InputValueReader {
        match self {
            PlainMergeInput::Dictionary(bytes_column) => InputValueReader::Dictionary(bytes_column),
            PlainMergeInput::Plain(plain_bytes_column) => {
                InputValueReader::Plain(plain_bytes_column.reader())
            }
        }
    }
}

/// Reads the values of a `PlainMergeInput`.
enum InputValueReader {
    Dictionary(BytesColumn),
    /// Keeps its current block: reading rows in increasing order loads each block once.
    Plain(PlainValueReader),
}

impl InputValueReader {
    /// Calls `add_value` with each value of `row_id`, in order.
    fn for_each_row_value(
        &mut self,
        row_id: RowId,
        value_buffer: &mut Vec<u8>,
        mut add_value: impl FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        match self {
            InputValueReader::Dictionary(bytes_column) => {
                for term_ord in bytes_column.term_ords(row_id) {
                    append_term(bytes_column, term_ord, value_buffer)?;
                    add_value(value_buffer)?;
                }
            }
            InputValueReader::Plain(plain_value_reader) => {
                let value_range = plain_value_reader
                    .column()
                    .column_index()
                    .value_row_ids(row_id);
                for value_idx in value_range {
                    value_buffer.clear();
                    plain_value_reader.append_value(value_idx, value_buffer)?;
                    add_value(value_buffer)?;
                }
            }
        }
        Ok(())
    }

    /// Calls `add_value` with all of the values, in value order.
    ///
    /// The values are in row order: this is what a stacked merge needs.
    fn for_each_value(
        &mut self,
        value_buffer: &mut Vec<u8>,
        mut add_value: impl FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        match self {
            InputValueReader::Dictionary(bytes_column) => {
                for term_ord in bytes_column.ords().values.iter() {
                    append_term(bytes_column, term_ord, value_buffer)?;
                    add_value(value_buffer)?;
                }
            }
            InputValueReader::Plain(plain_value_reader) => {
                for value_idx in 0..plain_value_reader.column().num_values() {
                    value_buffer.clear();
                    plain_value_reader.append_value(value_idx, value_buffer)?;
                    add_value(value_buffer)?;
                }
            }
        }
        Ok(())
    }
}

/// Replaces the content of `value_buffer` with the term `term_ord` of `bytes_column`.
///
/// This is a dictionary lookup per value: converting a dictionary encoded column is slower than
/// merging a plain column.
fn append_term(
    bytes_column: &BytesColumn,
    term_ord: u64,
    value_buffer: &mut Vec<u8>,
) -> io::Result<()> {
    value_buffer.clear();
    if !bytes_column.ord_to_bytes(term_ord, value_buffer)? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("term ordinal {term_ord} missing from the dictionary"),
        ));
    }
    Ok(())
}

/// Serializes the merge of `inputs` as a plain column.
///
/// Contract: `inputs` has one entry per merged columnar, in the order of `merge_row_order`, and
/// `column_index` is the merge of their column indexes (with an empty index for missing
/// inputs). The values are then written in the order expected by `column_index`.
pub(crate) fn merge_plain_column(
    column_index: SerializableColumnIndex<'_>,
    inputs: Vec<Option<PlainMergeInput>>,
    merge_row_order: &MergeRowOrder,
    output: impl io::Write,
) -> io::Result<()> {
    let mut value_readers: Vec<Option<InputValueReader>> = inputs
        .into_iter()
        .map(|input_opt| input_opt.map(PlainMergeInput::value_reader))
        .collect();
    let mut plain_serializer = PlainColumnSerializer::new(output);
    let mut value_buffer: Vec<u8> = Vec::new();
    match merge_row_order {
        MergeRowOrder::Stack(_) => {
            // No row is deleted or moved: the values are the values of each input, in order.
            for value_reader in value_readers.iter_mut().flatten() {
                value_reader
                    .for_each_value(&mut value_buffer, |value| plain_serializer.add_value(value))?;
            }
        }
        MergeRowOrder::Shuffled(shuffle_merge_order) => {
            for row_addr in shuffle_merge_order.iter_new_to_old_row_addrs() {
                let Some(value_reader) = value_readers[row_addr.segment_ord as usize].as_mut()
                else {
                    continue;
                };
                value_reader.for_each_row_value(row_addr.row_id, &mut value_buffer, |value| {
                    plain_serializer.add_value(value)
                })?;
            }
        }
    }
    plain_serializer.finish(column_index)
}
