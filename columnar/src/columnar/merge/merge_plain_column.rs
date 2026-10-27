use std::io;

use crate::column::PlainColumnSerializer;
use crate::column_index::SerializableColumnIndex;
use crate::{BytesColumn, ColumnIndex, MergeRowOrder, PlainBytesColumn, PlainValueReader, RowId};

/// A column merged into a plain-encoded column.
pub(crate) enum PlainMergeInput {
    Plain(PlainValueReader),
    /// Dictionary encoded inputs only happen if the codec of a column changed between the merged
    /// columnars. Their values are read through the term dictionary, which is slow.
    Dictionary(BytesColumn),
}

impl PlainMergeInput {
    pub(crate) fn plain(plain_bytes_column: &PlainBytesColumn) -> PlainMergeInput {
        PlainMergeInput::Plain(plain_bytes_column.reader())
    }

    pub(crate) fn column_index(&self) -> &ColumnIndex {
        match self {
            PlainMergeInput::Plain(reader) => reader.column().column_index(),
            PlainMergeInput::Dictionary(bytes_column) => &bytes_column.ords().index,
        }
    }

    fn num_values(&self) -> u32 {
        match self {
            PlainMergeInput::Plain(reader) => reader.column().num_values(),
            PlainMergeInput::Dictionary(bytes_column) => bytes_column.ords().values.num_vals(),
        }
    }

    /// Writes the bytes of the value `value_idx` into `output`, replacing its content.
    fn read_value(&mut self, value_idx: RowId, output: &mut Vec<u8>) -> io::Result<()> {
        output.clear();
        match self {
            PlainMergeInput::Plain(reader) => reader.append_value(value_idx, output),
            PlainMergeInput::Dictionary(bytes_column) => {
                let term_ord = bytes_column.ords().values.get_val(value_idx);
                if !bytes_column.ord_to_bytes(term_ord, output)? {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "term ordinal missing from the dictionary",
                    ));
                }
                Ok(())
            }
        }
    }
}

/// Merges `inputs` into a plain-encoded column.
///
/// `inputs[i]` is the column of the i-th columnar, if it has one. `column_index` is the merged
/// column index of the inputs: the values are written in the order it expects.
///
/// Blocks are cut again and their dictionaries are trained again.
pub(crate) fn merge_plain_bytes_or_str_column(
    column_index: SerializableColumnIndex<'_>,
    mut inputs: Vec<Option<PlainMergeInput>>,
    merge_row_order: &MergeRowOrder,
    output: &mut impl io::Write,
) -> io::Result<()> {
    let mut serializer = PlainColumnSerializer::new(output);
    let mut value_buffer: Vec<u8> = Vec::new();
    match merge_row_order {
        MergeRowOrder::Stack(_) => {
            for input in inputs.iter_mut().flatten() {
                for value_idx in 0..input.num_values() {
                    input.read_value(value_idx, &mut value_buffer)?;
                    serializer.add_value(&value_buffer)?;
                }
            }
        }
        MergeRowOrder::Shuffled(shuffle_merge_order) => {
            for old_row_addr in shuffle_merge_order.iter_new_to_old_row_addrs() {
                let Some(input) = inputs[old_row_addr.segment_ord as usize].as_mut() else {
                    continue;
                };
                let value_range = input.column_index().value_row_ids(old_row_addr.row_id);
                for value_idx in value_range {
                    input.read_value(value_idx, &mut value_buffer)?;
                    serializer.add_value(&value_buffer)?;
                }
            }
        }
    }
    serializer.finish(column_index)
}

#[cfg(test)]
mod tests {
    use crate::dynamic_column::str_or_bytes_values_per_doc;
    use crate::{
        ColumnType, ColumnarReader, ColumnarWriter, DynamicColumn, MergeRowOrder, RowAddr, RowId,
        ShuffleMergeOrder, StackMergeOrder, StrColumnCodec, merge_columnar,
        merge_columnar_with_codecs,
    };

    /// Builds a columnar with a `Str` column named "text".
    fn build_columnar(docs: &[&[&str]], codec: StrColumnCodec) -> ColumnarReader {
        let mut columnar_writer = ColumnarWriter::default();
        for (doc, doc_values) in docs.iter().enumerate() {
            for value in *doc_values {
                columnar_writer.record_str_with_codec(doc as RowId, "text", value, codec);
            }
        }
        columnar_writer.record_numerical(0, "text", 1u64);
        let mut buffer = Vec::new();
        columnar_writer
            .serialize(docs.len() as RowId, None, &mut buffer)
            .unwrap();
        ColumnarReader::open(buffer).unwrap()
    }

    fn merge(
        columnars: &[&ColumnarReader],
        required_columns: &[(String, ColumnType, StrColumnCodec)],
        merge_row_order: MergeRowOrder,
    ) -> ColumnarReader {
        let mut buffer = Vec::new();
        merge_columnar_with_codecs(columnars, required_columns, merge_row_order, &mut buffer)
            .unwrap();
        ColumnarReader::open(buffer).unwrap()
    }

    fn text_column(columnar: &ColumnarReader) -> DynamicColumn {
        let handles = columnar.read_columns("text").unwrap();
        let str_handle = handles
            .iter()
            .find(|handle| handle.column_type() == ColumnType::Str)
            .unwrap();
        str_handle.open().unwrap()
    }

    fn expected_values(docs: &[&[&str]]) -> Vec<Vec<Vec<u8>>> {
        docs.iter()
            .map(|doc_values| {
                doc_values
                    .iter()
                    .map(|value| value.as_bytes().to_vec())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn test_stack_merge_plain_columns() {
        let docs1: &[&[&str]] = &[&["a"], &[], &["b", "c"]];
        let docs2: &[&[&str]] = &[&["d"], &["e"]];
        let columnar1 = build_columnar(docs1, StrColumnCodec::Plain);
        let columnar2 = build_columnar(docs2, StrColumnCodec::Plain);
        let columnars = &[&columnar1, &columnar2];
        let merged = merge(
            columnars,
            &[],
            MergeRowOrder::Stack(StackMergeOrder::stack(columnars)),
        );
        let text_column = text_column(&merged);
        assert!(matches!(text_column, DynamicColumn::PlainStr(_)));
        let expected_docs: &[&[&str]] = &[&["a"], &[], &["b", "c"], &["d"], &["e"]];
        assert_eq!(
            str_or_bytes_values_per_doc(&text_column),
            expected_values(expected_docs)
        );
        // The numerical column with the same name is still there.
        assert_eq!(merged.read_columns("text").unwrap().len(), 2);
    }

    #[test]
    fn test_shuffled_merge_plain_columns() {
        let docs1: &[&[&str]] = &[&["a"], &[], &["b", "c"]];
        let docs2: &[&[&str]] = &[&["d"], &["e"]];
        let columnar1 = build_columnar(docs1, StrColumnCodec::Plain);
        let columnar2 = build_columnar(docs2, StrColumnCodec::Plain);
        let columnars = &[&columnar1, &columnar2];
        // Doc 1 of the first columnar is deleted.
        let new_to_old_row_addrs = vec![
            RowAddr {
                segment_ord: 1,
                row_id: 1,
            },
            RowAddr {
                segment_ord: 0,
                row_id: 2,
            },
            RowAddr {
                segment_ord: 0,
                row_id: 0,
            },
            RowAddr {
                segment_ord: 1,
                row_id: 0,
            },
        ];
        let shuffle_merge_order = ShuffleMergeOrder::for_test(&[3, 2], new_to_old_row_addrs);
        let merged = merge(columnars, &[], MergeRowOrder::Shuffled(shuffle_merge_order));
        let expected_docs: &[&[&str]] = &[&["e"], &["b", "c"], &["a"], &["d"]];
        assert_eq!(
            str_or_bytes_values_per_doc(&text_column(&merged)),
            expected_values(expected_docs)
        );
    }

    #[test]
    fn test_merge_dictionary_and_plain_columns_gives_a_plain_column() {
        let docs1: &[&[&str]] = &[&["a"], &["b", "a"]];
        let docs2: &[&[&str]] = &[&[], &["c"]];
        let columnar1 = build_columnar(docs1, StrColumnCodec::Dictionary);
        let columnar2 = build_columnar(docs2, StrColumnCodec::Plain);
        let columnars = &[&columnar1, &columnar2];
        let mut buffer = Vec::new();
        merge_columnar(
            columnars,
            &[],
            MergeRowOrder::Stack(StackMergeOrder::stack(columnars)),
            &mut buffer,
        )
        .unwrap();
        let merged = ColumnarReader::open(buffer).unwrap();
        let text_column = text_column(&merged);
        assert!(matches!(text_column, DynamicColumn::PlainStr(_)));
        let expected_docs: &[&[&str]] = &[&["a"], &["b", "a"], &[], &["c"]];
        assert_eq!(
            str_or_bytes_values_per_doc(&text_column),
            expected_values(expected_docs)
        );
    }

    #[test]
    fn test_merge_dictionary_columns_stays_dictionary_encoded() {
        let docs: &[&[&str]] = &[&["a"]];
        let columnar1 = build_columnar(docs, StrColumnCodec::Dictionary);
        let columnar2 = build_columnar(docs, StrColumnCodec::Dictionary);
        let columnars = &[&columnar1, &columnar2];
        let merged = merge(
            columnars,
            &[],
            MergeRowOrder::Stack(StackMergeOrder::stack(columnars)),
        );
        assert!(matches!(text_column(&merged), DynamicColumn::Str(_)));
    }

    #[test]
    fn test_merge_required_plain_column() {
        let docs: &[&[&str]] = &[&["a"]];
        let columnar1 = build_columnar(docs, StrColumnCodec::Dictionary);
        let columnar2 = build_columnar(docs, StrColumnCodec::Dictionary);
        let columnars = &[&columnar1, &columnar2];
        let required_columns = vec![
            ("text".to_string(), ColumnType::Str, StrColumnCodec::Plain),
            ("absent".to_string(), ColumnType::Str, StrColumnCodec::Plain),
        ];
        let merged = merge(
            columnars,
            &required_columns,
            MergeRowOrder::Stack(StackMergeOrder::stack(columnars)),
        );
        let text_column = text_column(&merged);
        assert!(matches!(text_column, DynamicColumn::PlainStr(_)));
        assert_eq!(
            str_or_bytes_values_per_doc(&text_column),
            expected_values(&[&["a"], &["a"]])
        );
        let absent_handles = merged.read_columns("absent").unwrap();
        assert_eq!(absent_handles.len(), 1);
        assert_eq!(absent_handles[0].str_codec(), Some(StrColumnCodec::Plain));
        assert_eq!(absent_handles[0].open().unwrap().num_values(), 0);
    }

    #[test]
    fn test_merge_required_plain_numerical_column_is_an_error() {
        let columnar = build_columnar(&[&["a"]], StrColumnCodec::Plain);
        let columnars = &[&columnar];
        let mut buffer = Vec::new();
        let merge_result = merge_columnar_with_codecs(
            columnars,
            &[(
                "numbers".to_string(),
                ColumnType::U64,
                StrColumnCodec::Plain,
            )],
            MergeRowOrder::Stack(StackMergeOrder::stack(columnars)),
            &mut buffer,
        );
        assert!(merge_result.is_err());
    }

    /// Builds a columnar with a single `Str` column named "text".
    fn build_text_columnar(docs: &[Vec<String>], codec: StrColumnCodec) -> ColumnarReader {
        let mut columnar_writer = ColumnarWriter::default();
        for (doc, doc_values) in docs.iter().enumerate() {
            for value in doc_values {
                columnar_writer.record_str_with_codec(doc as RowId, "text", value, codec);
            }
        }
        let mut buffer = Vec::new();
        columnar_writer
            .serialize(docs.len() as RowId, None, &mut buffer)
            .unwrap();
        ColumnarReader::open(buffer).unwrap()
    }

    /// Returns the values of the "text" column, and the codec of the column if it exists.
    fn text_values(columnar: &ColumnarReader) -> (Vec<Vec<Vec<u8>>>, Option<StrColumnCodec>) {
        let handles = columnar.read_columns("text").unwrap();
        let Some(handle) = handles.first() else {
            return (vec![Vec::new(); columnar.num_docs() as usize], None);
        };
        let dynamic_column = handle.open().unwrap();
        (
            str_or_bytes_values_per_doc(&dynamic_column),
            handle.str_codec(),
        )
    }

    fn merge_both_orders(columnars: &[&ColumnarReader]) -> [ColumnarReader; 2] {
        let stacked = merge(
            columnars,
            &[],
            MergeRowOrder::Stack(StackMergeOrder::stack(columnars)),
        );
        // Reverse the docs, and delete every third doc.
        let mut new_to_old_row_addrs: Vec<RowAddr> = Vec::new();
        for (segment_ord, columnar) in columnars.iter().enumerate().rev() {
            for row_id in (0..columnar.num_docs()).rev() {
                if row_id % 3 != 1 {
                    new_to_old_row_addrs.push(RowAddr {
                        segment_ord: segment_ord as u32,
                        row_id,
                    });
                }
            }
        }
        let segment_num_rows: Vec<RowId> = columnars
            .iter()
            .map(|columnar| columnar.num_docs())
            .collect();
        let shuffled = merge(
            columnars,
            &[],
            MergeRowOrder::Shuffled(ShuffleMergeOrder::for_test(
                &segment_num_rows,
                new_to_old_row_addrs,
            )),
        );
        [stacked, shuffled]
    }

    use proptest::prelude::*;

    fn docs_strategy() -> impl Strategy<Value = Vec<Vec<String>>> {
        proptest::collection::vec(proptest::collection::vec("[a-c]{0,3}", 0..3), 1..20)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]
        #[test]
        fn test_proptest_plain_matches_dictionary(
            columnars_docs in proptest::collection::vec(docs_strategy(), 1..4)
        ) {
            let plain_columnars: Vec<ColumnarReader> = columnars_docs
                .iter()
                .map(|docs| build_text_columnar(docs, StrColumnCodec::Plain))
                .collect();
            let dictionary_columnars: Vec<ColumnarReader> = columnars_docs
                .iter()
                .map(|docs| build_text_columnar(docs, StrColumnCodec::Dictionary))
                .collect();
            for (plain_columnar, dictionary_columnar) in plain_columnars.iter().zip(&dictionary_columnars) {
                prop_assert_eq!(text_values(plain_columnar).0, text_values(dictionary_columnar).0);
            }
            let plain_columnar_refs: Vec<&ColumnarReader> = plain_columnars.iter().collect();
            let dictionary_columnar_refs: Vec<&ColumnarReader> = dictionary_columnars.iter().collect();
            let plain_merged = merge_both_orders(&plain_columnar_refs);
            let dictionary_merged = merge_both_orders(&dictionary_columnar_refs);
            for (plain_merged_columnar, dictionary_merged_columnar) in plain_merged.iter().zip(&dictionary_merged) {
                let (plain_values, plain_codec) = text_values(plain_merged_columnar);
                let (dictionary_values, _) = text_values(dictionary_merged_columnar);
                prop_assert_eq!(plain_values, dictionary_values);
                prop_assert!(plain_codec.is_none() || plain_codec == Some(StrColumnCodec::Plain));
            }
        }
    }
}
