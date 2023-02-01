use std::io::{self, Write};

use common::CountingWriter;
use itertools::Itertools;
use sstable::{SSTable, TermOrdinal};

use super::term_merger::TermMerger;
use crate::column_index::{serialize_column_index, SerializableColumnIndex};
use crate::column_values::{serialize_u64_based_column_values, CodecType};
use crate::BytesColumn;

// Serialize [Dictionary, Column, dictionary num bytes U32::LE]
// Column: [Column Index, Column Values, column index num bytes U32::LE]
pub fn merge_bytes_or_str_column(
    column_index: SerializableColumnIndex<'_>,
    bytes_columns: &[BytesColumn],
    output: &mut impl Write,
) -> io::Result<()> {
    // Serialize dict and generate mapping for values
    let mut output = CountingWriter::wrap(output);
    let term_ord_mapping = serialize_merged_dict(bytes_columns, &mut output)?;
    let dictionary_num_bytes: u32 = output.written_bytes() as u32;
    let output = output.finish();

    serialize_bytes_or_str_column(column_index, bytes_columns, &term_ord_mapping, output)?;

    output.write_all(&dictionary_num_bytes.to_le_bytes())?;
    Ok(())
}

fn serialize_bytes_or_str_column(
    column_index: SerializableColumnIndex<'_>,
    bytes_columns: &[BytesColumn],
    term_ord_mapping: &TermOrdinalMapping,
    output: &mut impl Write,
) -> io::Result<()> {
    let column_index_num_bytes = serialize_column_index(column_index, output)?;

    let column_values = move || {
        let iter = bytes_columns
            .iter()
            .enumerate()
            .flat_map(|(segment_ord, byte_column)| {
                let segment_ord = term_ord_mapping.get_segment(segment_ord);
                byte_column
                    .ords()
                    .values
                    .iter()
                    .map(move |term_ord| segment_ord[term_ord as usize])
            });
        iter
    };

    serialize_u64_based_column_values(
        column_values,
        &[CodecType::Bitpacked, CodecType::BlockwiseLinear],
        output,
    )?;

    output.write_all(&column_index_num_bytes.to_le_bytes())?;

    Ok(())
}

fn serialize_merged_dict(
    bytes_columns: &[BytesColumn],
    output: &mut impl Write,
) -> io::Result<TermOrdinalMapping> {
    let mut term_ord_mapping = TermOrdinalMapping::default();

    let mut field_term_streams = Vec::new();
    for column in bytes_columns {
        term_ord_mapping.add_segment(column.dictionary.num_terms());
        let terms = column.dictionary.stream()?;
        field_term_streams.push(terms);
    }

    let mut merged_terms = TermMerger::new(field_term_streams);
    let mut sstable_builder = sstable::VoidSSTable::writer(output);

    let mut current_term_ord = 0;
    while merged_terms.advance() {
        let term_bytes: &[u8] = merged_terms.key();

        sstable_builder.insert(term_bytes, &())?;
        for (segment_ord, from_term_ord) in merged_terms.matching_segments() {
            term_ord_mapping.register_from_to(segment_ord, from_term_ord, current_term_ord);
        }
        current_term_ord += 1;
    }
    sstable_builder.finish()?;

    Ok(term_ord_mapping)
}

#[derive(Default)]
struct TermOrdinalMapping {
    per_segment_new_term_ordinals: Vec<Vec<TermOrdinal>>,
}

impl TermOrdinalMapping {
    fn add_segment(&mut self, max_term_ord: usize) {
        self.per_segment_new_term_ordinals
            .push(vec![TermOrdinal::default(); max_term_ord as usize]);
    }

    fn register_from_to(&mut self, segment_ord: usize, from_ord: TermOrdinal, to_ord: TermOrdinal) {
        self.per_segment_new_term_ordinals[segment_ord][from_ord as usize] = to_ord;
    }

    fn get_segment(&self, segment_ord: usize) -> &[TermOrdinal] {
        &(self.per_segment_new_term_ordinals[segment_ord])[..]
    }
}
