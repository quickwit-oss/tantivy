use columnar::{StrColumn, ValueRange};

use crate::collector::sort_key::sort_key_computer::{
    convert_optional_u64_range_to_u64_range, range_contains_none,
};
use crate::collector::sort_key::NaturalComparator;
use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
use crate::termdict::TermOrdinal;
use crate::{DocId, Score};

/// Sort by the first value of a string column.
///
/// The string can be dynamic (coming from a json field)
/// or static (being specificaly defined in the configuration).
///
/// If the field is multivalued, only the first value is considered.
///
/// Documents that do not have this value are still considered.
/// Their sort key will simply be `None`.
#[derive(Debug, Clone)]
pub struct SortByString {
    column_name: String,
}

impl SortByString {
    /// Creates a new sort by string sort key computer.
    pub fn for_field(column_name: impl ToString) -> Self {
        SortByString {
            column_name: column_name.to_string(),
        }
    }
}

impl SortKeyComputer for SortByString {
    type SortKey = Option<String>;
    type Child = ByStringColumnSegmentSortKeyComputer;
    type Comparator = NaturalComparator;

    fn segment_sort_key_computer(
        &self,
        segment_reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let str_column_opt = segment_reader.fast_fields().str(&self.column_name)?;
        Ok(ByStringColumnSegmentSortKeyComputer {
            str_column_opt,
            buffer: Vec::new(),
            fetch_buffer: Vec::new(),
        })
    }
}

pub struct ByStringColumnSegmentSortKeyComputer {
    str_column_opt: Option<StrColumn>,
    buffer: Vec<(DocId, Option<TermOrdinal>)>,
    fetch_buffer: Vec<Option<Option<TermOrdinal>>>,
}

impl SegmentSortKeyComputer for ByStringColumnSegmentSortKeyComputer {
    type SortKey = Option<String>;
    type SegmentSortKey = Option<TermOrdinal>;
    type SegmentComparator = NaturalComparator;

    #[inline(always)]
    fn segment_sort_key(&mut self, doc: DocId, _score: Score) -> Option<TermOrdinal> {
        let str_column = self.str_column_opt.as_ref()?;
        str_column.ords().first(doc)
    }

    fn segment_sort_keys(
        &mut self,
        docs: &[DocId],
        filter: ValueRange<Self::SegmentSortKey>,
    ) -> &mut Vec<(DocId, Self::SegmentSortKey)> {
        self.fetch_buffer.resize(docs.len(), None);
        if let Some(str_column) = &self.str_column_opt {
            let u64_filter = convert_optional_u64_range_to_u64_range(filter);
            str_column
                .ords()
                .first_vals_in_value_range(docs, &mut self.fetch_buffer, u64_filter);
        } else if range_contains_none(&filter) {
            self.fetch_buffer.fill(Some(None));
        } else {
            self.fetch_buffer.fill(None);
        }

        self.buffer.clear();
        for (&doc, val) in docs.iter().zip(self.fetch_buffer.iter()) {
            if let Some(val) = val {
                self.buffer.push((doc, *val));
            }
        }
        &mut self.buffer
    }

    fn convert_segment_sort_key(&self, term_ord_opt: Option<TermOrdinal>) -> Option<String> {
        // TODO: Individual lookups to the dictionary like this are very likely to repeatedly
        // decompress the same blocks. See https://github.com/quickwit-oss/tantivy/issues/2776
        let term_ord = term_ord_opt?;
        let str_column = self.str_column_opt.as_ref()?;
        let mut bytes = Vec::new();
        str_column
            .dictionary()
            .ord_to_term(term_ord, &mut bytes)
            .ok()?;
        String::try_from(bytes).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Schema, FAST, TEXT};
    use crate::Index;

    #[test]
    fn test_sort_by_string_batch() {
        let mut schema_builder = Schema::builder();
        let field_col = schema_builder.add_text_field("field", FAST | TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();

        index_writer
            .add_document(crate::doc!(field_col => "a"))
            .unwrap();
        index_writer
            .add_document(crate::doc!(field_col => "c"))
            .unwrap();
        index_writer.add_document(crate::doc!()).unwrap();
        index_writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);

        let sorter = SortByString::for_field("field");
        let mut computer = sorter.segment_sort_key_computer(segment_reader).unwrap();

        let docs = vec![0, 1, 2];
        let output = computer.segment_sort_keys(&docs, ValueRange::All);

        // We expect ordinals.
        // "a" -> 0
        // "c" -> 1
        assert_eq!(output, &[(0, Some(0)), (1, Some(1)), (2, None)]);
    }

    #[test]
    fn test_sort_by_string_batch_with_filter() {
        let mut schema_builder = Schema::builder();
        let field_col = schema_builder.add_text_field("field", FAST | TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests().unwrap();

        index_writer
            .add_document(crate::doc!(field_col => "a"))
            .unwrap();
        index_writer
            .add_document(crate::doc!(field_col => "c"))
            .unwrap();
        index_writer.add_document(crate::doc!()).unwrap();
        index_writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);

        let sorter = SortByString::for_field("field");
        let mut computer = sorter.segment_sort_key_computer(segment_reader).unwrap();

        let docs = vec![0, 1, 2];
        // Filter: > "b". "a" is 0, "c" is 1.
        // We want > "a" (ord 0). So we filter > ord 0.
        // 0 is "a", 1 is "c".
        let output = computer.segment_sort_keys(
            &docs,
            ValueRange::GreaterThan(Some(0), false /* inclusive */),
        );

        // Should contain only the document with value "c" (ord 1).
        assert_eq!(output, &[(1, Some(1))]);
    }
}
