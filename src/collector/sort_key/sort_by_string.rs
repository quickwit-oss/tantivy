use columnar::StrColumn;

use crate::collector::sort_key::{term_ords_to_terms, NaturalComparator};
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
        Ok(ByStringColumnSegmentSortKeyComputer { str_column_opt })
    }
}

pub struct ByStringColumnSegmentSortKeyComputer {
    str_column_opt: Option<StrColumn>,
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

    fn convert_segment_sort_key(&self, term_ord: Option<TermOrdinal>) -> Option<String> {
        self.convert_segment_sort_keys(&[term_ord]).pop().flatten()
    }

    fn convert_segment_sort_keys(&self, term_ords: &[Option<TermOrdinal>]) -> Vec<Option<String>> {
        let Some(str_column) = self.str_column_opt.as_ref() else {
            return vec![None; term_ords.len()];
        };
        term_ords_to_terms(str_column, term_ords)
            .into_iter()
            .map(|term| String::try_from(term?).ok())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::SortByString;
    use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
    use crate::schema::{Schema, FAST, STRING};
    use crate::termdict::TermOrdinal;
    use crate::{Index, IndexWriter};

    #[test]
    fn test_batch_conversion_resolves_every_ordinal() -> crate::Result<()> {
        // Enough distinct terms to span many dictionary blocks.
        const NUM_TERMS: u64 = 5_000;
        let mut schema_builder = Schema::builder();
        let name = schema_builder.add_text_field("name", STRING | FAST);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index.writer_for_tests()?;
        for term in 0..NUM_TERMS {
            index_writer.add_document(doc!(name => format!("term-{term:08}")))?;
        }
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let segment_reader = searcher.segment_reader(0);
        let computer = SortByString::for_field("name").segment_sort_key_computer(segment_reader)?;

        // Unsorted, duplicated and missing ordinals, across blocks.
        let term_ords: Vec<Option<TermOrdinal>> = vec![
            Some(4_321),
            Some(7),
            None,
            Some(4_321),
            Some(0),
            Some(NUM_TERMS - 1),
            Some(2_500),
            Some(8),
        ];
        let expected: Vec<Option<String>> = term_ords
            .iter()
            .map(|term_ord| term_ord.map(|term| format!("term-{term:08}")))
            .collect();
        assert_eq!(computer.convert_segment_sort_keys(&term_ords), expected);
        assert_eq!(
            computer.convert_segment_sort_key(Some(7)).as_deref(),
            Some("term-00000007")
        );
        Ok(())
    }
}
