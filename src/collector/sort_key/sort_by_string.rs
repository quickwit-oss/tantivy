use columnar::StrColumn;

use crate::collector::sort_key::NaturalComparator;
use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
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

/// All keys in one segment use the same variant. Only decoded keys cross segments.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum BytesSegmentSortKey {
    /// An ordinal from a dictionary-encoded column.
    Ordinal(u64),
    /// A decoded compact value, compared lexicographically.
    Value(Vec<u8>),
}

impl SegmentSortKeyComputer for ByStringColumnSegmentSortKeyComputer {
    type SortKey = Option<String>;
    type SegmentSortKey = Option<BytesSegmentSortKey>;
    type SegmentComparator = NaturalComparator;

    #[inline(always)]
    fn segment_sort_key(&mut self, doc: DocId, _score: Score) -> Self::SegmentSortKey {
        let str_column = self.str_column_opt.as_ref()?;
        if let Some(ords) = str_column.ords_opt() {
            return ords.first(doc).map(BytesSegmentSortKey::Ordinal);
        }
        let mut bytes = Vec::new();
        str_column
            .first(doc, &mut bytes)
            .expect("compact sort value read failed")
            .then_some(BytesSegmentSortKey::Value(bytes))
    }

    fn convert_segment_sort_key(&self, term_ord_opt: Self::SegmentSortKey) -> Option<String> {
        // TODO: Individual lookups to the dictionary like this are very likely to repeatedly
        // decompress the same blocks. See https://github.com/quickwit-oss/tantivy/issues/2776
        let term_ord = match term_ord_opt? {
            BytesSegmentSortKey::Value(bytes) => return String::from_utf8(bytes).ok(),
            BytesSegmentSortKey::Ordinal(ord) => ord,
        };
        let str_column = self.str_column_opt.as_ref()?;
        let mut bytes = Vec::new();
        str_column
            .dictionary()
            .ord_to_term(term_ord, &mut bytes)
            .ok()?;
        String::try_from(bytes).ok()
    }
}
