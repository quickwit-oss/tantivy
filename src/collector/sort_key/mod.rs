mod sort_key_computer;

use columnar::StrColumn;
pub use sort_key_computer::{NoScoreFn, SegmentSortKeyComputer, SortKeyComputer};

use crate::termdict::TermOrdinal;
use crate::{DocId, Order, Score};

impl<TSortKeyComputer> SortKeyComputer for (TSortKeyComputer, Order)
where
    TSortKeyComputer: SortKeyComputer,
    (TSortKeyComputer::Child, Order): SegmentSortKeyComputer<SortKey = TSortKeyComputer::SortKey>,
{
    type SortKey = TSortKeyComputer::SortKey;

    type Child = (TSortKeyComputer::Child, Order);

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring()
    }

    fn segment_sort_key_computer(
        &self,
        segment_reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let child = self.0.segment_sort_key_computer(segment_reader)?;
        Ok((child, self.1))
    }
}

impl<TSegmentSortKeyComputer, TSegmentSortKey> SegmentSortKeyComputer
    for (TSegmentSortKeyComputer, Order)
where
    TSegmentSortKeyComputer: SegmentSortKeyComputer<SegmentSortKey = TSegmentSortKey>,
    TSegmentSortKey: BizarroWorldInvolution + PartialOrd + Clone + 'static + Sync + Send,
{
    type SortKey = TSegmentSortKeyComputer::SortKey;
    type SegmentSortKey = TSegmentSortKey;

    fn sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey {
        let sort_key = self.0.sort_key(doc, score);
        sort_key.involution_if_asc(self.1)
    }

    fn convert_segment_sort_key(&self, bizarro_sort_key: Self::SegmentSortKey) -> Self::SortKey {
        let sort_key = bizarro_sort_key.involution_if_asc(self.1);
        self.0.convert_segment_sort_key(sort_key)
    }
}

// BizarroWorldInvolution is a transformation that flips the order of a value.
//
// It is useful when we want to sort things in a way that is given to use dynamically
// (unknown at compile time).
//
// That way the segment score keeps having the same type regardless of the order,
// and we do not rely on an enum.
trait BizarroWorldInvolution: Copy {
    fn involution(&self) -> Self;
    fn involution_if_asc(&self, order: Order) -> Self {
        match order {
            Order::Asc => self.involution(),
            Order::Desc => *self,
        }
    }
}

impl BizarroWorldInvolution for u64 {
    fn involution(&self) -> Self {
        u64::MAX - self
    }
}

// The point here is that for Option, we do not want None values to come on top
// when running a Asc query.
impl BizarroWorldInvolution for Option<u64> {
    #[inline]
    fn involution(&self) -> Self {
        self.map(|val| val.involution())
    }
}

/// Sort by similarity score.
#[derive(Clone, Debug, Copy)]
pub struct ByScore;

impl SortKeyComputer for ByScore {
    type SortKey = Score;

    type Child = ByScore;

    fn requires_scoring(&self) -> bool {
        false
    }

    fn segment_sort_key_computer(
        &self,
        _segment_reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        Ok(ByScore)
    }
}

impl SegmentSortKeyComputer for ByScore {
    type SortKey = Score;

    type SegmentSortKey = Score;

    fn sort_key(&mut self, _doc: DocId, score: Score) -> Score {
        score
    }

    fn convert_segment_sort_key(&self, score: Score) -> Score {
        score
    }
}

/// Sort by a string column
pub struct ByStringColumn {
    column_name: String,
}

impl ByStringColumn {
    pub fn with_column_name(column_name: String) -> Self {
        ByStringColumn { column_name }
    }
}

impl SortKeyComputer for ByStringColumn {
    type SortKey = Option<String>;

    type Child = ByStringColumnSegmentSortKeyComputer;

    fn requires_scoring(&self) -> bool {
        false
    }

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

    fn sort_key(&mut self, doc: DocId, _score: Score) -> Option<TermOrdinal> {
        let str_column = self.str_column_opt.as_ref()?;
        str_column.ords().first(doc)
    }

    fn convert_segment_sort_key(&self, term_ord_opt: Option<TermOrdinal>) -> Option<String> {
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
