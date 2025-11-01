mod sort_key_computer;

use columnar::StrColumn;
pub use sort_key_computer::{SegmentSortKeyComputer, SortKeyComputer};

use crate::fastfield::FastValue;
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

    fn order(&self) -> Order {
        self.1
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
    TSegmentSortKey:
        ReverseOrder<ReverseType = TSegmentSortKey> + PartialOrd + Clone + 'static + Sync + Send,
{
    type SortKey = TSegmentSortKeyComputer::SortKey;
    type SegmentSortKey = TSegmentSortKey;

    fn sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey {
        let sort_key = self.0.sort_key(doc, score);
        reverse_if_asc(sort_key, self.1)
    }

    fn convert_segment_sort_key(&self, reverse_sort_key: Self::SegmentSortKey) -> Self::SortKey {
        let sort_key = reverse_if_asc(reverse_sort_key, self.1);
        self.0.convert_segment_sort_key(sort_key)
    }
}

// ReverseOrder is a trait that flips the order of a value to match the
// expectation of sorting by "ascending order".
//
// From some type, it can differ a little from just applying `std::cmp::Reverse`.
// In particular, for `Option<T>`, the reverse order is not that of `std::cmp::Reverse<Option<T>>`,
// but rather `Option<std::cmp::Reverse<T>>`:
// Users typically still expect items without a value to appear at the end of the list.
//
// Also, when trying to apply an order dynamically (e.g. the order was passed by an API)
// we do not necessarily have the luxury to have a specific type for the new key.
//
// We then rely on an ReverseOrder implementation with a ReverseOrderType that maps to Self.
pub trait ReverseOrder: Clone {
    type ReverseType: PartialOrd + Clone;

    fn to_reverse_type(self) -> Self::ReverseType;

    fn from_reverse_type(reverse_value: Self::ReverseType) -> Self;
}

fn reverse_if_asc<T>(value: T, order: Order) -> T
where T: ReverseOrder<ReverseType = T> {
    match order {
        Order::Asc => value.to_reverse_type(),
        Order::Desc => value,
    }
}

impl<TFastValue: FastValue> ReverseOrder for TFastValue {
    type ReverseType = TFastValue;

    fn to_reverse_type(self) -> Self::ReverseType {
        // TODO check that the compiler is good enough to compile that to i64::MAX - self for i64
        // for instance.
        TFastValue::from_u64(u64::MAX - self.to_u64())
    }

    fn from_reverse_type(reverse_value: Self::ReverseType) -> Self {
        reverse_value.to_reverse_type()
    }
}

impl ReverseOrder for u32 {
    type ReverseType = u32;

    fn to_reverse_type(self) -> Self::ReverseType {
        u32::MAX - self
    }

    fn from_reverse_type(reverse_value: Self::ReverseType) -> Self {
        reverse_value.to_reverse_type()
    }
}

impl ReverseOrder for f32 {
    type ReverseType = f32;

    fn to_reverse_type(self) -> Self::ReverseType {
        f32::MAX - self
    }

    fn from_reverse_type(reverse_value: Self::ReverseType) -> Self {
        // That's an involution
        reverse_value.to_reverse_type()
    }
}

// The point here is that for Option, we do not want None values to come on top
// when running a Asc query.
impl<T: ReverseOrder> ReverseOrder for Option<T> {
    type ReverseType = Option<T::ReverseType>;

    fn to_reverse_type(self) -> Self::ReverseType {
        self.map(|val| val.to_reverse_type())
    }

    fn from_reverse_type(reverse_value: Self::ReverseType) -> Self {
        reverse_value.map(T::from_reverse_type)
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
