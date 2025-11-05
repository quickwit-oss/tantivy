use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
use crate::fastfield::FastValue;
use crate::schema::Schema;
use crate::{DocId, Order, Score};

/// ReverseOrder is a trait that offers a bijection to another type that flips the order of a value
/// to match the expectation of sorting by "ascending order".
///
/// From some type, it can differ a little from just applying `std::cmp::Reverse`.
/// In particular, for `Option<T>`, the reverse order is not that of `std::cmp::Reverse<Option<T>>`,
/// but rather `Option<std::cmp::Reverse<T>>`:
/// Users typically still expect items without a value to appear at the end of the list.
///
/// Also, when trying to apply an order dynamically (e.g. the order was passed by an API)
/// we do not necessarily have the luxury to have a specific type for the new key.
///
/// We then rely on an ReverseOrder implementation with a ReverseOrderType that maps to Self.
pub trait ReverseOrder: Clone {
    /// The type that is used for the reverse order representation.
    ///
    /// This type might be Self.
    type ReverseType: PartialOrd + Clone;

    /// Maps the value to its reverse value.
    fn to_reverse_type(self) -> Self::ReverseType;

    /// Converts a reverse value back to its orignal value.
    fn from_reverse_type(reverse_value: Self::ReverseType) -> Self;
}

/// Helper function: converts a value to its reverse order if order is Asc.
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
        // Disclaimer: This might be suboptimal. The compiler might not be always smart enough
        // to realize that this boils down to `i64::MAX - self` for i64 for instance.
        //
        // Whenever possible, rely on u64 for segment-level collection.
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
impl<TSortKeyComputer> SortKeyComputer for (TSortKeyComputer, Order)
where
    TSortKeyComputer: SortKeyComputer,
    (TSortKeyComputer::Child, Order): SegmentSortKeyComputer<SortKey = TSortKeyComputer::SortKey>,
{
    type SortKey = TSortKeyComputer::SortKey;

    type Child = (TSortKeyComputer::Child, Order);

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)
    }

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
