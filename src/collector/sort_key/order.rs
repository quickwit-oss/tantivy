use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
use crate::schema::Schema;
use crate::{DocId, Order, Score};

/// Comparator trait defining the order in which documents should be ordered.
pub trait Comparator<T>: Send + Sync + std::fmt::Debug + Default {
    /// Return the order between two values.
    fn compare(&self, lhs: &T, rhs: &T) -> Ordering;
}

/// With the natural comparator, the top k collector will return
/// the top documents in decreasing order.
#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize)]
pub struct NaturalComparator;

impl<T: PartialOrd> Comparator<T> for NaturalComparator {
    #[inline(always)]
    fn compare(&self, lhs: &T, rhs: &T) -> Ordering {
        lhs.partial_cmp(rhs).unwrap()
    }
}

/// Sorts document in reverse order.
///
/// If the sort key is None, it will considered as the lowest value, and will therefore appear
/// first.
///
/// The ReverseComparator does not necessarily imply that the sort order is reversed compared
/// to the NaturalComparator. In presence of a tie on the sort key, documents will always be
/// sorted by ascending `DocId`/`DocAddress` in TopN results, regardless of the comparator.
#[derive(Debug, Copy, Clone, Default, Serialize, Deserialize)]
pub struct ReverseComparator;

impl<T> Comparator<T> for ReverseComparator
where NaturalComparator: Comparator<T>
{
    #[inline(always)]
    fn compare(&self, lhs: &T, rhs: &T) -> Ordering {
        NaturalComparator.compare(rhs, lhs)
    }
}

/// Sorts document in reverse order, but considers None as having the lowest value.
///
/// This is usually what is wanted when sorting by a field in an ascending order.
/// For instance, in a e-commerce website, if I sort by price ascending, I most likely want the
/// cheapest items first, and the items without a price at last.
#[derive(Debug, Copy, Clone, Default)]
pub struct ReverseNoneIsLowerComparator;

impl<T> Comparator<Option<T>> for ReverseNoneIsLowerComparator
where ReverseComparator: Comparator<T>
{
    #[inline(always)]
    fn compare(&self, lhs_opt: &Option<T>, rhs_opt: &Option<T>) -> Ordering {
        match (lhs_opt, rhs_opt) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(lhs), Some(rhs)) => ReverseComparator.compare(lhs, rhs),
        }
    }
}

impl Comparator<u32> for ReverseNoneIsLowerComparator {
    #[inline(always)]
    fn compare(&self, lhs: &u32, rhs: &u32) -> Ordering {
        ReverseComparator.compare(lhs, rhs)
    }
}

impl Comparator<u64> for ReverseNoneIsLowerComparator {
    #[inline(always)]
    fn compare(&self, lhs: &u64, rhs: &u64) -> Ordering {
        ReverseComparator.compare(lhs, rhs)
    }
}

impl Comparator<f64> for ReverseNoneIsLowerComparator {
    #[inline(always)]
    fn compare(&self, lhs: &f64, rhs: &f64) -> Ordering {
        ReverseComparator.compare(lhs, rhs)
    }
}

impl Comparator<f32> for ReverseNoneIsLowerComparator {
    #[inline(always)]
    fn compare(&self, lhs: &f32, rhs: &f32) -> Ordering {
        ReverseComparator.compare(lhs, rhs)
    }
}

impl Comparator<i64> for ReverseNoneIsLowerComparator {
    #[inline(always)]
    fn compare(&self, lhs: &i64, rhs: &i64) -> Ordering {
        ReverseComparator.compare(lhs, rhs)
    }
}

impl Comparator<String> for ReverseNoneIsLowerComparator {
    #[inline(always)]
    fn compare(&self, lhs: &String, rhs: &String) -> Ordering {
        ReverseComparator.compare(lhs, rhs)
    }
}

/// An enum representing the different sort orders.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum ComparatorEnum {
    /// Natural order (See [NaturalComparator])
    #[default]
    Natural,
    /// Reverse order (See [ReverseComparator])
    Reverse,
    /// Reverse order by treating None as the lowest value.(See [ReverseNoneLowerComparator])
    ReverseNoneLower,
}

impl From<Order> for ComparatorEnum {
    fn from(order: Order) -> Self {
        match order {
            Order::Asc => ComparatorEnum::ReverseNoneLower,
            Order::Desc => ComparatorEnum::Natural,
        }
    }
}

impl<T> Comparator<T> for ComparatorEnum
where
    ReverseNoneIsLowerComparator: Comparator<T>,
    NaturalComparator: Comparator<T>,
    ReverseComparator: Comparator<T>,
{
    #[inline(always)]
    fn compare(&self, lhs: &T, rhs: &T) -> Ordering {
        match self {
            ComparatorEnum::Natural => NaturalComparator.compare(lhs, rhs),
            ComparatorEnum::Reverse => ReverseComparator.compare(lhs, rhs),
            ComparatorEnum::ReverseNoneLower => ReverseNoneIsLowerComparator.compare(lhs, rhs),
        }
    }
}

impl<Head, Tail, LeftComparator, RightComparator> Comparator<(Head, Tail)>
    for (LeftComparator, RightComparator)
where
    LeftComparator: Comparator<Head>,
    RightComparator: Comparator<Tail>,
{
    #[inline(always)]
    fn compare(&self, lhs: &(Head, Tail), rhs: &(Head, Tail)) -> Ordering {
        self.0
            .compare(&lhs.0, &rhs.0)
            .then_with(|| self.1.compare(&lhs.1, &rhs.1))
    }
}

impl<Type1, Type2, Type3, Comparator1, Comparator2, Comparator3> Comparator<(Type1, (Type2, Type3))>
    for (Comparator1, Comparator2, Comparator3)
where
    Comparator1: Comparator<Type1>,
    Comparator2: Comparator<Type2>,
    Comparator3: Comparator<Type3>,
{
    #[inline(always)]
    fn compare(&self, lhs: &(Type1, (Type2, Type3)), rhs: &(Type1, (Type2, Type3))) -> Ordering {
        self.0
            .compare(&lhs.0, &rhs.0)
            .then_with(|| self.1.compare(&lhs.1 .0, &rhs.1 .0))
            .then_with(|| self.2.compare(&lhs.1 .1, &rhs.1 .1))
    }
}

impl<Type1, Type2, Type3, Comparator1, Comparator2, Comparator3> Comparator<(Type1, Type2, Type3)>
    for (Comparator1, Comparator2, Comparator3)
where
    Comparator1: Comparator<Type1>,
    Comparator2: Comparator<Type2>,
    Comparator3: Comparator<Type3>,
{
    #[inline(always)]
    fn compare(&self, lhs: &(Type1, Type2, Type3), rhs: &(Type1, Type2, Type3)) -> Ordering {
        self.0
            .compare(&lhs.0, &rhs.0)
            .then_with(|| self.1.compare(&lhs.1, &rhs.1))
            .then_with(|| self.2.compare(&lhs.2, &rhs.2))
    }
}

impl<Type1, Type2, Type3, Type4, Comparator1, Comparator2, Comparator3, Comparator4>
    Comparator<(Type1, (Type2, (Type3, Type4)))>
    for (Comparator1, Comparator2, Comparator3, Comparator4)
where
    Comparator1: Comparator<Type1>,
    Comparator2: Comparator<Type2>,
    Comparator3: Comparator<Type3>,
    Comparator4: Comparator<Type4>,
{
    #[inline(always)]
    fn compare(
        &self,
        lhs: &(Type1, (Type2, (Type3, Type4))),
        rhs: &(Type1, (Type2, (Type3, Type4))),
    ) -> Ordering {
        self.0
            .compare(&lhs.0, &rhs.0)
            .then_with(|| self.1.compare(&lhs.1 .0, &rhs.1 .0))
            .then_with(|| self.2.compare(&lhs.1 .1 .0, &rhs.1 .1 .0))
            .then_with(|| self.3.compare(&lhs.1 .1 .1, &rhs.1 .1 .1))
    }
}

impl<Type1, Type2, Type3, Type4, Comparator1, Comparator2, Comparator3, Comparator4>
    Comparator<(Type1, Type2, Type3, Type4)>
    for (Comparator1, Comparator2, Comparator3, Comparator4)
where
    Comparator1: Comparator<Type1>,
    Comparator2: Comparator<Type2>,
    Comparator3: Comparator<Type3>,
    Comparator4: Comparator<Type4>,
{
    #[inline(always)]
    fn compare(
        &self,
        lhs: &(Type1, Type2, Type3, Type4),
        rhs: &(Type1, Type2, Type3, Type4),
    ) -> Ordering {
        self.0
            .compare(&lhs.0, &rhs.0)
            .then_with(|| self.1.compare(&lhs.1, &rhs.1))
            .then_with(|| self.2.compare(&lhs.2, &rhs.2))
            .then_with(|| self.3.compare(&lhs.3, &rhs.3))
    }
}

impl<TSortKeyComputer> SortKeyComputer for (TSortKeyComputer, ComparatorEnum)
where
    TSortKeyComputer: SortKeyComputer,
    ComparatorEnum: Comparator<TSortKeyComputer::SortKey>,
    ComparatorEnum: Comparator<
        <<TSortKeyComputer as SortKeyComputer>::Child as SegmentSortKeyComputer>::SegmentSortKey,
    >,
{
    type SortKey = TSortKeyComputer::SortKey;

    type Child = SegmentSortKeyComputerWithComparator<TSortKeyComputer::Child, Self::Comparator>;

    type Comparator = ComparatorEnum;

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring()
    }

    fn comparator(&self) -> Self::Comparator {
        self.1
    }

    fn segment_sort_key_computer(
        &self,
        segment_reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let child = self.0.segment_sort_key_computer(segment_reader)?;
        Ok(SegmentSortKeyComputerWithComparator {
            segment_sort_key_computer: child,
            comparator: self.comparator(),
        })
    }
}

impl<TSortKeyComputer> SortKeyComputer for (TSortKeyComputer, Order)
where
    TSortKeyComputer: SortKeyComputer,
    ComparatorEnum: Comparator<TSortKeyComputer::SortKey>,
    ComparatorEnum: Comparator<
        <<TSortKeyComputer as SortKeyComputer>::Child as SegmentSortKeyComputer>::SegmentSortKey,
    >,
{
    type SortKey = TSortKeyComputer::SortKey;

    type Child = SegmentSortKeyComputerWithComparator<TSortKeyComputer::Child, Self::Comparator>;

    type Comparator = ComparatorEnum;

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.0.check_schema(schema)
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring()
    }

    fn comparator(&self) -> Self::Comparator {
        self.1.into()
    }

    fn segment_sort_key_computer(
        &self,
        segment_reader: &crate::SegmentReader,
    ) -> crate::Result<Self::Child> {
        let child = self.0.segment_sort_key_computer(segment_reader)?;
        Ok(SegmentSortKeyComputerWithComparator {
            segment_sort_key_computer: child,
            comparator: self.comparator(),
        })
    }
}

/// A segment sort key computer with a custom ordering.
pub struct SegmentSortKeyComputerWithComparator<TSegmentSortKeyComputer, TComparator> {
    segment_sort_key_computer: TSegmentSortKeyComputer,
    comparator: TComparator,
}

impl<TSegmentSortKeyComputer, TSegmentSortKey, TComparator> SegmentSortKeyComputer
    for SegmentSortKeyComputerWithComparator<TSegmentSortKeyComputer, TComparator>
where
    TSegmentSortKeyComputer: SegmentSortKeyComputer<SegmentSortKey = TSegmentSortKey>,
    TSegmentSortKey: PartialOrd + Clone + 'static + Sync + Send,
    TComparator: Comparator<TSegmentSortKey> + 'static + Sync + Send,
{
    type SortKey = TSegmentSortKeyComputer::SortKey;
    type SegmentSortKey = TSegmentSortKey;

    fn segment_sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey {
        self.segment_sort_key_computer.segment_sort_key(doc, score)
    }

    #[inline(always)]
    fn compare_segment_sort_key(
        &self,
        left: &Self::SegmentSortKey,
        right: &Self::SegmentSortKey,
    ) -> Ordering {
        self.comparator.compare(left, right)
    }

    fn convert_segment_sort_key(&self, sort_key: Self::SegmentSortKey) -> Self::SortKey {
        self.segment_sort_key_computer
            .convert_segment_sort_key(sort_key)
    }
}
