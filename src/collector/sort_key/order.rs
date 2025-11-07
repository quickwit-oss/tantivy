use std::cmp::Ordering;

use crate::collector::{SegmentSortKeyComputer, SortKeyComputer};
use crate::schema::Schema;
use crate::{DocId, Order, Score};

pub trait Comparator<T>: Send + Sync + std::fmt::Debug + Default {
    fn compare(&self, lhs: &T, rhs: &T) -> Ordering;
}

#[derive(Debug, Copy, Clone, Default)]
pub struct NaturalComparator;

impl<T: PartialOrd> Comparator<T> for NaturalComparator {
    #[inline]
    fn compare(&self, lhs: &T, rhs: &T) -> Ordering {
        lhs.partial_cmp(rhs).unwrap()
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct ReverseComparator;

impl<T> Comparator<T> for ReverseComparator
where NaturalComparator: Comparator<T>
{
    #[inline]
    fn compare(&self, lhs: &T, rhs: &T) -> Ordering {
        NaturalComparator.compare(rhs, lhs)
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct ReverseNoneIsLowerComparator;

impl<T> Comparator<Option<T>> for ReverseNoneIsLowerComparator
where ReverseComparator: Comparator<T>
{
    #[inline]
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
    #[inline]
    fn compare(&self, lhs: &u32, rhs: &u32) -> Ordering {
        ReverseComparator.compare(lhs, rhs)
    }
}

impl Comparator<u64> for ReverseNoneIsLowerComparator {
    #[inline]
    fn compare(&self, lhs: &u64, rhs: &u64) -> Ordering {
        ReverseComparator.compare(lhs, rhs)
    }
}

impl Comparator<f64> for ReverseNoneIsLowerComparator {
    #[inline]
    fn compare(&self, lhs: &f64, rhs: &f64) -> Ordering {
        ReverseComparator.compare(lhs, rhs)
    }
}

impl Comparator<f32> for ReverseNoneIsLowerComparator {
    #[inline]
    fn compare(&self, lhs: &f32, rhs: &f32) -> Ordering {
        ReverseComparator.compare(lhs, rhs)
    }
}

impl Comparator<i64> for ReverseNoneIsLowerComparator {
    #[inline]
    fn compare(&self, lhs: &i64, rhs: &i64) -> Ordering {
        ReverseComparator.compare(lhs, rhs)
    }
}

impl Comparator<String> for ReverseNoneIsLowerComparator {
    #[inline]
    fn compare(&self, lhs: &String, rhs: &String) -> Ordering {
        ReverseComparator.compare(lhs, rhs)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum ComparatorEnum {
    #[default]
    Natural,
    Reverse,
    ReverseNullLower,
}

impl From<Order> for ComparatorEnum {
    fn from(order: Order) -> Self {
        match order {
            Order::Asc => ComparatorEnum::ReverseNullLower,
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
    #[inline]
    fn compare(&self, lhs: &T, rhs: &T) -> Ordering {
        match self {
            ComparatorEnum::Natural => NaturalComparator.compare(lhs, rhs),
            ComparatorEnum::Reverse => ReverseComparator.compare(lhs, rhs),
            ComparatorEnum::ReverseNullLower => ReverseNoneIsLowerComparator.compare(lhs, rhs),
        }
    }
}

impl<Head, Tail, LeftComparator, RightComparator> Comparator<(Head, Tail)>
    for (LeftComparator, RightComparator)
where
    LeftComparator: Comparator<Head>,
    RightComparator: Comparator<Tail>,
{
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

    fn sort_key(&mut self, doc: DocId, score: Score) -> Self::SegmentSortKey {
        self.segment_sort_key_computer.sort_key(doc, score)
    }

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

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use rand;
    use rand::seq::SliceRandom as _;

    use crate::collector::sort_key::SortBySimilarityScore;
    use crate::collector::SortKeyComputer;
    use crate::Order;

    fn test_sort_key_computer_with_order_aux(
        order: Order,
        doc_range: Range<usize>,
        expected: &[(crate::Score, usize)],
    ) {
        let sort_key_computer = (SortBySimilarityScore, order);
        let mut vals: Vec<(crate::Score, usize)> = (0..10).map(|val| (val as f32, val)).collect();
        vals.shuffle(&mut rand::thread_rng());
        let vals_merged =
            SortKeyComputer::merge_top_k(&sort_key_computer, vals.into_iter(), doc_range);
        assert_eq!(&vals_merged, expected);
    }

    #[test]
    fn test_sort_key_computer_with_order() {
        test_sort_key_computer_with_order_aux(Order::Asc, 0..0, &[]);
        test_sort_key_computer_with_order_aux(Order::Asc, 3..3, &[]);
        test_sort_key_computer_with_order_aux(
            Order::Asc,
            0..3,
            &[(0.0f32, 0), (1.0f32, 1), (2.0f32, 2)],
        );
        test_sort_key_computer_with_order_aux(
            Order::Asc,
            0..11,
            &[
                (0.0f32, 0),
                (1.0f32, 1),
                (2.0f32, 2),
                (3.0f32, 3),
                (4.0f32, 4),
                (5.0f32, 5),
                (6.0f32, 6),
                (7.0f32, 7),
                (8.0f32, 8),
                (9.0f32, 9),
            ],
        );
        test_sort_key_computer_with_order_aux(Order::Asc, 1..3, &[(1.0f32, 1), (2.0f32, 2)]);
        test_sort_key_computer_with_order_aux(Order::Desc, 0..2, &[(9.0f32, 9), (8.0f32, 8)]);
        test_sort_key_computer_with_order_aux(Order::Desc, 2..4, &[(7.0f32, 7), (6.0f32, 6)]);
    }
}
