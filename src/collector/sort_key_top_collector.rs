use std::ops::Range;

use crate::collector::sort_key::shared_threshold::SharedThresholdArcOpt;
use crate::collector::sort_key::{Comparator, SegmentSortKeyComputer, SortKeyComputer};
use crate::collector::{Collector, SegmentCollector, TopNComputer};
use crate::query::Weight;
use crate::schema::Schema;
use crate::{DocAddress, DocId, Result, Score, SegmentOrdinal, SegmentReader};

pub(crate) struct TopBySortKeyCollector<TSortKeyComputer>
where TSortKeyComputer: SortKeyComputer
{
    sort_key_computer: TSortKeyComputer,
    doc_range: Range<usize>,
    shared_threshold: SharedThresholdArcOpt<
        <<TSortKeyComputer as SortKeyComputer>::Child as SegmentSortKeyComputer>::SegmentSortKey,
    >,
}

impl<TSortKeyComputer> TopBySortKeyCollector<TSortKeyComputer>
where TSortKeyComputer: SortKeyComputer
{
    pub fn new(sort_key_computer: TSortKeyComputer, doc_range: Range<usize>) -> Self {
        let shared_threshold = sort_key_computer.shared_threshold();
        TopBySortKeyCollector {
            sort_key_computer,
            doc_range,
            shared_threshold,
        }
    }
}

impl<TSortKeyComputer> Collector for TopBySortKeyCollector<TSortKeyComputer>
where TSortKeyComputer: SortKeyComputer + Send + Sync + 'static
{
    type Fruit = Vec<(TSortKeyComputer::SortKey, DocAddress)>;

    type Child =
        TopBySortKeySegmentCollector<TSortKeyComputer::Child, TSortKeyComputer::Comparator>;

    fn check_schema(&self, schema: &Schema) -> crate::Result<()> {
        self.sort_key_computer.check_schema(schema)
    }

    fn for_segment(
        &self,
        segment_ord: SegmentOrdinal,
        segment_reader: &SegmentReader,
    ) -> Result<Self::Child> {
        let segment_sort_key_computer = self
            .sort_key_computer
            .segment_sort_key_computer(segment_reader)?;
        let mut topn_computer = TopNComputer::new_with_comparator(
            self.doc_range.end,
            self.sort_key_computer.comparator(),
        );
        topn_computer.segment_ord = segment_ord;
        topn_computer.shared_threshold = self.shared_threshold.clone();

        Ok(TopBySortKeySegmentCollector {
            topn_computer,
            segment_ord,
            segment_sort_key_computer,
        })
    }

    fn requires_scoring(&self) -> bool {
        self.sort_key_computer.requires_scoring()
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        Ok(merge_top_k(
            segment_fruits.into_iter().flatten(),
            self.doc_range.clone(),
            self.sort_key_computer.comparator(),
        ))
    }

    fn collect_segment(
        &self,
        weight: &dyn Weight,
        segment_ord: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> crate::Result<Vec<(TSortKeyComputer::SortKey, DocAddress)>> {
        let mut segment_collector = self.for_segment(segment_ord, reader)?;
        self.sort_key_computer
            .collect_segment_top_k(weight, reader, &mut segment_collector)?;
        Ok(segment_collector.harvest())
    }
}

fn merge_top_k<D: Ord, TSortKey: Clone + std::fmt::Debug, C: Comparator<TSortKey>>(
    sort_key_docs: impl Iterator<Item = (TSortKey, D)>,
    doc_range: Range<usize>,
    comparator: C,
) -> Vec<(TSortKey, D)> {
    if doc_range.is_empty() {
        return Vec::new();
    }
    let mut top_collector: TopNComputer<TSortKey, D, C> =
        TopNComputer::new_with_comparator(doc_range.end, comparator);
    for (sort_key, doc) in sort_key_docs {
        top_collector.push(sort_key, doc);
    }
    top_collector
        .into_sorted_vec()
        .into_iter()
        .skip(doc_range.start)
        .map(|cdoc| (cdoc.sort_key, cdoc.doc))
        .collect()
}

pub struct TopBySortKeySegmentCollector<TSegmentSortKeyComputer, C>
where
    TSegmentSortKeyComputer: SegmentSortKeyComputer,
    C: Comparator<TSegmentSortKeyComputer::SegmentSortKey>,
{
    pub(crate) topn_computer: TopNComputer<TSegmentSortKeyComputer::SegmentSortKey, DocId, C>,
    pub(crate) segment_ord: SegmentOrdinal,
    pub(crate) segment_sort_key_computer: TSegmentSortKeyComputer,
}

impl<TSegmentSortKeyComputer, C> SegmentCollector
    for TopBySortKeySegmentCollector<TSegmentSortKeyComputer, C>
where
    TSegmentSortKeyComputer: 'static + SegmentSortKeyComputer,
    C: Comparator<TSegmentSortKeyComputer::SegmentSortKey> + 'static,
{
    type Fruit = Vec<(TSegmentSortKeyComputer::SortKey, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.segment_sort_key_computer.compute_sort_key_and_collect(
            doc,
            score,
            &mut self.topn_computer,
        );
    }

    fn harvest(self) -> Self::Fruit {
        let segment_ord = self.segment_ord;
        let segment_hits: Vec<(TSegmentSortKeyComputer::SortKey, DocAddress)> = self
            .topn_computer
            .into_vec()
            .into_iter()
            .map(|comparable_doc| {
                let sort_key = self
                    .segment_sort_key_computer
                    .convert_segment_sort_key(comparable_doc.sort_key);
                (
                    sort_key,
                    DocAddress {
                        segment_ord,
                        doc_id: comparable_doc.doc,
                    },
                )
            })
            .collect();
        segment_hits
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use rand;
    use rand::seq::SliceRandom as _;

    use super::merge_top_k;
    use crate::collector::sort_key::ComparatorEnum;
    use crate::Order;

    fn test_merge_top_k_aux(
        order: Order,
        doc_range: Range<usize>,
        expected: &[(crate::Score, usize)],
    ) {
        let mut vals: Vec<(crate::Score, usize)> = (0..10).map(|val| (val as f32, val)).collect();
        vals.shuffle(&mut rand::rng());
        let vals_merged = merge_top_k(vals.into_iter(), doc_range, ComparatorEnum::from(order));
        assert_eq!(&vals_merged, expected);
    }

    #[test]
    fn test_merge_top_k() {
        test_merge_top_k_aux(Order::Asc, 0..0, &[]);
        test_merge_top_k_aux(Order::Asc, 3..3, &[]);
        test_merge_top_k_aux(Order::Asc, 0..3, &[(0.0f32, 0), (1.0f32, 1), (2.0f32, 2)]);
        test_merge_top_k_aux(
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
        test_merge_top_k_aux(Order::Asc, 1..3, &[(1.0f32, 1), (2.0f32, 2)]);
        test_merge_top_k_aux(Order::Desc, 0..2, &[(9.0f32, 9), (8.0f32, 8)]);
        test_merge_top_k_aux(Order::Desc, 2..4, &[(7.0f32, 7), (6.0f32, 6)]);
    }
}
