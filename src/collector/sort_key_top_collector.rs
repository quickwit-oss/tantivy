use crate::collector::sort_key::{ReverseOrder, SegmentSortKeyComputer, SortKeyComputer};
use crate::collector::top_collector::{merge_fruits, TopCollector, TopSegmentCollector};
use crate::collector::{Collector, SegmentCollector};
use crate::{DocAddress, DocId, Order, Result, Score, SegmentReader};

pub(crate) struct TopBySortKeyCollector<TSortKeyComputer, TSortKey> {
    sort_key_computer: TSortKeyComputer,
    collector: TopCollector<TSortKey>,
}

impl<TSortKeyComputer, TSortKey> TopBySortKeyCollector<TSortKeyComputer, TSortKey>
where TSortKey: Clone + PartialOrd
{
    pub fn new(
        sort_key_computer: TSortKeyComputer,
        collector: TopCollector<TSortKey>,
    ) -> TopBySortKeyCollector<TSortKeyComputer, TSortKey> {
        TopBySortKeyCollector {
            sort_key_computer,
            collector,
        }
    }
}

impl<TSortKeyComputer, TSortKey> Collector for TopBySortKeyCollector<TSortKeyComputer, TSortKey>
where
    TSortKeyComputer: SortKeyComputer<SortKey = TSortKey> + Send + Sync,
    TSortKey: 'static + Send + PartialOrd + Sync + Clone + ReverseOrder,
{
    type Fruit = Vec<(TSortKeyComputer::SortKey, DocAddress)>;

    type Child = TopBySortKeySegmentCollector<TSortKeyComputer::Child>;

    fn for_segment(
        &self,
        segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> Result<Self::Child> {
        let segment_sort_key_computer = self
            .sort_key_computer
            .segment_sort_key_computer(segment_reader)?;
        let segment_collector = self.collector.for_segment(segment_local_id, segment_reader);
        Ok(TopBySortKeySegmentCollector {
            segment_collector,
            segment_sort_key_computer,
        })
    }

    fn requires_scoring(&self) -> bool {
        self.sort_key_computer.requires_scoring()
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        let order = self.sort_key_computer.order();
        match order {
            Order::Asc => {
                let reverse_segment_fruits: Vec<
                    Vec<(
                        <TSortKeyComputer::SortKey as ReverseOrder>::ReverseType,
                        DocAddress,
                    )>,
                > = segment_fruits
                    .into_iter()
                    .map(|vec| {
                        vec.into_iter()
                            .map(|(sort_key, doc_addr)| (sort_key.to_reverse_type(), doc_addr))
                            .collect()
                    })
                    .collect();
                let merged_reverse_fruits = merge_fruits(
                    reverse_segment_fruits,
                    self.collector.limit,
                    self.collector.offset,
                )?;
                Ok(merged_reverse_fruits
                    .into_iter()
                    .map(|(reverse_sort_key, doc_addr)| {
                        (TSortKey::from_reverse_type(reverse_sort_key), doc_addr)
                    })
                    .collect())
            }
            Order::Desc => self.collector.merge_fruits(segment_fruits),
        }
    }
}

pub struct TopBySortKeySegmentCollector<TSegmentSortKeyComputer>
where TSegmentSortKeyComputer: SegmentSortKeyComputer
{
    segment_collector: TopSegmentCollector<TSegmentSortKeyComputer::SegmentSortKey>,
    segment_sort_key_computer: TSegmentSortKeyComputer,
}

impl<TSegmentSortKeyComputer> SegmentCollector
    for TopBySortKeySegmentCollector<TSegmentSortKeyComputer>
where TSegmentSortKeyComputer: 'static + SegmentSortKeyComputer
{
    type Fruit = Vec<(TSegmentSortKeyComputer::SortKey, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.segment_collector
            .collect_lazy(doc, score, &mut self.segment_sort_key_computer);
    }

    fn harvest(self) -> Self::Fruit {
        let segment_hits: Vec<(TSegmentSortKeyComputer::SegmentSortKey, DocAddress)> =
            self.segment_collector.harvest();
        segment_hits
            .into_iter()
            .map(|(sort_key, doc)| {
                (
                    self.segment_sort_key_computer
                        .convert_segment_sort_key(sort_key),
                    doc,
                )
            })
            .collect()
    }
}
