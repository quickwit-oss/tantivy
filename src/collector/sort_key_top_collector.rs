use std::ops::Range;

use crate::collector::sort_key::{Comparator, SegmentSortKeyComputer, SortKeyComputer};
use crate::collector::{Collector, SegmentCollector, TopNComputer};
use crate::query::Weight;
use crate::schema::Schema;
use crate::{DocAddress, DocId, Result, Score, SegmentReader};

pub(crate) struct TopBySortKeyCollector<TSortKeyComputer> {
    sort_key_computer: TSortKeyComputer,
    doc_range: Range<usize>,
}

impl<TSortKeyComputer> TopBySortKeyCollector<TSortKeyComputer> {
    pub fn new(sort_key_computer: TSortKeyComputer, doc_range: Range<usize>) -> Self {
        TopBySortKeyCollector {
            sort_key_computer,
            doc_range,
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

    fn for_segment(&self, segment_ord: u32, segment_reader: &SegmentReader) -> Result<Self::Child> {
        let segment_sort_key_computer = self
            .sort_key_computer
            .segment_sort_key_computer(segment_reader)?;
        let topn_computer = TopNComputer::new_with_comparator(
            self.doc_range.end,
            self.sort_key_computer.comparator(),
        );
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
        Ok(self
            .sort_key_computer
            .merge_top_k(segment_fruits.into_iter().flatten(), self.doc_range.clone()))
    }

    fn collect_segment(
        &self,
        weight: &dyn Weight,
        segment_ord: u32,
        reader: &SegmentReader,
    ) -> crate::Result<Vec<(TSortKeyComputer::SortKey, DocAddress)>> {
        let k = self.doc_range.end;
        let docs = self
            .sort_key_computer
            .collect_segment_top_k(k, weight, reader, segment_ord)?;
        Ok(docs)
    }
}

pub struct TopBySortKeySegmentCollector<TSegmentSortKeyComputer, C>
where
    TSegmentSortKeyComputer: SegmentSortKeyComputer,
    C: Comparator<TSegmentSortKeyComputer::SegmentSortKey>,
{
    pub(crate) topn_computer: TopNComputer<TSegmentSortKeyComputer::SegmentSortKey, DocId, C>,
    pub(crate) segment_ord: u32,
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
        self.topn_computer
            .push_lazy(doc, score, &mut self.segment_sort_key_computer);
    }

    fn harvest(self) -> Self::Fruit {
        let segment_ord = self.segment_ord;
        let segment_hits: Vec<(TSegmentSortKeyComputer::SortKey, DocAddress)> = self
            .topn_computer
            .into_sorted_vec()
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
