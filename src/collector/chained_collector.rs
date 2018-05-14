use collector::Collector;
use DocId;
use Result;
use Score;
use SegmentLocalId;
use SegmentReader;
use collector::SegmentCollector;
use collector::multi_collector::CollectorWrapper;

/// Collector that does nothing.
/// This is used in the chain Collector and will hopefully
/// be optimized away by the compiler.
pub struct DoNothingCollector;
impl Collector for DoNothingCollector {
    type Child = DoNothingCollector;
    #[inline]
    fn for_segment(&mut self, _: SegmentLocalId, _: &SegmentReader) -> Result<DoNothingCollector> {
        Ok(DoNothingCollector)
    }
    #[inline]
    fn requires_scoring(&self) -> bool {
        false
    }
    #[inline]
    fn merge_children(&mut self, _children: Vec<DoNothingCollector>) {}
}

impl SegmentCollector for DoNothingCollector {
    #[inline]
    fn collect(&mut self, _doc: DocId, _score: Score) {}
}

/// Zero-cost abstraction used to collect on multiple collectors.
/// This contraption is only usable if the type of your collectors
/// are known at compile time.
pub struct ChainedCollector<Left: Collector, Right: Collector> {
    left: Left,
    right: Right,
}

pub struct ChainedSegmentCollector<Left: SegmentCollector, Right: SegmentCollector> {
    left: Left,
    right: Right,
}

impl<Left: Collector, Right: Collector> ChainedCollector<Left, Right> {
    /// Adds a collector
    pub fn push<C: Collector>(self, new_collector: &mut C) -> ChainedCollector<Self, CollectorWrapper<C>> {
        ChainedCollector {
            left: self,
            right: CollectorWrapper::new(new_collector),
        }
    }
}

impl<Left: Collector, Right: Collector> Collector for ChainedCollector<Left, Right> {
    type Child = ChainedSegmentCollector<Left::Child, Right::Child>;
    fn for_segment(
        &mut self,
        segment_local_id: SegmentLocalId,
        segment: &SegmentReader,
    ) -> Result<Self::Child> {
        Ok(ChainedSegmentCollector {
            left: self.left.for_segment(segment_local_id, segment)?,
            right: self.right.for_segment(segment_local_id, segment)?,
        })
    }

    fn requires_scoring(&self) -> bool {
        self.left.requires_scoring() || self.right.requires_scoring()
    }

    fn merge_children(&mut self, children: Vec<Self::Child>) {
        let mut lefts = Vec::new();
        let mut rights = Vec::new();

        for child in children.into_iter() {
            lefts.push(child.left);
            rights.push(child.right);
        }

        self.left.merge_children(lefts);
        self.right.merge_children(rights);
    }
}

impl<Left: SegmentCollector, Right: SegmentCollector> SegmentCollector for ChainedSegmentCollector<Left, Right> {
    fn collect(&mut self, doc: DocId, score: Score) {
        self.left.collect(doc, score);
        self.right.collect(doc, score);
    }
}

/// Creates a `ChainedCollector`
pub fn chain() -> ChainedCollector<DoNothingCollector, DoNothingCollector> {
    ChainedCollector {
        left: DoNothingCollector,
        right: DoNothingCollector,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use collector::{CountCollector, SegmentCollector, TopCollector};
    use schema::SchemaBuilder;
    use Index;
    use Document;

    #[test]
    fn test_chained_collector() {
        let schema_builder = SchemaBuilder::new();
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        let mut index_writer = index.writer(3_000_000).unwrap();
        let doc = Document::new();
        index_writer.add_document(doc);
        index_writer.commit().unwrap();
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let segment_readers = searcher.segment_readers();

        let mut top_collector = TopCollector::with_limit(2);
        let mut count_collector = CountCollector::default();
        {
            let mut collectors = chain().push(&mut top_collector).push(&mut count_collector);
            let mut segment_collector = collectors.for_segment(0, &segment_readers[0]).unwrap();
            segment_collector.collect(1, 0.2);
            segment_collector.collect(2, 0.1);
            segment_collector.collect(3, 0.5);
            collectors.merge_children(vec![segment_collector]);
        }
        assert_eq!(count_collector.count(), 3);
        assert!(top_collector.at_capacity());
    }
}
