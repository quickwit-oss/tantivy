use collector::Collector;
use SegmentLocalId;
use SegmentReader;
use std::io;
use ScoredDoc;


pub struct DoNothingCollector;
impl Collector for DoNothingCollector {
    #[inline(always)]
    fn set_segment(&mut self, _: SegmentLocalId, _: &SegmentReader) -> io::Result<()> {
        Ok(())
    }
    #[inline(always)]
    fn collect(&mut self, _: ScoredDoc) {}
}

pub struct ChainedCollector<Left: Collector, Right: Collector> {
    left: Left,
    right: Right
}

impl<Left: Collector, Right: Collector> ChainedCollector<Left, Right> { 

    pub fn start() -> ChainedCollector<DoNothingCollector, DoNothingCollector> {
        ChainedCollector {
            left: DoNothingCollector,
            right: DoNothingCollector
        }
    }

    pub fn add<'b, C: Collector>(self, new_collector: &'b mut C) -> ChainedCollector<Self, MutRefCollector<'b, C>> {
        ChainedCollector {
            left: self,
            right: MutRefCollector(new_collector),
        }
    }
}

impl<Left: Collector, Right: Collector> Collector for ChainedCollector<Left, Right> {
    fn set_segment(&mut self, segment_local_id: SegmentLocalId, segment: &SegmentReader) -> io::Result<()> {
        try!(self.left.set_segment(segment_local_id, segment));
        try!(self.right.set_segment(segment_local_id, segment));
        Ok(())
    }

    fn collect(&mut self, scored_doc: ScoredDoc) {
        self.left.collect(scored_doc);
        self.right.collect(scored_doc);
    }
}

pub fn chain() -> ChainedCollector<DoNothingCollector, DoNothingCollector> {
    ChainedCollector {
        left: DoNothingCollector,
        right: DoNothingCollector,
    }
}

pub struct MutRefCollector<'a, C: Collector + 'a>(&'a mut C);

impl<'a, C: Collector> Collector for MutRefCollector<'a, C> {
    fn set_segment(&mut self, segment_local_id: SegmentLocalId, segment: &SegmentReader) -> io::Result<()> {
        self.0.set_segment(segment_local_id, segment)
    }

    fn collect(&mut self, scored_doc: ScoredDoc) {
        self.0.collect(scored_doc)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use ScoredDoc;
    use collector::{Collector, CountCollector, TopCollector};

    #[test]
    fn test_chained_collector() {
        let mut top_collector = TopCollector::with_limit(2);
        let mut count_collector = CountCollector::new();
        {
            let mut chain = collector_chain()
                .add(&mut top_collector)
                .add(&mut count_collector);
            chained_collector.collect(ScoredDoc(0.2, 1));
            chained_collector.collect(ScoredDoc(0.1, 2));
            chained_collector.collect(ScoredDoc(0.5, 3));
        }
        assert_eq!(count_collector.count(), 3);
        assert!(top_collector.at_capacity());
    }
}