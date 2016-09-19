use collector::Collector;
use SegmentLocalId;
use SegmentReader;
use std::io;
use ScoredDoc;


/// Collector that does nothing.
/// This is used in the chain Collector and will hopefully 
/// be optimized away by the compiler. 
pub struct DoNothingCollector;
impl Collector for DoNothingCollector {
    #[inline]
    fn set_segment(&mut self, _: SegmentLocalId, _: &SegmentReader) -> io::Result<()> {
        Ok(())
    }
    #[inline]
    fn collect(&mut self, _: ScoredDoc) {}
}

/// Zero-cost abstraction used to collect on multiple collectors.
/// This contraption is only usable if the type of your collectors
/// are known at compile time.
pub struct ChainedCollector<Left: Collector, Right: Collector> {
    left: Left,
    right: Right
}

impl<Left: Collector, Right: Collector> ChainedCollector<Left, Right> { 
    /// Adds a collector
    pub fn push<C: Collector>(self, new_collector: &mut C) -> ChainedCollector<Self, &mut C> {
        ChainedCollector {
            left: self,
            right: new_collector,
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
    use ScoredDoc;
    use collector::{Collector, CountCollector, TopCollector};

    #[test]
    fn test_chained_collector() {
        let mut top_collector = TopCollector::with_limit(2);
        let mut count_collector = CountCollector::default();
        {
            let mut collectors = chain()
                .push(&mut top_collector)
                .push(&mut count_collector);
            collectors.collect(ScoredDoc(0.2, 1));
            collectors.collect(ScoredDoc(0.1, 2));
            collectors.collect(ScoredDoc(0.5, 3));
        }
        assert_eq!(count_collector.count(), 3);
        assert!(top_collector.at_capacity());
    }
}