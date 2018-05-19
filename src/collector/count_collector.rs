use super::Collector;
use DocId;
use Result;
use Score;
use SegmentLocalId;
use SegmentReader;
use collector::SegmentCollector;
use collector::Combinable;

/// `CountCollector` collector only counts how many
/// documents match the query.
#[derive(Default)]
pub struct CountCollector {
    count: usize,
}

impl CountCollector {
    /// Returns the count of documents that were
    /// collected.
    pub fn count(&self) -> usize {
        self.count
    }
}

impl Collector for CountCollector {
    type Child = CountCollector;

    fn for_segment(&mut self, _: SegmentLocalId, _: &SegmentReader) -> Result<CountCollector> {
        Ok(CountCollector::default())
    }

    fn requires_scoring(&self) -> bool {
        false
    }
}

impl Combinable for CountCollector {
    fn combine_into(&mut self, other: Self) {
        self.count += other.count;
    }
}

impl SegmentCollector for CountCollector {
    type CollectionResult = CountCollector;

    fn collect(&mut self, _: DocId, _: Score) {
        self.count += 1;
    }

    fn finalize(self) -> CountCollector {
        self
    }
}

#[cfg(test)]
mod tests {

    use collector::{Collector, CountCollector, SegmentCollector};

    #[test]
    fn test_count_collector() {
        let mut count_collector = CountCollector::default();
        assert_eq!(count_collector.count(), 0);
        count_collector.collect(0u32, 1f32);
        assert_eq!(count_collector.count(), 1);
        assert_eq!(count_collector.count(), 1);
        count_collector.collect(1u32, 1f32);
        assert_eq!(count_collector.count(), 2);
        assert!(!count_collector.requires_scoring());
    }

}
