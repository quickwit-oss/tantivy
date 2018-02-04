use super::Collector;
use DocId;
use Score;
use Result;
use SegmentReader;
use SegmentLocalId;

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
    fn set_segment(&mut self, _: SegmentLocalId, _: &SegmentReader) -> Result<()> {
        Ok(())
    }

    fn collect(&mut self, _: DocId, _: Score) {
        self.count += 1;
    }
}

#[cfg(test)]
mod tests {

    use collector::{Collector, CountCollector};

    #[test]
    fn test_count_collector() {
        let mut count_collector = CountCollector::default();
        assert_eq!(count_collector.count(), 0);
        count_collector.collect(0u32, 1f32);
        assert_eq!(count_collector.count(), 1);
        assert_eq!(count_collector.count(), 1);
        count_collector.collect(1u32, 1f32);
        assert_eq!(count_collector.count(), 2);
    }
}
