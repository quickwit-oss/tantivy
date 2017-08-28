use super::Collector;
use DocId;
use Score;
use Result;
use SegmentReader;
use SegmentLocalId;


/// Multicollector makes it possible to collect on more than one collector.
/// It should only be used for use cases where the Collector types is unknown
/// at compile time.
/// If the type of the collectors is known, you should prefer to use `ChainedCollector`.
pub struct MultiCollector<'a> {
    collectors: Vec<&'a mut Collector>,
}

impl<'a> MultiCollector<'a> {
    /// Constructor
    pub fn from(collectors: Vec<&'a mut Collector>) -> MultiCollector {
        MultiCollector { collectors: collectors }
    }
}


impl<'a> Collector for MultiCollector<'a> {
    fn set_segment(
        &mut self,
        segment_local_id: SegmentLocalId,
        segment: &SegmentReader,
    ) -> Result<()> {
        for collector in &mut self.collectors {
            try!(collector.set_segment(segment_local_id, segment));
        }
        Ok(())
    }

    fn collect(&mut self, doc: DocId, score: Score) {
        for collector in &mut self.collectors {
            collector.collect(doc, score);
        }
    }
}



#[cfg(test)]
mod tests {

    use super::*;
    use collector::{Collector, CountCollector, TopCollector};

    #[test]
    fn test_multi_collector() {
        let mut top_collector = TopCollector::with_limit(2);
        let mut count_collector = CountCollector::default();
        {
            let mut collectors =
                MultiCollector::from(vec![&mut top_collector, &mut count_collector]);
            collectors.collect(1, 0.2);
            collectors.collect(2, 0.1);
            collectors.collect(3, 0.5);
        }
        assert_eq!(count_collector.count(), 3);
        assert!(top_collector.at_capacity());
    }
}
