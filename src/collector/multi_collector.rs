use std::io;
use super::Collector;
use ScoredDoc;
use SegmentReader;
use SegmentLocalId;

pub struct MultiCollector<'a> {
    collectors: Vec<&'a mut Collector>,
}

impl<'a> MultiCollector<'a> {
    pub fn from(collectors: Vec<&'a mut Collector>) -> MultiCollector {
        MultiCollector {
            collectors: collectors,
        }
    }
}

impl<'a> Collector for MultiCollector<'a> {

    fn set_segment(&mut self, segment_local_id: SegmentLocalId, segment: &SegmentReader) -> io::Result<()> {
        for collector in &mut self.collectors {
            try!(collector.set_segment(segment_local_id, segment));
        }
        Ok(())
    }

    fn collect(&mut self, scored_doc: ScoredDoc) {
        for collector in &mut self.collectors {
            collector.collect(scored_doc);
        }
    }
}



#[cfg(test)]
mod tests {

    use super::*;
    use ScoredDoc;
    use collector::{Collector, CountCollector, TopCollector};

    #[test]
    fn test_multi_collector() {
        let mut top_collector = TopCollector::with_limit(2);
        let mut count_collector = CountCollector::default();
        {
            let mut collectors = MultiCollector::from(vec!(&mut top_collector, &mut count_collector));
            collectors.collect(ScoredDoc(0.2, 1));
            collectors.collect(ScoredDoc(0.1, 2));
            collectors.collect(ScoredDoc(0.5, 3));
        }
        assert_eq!(count_collector.count(), 3);
        assert!(top_collector.at_capacity());
    }
}