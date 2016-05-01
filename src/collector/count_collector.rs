use std::io;
use super::Collector;
use DocId;
use SegmentReader;
use SegmentLocalId;

pub struct CountCollector {
    count: usize,
}

impl CountCollector {
    pub fn new() -> CountCollector {
        CountCollector {
            count: 0,
        }
    }

    pub fn count(&self,) -> usize {
        self.count
    }
}

impl Collector for CountCollector {

    fn set_segment(&mut self, _: SegmentLocalId, _: &SegmentReader) -> io::Result<()> {
        Ok(())
    }

    fn collect(&mut self, _: DocId) {
        self.count += 1;
    }
}
