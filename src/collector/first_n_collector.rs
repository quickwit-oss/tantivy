use std::io;
use super::Collector;
use DocId;
use SegmentReader;
use SegmentLocalId;
use core::searcher::DocAddress;

pub struct FirstNCollector {
    docs: Vec<DocAddress>,
    current_segment: u32,
    limit: usize,
}

impl FirstNCollector {
    pub fn with_limit(limit: usize) -> FirstNCollector {
        FirstNCollector {
            docs: Vec::new(),
            limit: limit,
            current_segment: 0,
        }
    }

    pub fn docs(self,) -> Vec<DocAddress> {
        self.docs
    }
}

impl Collector for FirstNCollector {

    fn set_segment(&mut self, segment_local_id: SegmentLocalId, _: &SegmentReader) -> io::Result<()> {
        self.current_segment = segment_local_id;
        Ok(())
    }

    fn collect(&mut self, doc_id: DocId, _: f32) {
        if self.docs.len() < self.limit {
            self.docs.push(DocAddress(self.current_segment.clone(), doc_id));
        }
    }
}
