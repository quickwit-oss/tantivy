use core::DocId;
use core::reader::SegmentReader;
use core::directory::SegmentId;
use core::searcher::DocAddress;

pub trait Collector {
    fn set_segment(&mut self, segment: &SegmentReader);
    fn collect(&mut self, doc_id: DocId);
}

pub struct TestCollector {
    docs: Vec<DocAddress>,
    current_segment: Option<SegmentId>,
}

impl TestCollector {
    pub fn new() -> TestCollector {
        TestCollector {
            docs: Vec::new(),
            current_segment: None,
        }
    }

    pub fn docs(self,) -> Vec<DocAddress> {
        self.docs
    }
}

impl Collector for TestCollector {

    fn set_segment(&mut self, segment: &SegmentReader) {
        self.current_segment = Some(segment.id());
    }

    fn collect(&mut self, doc_id: DocId) {
        self.docs.push(DocAddress(self.current_segment.clone().unwrap(), doc_id));
    }
}
