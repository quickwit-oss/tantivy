use core::schema::DocId;
use core::reader::SegmentReader;
use core::directory::SegmentId;
use core::searcher::DocAddress;

pub trait Collector {
    fn set_segment(&mut self, segment: &SegmentReader);
    fn collect(&mut self, doc_id: DocId);
}

pub struct FirstNCollector {
    docs: Vec<DocAddress>,
    current_segment: Option<SegmentId>,
    limit: usize,
}

impl FirstNCollector {
    pub fn with_limit(limit: usize) -> FirstNCollector {
        FirstNCollector {
            docs: Vec::new(),
            limit: limit,
            current_segment: None,
        }
    }

    pub fn docs(self,) -> Vec<DocAddress> {
        self.docs
    }
}

impl Collector for FirstNCollector {

    fn set_segment(&mut self, segment: &SegmentReader) {
        self.current_segment = Some(segment.id());
    }

    fn collect(&mut self, doc_id: DocId) {
        if self.docs.len() < self.limit {
            self.docs.push(DocAddress(self.current_segment.clone().unwrap(), doc_id));
        }
    }
}


//

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

    fn set_segment(&mut self, segment: &SegmentReader) {
    }

    fn collect(&mut self, doc_id: DocId) {
        self.count += 1;
    }
}





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

    fn set_segment(&mut self, segment: &SegmentReader) {
        for collector in self.collectors.iter_mut() {
            collector.set_segment(segment);
        }
    }

    fn collect(&mut self, doc_id: DocId) {
        for collector in self.collectors.iter_mut() {
            collector.collect(doc_id);
        }
    }
}
