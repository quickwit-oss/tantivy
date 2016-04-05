use core::schema::DocId;
use core::reader::SegmentReader;
use core::searcher::SegmentLocalId;
use core::searcher::DocAddress;

pub trait Collector {
    fn set_segment(&mut self, segment_local_id: SegmentLocalId, segment: &SegmentReader);
    fn collect(&mut self, doc_id: DocId);
}

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

    fn set_segment(&mut self, segment_local_id: SegmentLocalId, _: &SegmentReader) {
        self.current_segment = segment_local_id;
    }

    fn collect(&mut self, doc_id: DocId) {
        if self.docs.len() < self.limit {
            self.docs.push(DocAddress(self.current_segment.clone(), doc_id));
        }
    }
}

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

    fn set_segment(&mut self, _: SegmentLocalId, _: &SegmentReader) {}

    fn collect(&mut self, _: DocId) {
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

    fn set_segment(&mut self, segment_local_id: SegmentLocalId, segment: &SegmentReader) {
        for collector in self.collectors.iter_mut() {
            collector.set_segment(segment_local_id, segment);
        }
    }

    fn collect(&mut self, doc_id: DocId) {
        for collector in self.collectors.iter_mut() {
            collector.collect(doc_id);
        }
    }
}

pub struct TestCollector {
    offset: DocId,
    segment_max_doc: DocId,
    docs: Vec<DocId>,
}

impl TestCollector {
    pub fn new() -> TestCollector {
        TestCollector {
            docs: Vec::new(),
            offset: 0,
            segment_max_doc: 0,
        }
    }

    pub fn docs(self,) -> Vec<DocId> {
        self.docs
    }
}

impl Collector for TestCollector {

    fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) {
        self.offset += self.segment_max_doc;
        self.segment_max_doc = reader.max_doc();
    }

    fn collect(&mut self, doc_id: DocId) {
        self.docs.push(doc_id + self.offset);
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use test::Bencher;

    #[bench]
    fn build_collector(b: &mut Bencher) {
        b.iter(|| {
            let mut count_collector = CountCollector::new();
            let docs: Vec<u32> = (0..1_000_000).collect();
            for doc in docs {
                count_collector.collect(doc);
            }
            count_collector.count()
        });
    }

    // #[bench]
    // fn build_first_3_collector(b: &mut Bencher) {
    //     b.iter(|| {
    //         let mut first3collector = FirstNCollector::with_limit(3);
    //         let docs: Vec<u32> = (0..1_000_000).collect();
    //         for doc in docs {
    //             first3collector.collect(doc);
    //         }
    //         first3collector.docs()
    //     });
    // }
}
