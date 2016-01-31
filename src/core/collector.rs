use core::DocId;
use core::reader::SegmentReader;

pub trait Collector {
    fn set_segment(&mut self, segment: &SegmentReader);
    fn collect(&self, doc_id: DocId);
}

pub struct DisplayCollector;

impl Collector for DisplayCollector {

    fn set_segment(&mut self, segment: &SegmentReader) {
    }

    fn collect(&self, doc_id: DocId) {
        println!("{:?}", doc_id);
    }
}
