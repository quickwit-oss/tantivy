use DocId;
use SegmentReader;
use SegmentLocalId;
use fastfield::U32FastFieldReader;
use schema::U32Field;
use std::io;


mod count_collector;
pub use self::count_collector::CountCollector;

mod first_n_collector;
pub use self::first_n_collector::FirstNCollector;

mod multi_collector;
pub use self::multi_collector::MultiCollector;

pub trait Collector {
    fn set_segment(&mut self, segment_local_id: SegmentLocalId, segment: &SegmentReader) -> io::Result<()>;
    fn collect(&mut self, doc_id: DocId);
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

    fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> io::Result<()> {
        self.offset += self.segment_max_doc;
        self.segment_max_doc = reader.max_doc();
        Ok(())
    }

    fn collect(&mut self, doc_id: DocId) {
        self.docs.push(doc_id + self.offset);
    }
}


pub struct FastFieldTestCollector {
    vals: Vec<u32>,
    u32_field: U32Field,
    ff_reader: Option<U32FastFieldReader>,
}

impl FastFieldTestCollector {
    pub fn for_field(u32_field: U32Field) -> FastFieldTestCollector {
        FastFieldTestCollector {
            vals: Vec::new(),
            u32_field: u32_field,
            ff_reader: None,
        }
    }

    pub fn vals(&self,) -> &Vec<u32> {
        &self.vals
    }
}

impl Collector for FastFieldTestCollector {

    fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> io::Result<()> {
        self.ff_reader = Some(try!(reader.get_fast_field_reader(&self.u32_field)));
        Ok(())
    }

    fn collect(&mut self, doc_id: DocId) {
        let val = self.ff_reader.as_ref().unwrap().get(doc_id);
        self.vals.push(val);
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
}
