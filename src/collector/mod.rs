
use DocId;
use SegmentReader;
use SegmentLocalId;
use fastfield::U32FastFieldReader;
use schema::Field;
use ScoredDoc;
use std::io;

mod collector;
pub use self::collector::Collector;

mod count_collector;
pub use self::count_collector::CountCollector;

mod multi_collector;
pub use self::multi_collector::MultiCollector;

mod top_collector;
pub use self::top_collector::TopCollector;

mod chained_collector;
pub use self::chained_collector::chain;

pub struct TestCollector {
    offset: DocId,
    segment_max_doc: DocId,
    docs: Vec<DocId>,
}

impl TestCollector {
    pub fn docs(self,) -> Vec<DocId> {
        self.docs
    }
}

impl Default for TestCollector {
    fn default() -> TestCollector {
        TestCollector {
            docs: Vec::new(),
            offset: 0,
            segment_max_doc: 0,
        }
    }
}

impl Collector for TestCollector {

    fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> io::Result<()> {
        self.offset += self.segment_max_doc;
        self.segment_max_doc = reader.max_doc();
        Ok(())
    }

    fn collect(&mut self, scored_doc: ScoredDoc) {
        self.docs.push(scored_doc.doc() + self.offset);
    }
}


pub struct FastFieldTestCollector {
    vals: Vec<u32>,
    field: Field,
    ff_reader: Option<U32FastFieldReader>,
}

impl FastFieldTestCollector {
    pub fn for_field(field: Field) -> FastFieldTestCollector {
        FastFieldTestCollector {
            vals: Vec::new(),
            field: field,
            ff_reader: None,
        }
    }

    pub fn vals(&self,) -> &Vec<u32> {
        &self.vals
    }
}

impl Collector for FastFieldTestCollector {

    fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> io::Result<()> {
        self.ff_reader = Some(try!(reader.get_fast_field_reader(self.field)));
        Ok(())
    }

    fn collect(&mut self, scored_doc: ScoredDoc) {
        let val = self.ff_reader.as_ref().unwrap().get(scored_doc.doc());
        self.vals.push(val);
    }
}



#[cfg(test)]
mod tests {

    use super::*;
    use test::Bencher;
    use ScoredDoc;

    #[bench]
    fn build_collector(b: &mut Bencher) {
        b.iter(|| {
            let mut count_collector = CountCollector::default();
            let docs: Vec<u32> = (0..1_000_000).collect();
            for doc in docs {
                count_collector.collect(ScoredDoc(1f32, doc));
            }
            count_collector.count()
        });
    }
}
