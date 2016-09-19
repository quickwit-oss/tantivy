use SegmentReader;
use SegmentLocalId;
use ScoredDoc;
use std::io;

mod count_collector;
pub use self::count_collector::CountCollector;

mod multi_collector;
pub use self::multi_collector::MultiCollector;

mod top_collector;
pub use self::top_collector::TopCollector;

mod chained_collector;
pub use self::chained_collector::chain;

/// Collectors are in charge of collecting and retaining relevant 
/// information from the document found and scored by the query.
///
///
/// For instance, 
/// - keeping track of the top 10 best documents
/// - computing a break down over a fast field
/// - computing the number of documents matching the query
///
///
/// Queries are in charge of pushing the `DocSet` to the collector.
///
/// As they work on multiple segment, they first inform
/// the collector of a change in segment and then 
/// call the collect method to push document to the collector.
///
/// Temporally, our collector will receive calls
/// - `.set_segment(0, segment_reader_0)`
/// - `.collect(doc0_of_segment_0)`
/// - `.collect(...)`
/// - `.collect(last_doc_of_segment_0)`
/// - `.set_segment(1, segment_reader_1)`
/// - `.collect(doc0_of_segment_1)`
/// - `.collect(...)`
/// - `.collect(last_doc_of_segment_1)`
/// - `...`
/// - `.collect(last_doc_of_last_segment)`
///
/// Segments are not guaranteed to be visited in any specific order.
pub trait Collector {
    /// `set_segment` is called before starting enumerating
    /// on this segment.
    fn set_segment(&mut self, segment_local_id: SegmentLocalId, segment: &SegmentReader) -> io::Result<()>;
    /// The query pushes scored document to the collector via this method.
    fn collect(&mut self, scored_doc: ScoredDoc);
}


impl<'a, C: Collector> Collector for &'a mut C {
    fn set_segment(&mut self, segment_local_id: SegmentLocalId, segment: &SegmentReader) -> io::Result<()> {
        (*self).set_segment(segment_local_id, segment)
    }
    /// The query pushes scored document to the collector via this method.
    fn collect(&mut self, scored_doc: ScoredDoc) {
        (*self).collect(scored_doc);
    }
}


#[cfg(test)]
pub mod tests {

    use super::*;
    use test::Bencher;
    use ScoredDoc;
    use DocId;
    use core::SegmentReader;
    use std::io;
    use SegmentLocalId;
    use fastfield::U32FastFieldReader;
    use schema::Field;
    
    /// Stores all of the doc ids.
    /// This collector is only used for tests.
    /// It is unusable in practise, as it does not store
    /// the segment ordinals
    pub struct TestCollector {
        offset: DocId,
        segment_max_doc: DocId,
        docs: Vec<DocId>,
    }

    impl TestCollector {
        /// Return the exhalist of documents.
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
    
    
    
    
    /// Collects in order all of the fast field for all of the
    /// doc of the `DocSet`
    ///
    /// This collector is essentially useful for tests.
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
