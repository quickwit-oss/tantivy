use SegmentReader;
use SegmentLocalId;
use DocId;
use Score;
use Result;

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
///
/// - keeping track of the top 10 best documents
/// - computing a breakdown over a fast field
/// - computing the number of documents matching the query
///
/// Queries are in charge of pushing the `DocSet` to the collector.
///
/// As they work on multiple segments, they first inform
/// the collector of a change in a segment and then
/// call the `collect` method to push the document to the collector.
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
    /// `set_segment` is called before beginning to enumerate
    /// on this segment.
    fn set_segment(&mut self,
                   segment_local_id: SegmentLocalId,
                   segment: &SegmentReader)
                   -> Result<()>;
    /// The query pushes the scored document to the collector via this method.
    fn collect(&mut self, doc: DocId, score: Score);
}


impl<'a, C: Collector> Collector for &'a mut C {
    fn set_segment(&mut self,
                   segment_local_id: SegmentLocalId,
                   segment: &SegmentReader)
                   -> Result<()> {
        (*self).set_segment(segment_local_id, segment)
    }
    /// The query pushes the scored document to the collector via this method.
    fn collect(&mut self, doc: DocId, score: Score) {
        (*self).collect(doc, score);
    }
}


#[cfg(test)]
pub mod tests {

    use super::*;
    use test::Bencher;
    use DocId;
    use Score;
    use core::SegmentReader;
    use SegmentLocalId;
    use fastfield::U64FastFieldReader;
    use fastfield::FastFieldReader;
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
        pub fn docs(self) -> Vec<DocId> {
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
        fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> Result<()> {
            self.offset += self.segment_max_doc;
            self.segment_max_doc = reader.max_doc();
            Ok(())
        }

        fn collect(&mut self, doc: DocId, _score: Score) {
            self.docs.push(doc + self.offset);
        }
    }




    /// Collects in order all of the fast fields for all of the
    /// doc in the `DocSet`
    ///
    /// This collector is mainly useful for tests.
    pub struct FastFieldTestCollector {
        vals: Vec<u64>,
        field: Field,
        ff_reader: Option<U64FastFieldReader>,
    }

    impl FastFieldTestCollector {
        pub fn for_field(field: Field) -> FastFieldTestCollector {
            FastFieldTestCollector {
                vals: Vec::new(),
                field: field,
                ff_reader: None,
            }
        }

        pub fn vals(self) -> Vec<u64> {
            self.vals
        }
    }

    impl Collector for FastFieldTestCollector {
        fn set_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> Result<()> {
            self.ff_reader = Some(reader.get_fast_field_reader(self.field)?);
            Ok(())
        }

        fn collect(&mut self, doc: DocId, _score: Score) {
            let val = self.ff_reader.as_ref().unwrap().get(doc);
            self.vals.push(val);
        }
    }


    #[bench]
    fn build_collector(b: &mut Bencher) {
        b.iter(|| {
                   let mut count_collector = CountCollector::default();
                   let docs: Vec<u32> = (0..1_000_000).collect();
                   for doc in docs {
                       count_collector.collect(doc, 1f32);
                   }
                   count_collector.count()
               });
    }
}
