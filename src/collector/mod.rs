/*!
Defines how the documents matching a search query should be processed.
*/

use DocId;
use Result;
use Score;
use SegmentLocalId;
use SegmentReader;
use query::Query;
use Searcher;
use downcast;

mod count_collector;
pub use self::count_collector::CountCollector;

mod multi_collector;
pub use self::multi_collector::MultiCollector;

mod top_collector;
pub use self::top_collector::TopCollector;

mod facet_collector;
pub use self::facet_collector::FacetCollector;

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
    type Child : SegmentCollector + 'static;
    /// `set_segment` is called before beginning to enumerate
    /// on this segment.
    fn for_segment(
        &mut self,
        segment_local_id: SegmentLocalId,
        segment: &SegmentReader,
    ) -> Result<Self::Child>;

    /// Returns true iff the collector requires to compute scores for documents.
    fn requires_scoring(&self) -> bool;

    fn merge_children(&mut self, children: Vec<Self::Child>);

    /// Search works as follows :
    ///
    /// First the weight object associated to the query is created.
    ///
    /// Then, the query loops over the segments and for each segment :
    /// - setup the collector and informs it that the segment being processed has changed.
    /// - creates a SegmentCollector for collecting documents associated to the segment
    /// - creates a `Scorer` object associated for this segment
    /// - iterate throw the matched documents and push them to the segment collector.
    ///
    /// Finally, the Collector merges each of the child collectors into itself for result usability
    /// by the caller.
    fn search(&mut self, searcher: &Searcher, query: &Query) -> Result<()> {
        let scoring_enabled = self.requires_scoring();
        let weight = query.weight(searcher, scoring_enabled)?;
        let mut children = Vec::new();
        for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
            let mut child: Self::Child = self.for_segment(segment_ord as SegmentLocalId, segment_reader)?;
            let mut scorer = weight.scorer(segment_reader)?;
            scorer.collect(&mut child, segment_reader.delete_bitset());
            children.push(child);
        }
        self.merge_children(children);
        Ok(())
    }
}

pub trait SegmentCollector: downcast::Any + 'static {
    /// The query pushes the scored document to the collector via this method.
    fn collect(&mut self, doc: DocId, score: Score);
}

#[allow(missing_docs)]
mod downcast_impl {
    downcast!(super::SegmentCollector);
}

impl<'a, C: Collector> Collector for &'a mut C {
    type Child = C::Child;

    fn for_segment(
        &mut self, // TODO Ask Jason : why &mut self here!?
        segment_local_id: SegmentLocalId,
        segment: &SegmentReader,
    ) -> Result<C::Child> {
        (*self).for_segment(segment_local_id, segment)
    }

    fn requires_scoring(&self) -> bool {
        C::requires_scoring(self)
    }

    fn merge_children(&mut self, children: Vec<C::Child>) {
        (*self).merge_children(children);
    }
}


#[cfg(test)]
pub mod tests {

    use super::*;
    use core::SegmentReader;
    use fastfield::FastFieldReader;
    use schema::Field;
    use DocId;
    use Score;
    use SegmentLocalId;

    /// Stores all of the doc ids.
    /// This collector is only used for tests.
    /// It is unusable in practise, as it does not store
    /// the segment ordinals
    pub struct TestCollector {
        next_offset: DocId,
        docs: Vec<DocId>,
        scores: Vec<Score>,
    }

    pub struct TestSegmentCollector {
        offset: DocId,
        docs: Vec<DocId>,
        scores: Vec<Score>,
    }

    impl TestCollector {
        /// Return the exhalist of documents.
        pub fn docs(self) -> Vec<DocId> {
            self.docs
        }

        pub fn scores(self) -> Vec<Score> {
            self.scores
        }
    }

    impl Default for TestCollector {
        fn default() -> TestCollector {
            TestCollector {
                next_offset: 0,
                docs: Vec::new(),
                scores: Vec::new(),
            }
        }
    }

    impl Collector for TestCollector {
        type Child = TestSegmentCollector;

        fn for_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> Result<TestSegmentCollector> {
            let offset = self.next_offset;
            self.next_offset += reader.max_doc();
            Ok(TestSegmentCollector {
                offset,
                docs: Vec::new(),
                scores: Vec::new(),
            })
        }

        fn requires_scoring(&self) -> bool {
            true
        }

        fn merge_children(&mut self, mut children: Vec<TestSegmentCollector>) {
            children.sort_by_key(|x| x.offset);
            for child in children.into_iter() {
                self.docs.extend(child.docs);
                self.scores.extend(child.scores);
            }
        }
    }

    impl SegmentCollector for TestSegmentCollector {
        fn collect(&mut self, doc: DocId, score: Score) {
            self.docs.push(doc + self.offset);
            self.scores.push(score);
        }
    }

    /// Collects in order all of the fast fields for all of the
    /// doc in the `DocSet`
    ///
    /// This collector is mainly useful for tests.
    pub struct FastFieldTestCollector {
        next_counter: usize,
        vals: Vec<u64>,
        field: Field,
    }

    pub struct FastFieldSegmentCollector {
        counter: usize,
        vals: Vec<u64>,
        reader: FastFieldReader<u64>,
    }

    impl FastFieldTestCollector {
        pub fn for_field(field: Field) -> FastFieldTestCollector {
            FastFieldTestCollector {
                next_counter: 0,
                vals: Vec::new(),
                field,
            }
        }

        pub fn vals(self) -> Vec<u64> {
            self.vals
        }
    }

    impl Collector for FastFieldTestCollector {
        type Child = FastFieldSegmentCollector;

        fn for_segment(&mut self, _: SegmentLocalId, reader: &SegmentReader) -> Result<FastFieldSegmentCollector> {
            let counter = self.next_counter;
            self.next_counter += 1;
            Ok(FastFieldSegmentCollector {
                counter,
                vals: Vec::new(),
                reader: reader.fast_field_reader(self.field)?,
            })
        }

        fn requires_scoring(&self) -> bool {
            false
        }

        fn merge_children(&mut self, mut children: Vec<FastFieldSegmentCollector>) {
            children.sort_by_key(|x| x.counter);
            for child in children.into_iter() {
                self.vals.extend(child.vals);
            }
        }
    }

    impl SegmentCollector for FastFieldSegmentCollector {
        fn collect(&mut self, doc: DocId, _score: Score) {
            let val = self.reader.get(doc);
            self.vals.push(val);
        }
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use collector::{Collector, CountCollector};
    use test::Bencher;

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
