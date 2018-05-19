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

//mod multi_collector;
//pub use self::multi_collector::MultiCollector;

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

    /// Search works as follows :
    ///
    /// First the weight object associated to the query is created.
    ///
    /// Then, the query loops over the segments and for each segment :
    /// - setup the collector and informs it that the segment being processed has changed.
    /// - creates a SegmentCollector for collecting documents associated to the segment
    /// - creates a `Scorer` object associated for this segment
    /// - iterate through the matched documents and push them to the segment collector.
    /// - turn the segment collector into a Combinable segment result
    ///
    /// Combining all of the segment results gives a single Child::CollectionResult, which is returned.
    ///
    /// The result will be Ok(None) in case of having no segments.
    fn search(&mut self, searcher: &Searcher, query: &Query) -> Result<Option<<Self::Child as SegmentCollector>::CollectionResult>> {
        let scoring_enabled = self.requires_scoring();
        let weight = query.weight(searcher, scoring_enabled)?;
        let mut results = Vec::new();
        for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
            let mut child: Self::Child = self.for_segment(segment_ord as SegmentLocalId, segment_reader)?;
            let mut scorer = weight.scorer(segment_reader)?;
            scorer.collect(&mut child, segment_reader.delete_bitset());
            results.push(child.finalize());
        }
        Ok(results.into_iter().fold1(|x,y| {
            x.combine_into(y);
            x
        }))
    }
}

pub trait Combinable {
    fn combine_into(&mut self, other: Self);
}

impl Combinable for () {
    fn combine_into(&mut self, other: Self) {
        ()
    }
}

impl<T> Combinable for Vec<T> {
    fn combine_into(&mut self, other: Self) {
        self.extend(other.into_iter());
    }
}

impl<L: Combinable, R: Combinable> Combinable for (L, R) {
    fn combine_into(&mut self, other: Self) {
        self.0.combine_into(other.0);
        self.1.combine_into(other.1);
    }
}

pub trait SegmentCollector: downcast::Any + 'static {
    type CollectionResult: Combinable + downcast::Any + 'static;
    /// The query pushes the scored document to the collector via this method.
    fn collect(&mut self, doc: DocId, score: Score);

    /// Turn into the final result
    fn finalize(self) -> Self::CollectionResult;
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
}

pub struct CollectorWrapper<'a, TCollector: 'a + Collector>(&'a mut TCollector);

impl<'a, T: 'a + Collector> CollectorWrapper<'a, T> {
    pub fn new(collector: &'a mut T) -> CollectorWrapper<'a, T> {
        CollectorWrapper(collector)
    }
}

impl<'a, T: 'a + Collector> Collector for CollectorWrapper<'a, T> {
    type Child = T::Child;

    fn for_segment(&mut self, segment_local_id: u32, segment: &SegmentReader) -> Result<T::Child> {
        self.0.for_segment(segment_local_id, segment)
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring()
    }
}

trait UntypedCollector {
    fn for_segment(&mut self, segment_local_id: u32, segment: &SegmentReader) -> Result<Box<UntypedSegmentCollector>>;
}


impl<'a, TCollector:'a + Collector> UntypedCollector for CollectorWrapper<'a, TCollector> {
    fn for_segment(&mut self, segment_local_id: u32, segment: &SegmentReader) -> Result<Box<UntypedSegmentCollector>> {
        let segment_collector = self.0.for_segment(segment_local_id, segment)?;
        Ok(Box::new(segment_collector))
    }
}

trait UntypedSegmentCollector {
    fn finalize(self) -> Box<UntypedCombinable>;
}

trait UntypedCombinable {
    fn combine_into(&mut self, other: Box<UntypedCombinable>);
}

pub struct CombinableWrapper<'a, T: 'a + Combinable>(&'a mut T);

impl<'a, T: 'a + Combinable> CombinableWrapper<'a, T> {
    pub fn new(combinable: &'a mut T) -> CombinableWrapper<'a, T> {
        CombinableWrapper(combinable)
    }
}

impl<'a, T: 'a + Combinable> Combinable for CombinableWrapper<'a, T> {
    fn combine_into(&mut self, other: Self) {
        self.0.combine_into(*::downcast::Downcast::<T>::downcast(other).unwrap())
    }
}


#[cfg(test)]
pub mod tests {

    use super::*;
    use core::SegmentReader;
    use fastfield::BytesFastFieldReader;
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
    }

    impl SegmentCollector for TestSegmentCollector {
        type CollectionResult = Vec<TestSegmentCollector>;

        fn collect(&mut self, doc: DocId, score: Score) {
            self.docs.push(doc + self.offset);
            self.scores.push(score);
        }

        fn finalize(self) -> Vec<TestSegmentCollector> {
            vec![self]
        }
    }

    /// Collects in order all of the fast fields for all of the
    /// doc in the `DocSet`
    ///
    /// This collector is mainly useful for tests.
    pub struct FastFieldTestCollector {
        next_counter: usize,
        field: Field,
    }

    #[derive(Default)]
    pub struct FastFieldSegmentCollectorState {
        counter: usize,
        vals: Vec<u64>,
    }

    pub struct FastFieldSegmentCollector {
        state: FastFieldSegmentCollectorState,
        reader: FastFieldReader<u64>,
    }

    impl FastFieldTestCollector {
        pub fn for_field(field: Field) -> FastFieldTestCollector {
            FastFieldTestCollector {
                next_counter: 0,
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
                state: FastFieldSegmentCollectorState::default(),
                reader: reader.fast_field_reader(self.field)?,
            })
        }

        fn requires_scoring(&self) -> bool {
            false
        }
    }

    impl SegmentCollector for FastFieldSegmentCollector {
        type CollectionResult = Vec<FastFieldSegmentCollectorState>;

        fn collect(&mut self, doc: DocId, _score: Score) {
            let val = self.reader.get(doc);
            self.vals.push(val);
        }

        fn finalize(self) -> Vec<FastFieldSegmentCollectorState> {
            vec![self.state]
        }
    }

    /// Collects in order all of the fast field bytes for all of the
    /// docs in the `DocSet`
    ///
    /// This collector is mainly useful for tests.
    pub struct BytesFastFieldTestCollector {
        vals: Vec<u8>,
        field: Field,
    }

    pub struct BytesFastFieldSegmentCollector {
        vals: Vec<u8>,
        reader: BytesFastFieldReader,
    }

    impl BytesFastFieldTestCollector {
        pub fn for_field(field: Field) -> BytesFastFieldTestCollector {
            BytesFastFieldTestCollector {
                vals: Vec::new(),
                field,
            }
        }

        pub fn vals(self) -> Vec<u8> {
            self.vals
        }
    }

    impl Collector for BytesFastFieldTestCollector {
        type Child = BytesFastFieldSegmentCollector;

        fn for_segment(&mut self, _segment_local_id: u32, segment: &SegmentReader) -> Result<BytesFastFieldSegmentCollector> {
            Ok(BytesFastFieldSegmentCollector {
                vals: Vec::new(),
                reader: segment.bytes_fast_field_reader(self.field)?,
            })
        }

        fn requires_scoring(&self) -> bool {
            false
        }
    }

    impl SegmentCollector for BytesFastFieldSegmentCollector {
        type CollectionResult = Vec<Vec<u8>>;

        fn collect(&mut self, doc: u32, _score: f32) {
            let val = self.reader.get_val(doc);
            self.vals.extend(val);
        }

        fn finalize(self) -> Vec<Vec<u8>> {
            vec![self.vals]
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
