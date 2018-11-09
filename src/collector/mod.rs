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

mod top_score_collector;
pub use self::top_score_collector::TopScoreCollector;
#[deprecated]
pub use self::top_score_collector::TopScoreCollector as TopCollector;

mod top_field_collector;
pub use self::top_field_collector::TopFieldCollector;

mod facet_collector;
pub use self::facet_collector::FacetCollector;

pub trait Fruit: Send + downcast::Any {}

impl<T> Fruit for T where T: Send + downcast::Any {}


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

    type Fruit: Fruit;

    type Child: SegmentCollector<Fruit=Self::Fruit> + 'static;

    /// `set_segment` is called before beginning to enumerate
    /// on this segment.
    fn for_segment(
        &self,
        segment_local_id: SegmentLocalId,
        segment: &SegmentReader,
    ) -> Result<Self::Child>;

    /// Returns true iff the collector requires to compute scores for documents.
    fn requires_scoring(&self) -> bool;

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Self::Fruit;

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
    fn search(&self, searcher: &Searcher, query: &Query) -> Result<Self::Fruit> {
        let scoring_enabled = self.requires_scoring();
        let weight = query.weight(searcher, scoring_enabled)?;
        let mut fruits = Vec::new();
        for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
            let mut child: Self::Child = self.for_segment(segment_ord as SegmentLocalId, segment_reader)?;
            let mut scorer = weight.scorer(segment_reader)?;
            let delete_bitset_opt = segment_reader.delete_bitset();
            if let Some(delete_bitset) = delete_bitset_opt {
                scorer.for_each(&mut |doc, score|
                    if !delete_bitset.is_deleted(doc) {
                        child.collect(doc, score);
                    });
                fruits.push(child.harvest());
            } else {
                scorer.for_each(&mut |doc, score| child.collect(doc, score));
                fruits.push(child.harvest());
            }

        }
        Ok(self.merge_fruits(fruits))
    }
}


pub trait CollectDocScore {
    /// The query pushes the scored document to the collector via this method.
    fn collect(&mut self, doc: DocId, score: Score);
}

pub trait SegmentCollector: 'static + CollectDocScore {

    type Fruit: Fruit;

    fn harvest(self) -> Self::Fruit;
}


impl<Left, Right> Collector for (Left, Right)
where
    Left: Collector,
    Right: Collector
{
    type Fruit = (Left::Fruit, Right::Fruit);
    type Child = (Left::Child, Right::Child);

    fn for_segment(&self, segment_local_id: u32, segment: &SegmentReader) -> Result<Self::Child> {
        let left = self.0.for_segment(segment_local_id, segment)?;
        let right = self.1.for_segment(segment_local_id, segment)?;
        Ok((left, right))
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring() || self.1.requires_scoring()
    }

    fn merge_fruits(&self, children: Vec<(Left::Fruit, Right::Fruit)>) -> (Left::Fruit, Right::Fruit) {
        let mut left_fruits = vec![];
        let mut right_fruits = vec![];
        for (left_fruit, right_fruit) in children {
            left_fruits.push(left_fruit);
            right_fruits.push(right_fruit);
        }
        (self.0.merge_fruits(left_fruits), self.1.merge_fruits(right_fruits))
    }
}

impl<Left, Right> SegmentCollector for (Left, Right)
    where
        Left: SegmentCollector,
        Right: SegmentCollector
{
    type Fruit = (Left::Fruit, Right::Fruit);

    fn harvest(self) -> <Self as SegmentCollector>::Fruit {
        (self.0.harvest(), self.1.harvest())
    }
}

impl<Left, Right> CollectDocScore for (Left, Right)
    where
        Left: CollectDocScore,
        Right: CollectDocScore
{
    fn collect(&mut self, doc: DocId, score: Score) {
        self.0.collect(doc, score);
        self.1.collect(doc, score);
    }
}

#[allow(missing_docs)]
mod downcast_impl {
    downcast!(super::Fruit);
}


#[cfg(test)]
pub mod tests;

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use collector::{Collector, CountCollector};
    use test::Bencher;
    use collector::CollectDocScore;
    use super::CountCollector;

    #[bench]
    fn build_collector(b: &mut Bencher) {
        b.iter(|| {
            let mut count_collector = SegmentCountCollector::default();
            let docs: Vec<u32> = (0..1_000_000).collect();
            for doc in docs {
                count_collector.collect(doc, 1f32);
            }
            count_collector.harvest()
        });
    }
}

