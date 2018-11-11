/*!

# Collectors

Collectors define the information you want to extract from the documents matching the queries.
In tantivy jargon, we call this information your search "fruit".

Your fruit could for instance be :
- [the count of matching documents](./struct.Count.html)
- [the top 10 documents, by relevancy or by a fast field](./struct.TopDocs.html)
- facet counts

At one point in your code, you will trigger the actual search operation by calling
[the `search(...)` method of your `Searcher` object](../struct.Searcher.html#method.search).
This call will look like this.

```verbatim
let fruit = searcher.search(&query, &collector)?;
```

Here the type of fruit is actually determined as an associated type of the collector (`Collector::Fruit`).


# Combining several collectors


# Implementing your own collectors.



*/

use DocId;
use Result;
use Score;
use SegmentLocalId;
use SegmentReader;
use query::Query;
use Searcher;
use downcast;
use rayon;
use rayon::prelude::*;

mod count_collector;
pub use self::count_collector::Count;

mod multi_collector;
pub use self::multi_collector::MultiCollector;

mod top_collector;

mod top_score_collector;
pub use self::top_score_collector::TopDocs;

mod top_field_collector;
pub use self::top_field_collector::TopDocsByField;

mod facet_collector;
pub use self::facet_collector::FacetCollector;
use TantivyError;

/// `Fruit` is the type for the result of our collection.
/// e.g. `usize` for the `Count` collector.
pub trait Fruit: Send + downcast::Any {}

impl<T> Fruit for T where T: Send + downcast::Any {}

/// Collectors are in charge of collecting and retaining relevant
/// information from the document found and scored by the query.
///
/// For instance,
///
/// - keeping track of the top 10 best documents
/// - computing a breakdown over a fast field
/// - computing the number of documents matching the query
///
/// Our search index is in fact a collection of segments, so
/// a `Collector` trait is actually more of a factory to instance
/// `SegmentCollector`s for each segments.
///
/// The collection logic itself is in the `SegmentCollector`.
///
/// Segments are not guaranteed to be visited in any specific order.
pub trait Collector: Sync {

    /// `Fruit` is the type for the result of our collection.
    /// e.g. `usize` for the `Count` collector.
    type Fruit: Fruit;

    /// Type of the `SegmentCollector` associated to this collector.
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

    /// Combines the fruit associated to the collection of each segments
    /// into one fruit.
    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Self::Fruit;

    /// You should not use this method.
    ///
    /// Instead, please use [the `Searcher`'s search(...) method](../struct.Searcher.html#method.search).
    #[doc(hidden)]
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

    /// You should not use this method.
    ///
    /// Instead, please use [the `Searcher`'s search(...) method](../struct.Searcher.html#method.search).
    #[doc(hidden)]
    fn search_multithreads(&self, searcher: &Searcher, query: &Query, num_threads: usize) -> Result<Self::Fruit> {
        let segment_readers = searcher.segment_readers();
        let actual_num_threads = (segment_readers.len() + 1).max(num_threads);
        let scoring_enabled = self.requires_scoring();
        let weight = query.weight(searcher, scoring_enabled)?;
        let thread_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(actual_num_threads)
            .thread_name(|thread_id|
                format!("SearchThread-{}", thread_id))
            .build()
            .map_err(|_| TantivyError::SystemError("Failed to spawn a search thread".to_string()))?;
        let segment_fruits: Vec<Self::Fruit> = thread_pool.install(|| {
            (0..segment_readers.len())
                .into_par_iter()
                .map(|segment_ord| {
                    let segment_reader = &segment_readers[segment_ord];
                    let mut scorer = weight.scorer(segment_reader).unwrap();
                    let mut child = self
                        .for_segment(segment_ord as u32, segment_reader)?;

                    let delete_bitset_opt = segment_reader.delete_bitset();
                    if let Some(delete_bitset) = delete_bitset_opt {
                        scorer.for_each(&mut |doc, score|
                            if !delete_bitset.is_deleted(doc) {
                                child.collect(doc, score);
                            });

                    } else {
                        scorer.for_each(&mut |doc, score|
                            child.collect(doc, score));
                    }
                    Ok(child.harvest())
                })
                .collect::<Result<_>>()
        })?;
        Ok(self.merge_fruits(segment_fruits))
    }
}


/// The `SegmentCollector` is the trait in charge of defining the
/// collect operation at the scale of the segment.
///
/// `.collect(doc, score)` will be called for every documents
/// matching the query.
pub trait SegmentCollector: Sync + 'static {
    /// `Fruit` is the type for the result of our collection.
    /// e.g. `usize` for the `Count` collector.
    type Fruit: Fruit;

    /// The query pushes the scored document to the collector via this method.
    fn collect(&mut self, doc: DocId, score: Score);

    /// Extract the fruit of the collection from the `SegmentCollector`.
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

    fn collect(&mut self, doc: DocId, score: Score) {
        self.0.collect(doc, score);
        self.1.collect(doc, score);
    }

    fn harvest(self) -> <Self as SegmentCollector>::Fruit {
        (self.0.harvest(), self.1.harvest())
    }
}


/// Tuple implementations.
/// ---------------------------------
/// TODO: can we macro this out

impl<One, Two, Three> Collector for (One, Two, Three)
    where One: Collector,
          Two: Collector,
          Three: Collector
{
    type Fruit = (One::Fruit, Two::Fruit, Three::Fruit);
    type Child = (One::Child, Two::Child, Three::Child);

    fn for_segment(&self, segment_local_id: u32, segment: &SegmentReader) -> Result<Self::Child> {
        let one = self.0.for_segment(segment_local_id, segment)?;
        let two = self.1.for_segment(segment_local_id, segment)?;
        let three = self.2.for_segment(segment_local_id, segment)?;
        Ok((one, two, three))
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring() ||
        self.1.requires_scoring() ||
        self.2.requires_scoring()
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Self::Fruit {
        let mut one_fruits = vec![];
        let mut two_fruits = vec![];
        let mut three_fruits = vec![];
        for (one_fruit, two_fruit, three_fruit) in children {
            one_fruits.push(one_fruit);
            two_fruits.push(two_fruit);
            three_fruits.push(three_fruit);
        }
        (self.0.merge_fruits(one_fruits),
         self.1.merge_fruits(two_fruits),
         self.2.merge_fruits(three_fruits))
    }
}

impl<One, Two, Three> SegmentCollector for (One, Two, Three)
    where
        One: SegmentCollector,
        Two: SegmentCollector,
        Three: SegmentCollector
{
    type Fruit = (One::Fruit, Two::Fruit, Three::Fruit);

    fn collect(&mut self, doc: DocId, score: Score) {
        self.0.collect(doc, score);
        self.1.collect(doc, score);
        self.2.collect(doc, score);
    }

    fn harvest(self) -> <Self as SegmentCollector>::Fruit {
        (self.0.harvest(), self.1.harvest(), self.2.harvest())
    }
}





#[allow(missing_docs)]
mod downcast_impl {
    downcast!(super::Fruit);
}


#[cfg(test)]
pub mod tests;
