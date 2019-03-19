/*!

# Collectors

Collectors define the information you want to extract from the documents matching the queries.
In tantivy jargon, we call this information your search "fruit".

Your fruit could for instance be :
- [the count of matching documents](./struct.Count.html)
- [the top 10 documents, by relevancy or by a fast field](./struct.TopDocs.html)
- [facet counts](./struct.FacetCollector.html)

At one point in your code, you will trigger the actual search operation by calling
[the `search(...)` method of your `Searcher` object](../struct.Searcher.html#method.search).
This call will look like this.

```verbatim
let fruit = searcher.search(&query, &collector)?;
```

Here the type of fruit is actually determined as an associated type of the collector (`Collector::Fruit`).


# Combining several collectors

A rich search experience often requires to run several collectors on your search query.
For instance,
- selecting the top-K products matching your query
- counting the matching documents
- computing several facets
- computing statistics about the matching product prices

A simple and efficient way to do that is to pass your collectors as one tuple.
The resulting `Fruit` will then be a typed tuple with each collector's original fruits
in their respective position.

```rust
# extern crate tantivy;
# use tantivy::schema::*;
# use tantivy::*;
# use tantivy::query::*;
use tantivy::collector::{Count, TopDocs};
#
# fn main() -> tantivy::Result<()> {
# let mut schema_builder = Schema::builder();
#     let title = schema_builder.add_text_field("title", TEXT);
#     let schema = schema_builder.build();
#     let index = Index::create_in_ram(schema);
#     let mut index_writer = index.writer(3_000_000)?;
#       index_writer.add_document(doc!(
#       title => "The Name of the Wind",
#      ));
#     index_writer.add_document(doc!(
#        title => "The Diary of Muadib",
#     ));
#     index_writer.commit()?;
#     let reader = index.reader()?;
#     let searcher = reader.searcher();
#     let query_parser = QueryParser::for_index(&index, vec![title]);
#     let query = query_parser.parse_query("diary")?;
let (doc_count, top_docs): (usize, Vec<(Score, DocAddress)>) =
    searcher.search(&query, &(Count, TopDocs::with_limit(2)))?;
#     Ok(())
# }
```

The `Collector` trait is implemented for up to 4 collectors.
If you have more than 4 collectors, you can either group them into
tuples of tuples `(a,(b,(c,d)))`, or rely on `MultiCollector`'s.

# Combining several collectors dynamically

Combining collectors into a tuple is a zero-cost abstraction: everything
happens as if you had manually implemented a single collector
combining all of our features.

Unfortunately it requires you to know at compile time your collector types.
If on the other hand, the collectors depend on some query parameter,
you can rely on `MultiCollector`'s.


# Implementing your own collectors.

See the `custom_collector` example.

*/

use downcast_rs;
use DocId;
use Result;
use Score;
use SegmentLocalId;
use SegmentReader;

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

/// `Fruit` is the type for the result of our collection.
/// e.g. `usize` for the `Count` collector.
pub trait Fruit: Send + downcast_rs::Downcast {}

impl<T> Fruit for T where T: Send + downcast_rs::Downcast {}

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
    type Child: SegmentCollector<Fruit = Self::Fruit>;

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
    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> Result<Self::Fruit>;
}

/// The `SegmentCollector` is the trait in charge of defining the
/// collect operation at the scale of the segment.
///
/// `.collect(doc, score)` will be called for every documents
/// matching the query.
pub trait SegmentCollector: 'static {
    /// `Fruit` is the type for the result of our collection.
    /// e.g. `usize` for the `Count` collector.
    type Fruit: Fruit;

    /// The query pushes the scored document to the collector via this method.
    fn collect(&mut self, doc: DocId, score: Score);

    /// Extract the fruit of the collection from the `SegmentCollector`.
    fn harvest(self) -> Self::Fruit;
}

// -----------------------------------------------
// Tuple implementations.

impl<Left, Right> Collector for (Left, Right)
where
    Left: Collector,
    Right: Collector,
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

    fn merge_fruits(
        &self,
        children: Vec<(Left::Fruit, Right::Fruit)>,
    ) -> Result<(Left::Fruit, Right::Fruit)> {
        let mut left_fruits = vec![];
        let mut right_fruits = vec![];
        for (left_fruit, right_fruit) in children {
            left_fruits.push(left_fruit);
            right_fruits.push(right_fruit);
        }
        Ok((
            self.0.merge_fruits(left_fruits)?,
            self.1.merge_fruits(right_fruits)?,
        ))
    }
}

impl<Left, Right> SegmentCollector for (Left, Right)
where
    Left: SegmentCollector,
    Right: SegmentCollector,
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

// 3-Tuple

impl<One, Two, Three> Collector for (One, Two, Three)
where
    One: Collector,
    Two: Collector,
    Three: Collector,
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
        self.0.requires_scoring() || self.1.requires_scoring() || self.2.requires_scoring()
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        let mut one_fruits = vec![];
        let mut two_fruits = vec![];
        let mut three_fruits = vec![];
        for (one_fruit, two_fruit, three_fruit) in children {
            one_fruits.push(one_fruit);
            two_fruits.push(two_fruit);
            three_fruits.push(three_fruit);
        }
        Ok((
            self.0.merge_fruits(one_fruits)?,
            self.1.merge_fruits(two_fruits)?,
            self.2.merge_fruits(three_fruits)?,
        ))
    }
}

impl<One, Two, Three> SegmentCollector for (One, Two, Three)
where
    One: SegmentCollector,
    Two: SegmentCollector,
    Three: SegmentCollector,
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

// 4-Tuple

impl<One, Two, Three, Four> Collector for (One, Two, Three, Four)
where
    One: Collector,
    Two: Collector,
    Three: Collector,
    Four: Collector,
{
    type Fruit = (One::Fruit, Two::Fruit, Three::Fruit, Four::Fruit);
    type Child = (One::Child, Two::Child, Three::Child, Four::Child);

    fn for_segment(&self, segment_local_id: u32, segment: &SegmentReader) -> Result<Self::Child> {
        let one = self.0.for_segment(segment_local_id, segment)?;
        let two = self.1.for_segment(segment_local_id, segment)?;
        let three = self.2.for_segment(segment_local_id, segment)?;
        let four = self.3.for_segment(segment_local_id, segment)?;
        Ok((one, two, three, four))
    }

    fn requires_scoring(&self) -> bool {
        self.0.requires_scoring()
            || self.1.requires_scoring()
            || self.2.requires_scoring()
            || self.3.requires_scoring()
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        let mut one_fruits = vec![];
        let mut two_fruits = vec![];
        let mut three_fruits = vec![];
        let mut four_fruits = vec![];
        for (one_fruit, two_fruit, three_fruit, four_fruit) in children {
            one_fruits.push(one_fruit);
            two_fruits.push(two_fruit);
            three_fruits.push(three_fruit);
            four_fruits.push(four_fruit);
        }
        Ok((
            self.0.merge_fruits(one_fruits)?,
            self.1.merge_fruits(two_fruits)?,
            self.2.merge_fruits(three_fruits)?,
            self.3.merge_fruits(four_fruits)?,
        ))
    }
}

impl<One, Two, Three, Four> SegmentCollector for (One, Two, Three, Four)
where
    One: SegmentCollector,
    Two: SegmentCollector,
    Three: SegmentCollector,
    Four: SegmentCollector,
{
    type Fruit = (One::Fruit, Two::Fruit, Three::Fruit, Four::Fruit);

    fn collect(&mut self, doc: DocId, score: Score) {
        self.0.collect(doc, score);
        self.1.collect(doc, score);
        self.2.collect(doc, score);
        self.3.collect(doc, score);
    }

    fn harvest(self) -> <Self as SegmentCollector>::Fruit {
        (
            self.0.harvest(),
            self.1.harvest(),
            self.2.harvest(),
            self.3.harvest(),
        )
    }
}

impl_downcast!(Fruit);

#[cfg(test)]
pub mod tests;
