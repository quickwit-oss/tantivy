use super::Collector;
use crate::collector::top_collector::TopCollector;
use crate::collector::top_collector::TopSegmentCollector;
use crate::collector::top_custom_score_collector::ScoreTweaker;
use crate::collector::top_custom_score_collector::SegmentScoreTweaker;
use crate::collector::top_custom_score_collector::TweakedScoreCollector;
use crate::collector::SegmentCollector;
use crate::collector::SegmentCollector;
use crate::collector::TopDocsByField;
use crate::fastfield::FastValue;
use crate::schema::Field;
use crate::DocAddress;
use crate::DocId;
use crate::Result;
use crate::Score;
use crate::SegmentLocalId;
use crate::SegmentReader;

/// The Top Score Collector keeps track of the K documents
/// sorted by their score.
///
/// The implementation is based on a `BinaryHeap`.
/// The theorical complexity for collecting the top `K` out of `n` documents
/// is `O(n log K)`.
///
/// ```rust
/// #[macro_use]
/// extern crate tantivy;
/// use tantivy::DocAddress;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{Index, Result};
/// use tantivy::collector::TopDocs;
/// use tantivy::query::QueryParser;
///
/// # fn main() { example().unwrap(); }
/// fn example() -> Result<()> {
///     let mut schema_builder = Schema::builder();
///     let title = schema_builder.add_text_field("title", TEXT);
///     let schema = schema_builder.build();
///     let index = Index::create_in_ram(schema);
///     {
///         let mut index_writer = index.writer_with_num_threads(1, 3_000_000)?;
///         index_writer.add_document(doc!(
///             title => "The Name of the Wind",
///         ));
///         index_writer.add_document(doc!(
///             title => "The Diary of Muadib",
///         ));
///         index_writer.add_document(doc!(
///             title => "A Dairy Cow",
///         ));
///         index_writer.add_document(doc!(
///             title => "The Diary of a Young Girl",
///         ));
///         index_writer.commit().unwrap();
///     }
///
///     let reader = index.reader()?;
///     let searcher = reader.searcher();
///
///     let query_parser = QueryParser::for_index(&index, vec![title]);
///     let query = query_parser.parse_query("diary")?;
///     let top_docs = searcher.search(&query, &TopDocs::with_limit(2))?;
///
///     assert_eq!(&top_docs[0], &(0.7261542, DocAddress(0, 1)));
///     assert_eq!(&top_docs[1], &(0.6099695, DocAddress(0, 3)));
///
///     Ok(())
/// }
/// ```
pub struct TopDocs(TopCollector<Score>);

impl TopDocs {
    /// Creates a top score collector, with a number of documents equal to "limit".
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(limit: usize) -> TopDocs {
        TopDocs(TopCollector::with_limit(limit))
    }

    /// Set top-K to rank documents by a given fast field.
    ///
    /// ```rust
    /// #[macro_use]
    /// extern crate tantivy;
    /// # use tantivy::schema::{Schema, FAST, TEXT};
    /// # use tantivy::{Index, Result, DocAddress};
    /// # use tantivy::query::{Query, QueryParser};
    /// use tantivy::Searcher;
    /// use tantivy::collector::TopDocs;
    /// use tantivy::schema::Field;
    ///
    /// # fn main() -> tantivy::Result<()> {
    /// #   let mut schema_builder = Schema::builder();
    /// #   let title = schema_builder.add_text_field("title", TEXT);
    /// #   let rating = schema_builder.add_u64_field("rating", FAST);
    /// #   let schema = schema_builder.build();
    /// #  
    /// #   let index = Index::create_in_ram(schema);
    /// #   let mut index_writer = index.writer_with_num_threads(1, 3_000_000)?;
    /// #   index_writer.add_document(doc!(
    /// #       title => "The Name of the Wind",
    /// #       rating => 92u64,
    /// #   ));
    /// #   index_writer.add_document(doc!(title => "The Diary of Muadib", rating => 97u64));
    /// #   index_writer.add_document(doc!(title => "A Dairy Cow", rating => 63u64));
    /// #   index_writer.add_document(doc!(title => "The Diary of a Young Girl", rating => 80u64));
    /// #   index_writer.commit()?;
    /// #   let reader = index.reader()?;
    /// #   let query = QueryParser::for_index(&index, vec![title]).parse_query("diary")?;
    /// #   let top_docs = docs_sorted_by_rating(&reader.searcher(), &query, rating)?;
    /// #   assert_eq!(top_docs,
    /// #            vec![(97u64, DocAddress(0u32, 1)),
    /// #                 (80u64, DocAddress(0u32, 3))]);
    /// #   Ok(())
    /// # }
    ///
    ///
    /// /// Searches the document matching the given query, and
    /// /// collects the top 10 documents, order by the `field`
    /// /// given in argument.
    /// ///
    /// /// `field` is required to be a FAST field.
    /// fn docs_sorted_by_rating(searcher: &Searcher,
    ///                          query: &Query,
    ///                          sort_by_field: Field)
    ///     -> Result<Vec<(u64, DocAddress)>> {
    ///
    ///     // This is where we build our topdocs collector
    ///     //
    ///     // Note the generics parameter that needs to match the
    ///     // type `sort_by_field`.
    ///     let top_docs_by_rating = TopDocs
    ///                 ::with_limit(10)
    ///                  .order_by_field::<u64>(sort_by_field);
    ///     
    ///     // ... and here are our documents. Note this is a simple vec.
    ///     // The `u64` in the pair is the value of our fast field for
    ///     // each documents.
    ///     //
    ///     // The vec is sorted decreasingly by `sort_by_field`, and has a
    ///     // length of 10, or less if not enough documents matched the
    ///     // query.
    ///     let resulting_docs: Vec<(u64, DocAddress)> =
    ///          searcher.search(query, &top_docs_by_rating)?;
    ///     
    ///     Ok(resulting_docs)
    /// }
    /// ```
    ///
    /// # Panics
    ///
    /// May panic if the field requested is not a fast field.
    ///
    pub fn order_by_field<TFastValue>(
        self,
        field: Field,
    ) -> impl Collector<Fruit = Vec<(TFastValue, DocAddress)>>
    where
        TFastValue: FastValue + 'static,
    {
        self.custom_score(move |segment_reader: &SegmentReader| {
            let ff_reader: FastFieldReader<u64> = segment_reader
                .fast_fields()
                .u64_lenient(field)
                .expect("Field requested is not a i64/u64 fast field.");
            move |doc: DocId| TFastValue::from_u64(ff_reader.get(doc))
        })
    }

    /// Ranks the documents using a custom score.
    ///
    /// This method offers a convenient way to tweak or replace
    /// the documents score. As suggested by the prototype you can
    /// manually define your own [`ScoreTweaker`](./trait.ScoreTweaker.html)
    /// and pass it as an argument, but there is a much simpler way to
    /// tweak your score: you can use a closure as in the following
    /// example.
    ///
    /// # Example
    ///
    /// Typically, you will want to rely on one or more fast fields,
    /// to alter or replace the original relevance `Score`.
    ///
    /// For instance, in the following, we assume that we are implementing
    /// an e-commerce website that has a fast field called `popularity`
    /// that rates whether a product is typically often bought by users.
    ///
    /// In the following example will will tweak our ranking a bit by
    /// boosting popular products a notch.
    ///  
    /// In more serious application, this tweaking could involved running a
    /// learning-to-rank model over various features
    ///
    /// ```rust
    /// #[macro_use]
    /// extern crate tantivy;
    /// # use tantivy::schema::{Schema, FAST, TEXT};
    /// # use tantivy::{Index, DocAddress, DocId, Score};
    /// # use tantivy::query::QueryParser;
    /// use tantivy::SegmentReader;
    /// use tantivy::collector::TopDocs;
    /// use tantivy::schema::Field;
    ///
    /// # fn create_schema() -> Schema {
    /// #    let mut schema_builder = Schema::builder();
    /// #    schema_builder.add_text_field("product_name", TEXT);
    /// #    schema_builder.add_u64_field("popularity", FAST);
    /// #    schema_builder.build()
    /// # }
    /// #
    /// # fn main() -> tantivy::Result<()> {
    /// #   let schema = create_schema();
    /// #   let index = Index::create_in_ram(schema);
    /// #   let mut index_writer = index.writer_with_num_threads(1, 3_000_000)?;
    /// #   let product_name = index.schema().get_field("product_name").unwrap();
    /// #   
    /// let popularity: Field = index.schema().get_field("popularity").unwrap();
    /// #   index_writer.add_document(doc!(product_name => "The Diary of Muadib", popularity => 1u64));
    /// #   index_writer.add_document(doc!(product_name => "A Dairy Cow", popularity => 10u64));
    /// #   index_writer.add_document(doc!(product_name => "The Diary of a Young Girl", popularity => 15u64));
    /// #   index_writer.commit()?;
    /// // ...
    /// # let user_query = "diary";
    /// # let query = QueryParser::for_index(&index, vec![product_name]).parse_query(user_query)?;
    ///
    /// // This is where we build our collector with our custom score.
    /// let top_docs_by_custom_score = TopDocs
    ///         ::with_limit(10)
    ///          .tweak_score(move |segment_reader: &SegmentReader| {
    ///             // We pass here function, that return our scoring
    ///             // function.
    ///             //
    ///             // The raison d'être of the "mother" function,
    ///             // is to gather all of the segment level information
    ///             // we need for scoring. Typically, fast_fields.
    ///             //
    ///             // In our case, we will get a reader for the popularity
    ///             // fast field.
    ///             let popularity_reader =
    ///                 segment_reader.fast_fields().u64(popularity).unwrap();
    ///
    ///             // We can now define our actual scoring function
    ///             Ok(move |doc: DocId, original_score: Score| {
    ///                 let popularity: u64 = popularity_reader.get(doc);
    ///                 // Well.. For the sake of the example we use a simple logarithm
    ///                 // function.
    ///                 let popularity_boost_score = ((2u64 + popularity) as f32).log2();
    ///                 popularity_boost_score * original_score
    ///             })
    ///           });
    /// # let reader = index.reader()?;
    /// # let searcher = reader.searcher();
    /// // ... and here are our documents. Note this is a simple vec.
    /// // The `Score` in the pair is our tweaked score.
    /// let resulting_docs: Vec<(Score, DocAddress)> =
    ///      searcher.search(&*query, &top_docs_by_custom_score)?;
    ///
    /// # Ok(())
    /// # }
    pub fn tweak_score<TScore, TScoreSegmentTweaker, TScoreTweaker>(
        self,
        score_tweaker: TScoreTweaker,
    ) -> TweakedScoreTopCollector<TScoreTweaker, TScore>
    where
        TScore: Send + Sync + Clone + PartialOrd,
        TScoreSegmentTweaker: ScoreSegmentTweaker<TScore> + 'static,
        TScoreTweaker: ScoreTweaker<TScore, Child = TScoreSegmentTweaker>,
    {
        TweakedScoreTopCollector::new(score_tweaker, self.0.limit())
    }

    pub fn custom_score<TScore, TCustomSegmentScorer, TCustomScorer>(
        self,
        custom_score: TCustomScorer,
    ) -> CustomScoreTopCollector<TCustomScorer, TScore>
    where
        TScore: Send + Sync + Clone + PartialOrd,
        TCustomSegmentScorer: CustomSegmentScorer<TScore> + 'static,
        TCustomScorer: CustomScorer<TScore, Child = TCustomSegmentScorer>,
    {
        CustomScoreTopCollector::new(custom_score, self.0.limit())
    }
}

impl Collector for TopDocs {
    type Fruit = Vec<(Score, DocAddress)>;

    type Child = TopScoreSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: SegmentLocalId,
        reader: &SegmentReader,
    ) -> Result<Self::Child> {
        let collector = self.0.for_segment(segment_local_id, reader)?;
        Ok(TopScoreSegmentCollector(collector))
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, child_fruits: Vec<Vec<(Score, DocAddress)>>) -> Result<Self::Fruit> {
        self.0.merge_fruits(child_fruits)
    }
}

/// Segment Collector associated to `TopDocs`.
pub struct TopScoreSegmentCollector(TopSegmentCollector<Score>);

impl SegmentCollector for TopScoreSegmentCollector {
    type Fruit = Vec<(Score, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.0.collect(doc, score)
    }

    fn harvest(self) -> Vec<(Score, DocAddress)> {
        self.0.harvest()
    }
}

#[cfg(test)]
mod tests {
    use super::TopDocs;
    use crate::query::QueryParser;
    use crate::schema::Schema;
    use crate::schema::TEXT;
    use crate::DocAddress;
    use crate::Index;
    use crate::Score;
    use crate::{DocAddress, Index, IndexWriter, TantivyError};
    use {DocAddress, Index, IndexWriter};

    fn make_index() -> Index {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
            index_writer.add_document(doc!(text_field=>"Hello happy tax payer."));
            index_writer.add_document(doc!(text_field=>"Droopy says hello happy tax payer"));
            index_writer.add_document(doc!(text_field=>"I like Droopy"));
            assert!(index_writer.commit().is_ok());
        }
        index
    }

    #[test]
    fn test_top_collector_not_at_capacity() {
        let index = make_index();
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax").unwrap();
        let score_docs: Vec<(Score, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(&text_query, &TopDocs::with_limit(4))
            .unwrap();
        assert_eq!(
            score_docs,
            vec![
                (0.81221175, DocAddress(0u32, 1)),
                (0.5376842, DocAddress(0u32, 2)),
                (0.48527452, DocAddress(0, 0))
            ]
        );
    }

    #[test]
    fn test_top_collector_at_capacity() {
        let index = make_index();
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax").unwrap();
        let score_docs: Vec<(Score, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(&text_query, &TopDocs::with_limit(2))
            .unwrap();
        assert_eq!(
            score_docs,
            vec![
                (0.81221175, DocAddress(0u32, 1)),
                (0.5376842, DocAddress(0u32, 2)),
            ]
        );
    }

    #[test]
    #[should_panic]
    fn test_top_0() {
        TopDocs::with_limit(0);
    }

    const TITLE: &str = "title";
    const SIZE: &str = "size";

    #[test]
    fn test_top_field_collector_not_at_capacity() {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field(TITLE, TEXT);
        let size = schema_builder.add_u64_field(SIZE, FAST);
        let schema = schema_builder.build();
        let (index, query) = index("beer", title, schema, |index_writer| {
            index_writer.add_document(doc!(
                title => "bottle of beer",
                size => 12u64,
            ));
            index_writer.add_document(doc!(
                title => "growler of beer",
                size => 64u64,
            ));
            index_writer.add_document(doc!(
                title => "pint of beer",
                size => 16u64,
            ));
        });
        let searcher = index.reader().unwrap().searcher();

        let top_collector = TopDocs::with_limit(4).order_by_field(size);
        let top_docs: Vec<(u64, DocAddress)> = searcher.search(&query, &top_collector).unwrap();
        assert_eq!(
            top_docs,
            vec![
                (64, DocAddress(0, 1)),
                (16, DocAddress(0, 2)),
                (12, DocAddress(0, 0))
            ]
        );
    }

    #[test]
    #[should_panic]
    fn test_field_does_not_exist() {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field(TITLE, TEXT);
        let size = schema_builder.add_u64_field(SIZE, FAST);
        let schema = schema_builder.build();
        let (index, _) = index("beer", title, schema, |index_writer| {
            index_writer.add_document(doc!(
                title => "bottle of beer",
                size => 12u64,
            ));
        });
        let searcher = index.reader().unwrap().searcher();
        let top_collector = TopDocs::with_limit(4).order_by_field::<u64>(Field(2));
        let segment_reader = searcher.segment_reader(0u32);
        top_collector
            .for_segment(0, segment_reader)
            .expect("should panic");
    }

    #[test]
    #[should_panic(expected = "Field requested is not a i64/u64 fast field")]
    fn test_field_not_fast_field() {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field(TITLE, TEXT);
        let size = schema_builder.add_u64_field(SIZE, STORED);
        let schema = schema_builder.build();
        let (index, _) = index("beer", title, schema, |index_writer| {
            index_writer.add_document(doc!(
                title => "bottle of beer",
                size => 12u64,
            ));
        });
        let searcher = index.reader().unwrap().searcher();
        let segment = searcher.segment_reader(0);
        let top_collector = TopDocs::with_limit(4).order_by_field::<u64>(size);
        assert!(top_collector.for_segment(0, segment).is_ok());
    }

    fn index(
        query: &str,
        query_field: Field,
        schema: Schema,
        mut doc_adder: impl FnMut(&mut IndexWriter) -> (),
    ) -> (Index, Box<Query>) {
        let index = Index::create_in_ram(schema);

        let mut index_writer = index.writer_with_num_threads(1, 3_000_000).unwrap();
        doc_adder(&mut index_writer);
        index_writer.commit().unwrap();
        let query_parser = QueryParser::for_index(&index, vec![query_field]);
        let query = query_parser.parse_query(query).unwrap();
        (index, query)
    }

}
