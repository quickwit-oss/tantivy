use super::Collector;
use crate::collector::top_collector::{ComparableDoc, TopCollector};
use crate::collector::tweak_score_top_collector::TweakedScoreTopCollector;
use crate::collector::{
    CustomScorer, CustomSegmentScorer, ScoreSegmentTweaker, ScoreTweaker, SegmentCollector,
};
use crate::fastfield::FastFieldReader;
use crate::query::Weight;
use crate::schema::Field;
use crate::DocAddress;
use crate::DocId;
use crate::Score;
use crate::SegmentOrdinal;
use crate::SegmentReader;
use crate::{collector::custom_score_top_collector::CustomScoreTopCollector, fastfield::FastValue};
use crate::{collector::top_collector::TopSegmentCollector, TantivyError};
use std::fmt;
use std::{collections::BinaryHeap, marker::PhantomData};

struct FastFieldConvertCollector<
    TCollector: Collector<Fruit = Vec<(u64, DocAddress)>>,
    TFastValue: FastValue,
> {
    pub collector: TCollector,
    pub field: Field,
    pub fast_value: std::marker::PhantomData<TFastValue>,
}

impl<TCollector, TFastValue> Collector for FastFieldConvertCollector<TCollector, TFastValue>
where
    TCollector: Collector<Fruit = Vec<(u64, DocAddress)>>,
    TFastValue: FastValue,
{
    type Fruit = Vec<(TFastValue, DocAddress)>;

    type Child = TCollector::Child;

    fn for_segment(
        &self,
        segment_local_id: crate::SegmentOrdinal,
        segment: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        let schema = segment.schema();
        let field_entry = schema.get_field_entry(self.field);
        if !field_entry.is_fast() {
            return Err(TantivyError::SchemaError(format!(
                "Field {:?} is not a fast field.",
                field_entry.name()
            )));
        }
        let schema_type = TFastValue::to_type();
        let requested_type = field_entry.field_type().value_type();
        if schema_type != requested_type {
            return Err(TantivyError::SchemaError(format!(
                "Field {:?} is of type {:?}!={:?}",
                field_entry.name(),
                schema_type,
                requested_type
            )));
        }
        self.collector.for_segment(segment_local_id, segment)
    }

    fn requires_scoring(&self) -> bool {
        self.collector.requires_scoring()
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        let raw_result = self.collector.merge_fruits(segment_fruits)?;
        let transformed_result = raw_result
            .into_iter()
            .map(|(score, doc_address)| (TFastValue::from_u64(score), doc_address))
            .collect::<Vec<_>>();
        Ok(transformed_result)
    }
}

/// The `TopDocs` collector keeps track of the top `K` documents
/// sorted by their score.
///
/// The implementation is based on a `BinaryHeap`.
/// The theorical complexity for collecting the top `K` out of `n` documents
/// is `O(n log K)`.
///
/// This collector guarantees a stable sorting in case of a tie on the
/// document score. As such, it is suitable to implement pagination.
///
/// ```rust
/// use tantivy::collector::TopDocs;
/// use tantivy::query::QueryParser;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{doc, DocAddress, Index};
///
/// let mut schema_builder = Schema::builder();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let schema = schema_builder.build();
/// let index = Index::create_in_ram(schema);
///
/// let mut index_writer = index.writer_with_num_threads(1, 10_000_000).unwrap();
/// index_writer.add_document(doc!(title => "The Name of the Wind"));
/// index_writer.add_document(doc!(title => "The Diary of Muadib"));
/// index_writer.add_document(doc!(title => "A Dairy Cow"));
/// index_writer.add_document(doc!(title => "The Diary of a Young Girl"));
/// assert!(index_writer.commit().is_ok());
///
/// let reader = index.reader().unwrap();
/// let searcher = reader.searcher();
///
/// let query_parser = QueryParser::for_index(&index, vec![title]);
/// let query = query_parser.parse_query("diary").unwrap();
/// let top_docs = searcher.search(&query, &TopDocs::with_limit(2)).unwrap();
///
/// assert_eq!(top_docs[0].1, DocAddress::new(0, 1));
/// assert_eq!(top_docs[1].1, DocAddress::new(0, 3));
/// ```
pub struct TopDocs(TopCollector<Score>);

impl fmt::Debug for TopDocs {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "TopDocs(limit={}, offset={})",
            self.0.limit, self.0.offset
        )
    }
}

struct ScorerByFastFieldReader {
    ff_reader: FastFieldReader<u64>,
}

impl CustomSegmentScorer<u64> for ScorerByFastFieldReader {
    fn score(&mut self, doc: DocId) -> u64 {
        self.ff_reader.get(doc)
    }
}

struct ScorerByField {
    field: Field,
}

impl CustomScorer<u64> for ScorerByField {
    type Child = ScorerByFastFieldReader;

    fn segment_scorer(&self, segment_reader: &SegmentReader) -> crate::Result<Self::Child> {
        // We interpret this field as u64, regardless of its type, that way,
        // we avoid needless conversion. Regardless of the fast field type, the
        // mapping is monotonic, so it is sufficient to compute our top-K docs.
        //
        // The conversion will then happen only on the top-K docs.
        let ff_reader: FastFieldReader<u64> = segment_reader
            .fast_fields()
            .typed_fast_field_reader(self.field)?;
        Ok(ScorerByFastFieldReader { ff_reader })
    }
}

impl TopDocs {
    /// Creates a top score collector, with a number of documents equal to "limit".
    ///
    /// # Panics
    /// The method panics if limit is 0
    pub fn with_limit(limit: usize) -> TopDocs {
        TopDocs(TopCollector::with_limit(limit))
    }

    /// Skip the first "offset" documents when collecting.
    ///
    /// This is equivalent to `OFFSET` in MySQL or PostgreSQL and `start` in
    /// Lucene's TopDocsCollector.
    ///
    /// # Example
    ///
    /// ```rust
    /// use tantivy::collector::TopDocs;
    /// use tantivy::query::QueryParser;
    /// use tantivy::schema::{Schema, TEXT};
    /// use tantivy::{doc, DocAddress, Index};
    ///
    /// let mut schema_builder = Schema::builder();
    /// let title = schema_builder.add_text_field("title", TEXT);
    /// let schema = schema_builder.build();
    /// let index = Index::create_in_ram(schema);
    ///
    /// let mut index_writer = index.writer_with_num_threads(1, 10_000_000).unwrap();
    /// index_writer.add_document(doc!(title => "The Name of the Wind"));
    /// index_writer.add_document(doc!(title => "The Diary of Muadib"));
    /// index_writer.add_document(doc!(title => "A Dairy Cow"));
    /// index_writer.add_document(doc!(title => "The Diary of a Young Girl"));
    /// index_writer.add_document(doc!(title => "The Diary of Lena Mukhina"));
    /// assert!(index_writer.commit().is_ok());
    ///
    /// let reader = index.reader().unwrap();
    /// let searcher = reader.searcher();
    ///
    /// let query_parser = QueryParser::for_index(&index, vec![title]);
    /// let query = query_parser.parse_query("diary").unwrap();
    /// let top_docs = searcher.search(&query, &TopDocs::with_limit(2).and_offset(1)).unwrap();
    ///
    /// assert_eq!(top_docs.len(), 2);
    /// assert_eq!(top_docs[0].1, DocAddress::new(0, 4));
    /// assert_eq!(top_docs[1].1, DocAddress::new(0, 3));
    /// ```
    pub fn and_offset(self, offset: usize) -> TopDocs {
        TopDocs(self.0.and_offset(offset))
    }

    /// Set top-K to rank documents by a given fast field.
    ///
    /// If the field is not a fast or does not exist, this method returns successfully (it is not aware of any schema).
    /// An error will be returned at the moment of search.
    ///
    /// If the field is a FAST field but not a u64 field, search will return successfully but it will return
    /// returns a monotonic u64-representation (ie. the order is still correct) of the requested field type.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use tantivy::schema::{Schema, FAST, TEXT};
    /// # use tantivy::{doc, Index, DocAddress};
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
    /// #   let mut index_writer = index.writer_with_num_threads(1, 10_000_000)?;
    /// #   index_writer.add_document(doc!(title => "The Name of the Wind", rating => 92u64));
    /// #   index_writer.add_document(doc!(title => "The Diary of Muadib", rating => 97u64));
    /// #   index_writer.add_document(doc!(title => "A Dairy Cow", rating => 63u64));
    /// #   index_writer.add_document(doc!(title => "The Diary of a Young Girl", rating => 80u64));
    /// #   assert!(index_writer.commit().is_ok());
    /// #   let reader = index.reader()?;
    /// #   let query = QueryParser::for_index(&index, vec![title]).parse_query("diary")?;
    /// #   let top_docs = docs_sorted_by_rating(&reader.searcher(), &query, rating)?;
    /// #   assert_eq!(top_docs,
    /// #            vec![(97u64, DocAddress::new(0u32, 1)),
    /// #                 (80u64, DocAddress::new(0u32, 3))]);
    /// #   Ok(())
    /// # }
    /// /// Searches the document matching the given query, and
    /// /// collects the top 10 documents, order by the u64-`field`
    /// /// given in argument.
    /// fn docs_sorted_by_rating(searcher: &Searcher,
    ///                          query: &dyn Query,
    ///                          rating_field: Field)
    ///     -> tantivy::Result<Vec<(u64, DocAddress)>> {
    ///
    ///     // This is where we build our topdocs collector
    ///     //
    ///     // Note the `rating_field` needs to be a FAST field here.
    ///     let top_books_by_rating = TopDocs
    ///                 ::with_limit(10)
    ///                  .order_by_u64_field(rating_field);
    ///
    ///     // ... and here are our documents. Note this is a simple vec.
    ///     // The `u64` in the pair is the value of our fast field for
    ///     // each documents.
    ///     //
    ///     // The vec is sorted decreasingly by `sort_by_field`, and has a
    ///     // length of 10, or less if not enough documents matched the
    ///     // query.
    ///     let resulting_docs: Vec<(u64, DocAddress)> =
    ///          searcher.search(query, &top_books_by_rating)?;
    ///
    ///     Ok(resulting_docs)
    /// }
    /// ```
    ///
    /// # See also
    ///
    /// To confortably work with `u64`s, `i64`s, `f64`s, or `date`s, please refer to
    /// [.order_by_fast_field(...)](#method.order_by_fast_field) method.
    pub fn order_by_u64_field(
        self,
        field: Field,
    ) -> impl Collector<Fruit = Vec<(u64, DocAddress)>> {
        CustomScoreTopCollector::new(ScorerByField { field }, self.0.into_tscore())
    }

    /// Set top-K to rank documents by a given fast field.
    ///
    /// If the field is not a fast field, or its field type does not match the generic type, this method does not panic,
    /// but an explicit error will be returned at the moment of collection.
    ///
    /// Note that this method is a generic. The requested fast field type will be often
    /// inferred in your code by the rust compiler.
    ///
    /// Implementation-wise, for performance reason, tantivy will manipulate the u64 representation of your fast
    /// field until the last moment.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use tantivy::schema::{Schema, FAST, TEXT};
    /// # use tantivy::{doc, Index, DocAddress};
    /// # use tantivy::query::{Query, AllQuery};
    /// use tantivy::Searcher;
    /// use tantivy::collector::TopDocs;
    /// use tantivy::schema::Field;
    ///
    /// # fn main() -> tantivy::Result<()> {
    /// #   let mut schema_builder = Schema::builder();
    /// #   let title = schema_builder.add_text_field("company", TEXT);
    /// #   let rating = schema_builder.add_i64_field("revenue", FAST);
    /// #   let schema = schema_builder.build();
    /// #
    /// #   let index = Index::create_in_ram(schema);
    /// #   let mut index_writer = index.writer_with_num_threads(1, 10_000_000)?;
    /// #   index_writer.add_document(doc!(title => "MadCow Inc.", rating => 92_000_000i64));
    /// #   index_writer.add_document(doc!(title => "Zozo Cow KKK", rating => 119_000_000i64));
    /// #   index_writer.add_document(doc!(title => "Declining Cow", rating => -63_000_000i64));
    /// #   assert!(index_writer.commit().is_ok());
    /// #   let reader = index.reader()?;
    /// #   let top_docs = docs_sorted_by_revenue(&reader.searcher(), &AllQuery, rating)?;
    /// #   assert_eq!(top_docs,
    /// #            vec![(119_000_000i64, DocAddress::new(0, 1)),
    /// #                 (92_000_000i64, DocAddress::new(0, 0))]);
    /// #   Ok(())
    /// # }
    /// /// Searches the document matching the given query, and
    /// /// collects the top 10 documents, order by the u64-`field`
    /// /// given in argument.
    /// fn docs_sorted_by_revenue(searcher: &Searcher,
    ///                          query: &dyn Query,
    ///                          revenue_field: Field)
    ///     -> tantivy::Result<Vec<(i64, DocAddress)>> {
    ///
    ///     // This is where we build our topdocs collector
    ///     //
    ///     // Note the generics parameter that needs to match the
    ///     // type `sort_by_field`. revenue_field here is a FAST i64 field.
    ///     let top_company_by_revenue = TopDocs
    ///                 ::with_limit(2)
    ///                  .order_by_fast_field(revenue_field);
    ///
    ///     // ... and here are our documents. Note this is a simple vec.
    ///     // The `i64` in the pair is the value of our fast field for
    ///     // each documents.
    ///     //
    ///     // The vec is sorted decreasingly by `sort_by_field`, and has a
    ///     // length of 10, or less if not enough documents matched the
    ///     // query.
    ///     let resulting_docs: Vec<(i64, DocAddress)> =
    ///          searcher.search(query, &top_company_by_revenue)?;
    ///
    ///     Ok(resulting_docs)
    /// }
    /// ```
    pub fn order_by_fast_field<TFastValue>(
        self,
        fast_field: Field,
    ) -> impl Collector<Fruit = Vec<(TFastValue, DocAddress)>>
    where
        TFastValue: FastValue,
    {
        let u64_collector = self.order_by_u64_field(fast_field);
        FastFieldConvertCollector {
            collector: u64_collector,
            field: fast_field,
            fast_value: PhantomData,
        }
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
    /// to alter the original relevance `Score`.
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
    /// # use tantivy::schema::{Schema, FAST, TEXT};
    /// # use tantivy::{doc, Index, DocAddress, DocId, Score};
    /// # use tantivy::query::QueryParser;
    /// use tantivy::SegmentReader;
    /// use tantivy::collector::TopDocs;
    /// use tantivy::schema::Field;
    ///
    /// fn create_schema() -> Schema {
    ///    let mut schema_builder = Schema::builder();
    ///    schema_builder.add_text_field("product_name", TEXT);
    ///    schema_builder.add_u64_field("popularity", FAST);
    ///    schema_builder.build()
    /// }
    ///
    /// fn create_index() -> tantivy::Result<Index> {
    ///   let schema = create_schema();
    ///   let index = Index::create_in_ram(schema);
    ///   let mut index_writer = index.writer_with_num_threads(1, 10_000_000)?;
    ///   let product_name = index.schema().get_field("product_name").unwrap();
    ///   let popularity: Field = index.schema().get_field("popularity").unwrap();
    ///   index_writer.add_document(doc!(product_name => "The Diary of Muadib", popularity => 1u64));
    ///   index_writer.add_document(doc!(product_name => "A Dairy Cow", popularity => 10u64));
    ///   index_writer.add_document(doc!(product_name => "The Diary of a Young Girl", popularity => 15u64));
    ///   index_writer.commit()?;
    ///   Ok(index)
    /// }
    ///
    /// let index = create_index().unwrap();
    /// let product_name = index.schema().get_field("product_name").unwrap();
    /// let popularity: Field = index.schema().get_field("popularity").unwrap();
    ///
    /// let user_query_str = "diary";
    /// let query_parser = QueryParser::for_index(&index, vec![product_name]);
    /// let query = query_parser.parse_query(user_query_str).unwrap();
    ///
    /// // This is where we build our collector with our custom score.
    /// let top_docs_by_custom_score = TopDocs
    ///         ::with_limit(10)
    ///          .tweak_score(move |segment_reader: &SegmentReader| {
    ///             // The argument is a function that returns our scoring
    ///             // function.
    ///             //
    ///             // The point of this "mother" function is to gather all
    ///             // of the segment level information we need for scoring.
    ///             // Typically, fast_fields.
    ///             //
    ///             // In our case, we will get a reader for the popularity
    ///             // fast field.
    ///             let popularity_reader =
    ///                 segment_reader.fast_fields().u64(popularity).unwrap();
    ///
    ///             // We can now define our actual scoring function
    ///             move |doc: DocId, original_score: Score| {
    ///                 let popularity: u64 = popularity_reader.get(doc);
    ///                 // Well.. For the sake of the example we use a simple logarithm
    ///                 // function.
    ///                 let popularity_boost_score = ((2u64 + popularity) as Score).log2();
    ///                 popularity_boost_score * original_score
    ///             }
    ///           });
    /// let reader = index.reader().unwrap();
    /// let searcher = reader.searcher();
    /// // ... and here are our documents. Note this is a simple vec.
    /// // The `Score` in the pair is our tweaked score.
    /// let resulting_docs: Vec<(Score, DocAddress)> =
    ///      searcher.search(&query, &top_docs_by_custom_score).unwrap();
    /// ```
    ///
    /// # See also
    /// [custom_score(...)](#method.custom_score).
    pub fn tweak_score<TScore, TScoreSegmentTweaker, TScoreTweaker>(
        self,
        score_tweaker: TScoreTweaker,
    ) -> impl Collector<Fruit = Vec<(TScore, DocAddress)>>
    where
        TScore: 'static + Send + Sync + Clone + PartialOrd,
        TScoreSegmentTweaker: ScoreSegmentTweaker<TScore> + 'static,
        TScoreTweaker: ScoreTweaker<TScore, Child = TScoreSegmentTweaker> + Send + Sync,
    {
        TweakedScoreTopCollector::new(score_tweaker, self.0.into_tscore())
    }

    /// Ranks the documents using a custom score.
    ///
    /// This method offers a convenient way to use a different score.
    ///
    /// As suggested by the prototype you can manually define your
    /// own [`CustomScorer`](./trait.CustomScorer.html)
    /// and pass it as an argument, but there is a much simpler way to
    /// tweak your score: you can use a closure as in the following
    /// example.
    ///
    /// # Limitation
    ///
    /// This method only makes it possible to compute the score from a given
    /// `DocId`, fastfield values for the doc and any information you could
    /// have precomputed beforehands. It does not make it possible for instance
    /// to compute something like TfIdf as it does not have access to the list of query
    /// terms present in the document, nor the term frequencies for the different terms.
    ///
    /// It can be used if your search engine relies on a learning-to-rank model for instance,
    /// which does not rely on the term frequencies or positions as features.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use tantivy::schema::{Schema, FAST, TEXT};
    /// # use tantivy::{doc, Index, DocAddress, DocId};
    /// # use tantivy::query::QueryParser;
    /// use tantivy::SegmentReader;
    /// use tantivy::collector::TopDocs;
    /// use tantivy::schema::Field;
    ///
    /// # fn create_schema() -> Schema {
    /// #    let mut schema_builder = Schema::builder();
    /// #    schema_builder.add_text_field("product_name", TEXT);
    /// #    schema_builder.add_u64_field("popularity", FAST);
    /// #    schema_builder.add_u64_field("boosted", FAST);
    /// #    schema_builder.build()
    /// # }
    /// #
    /// # fn main() -> tantivy::Result<()> {
    /// #   let schema = create_schema();
    /// #   let index = Index::create_in_ram(schema);
    /// #   let mut index_writer = index.writer_with_num_threads(1, 10_000_000)?;
    /// #   let product_name = index.schema().get_field("product_name").unwrap();
    /// #
    /// let popularity: Field = index.schema().get_field("popularity").unwrap();
    /// let boosted: Field = index.schema().get_field("boosted").unwrap();
    /// #   index_writer.add_document(doc!(boosted=>1u64, product_name => "The Diary of Muadib", popularity => 1u64));
    /// #   index_writer.add_document(doc!(boosted=>0u64, product_name => "A Dairy Cow", popularity => 10u64));
    /// #   index_writer.add_document(doc!(boosted=>0u64, product_name => "The Diary of a Young Girl", popularity => 15u64));
    /// #   index_writer.commit()?;
    /// // ...
    /// # let user_query = "diary";
    /// # let query = QueryParser::for_index(&index, vec![product_name]).parse_query(user_query)?;
    ///
    /// // This is where we build our collector with our custom score.
    /// let top_docs_by_custom_score = TopDocs
    ///         ::with_limit(10)
    ///          .custom_score(move |segment_reader: &SegmentReader| {
    ///             // The argument is a function that returns our scoring
    ///             // function.
    ///             //
    ///             // The point of this "mother" function is to gather all
    ///             // of the segment level information we need for scoring.
    ///             // Typically, fast_fields.
    ///             //
    ///             // In our case, we will get a reader for the popularity
    ///             // fast field and a boosted field.
    ///             //
    ///             // We want to get boosted items score, and when we get
    ///             // a tie, return the item with the highest popularity.
    ///             //
    ///             // Note that this is implemented by using a `(u64, u64)`
    ///             // as a score.
    ///             let popularity_reader =
    ///                 segment_reader.fast_fields().u64(popularity).unwrap();
    ///             let boosted_reader =
    ///                 segment_reader.fast_fields().u64(boosted).unwrap();
    ///
    ///             // We can now define our actual scoring function
    ///             move |doc: DocId| {
    ///                 let popularity: u64 = popularity_reader.get(doc);
    ///                 let boosted: u64 = boosted_reader.get(doc);
    ///                 // Score do not have to be `f64` in tantivy.
    ///                 // Here we return a couple to get lexicographical order
    ///                 // for free.
    ///                 (boosted, popularity)
    ///             }
    ///           });
    /// # let reader = index.reader()?;
    /// # let searcher = reader.searcher();
    /// // ... and here are our documents. Note this is a simple vec.
    /// // The `Score` in the pair is our tweaked score.
    /// let resulting_docs: Vec<((u64, u64), DocAddress)> =
    ///      searcher.search(&*query, &top_docs_by_custom_score)?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # See also
    /// [tweak_score(...)](#method.tweak_score).
    pub fn custom_score<TScore, TCustomSegmentScorer, TCustomScorer>(
        self,
        custom_score: TCustomScorer,
    ) -> impl Collector<Fruit = Vec<(TScore, DocAddress)>>
    where
        TScore: 'static + Send + Sync + Clone + PartialOrd,
        TCustomSegmentScorer: CustomSegmentScorer<TScore> + 'static,
        TCustomScorer: CustomScorer<TScore, Child = TCustomSegmentScorer> + Send + Sync,
    {
        CustomScoreTopCollector::new(custom_score, self.0.into_tscore())
    }
}

impl Collector for TopDocs {
    type Fruit = Vec<(Score, DocAddress)>;

    type Child = TopScoreSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        let collector = self.0.for_segment(segment_local_id, reader);
        Ok(TopScoreSegmentCollector(collector))
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(
        &self,
        child_fruits: Vec<Vec<(Score, DocAddress)>>,
    ) -> crate::Result<Self::Fruit> {
        self.0.merge_fruits(child_fruits)
    }

    fn collect_segment(
        &self,
        weight: &dyn Weight,
        segment_ord: u32,
        reader: &SegmentReader,
    ) -> crate::Result<<Self::Child as SegmentCollector>::Fruit> {
        let heap_len = self.0.limit + self.0.offset;
        let mut heap: BinaryHeap<ComparableDoc<Score, DocId>> = BinaryHeap::with_capacity(heap_len);

        if let Some(delete_bitset) = reader.delete_bitset() {
            let mut threshold = Score::MIN;
            weight.for_each_pruning(threshold, reader, &mut |doc, score| {
                if delete_bitset.is_deleted(doc) {
                    return threshold;
                }
                let heap_item = ComparableDoc {
                    feature: score,
                    doc,
                };
                if heap.len() < heap_len {
                    heap.push(heap_item);
                    if heap.len() == heap_len {
                        threshold = heap.peek().map(|el| el.feature).unwrap_or(Score::MIN);
                    }
                    return threshold;
                }
                *heap.peek_mut().unwrap() = heap_item;
                threshold = heap.peek().map(|el| el.feature).unwrap_or(Score::MIN);
                threshold
            })?;
        } else {
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                let heap_item = ComparableDoc {
                    feature: score,
                    doc,
                };
                if heap.len() < heap_len {
                    heap.push(heap_item);
                    // TODO the threshold is suboptimal for heap.len == heap_len
                    if heap.len() == heap_len {
                        return heap.peek().map(|el| el.feature).unwrap_or(Score::MIN);
                    } else {
                        return Score::MIN;
                    }
                }
                *heap.peek_mut().unwrap() = heap_item;
                heap.peek().map(|el| el.feature).unwrap_or(Score::MIN)
            })?;
        }

        let fruit = heap
            .into_sorted_vec()
            .into_iter()
            .map(|cid| {
                (
                    cid.feature,
                    DocAddress {
                        segment_ord,
                        doc_id: cid.doc,
                    },
                )
            })
            .collect();
        Ok(fruit)
    }
}

/// Segment Collector associated to `TopDocs`.
pub struct TopScoreSegmentCollector(TopSegmentCollector<Score>);

impl SegmentCollector for TopScoreSegmentCollector {
    type Fruit = Vec<(Score, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.0.collect(doc, score);
    }

    fn harvest(self) -> Vec<(Score, DocAddress)> {
        self.0.harvest()
    }
}

#[cfg(test)]
mod tests {
    use super::TopDocs;
    use crate::collector::Collector;
    use crate::query::{AllQuery, Query, QueryParser};
    use crate::schema::{Field, Schema, FAST, STORED, TEXT};
    use crate::Index;
    use crate::IndexWriter;
    use crate::Score;
    use crate::{DocAddress, DocId, SegmentReader};

    fn make_index() -> Index {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            // writing the segment
            let mut index_writer = index.writer_with_num_threads(1, 10_000_000).unwrap();
            index_writer.add_document(doc!(text_field=>"Hello happy tax payer."));
            index_writer.add_document(doc!(text_field=>"Droopy says hello happy tax payer"));
            index_writer.add_document(doc!(text_field=>"I like Droopy"));
            assert!(index_writer.commit().is_ok());
        }
        index
    }

    fn assert_results_equals(results: &[(Score, DocAddress)], expected: &[(Score, DocAddress)]) {
        for (result, expected) in results.iter().zip(expected.iter()) {
            assert_eq!(result.1, expected.1);
            crate::assert_nearly_equals!(result.0, expected.0);
        }
    }

    #[test]
    fn test_top_collector_not_at_capacity_without_offset() {
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
        assert_results_equals(
            &score_docs,
            &[
                (0.81221175, DocAddress::new(0u32, 1)),
                (0.5376842, DocAddress::new(0u32, 2)),
                (0.48527452, DocAddress::new(0, 0)),
            ],
        );
    }

    #[test]
    fn test_top_collector_not_at_capacity_with_offset() {
        let index = make_index();
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax").unwrap();
        let score_docs: Vec<(Score, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(&text_query, &TopDocs::with_limit(4).and_offset(2))
            .unwrap();
        assert_results_equals(&score_docs[..], &[(0.48527452, DocAddress::new(0, 0))]);
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
        assert_results_equals(
            &score_docs,
            &[
                (0.81221175, DocAddress::new(0u32, 1)),
                (0.5376842, DocAddress::new(0u32, 2)),
            ],
        );
    }

    #[test]
    fn test_top_collector_at_capacity_with_offset() {
        let index = make_index();
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax").unwrap();
        let score_docs: Vec<(Score, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(&text_query, &TopDocs::with_limit(2).and_offset(1))
            .unwrap();
        assert_results_equals(
            &score_docs[..],
            &[
                (0.5376842, DocAddress::new(0u32, 2)),
                (0.48527452, DocAddress::new(0, 0)),
            ],
        );
    }

    #[test]
    fn test_top_collector_stable_sorting() {
        let index = make_index();

        // using AllQuery to get a constant score
        let searcher = index.reader().unwrap().searcher();

        let page_1 = searcher.search(&AllQuery, &TopDocs::with_limit(2)).unwrap();

        let page_2 = searcher.search(&AllQuery, &TopDocs::with_limit(3)).unwrap();

        // precondition for the test to be meaningful: we did get documents
        // with the same score
        assert!(page_1.iter().all(|result| result.0 == page_1[0].0));
        assert!(page_2.iter().all(|result| result.0 == page_2[0].0));

        // sanity check since we're relying on make_index()
        assert_eq!(page_1.len(), 2);
        assert_eq!(page_2.len(), 3);

        assert_eq!(page_1, &page_2[..page_1.len()]);
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

        let top_collector = TopDocs::with_limit(4).order_by_u64_field(size);
        let top_docs: Vec<(u64, DocAddress)> = searcher.search(&query, &top_collector).unwrap();
        assert_eq!(
            &top_docs[..],
            &[
                (64, DocAddress::new(0, 1)),
                (16, DocAddress::new(0, 2)),
                (12, DocAddress::new(0, 0))
            ]
        );
    }

    #[test]
    fn test_top_field_collector_datetime() -> crate::Result<()> {
        use std::str::FromStr;
        let mut schema_builder = Schema::builder();
        let name = schema_builder.add_text_field("name", TEXT);
        let birthday = schema_builder.add_date_field("birthday", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        let pr_birthday = crate::DateTime::from_str("1898-04-09T00:00:00+00:00")?;
        index_writer.add_document(doc!(
            name => "Paul Robeson",
            birthday => pr_birthday
        ));
        let mr_birthday = crate::DateTime::from_str("1947-11-08T00:00:00+00:00")?;
        index_writer.add_document(doc!(
            name => "Minnie Riperton",
            birthday => mr_birthday
        ));
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let top_collector = TopDocs::with_limit(3).order_by_fast_field(birthday);
        let top_docs: Vec<(crate::DateTime, DocAddress)> =
            searcher.search(&AllQuery, &top_collector)?;
        assert_eq!(
            &top_docs[..],
            &[
                (mr_birthday, DocAddress::new(0, 1)),
                (pr_birthday, DocAddress::new(0, 0)),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_top_field_collector_i64() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let city = schema_builder.add_text_field("city", TEXT);
        let altitude = schema_builder.add_i64_field("altitude", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(
                city => "georgetown",
                altitude =>  -1i64,
        ));
        index_writer.add_document(doc!(
            city => "tokyo",
            altitude =>  40i64,
        ));
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let top_collector = TopDocs::with_limit(3).order_by_fast_field(altitude);
        let top_docs: Vec<(i64, DocAddress)> = searcher.search(&AllQuery, &top_collector)?;
        assert_eq!(
            &top_docs[..],
            &[
                (40i64, DocAddress::new(0, 1)),
                (-1i64, DocAddress::new(0, 0)),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_top_field_collector_f64() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let city = schema_builder.add_text_field("city", TEXT);
        let altitude = schema_builder.add_f64_field("altitude", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(
                city => "georgetown",
                altitude =>  -1.0f64,
        ));
        index_writer.add_document(doc!(
            city => "tokyo",
            altitude =>  40f64,
        ));
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let top_collector = TopDocs::with_limit(3).order_by_fast_field(altitude);
        let top_docs: Vec<(f64, DocAddress)> = searcher.search(&AllQuery, &top_collector)?;
        assert_eq!(
            &top_docs[..],
            &[
                (40f64, DocAddress::new(0, 1)),
                (-1.0f64, DocAddress::new(0, 0)),
            ]
        );
        Ok(())
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
        let top_collector = TopDocs::with_limit(4).order_by_u64_field(Field::from_field_id(2));
        let segment_reader = searcher.segment_reader(0u32);
        top_collector
            .for_segment(0, segment_reader)
            .expect("should panic");
    }

    #[test]
    fn test_field_not_fast_field() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let size = schema_builder.add_u64_field(SIZE, STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(size=>1u64));
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let segment = searcher.segment_reader(0);
        let top_collector = TopDocs::with_limit(4).order_by_u64_field(size);
        let err = top_collector.for_segment(0, segment).err().unwrap();
        assert!(matches!(err, crate::TantivyError::SchemaError(_)));
        Ok(())
    }

    #[test]
    fn test_field_wrong_type() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let size = schema_builder.add_u64_field(SIZE, STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(size=>1u64));
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let segment = searcher.segment_reader(0);
        let top_collector = TopDocs::with_limit(4).order_by_fast_field::<i64>(size);
        let err = top_collector.for_segment(0, segment).err().unwrap();
        assert!(
            matches!(err, crate::TantivyError::SchemaError(msg) if msg == "Field \"size\" is not a fast field.")
        );
        Ok(())
    }

    #[test]
    fn test_tweak_score_top_collector_with_offset() {
        let index = make_index();
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax").unwrap();
        let collector = TopDocs::with_limit(2).and_offset(1).tweak_score(
            move |_segment_reader: &SegmentReader| move |doc: DocId, _original_score: Score| doc,
        );
        let score_docs: Vec<(u32, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(&text_query, &collector)
            .unwrap();

        assert_eq!(
            score_docs,
            vec![(1, DocAddress::new(0, 1)), (0, DocAddress::new(0, 0)),]
        );
    }

    #[test]
    fn test_custom_score_top_collector_with_offset() {
        let index = make_index();
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax").unwrap();
        let collector = TopDocs::with_limit(2)
            .and_offset(1)
            .custom_score(move |_segment_reader: &SegmentReader| move |doc: DocId| doc);
        let score_docs: Vec<(u32, DocAddress)> = index
            .reader()
            .unwrap()
            .searcher()
            .search(&text_query, &collector)
            .unwrap();

        assert_eq!(
            score_docs,
            vec![(1, DocAddress::new(0, 1)), (0, DocAddress::new(0, 0)),]
        );
    }

    fn index(
        query: &str,
        query_field: Field,
        schema: Schema,
        mut doc_adder: impl FnMut(&mut IndexWriter) -> (),
    ) -> (Index, Box<dyn Query>) {
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 10_000_000).unwrap();
        doc_adder(&mut index_writer);
        index_writer.commit().unwrap();
        let query_parser = QueryParser::for_index(&index, vec![query_field]);
        let query = query_parser.parse_query(query).unwrap();
        (index, query)
    }
}
