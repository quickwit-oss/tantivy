use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use columnar::{ColumnValues, StrColumn};
use serde::{Deserialize, Serialize};

use super::Collector;
use crate::collector::custom_score_top_collector::{
    CustomScoreTopCollector, CustomScoreTopSegmentCollector,
};
use crate::collector::top_collector::{ComparableDoc, TopCollector, TopSegmentCollector};
use crate::collector::tweak_score_top_collector::TweakedScoreTopCollector;
use crate::collector::{
    CustomScorer, CustomSegmentScorer, ScoreSegmentTweaker, ScoreTweaker, SegmentCollector,
};
use crate::fastfield::{FastFieldNotAvailableError, FastValue};
use crate::query::Weight;
use crate::termdict::TermOrdinal;
use crate::{DocAddress, DocId, Order, Score, SegmentOrdinal, SegmentReader, TantivyError};

struct FastFieldConvertCollector<
    TCollector: Collector<Fruit = Vec<(u64, DocAddress)>>,
    TFastValue: FastValue,
> {
    pub collector: TCollector,
    pub field: String,
    pub fast_value: std::marker::PhantomData<TFastValue>,
    order: Order,
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
        let field = schema.get_field(&self.field)?;
        let field_entry = schema.get_field_entry(field);
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
                "Field {:?} is of type {schema_type:?}!={requested_type:?}",
                field_entry.name()
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
            .map(|(score, doc_address)| {
                if self.order.is_desc() {
                    (TFastValue::from_u64(score), doc_address)
                } else {
                    (TFastValue::from_u64(u64::MAX - score), doc_address)
                }
            })
            .collect::<Vec<_>>();
        Ok(transformed_result)
    }
}

struct StringConvertCollector {
    pub collector: CustomScoreTopCollector<ScorerByField, u64>,
    pub field: String,
    order: Order,
    limit: usize,
    offset: usize,
}

impl Collector for StringConvertCollector {
    type Fruit = Vec<(String, DocAddress)>;

    type Child = StringConvertSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: crate::SegmentOrdinal,
        segment: &SegmentReader,
    ) -> crate::Result<Self::Child> {
        let schema = segment.schema();
        let field = schema.get_field(&self.field)?;
        let field_entry = schema.get_field_entry(field);
        if !field_entry.is_fast() {
            return Err(TantivyError::SchemaError(format!(
                "Field {:?} is not a fast field.",
                field_entry.name()
            )));
        }
        let requested_type = crate::schema::Type::Str;
        let schema_type = field_entry.field_type().value_type();
        if schema_type != requested_type {
            return Err(TantivyError::SchemaError(format!(
                "Field {:?} is of type {schema_type:?}!={requested_type:?}",
                field_entry.name()
            )));
        }
        let ff = segment
            .fast_fields()
            .str(&self.field)?
            .expect("ff should be a str field");
        Ok(StringConvertSegmentCollector {
            collector: self.collector.for_segment(segment_local_id, segment)?,
            ff,
            order: self.order.clone(),
        })
    }

    fn requires_scoring(&self) -> bool {
        self.collector.requires_scoring()
    }

    fn merge_fruits(
        &self,
        child_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> crate::Result<Self::Fruit> {
        if self.limit == 0 {
            return Ok(Vec::new());
        }
        if self.order.is_desc() {
            let mut top_collector: TopNComputer<_, _, true> =
                TopNComputer::new(self.limit + self.offset);
            for child_fruit in child_fruits {
                for (feature, doc) in child_fruit {
                    top_collector.push(feature, doc);
                }
            }
            Ok(top_collector
                .into_sorted_vec()
                .into_iter()
                .skip(self.offset)
                .map(|cdoc| (cdoc.feature, cdoc.doc))
                .collect())
        } else {
            let mut top_collector: TopNComputer<_, _, false> =
                TopNComputer::new(self.limit + self.offset);
            for child_fruit in child_fruits {
                for (feature, doc) in child_fruit {
                    top_collector.push(feature, doc);
                }
            }

            Ok(top_collector
                .into_sorted_vec()
                .into_iter()
                .skip(self.offset)
                .map(|cdoc| (cdoc.feature, cdoc.doc))
                .collect())
        }
    }
}

struct StringConvertSegmentCollector {
    pub collector: CustomScoreTopSegmentCollector<ScorerByFastFieldReader, u64>,
    ff: StrColumn,
    order: Order,
}

impl SegmentCollector for StringConvertSegmentCollector {
    type Fruit = Vec<(String, DocAddress)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.collector.collect(doc, score);
    }

    fn harvest(self) -> Vec<(String, DocAddress)> {
        let top_ordinals: Vec<(TermOrdinal, DocAddress)> = self.collector.harvest();

        // Collect terms.
        let mut terms: Vec<String> = Vec::with_capacity(top_ordinals.len());
        let result = if self.order.is_asc() {
            self.ff.dictionary().sorted_ords_to_term_cb(
                top_ordinals.iter().map(|(term_ord, _)| u64::MAX - term_ord),
                |term| {
                    terms.push(
                        std::str::from_utf8(term)
                            .expect("Failed to decode term as unicode")
                            .to_owned(),
                    );
                    Ok(())
                },
            )
        } else {
            self.ff.dictionary().sorted_ords_to_term_cb(
                top_ordinals.iter().rev().map(|(term_ord, _)| *term_ord),
                |term| {
                    terms.push(
                        std::str::from_utf8(term)
                            .expect("Failed to decode term as unicode")
                            .to_owned(),
                    );
                    Ok(())
                },
            )
        };

        assert!(
            result.expect("Failed to read terms from term dictionary"),
            "Not all terms were matched in segment."
        );

        // Zip them back with their docs.
        if self.order.is_asc() {
            terms
                .into_iter()
                .zip(top_ordinals)
                .map(|(term, (_, doc))| (term, doc))
                .collect()
        } else {
            terms
                .into_iter()
                .rev()
                .zip(top_ordinals)
                .map(|(term, (_, doc))| (term, doc))
                .collect()
        }
    }
}

/// The `TopDocs` collector keeps track of the top `K` documents
/// sorted by their score.
///
/// The implementation is based on a repeatedly truncating on the median after K * 2 documents
/// with pattern defeating QuickSort.
/// The theoretical complexity for collecting the top `K` out of `N` documents
/// is `O(N + K)`.
///
/// This collector does not guarantee a stable sorting in case of a tie on the
/// document score, for stable sorting `PartialOrd` needs to resolve on other fields
/// like docid in case of score equality.
/// Only then, it is suitable for pagination.
///
/// ```rust
/// use tantivy::collector::TopDocs;
/// use tantivy::query::QueryParser;
/// use tantivy::schema::{Schema, TEXT};
/// use tantivy::{doc, DocAddress, Index};
///
/// # fn main() -> tantivy::Result<()> {
/// let mut schema_builder = Schema::builder();
/// let title = schema_builder.add_text_field("title", TEXT);
/// let schema = schema_builder.build();
/// let index = Index::create_in_ram(schema);
///
/// let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
/// index_writer.add_document(doc!(title => "The Name of the Wind"))?;
/// index_writer.add_document(doc!(title => "The Diary of Muadib"))?;
/// index_writer.add_document(doc!(title => "A Dairy Cow"))?;
/// index_writer.add_document(doc!(title => "The Diary of a Young Girl"))?;
/// index_writer.commit()?;
///
/// let reader = index.reader()?;
/// let searcher = reader.searcher();
///
/// let query_parser = QueryParser::for_index(&index, vec![title]);
/// let query = query_parser.parse_query("diary")?;
/// let top_docs = searcher.search(&query, &TopDocs::with_limit(2))?;
///
/// assert_eq!(top_docs[0].1, DocAddress::new(0, 1));
/// assert_eq!(top_docs[1].1, DocAddress::new(0, 3));
/// # Ok(())
/// # }
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
    sort_column: Arc<dyn ColumnValues<u64>>,
    order: Order,
}

impl CustomSegmentScorer<u64> for ScorerByFastFieldReader {
    fn score(&mut self, doc: DocId) -> u64 {
        let value = self.sort_column.get_val(doc);
        if self.order.is_desc() {
            value
        } else {
            u64::MAX - value
        }
    }
}

struct ScorerByField {
    field: String,
    order: Order,
}

impl CustomScorer<u64> for ScorerByField {
    type Child = ScorerByFastFieldReader;

    fn segment_scorer(&self, segment_reader: &SegmentReader) -> crate::Result<Self::Child> {
        // We interpret this field as u64, regardless of its type, that way,
        // we avoid needless conversion. Regardless of the fast field type, the
        // mapping is monotonic, so it is sufficient to compute our top-K docs.
        //
        // The conversion will then happen only on the top-K docs.
        let sort_column_opt = segment_reader.fast_fields().u64_lenient(&self.field)?;
        let (sort_column, _sort_column_type) =
            sort_column_opt.ok_or_else(|| FastFieldNotAvailableError {
                field_name: self.field.clone(),
            })?;
        let mut default_value = 0u64;
        if self.order.is_asc() {
            default_value = u64::MAX;
        }
        Ok(ScorerByFastFieldReader {
            sort_column: sort_column.first_or_default_col(default_value),
            order: self.order.clone(),
        })
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
    /// # fn main() -> tantivy::Result<()> {
    /// let mut schema_builder = Schema::builder();
    /// let title = schema_builder.add_text_field("title", TEXT);
    /// let schema = schema_builder.build();
    /// let index = Index::create_in_ram(schema);
    ///
    /// let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
    /// index_writer.add_document(doc!(title => "The Name of the Wind"))?;
    /// index_writer.add_document(doc!(title => "The Diary of Muadib"))?;
    /// index_writer.add_document(doc!(title => "A Dairy Cow"))?;
    /// index_writer.add_document(doc!(title => "The Diary of a Young Girl"))?;
    /// index_writer.add_document(doc!(title => "The Diary of Lena Mukhina"))?;
    /// index_writer.commit()?;
    ///
    /// let reader = index.reader()?;
    /// let searcher = reader.searcher();
    ///
    /// let query_parser = QueryParser::for_index(&index, vec![title]);
    /// let query = query_parser.parse_query("diary")?;
    /// let top_docs = searcher.search(&query, &TopDocs::with_limit(2).and_offset(1))?;
    ///
    /// assert_eq!(top_docs.len(), 2);
    /// assert_eq!(top_docs[0].1, DocAddress::new(0, 4));
    /// assert_eq!(top_docs[1].1, DocAddress::new(0, 3));
    /// Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn and_offset(self, offset: usize) -> TopDocs {
        TopDocs(self.0.and_offset(offset))
    }

    /// Set top-K to rank documents by a given fast field.
    ///
    /// If the field is not a fast or does not exist, this method returns successfully (it is not
    /// aware of any schema). An error will be returned at the moment of search.
    ///
    /// If the field is a FAST field but not a u64 field, search will return successfully but it
    /// will return returns a monotonic u64-representation (ie. the order is still correct) of
    /// the requested field type.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use tantivy::schema::{Schema, FAST, TEXT};
    /// # use tantivy::{doc, Index, DocAddress, Order};
    /// # use tantivy::query::{Query, QueryParser};
    /// use tantivy::Searcher;
    /// use tantivy::collector::TopDocs;
    ///
    /// # fn main() -> tantivy::Result<()> {
    /// #   let mut schema_builder = Schema::builder();
    /// #   let title = schema_builder.add_text_field("title", TEXT);
    /// #   let rating = schema_builder.add_u64_field("rating", FAST);
    /// #   let schema = schema_builder.build();
    /// #
    /// #   let index = Index::create_in_ram(schema);
    /// #   let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
    /// #   index_writer.add_document(doc!(title => "The Name of the Wind", rating => 92u64))?;
    /// #   index_writer.add_document(doc!(title => "The Diary of Muadib", rating => 97u64))?;
    /// #   index_writer.add_document(doc!(title => "A Dairy Cow", rating => 63u64))?;
    /// #   index_writer.add_document(doc!(title => "The Diary of a Young Girl", rating => 80u64))?;
    /// #   index_writer.commit()?;
    /// #   let reader = index.reader()?;
    /// #   let query = QueryParser::for_index(&index, vec![title]).parse_query("diary")?;
    /// #   let top_docs = docs_sorted_by_rating(&reader.searcher(), &query)?;
    /// #   assert_eq!(top_docs,
    /// #            vec![(97u64, DocAddress::new(0u32, 1)),
    /// #                 (80u64, DocAddress::new(0u32, 3))]);
    /// #   Ok(())
    /// # }
    /// /// Searches the document matching the given query, and
    /// /// collects the top 10 documents, order by the u64-`field`
    /// /// given in argument.
    /// fn docs_sorted_by_rating(searcher: &Searcher,
    ///                          query: &dyn Query)
    ///     -> tantivy::Result<Vec<(u64, DocAddress)>> {
    ///
    ///     // This is where we build our topdocs collector
    ///     //
    ///     // Note the `rating_field` needs to be a FAST field here.
    ///     let top_books_by_rating = TopDocs
    ///                 ::with_limit(10)
    ///                  .order_by_fast_field("rating", Order::Desc);
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
    /// To comfortably work with `u64`s, `i64`s, `f64`s, or `date`s, please refer to
    /// the [.order_by_fast_field(...)](TopDocs::order_by_fast_field) method.
    pub fn order_by_u64_field(
        self,
        field: impl ToString,
        order: Order,
    ) -> impl Collector<Fruit = Vec<(u64, DocAddress)>> {
        CustomScoreTopCollector::new(
            ScorerByField {
                field: field.to_string(),
                order,
            },
            self.0.into_tscore(),
        )
    }

    /// Set top-K to rank documents by a given fast field.
    ///
    /// If the field is not a fast field, or its field type does not match the generic type, this
    /// method does not panic, but an explicit error will be returned at the moment of
    /// collection.
    ///
    /// Note that this method is a generic. The requested fast field type will be often
    /// inferred in your code by the rust compiler.
    ///
    /// Implementation-wise, for performance reason, tantivy will manipulate the u64 representation
    /// of your fast field until the last moment.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use tantivy::schema::{Schema, FAST, TEXT};
    /// # use tantivy::{doc, Index, DocAddress,Order};
    /// # use tantivy::query::{Query, AllQuery};
    /// use tantivy::Searcher;
    /// use tantivy::collector::TopDocs;
    ///
    /// # fn main() -> tantivy::Result<()> {
    /// #   let mut schema_builder = Schema::builder();
    /// #   let title = schema_builder.add_text_field("company", TEXT);
    /// #   let revenue = schema_builder.add_i64_field("revenue", FAST);
    /// #   let schema = schema_builder.build();
    /// #
    /// #   let index = Index::create_in_ram(schema);
    /// #   let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
    /// #   index_writer.add_document(doc!(title => "MadCow Inc.", revenue => 92_000_000i64))?;
    /// #   index_writer.add_document(doc!(title => "Zozo Cow KKK", revenue => 119_000_000i64))?;
    /// #   index_writer.add_document(doc!(title => "Declining Cow", revenue => -63_000_000i64))?;
    /// #   assert!(index_writer.commit().is_ok());
    /// #   let reader = index.reader()?;
    /// #   let top_docs = docs_sorted_by_revenue(&reader.searcher(), &AllQuery, "revenue")?;
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
    ///                          revenue_field: &str)
    ///     -> tantivy::Result<Vec<(i64, DocAddress)>> {
    ///
    ///     // This is where we build our topdocs collector
    ///     //
    ///     // Note the generics parameter that needs to match the
    ///     // type `sort_by_field`. revenue_field here is a FAST i64 field.
    ///     let top_company_by_revenue = TopDocs
    ///                 ::with_limit(2)
    ///                  .order_by_fast_field("revenue", Order::Desc);
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
        fast_field: impl ToString,
        order: Order,
    ) -> impl Collector<Fruit = Vec<(TFastValue, DocAddress)>>
    where
        TFastValue: FastValue,
    {
        let u64_collector = self.order_by_u64_field(fast_field.to_string(), order.clone());
        FastFieldConvertCollector {
            collector: u64_collector,
            field: fast_field.to_string(),
            fast_value: PhantomData,
            order,
        }
    }

    /// Like `order_by_fast_field`, but for a `String` fast field.
    pub fn order_by_string_fast_field(
        self,
        fast_field: impl ToString,
        order: Order,
    ) -> impl Collector<Fruit = Vec<(String, DocAddress)>> {
        let limit = self.0.limit;
        let offset = self.0.offset;
        let u64_collector = CustomScoreTopCollector::new(
            ScorerByField {
                field: fast_field.to_string(),
                order: order.clone(),
            },
            self.0.into_tscore(),
        );
        StringConvertCollector {
            collector: u64_collector,
            field: fast_field.to_string(),
            order,
            limit,
            offset,
        }
    }

    /// Ranks the documents using a custom score.
    ///
    /// This method offers a convenient way to tweak or replace
    /// the documents score. As suggested by the prototype you can
    /// manually define your own [`ScoreTweaker`]
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
    /// In more serious application, this tweaking could involve running a
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
    ///   let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
    ///   let product_name = index.schema().get_field("product_name").unwrap();
    ///   let popularity: Field = index.schema().get_field("popularity").unwrap();
    ///   index_writer.add_document(doc!(product_name => "The Diary of Muadib", popularity => 1u64))?;
    ///   index_writer.add_document(doc!(product_name => "A Dairy Cow", popularity => 10u64))?;
    ///   index_writer.add_document(doc!(product_name => "The Diary of a Young Girl", popularity => 15u64))?;
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
    ///             // fast field. For simplicity we read the first or default value in the fast
    ///             // field.
    ///             let popularity_reader =
    ///                 segment_reader.fast_fields().u64("popularity").unwrap().first_or_default_col(0);
    ///
    ///             // We can now define our actual scoring function
    ///             move |doc: DocId, original_score: Score| {
    ///                 let popularity: u64 = popularity_reader.get_val(doc);
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
    /// - [custom_score(...)](TopDocs::custom_score)
    pub fn tweak_score<TScoreTweaker, TScore>(
        self,
        score_tweaker: TScoreTweaker,
    ) -> impl Collector<Fruit = Vec<(TScore, DocAddress)>>
    where
        TScore: 'static + Clone + Send + Sync + PartialOrd,
        TScoreTweaker: ScoreTweaker<Score=TScore> + Send + Sync,
    {
        TweakedScoreTopCollector::new(score_tweaker, self.0.into_tscore())
    }

    /// Ranks the documents using a custom score.
    ///
    /// This method offers a convenient way to use a different score.
    ///
    /// As suggested by the prototype you can manually define your own [`CustomScorer`]
    /// and pass it as an argument, but there is a much simpler way to
    /// tweak your score: you can use a closure as in the following
    /// example.
    ///
    /// # Limitation
    ///
    /// This method only makes it possible to compute the score from a given
    /// `DocId`, fastfield values for the doc and any information you could
    /// have precomputed beforehand. It does not make it possible for instance
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
    /// #   let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
    /// #   let product_name = index.schema().get_field("product_name").unwrap();
    /// #
    /// let popularity: Field = index.schema().get_field("popularity").unwrap();
    /// let boosted: Field = index.schema().get_field("boosted").unwrap();
    /// #   index_writer.add_document(doc!(boosted=>1u64, product_name => "The Diary of Muadib", popularity => 1u64))?;
    /// #   index_writer.add_document(doc!(boosted=>0u64, product_name => "A Dairy Cow", popularity => 10u64))?;
    /// #   index_writer.add_document(doc!(boosted=>0u64, product_name => "The Diary of a Young Girl", popularity => 15u64))?;
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
    ///                 segment_reader.fast_fields().u64("popularity").unwrap().first_or_default_col(0);
    ///             let boosted_reader =
    ///                 segment_reader.fast_fields().u64("boosted").unwrap().first_or_default_col(0);
    ///
    ///             // We can now define our actual scoring function
    ///             move |doc: DocId| {
    ///                 let popularity: u64 = popularity_reader.get_val(doc);
    ///                 let boosted: u64 = boosted_reader.get_val(doc);
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
    /// - [tweak_score(...)](TopDocs::tweak_score)
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
        let mut top_n: TopNComputer<_, _> = TopNComputer::new(heap_len);

        if let Some(alive_bitset) = reader.alive_bitset() {
            let mut threshold = Score::MIN;
            top_n.threshold = Some(threshold);
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                if alive_bitset.is_deleted(doc) {
                    return threshold;
                }
                top_n.push(score, doc);
                threshold = top_n.threshold.unwrap_or(Score::MIN);
                threshold
            })?;
        } else {
            weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
                top_n.push(score, doc);
                top_n.threshold.unwrap_or(Score::MIN)
            })?;
        }

        let fruit = top_n
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

/// Segment Collector associated with `TopDocs`.
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

/// Fast TopN Computation
///
/// Capacity of the vec is 2 * top_n.
/// The buffer is truncated to the top_n elements when it reaches the capacity of the Vec.
/// That means capacity has special meaning and should be carried over when cloning or serializing.
///
/// For TopN == 0, it will be relative expensive.
#[derive(Serialize, Deserialize)]
#[serde(from = "TopNComputerDeser<Score, D, REVERSE_ORDER>")]
pub struct TopNComputer<Score, D, const REVERSE_ORDER: bool = true> {
    /// The buffer reverses sort order to get top-semantics instead of bottom-semantics
    buffer: Vec<ComparableDoc<Score, D, REVERSE_ORDER>>,
    top_n: usize,
    pub(crate) threshold: Option<Score>,
}

impl<Score: std::fmt::Debug, D, const REVERSE_ORDER: bool> std::fmt::Debug
    for TopNComputer<Score, D, REVERSE_ORDER>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopNComputer")
            .field("buffer_len", &self.buffer.len())
            .field("top_n", &self.top_n)
            .field("current_threshold", &self.threshold)
            .finish()
    }
}

// Intermediate struct for TopNComputer for deserialization, to keep vec capacity
#[derive(Deserialize)]
struct TopNComputerDeser<Score, D, const REVERSE_ORDER: bool> {
    buffer: Vec<ComparableDoc<Score, D, REVERSE_ORDER>>,
    top_n: usize,
    threshold: Option<Score>,
}

// Custom clone to keep capacity
impl<Score: Clone, D: Clone, const REVERSE_ORDER: bool> Clone
    for TopNComputer<Score, D, REVERSE_ORDER>
{
    fn clone(&self) -> Self {
        let mut buffer_clone = Vec::with_capacity(self.buffer.capacity());
        buffer_clone.extend(self.buffer.iter().cloned());

        TopNComputer {
            buffer: buffer_clone,
            top_n: self.top_n,
            threshold: self.threshold.clone(),
        }
    }
}

impl<Score, D, const R: bool> From<TopNComputerDeser<Score, D, R>> for TopNComputer<Score, D, R> {
    fn from(mut value: TopNComputerDeser<Score, D, R>) -> Self {
        let expected_cap = value.top_n.max(1) * 2;
        let current_cap = value.buffer.capacity();
        if current_cap < expected_cap {
            value.buffer.reserve_exact(expected_cap - current_cap);
        } else {
            value.buffer.shrink_to(expected_cap);
        }

        TopNComputer {
            buffer: value.buffer,
            top_n: value.top_n,
            threshold: value.threshold,
        }
    }
}

impl<Score, D, const REVERSE_ORDER: bool> TopNComputer<Score, D, REVERSE_ORDER>
where
    Score: PartialOrd + Clone,
    D: Ord,
{
    /// Create a new `TopNComputer`.
    /// Internally it will allocate a buffer of size `2 * top_n`.
    pub fn new(top_n: usize) -> Self {
        let vec_cap = top_n.max(1) * 2;
        TopNComputer {
            buffer: Vec::with_capacity(vec_cap),
            top_n,
            threshold: None,
        }
    }

    /// Push a new document to the top n.
    /// If the document is below the current threshold, it will be ignored.
    #[inline]
    pub fn push(&mut self, feature: Score, doc: D) {
        if let Some(last_median) = self.threshold.clone() {
            if !REVERSE_ORDER && feature > last_median {
                return;
            }
            if REVERSE_ORDER && feature < last_median {
                return;
            }
        }
        if self.buffer.len() == self.buffer.capacity() {
            let median = self.truncate_top_n();
            self.threshold = Some(median);
        }

        // This cannot panic, because we truncate_median will at least remove one element, since
        // the min capacity is 2.
        self.append_within_capacity(ComparableDoc { doc, feature });
    }

    // ONLY CALL THIS FUNCTION WHEN YOU KNOW THE BUFFER HAS ENOUGH CAPACITY.
    #[inline(always)]
    fn append_within_capacity(&mut self, comparable_doc: ComparableDoc<Score, D, REVERSE_ORDER>) {
        push_within_capacity(comparable_doc, &mut self.buffer);
    }

    #[inline(never)]
    fn truncate_top_n(&mut self) -> Score {
        // Use select_nth_unstable to find the top nth score
        let (_, median_el, _) = self.buffer.select_nth_unstable(self.top_n);

        let median_score = median_el.feature.clone();
        // Remove all elements below the top_n
        self.buffer.truncate(self.top_n);

        median_score
    }

    /// Returns the top n elements in sorted order.
    pub fn into_sorted_vec(mut self) -> Vec<ComparableDoc<Score, D, REVERSE_ORDER>> {
        if self.buffer.len() > self.top_n {
            self.truncate_top_n();
        }
        self.buffer.sort_unstable();
        self.buffer
    }

    /// Returns the top n elements in stored order.
    /// Useful if you do not need the elements in sorted order,
    /// for example when merging the results of multiple segments.
    pub fn into_vec(mut self) -> Vec<ComparableDoc<Score, D, REVERSE_ORDER>> {
        if self.buffer.len() > self.top_n {
            self.truncate_top_n();
        }
        self.buffer
    }
}

impl<TScore, const REVERSE_ORDER: bool> TopNComputer<TScore, DocId, REVERSE_ORDER>
where TScore: PartialOrd + Clone
{
    #[inline(always)]
    pub(crate) fn push_lazy(
        &mut self,
        doc: DocId,
        score: Score,
        score_tweaker: &mut impl ScoreSegmentTweaker<SegmentScore = TScore>,
    ) {
        if let Some(threshold) = self.threshold.as_ref() {
            if let Some((_cmp, feature)) =
                score_tweaker.accept_score_lazy::<REVERSE_ORDER>(doc, score, threshold)
            {
                self.append_within_capacity(ComparableDoc { feature, doc });
            }
        } else {
            let feature = score_tweaker.score(doc, score);
            self.append_within_capacity(ComparableDoc { feature, doc });
        }
    }
}

// Push an element provided there is enough capacity to do so.
// TODO replace me when push_within_capacity is stabilized.
#[inline(always)]
fn push_within_capacity<T>(el: T, buf: &mut Vec<T>) {
    let prev_len = buf.len();
    if prev_len == buf.capacity() {
        return;
    }
    // This is mimicking the current (non-stabilized) implementation in std.
    // SAFETY: we just checked we have enough capacity.
    unsafe {
        let end = buf.as_mut_ptr().add(prev_len);
        std::ptr::write(end, el);
        buf.set_len(prev_len + 1);
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{TopDocs, TopNComputer};
    use crate::collector::top_collector::ComparableDoc;
    use crate::collector::{Collector, DocSetCollector};
    use crate::query::{AllQuery, Query, QueryParser};
    use crate::schema::{Field, Schema, FAST, STORED, TEXT};
    use crate::time::format_description::well_known::Rfc3339;
    use crate::time::OffsetDateTime;
    use crate::{
        assert_nearly_equals, DateTime, DocAddress, DocId, Index, IndexWriter, Order, Score,
        SegmentReader,
    };

    fn make_index() -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        // writing the segment
        let mut index_writer = index.writer_with_num_threads(1, 20_000_000)?;
        index_writer.add_document(doc!(text_field=>"Hello happy tax payer."))?;
        index_writer.add_document(doc!(text_field=>"Droopy says hello happy tax payer"))?;
        index_writer.add_document(doc!(text_field=>"I like Droopy"))?;
        index_writer.commit()?;
        Ok(index)
    }

    fn assert_results_equals(results: &[(Score, DocAddress)], expected: &[(Score, DocAddress)]) {
        for (result, expected) in results.iter().zip(expected.iter()) {
            assert_eq!(result.1, expected.1);
            crate::assert_nearly_equals!(result.0, expected.0);
        }
    }
    #[test]
    fn test_topn_computer_serde() {
        let computer: TopNComputer<u32, u32> = TopNComputer::new(1);

        let computer_ser = serde_json::to_string(&computer).unwrap();
        let mut computer: TopNComputer<u32, u32> = serde_json::from_str(&computer_ser).unwrap();

        computer.push(1u32, 5u32);
        computer.push(1u32, 0u32);
        computer.push(1u32, 7u32);

        assert_eq!(
            computer.into_sorted_vec(),
            &[ComparableDoc {
                feature: 1u32,
                doc: 0u32,
            },]
        );
    }

    #[test]
    fn test_empty_topn_computer() {
        let mut computer: TopNComputer<u32, u32> = TopNComputer::new(0);

        computer.push(1u32, 1u32);
        computer.push(1u32, 2u32);
        computer.push(1u32, 3u32);
        assert!(computer.into_sorted_vec().is_empty());
    }
    #[test]
    fn test_topn_computer() {
        let mut computer: TopNComputer<u32, u32> = TopNComputer::new(2);

        computer.push(1u32, 1u32);
        computer.push(2u32, 2u32);
        computer.push(3u32, 3u32);
        computer.push(2u32, 4u32);
        computer.push(1u32, 5u32);
        assert_eq!(
            computer.into_sorted_vec(),
            &[
                ComparableDoc {
                    feature: 3u32,
                    doc: 3u32,
                },
                ComparableDoc {
                    feature: 2u32,
                    doc: 2u32,
                }
            ]
        );
    }

    #[test]
    fn test_topn_computer_no_panic() {
        for top_n in 0..10 {
            let mut computer: TopNComputer<u32, u32> = TopNComputer::new(top_n);

            for _ in 0..1 + top_n * 2 {
                computer.push(1u32, 1u32);
            }
            let _vals = computer.into_sorted_vec();
        }
    }

    proptest! {
        #[test]
        fn test_topn_computer_asc_prop(
          limit in 0..10_usize,
          docs in proptest::collection::vec((0..100_u64, 0..100_u64), 0..100_usize),
        ) {
            let mut computer: TopNComputer<_, _, false> = TopNComputer::new(limit);
            for (feature, doc) in &docs {
                computer.push(*feature, *doc);
            }
            let mut comparable_docs = docs.into_iter().map(|(feature, doc)| ComparableDoc { feature, doc }).collect::<Vec<_>>();
            comparable_docs.sort();
            comparable_docs.truncate(limit);
            prop_assert_eq!(
                computer.into_sorted_vec(),
                comparable_docs,
            );
        }

        #[test]
        fn test_topn_computer_desc_prop(
          limit in 0..10_usize,
          docs in proptest::collection::vec((0..100_u64, 0..100_u64), 0..100_usize),
        ) {
            let mut computer: TopNComputer<_, _, true> = TopNComputer::new(limit);
            for (feature, doc) in &docs {
                computer.push(*feature, *doc);
            }
            let mut comparable_docs = docs.into_iter().map(|(feature, doc)| ComparableDoc { feature, doc }).collect::<Vec<_>>();
            comparable_docs.sort();
            comparable_docs.truncate(limit);
            prop_assert_eq!(
                computer.into_sorted_vec(),
                comparable_docs,
            );
        }
    }

    #[test]
    fn test_top_collector_not_at_capacity_without_offset() -> crate::Result<()> {
        let index = make_index()?;
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax")?;
        let score_docs: Vec<(Score, DocAddress)> = index
            .reader()?
            .searcher()
            .search(&text_query, &TopDocs::with_limit(4))?;
        assert_results_equals(
            &score_docs,
            &[
                (0.81221175, DocAddress::new(0u32, 1)),
                (0.5376842, DocAddress::new(0u32, 2)),
                (0.48527452, DocAddress::new(0, 0)),
            ],
        );
        Ok(())
    }

    #[test]
    fn test_top_collector_not_at_capacity_with_offset() {
        let index = make_index().unwrap();
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
        let index = make_index().unwrap();
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
        let index = make_index().unwrap();
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
        let index = make_index().unwrap();

        // using AllQuery to get a constant score
        let searcher = index.reader().unwrap().searcher();

        let page_0 = searcher.search(&AllQuery, &TopDocs::with_limit(1)).unwrap();

        let page_1 = searcher.search(&AllQuery, &TopDocs::with_limit(2)).unwrap();

        let page_2 = searcher.search(&AllQuery, &TopDocs::with_limit(3)).unwrap();

        // precondition for the test to be meaningful: we did get documents
        // with the same score
        assert!(page_0.iter().all(|result| result.0 == page_1[0].0));
        assert!(page_1.iter().all(|result| result.0 == page_1[0].0));
        assert!(page_2.iter().all(|result| result.0 == page_2[0].0));

        // sanity check since we're relying on make_index()
        assert_eq!(page_0.len(), 1);
        assert_eq!(page_1.len(), 2);
        assert_eq!(page_2.len(), 3);

        assert_eq!(page_1, &page_2[..page_1.len()]);
        assert_eq!(page_0, &page_2[..page_0.len()]);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20))]
        /// Build multiple segments with equal-scoring docs and verify stable ordering
        /// across pages when increasing limit or offset.
        #[test]
        fn proptest_stable_ordering_across_segments_with_pagination(
            docs_per_segment in proptest::collection::vec(1usize..50, 2..5)
        ) {
            use crate::indexer::NoMergePolicy;

            // Build an index with multiple segments; all docs will have the same score using AllQuery.
            let mut schema_builder = Schema::builder();
            let text = schema_builder.add_text_field("text", TEXT);
            let schema = schema_builder.build();
            let index = Index::create_in_ram(schema);
            let mut writer = index.writer_for_tests().unwrap();
            writer.set_merge_policy(Box::new(NoMergePolicy));

            for num_docs in &docs_per_segment {
                for _ in 0..*num_docs {
                    writer.add_document(doc!(text => "x")).unwrap();
                }
                writer.commit().unwrap();
            }

            let reader = index.reader().unwrap();
            let searcher = reader.searcher();

            let total_docs: usize = docs_per_segment.iter().sum();
            // Full result set, first assert all scores are identical.
            let full_with_scores: Vec<(Score, DocAddress)> = searcher
                .search(&AllQuery, &TopDocs::with_limit(total_docs))
                .unwrap();
            // Sanity: at least one document was returned.
            prop_assert!(!full_with_scores.is_empty());
            let first_score = full_with_scores[0].0;
            prop_assert!(full_with_scores.iter().all(|(score, _)| *score == first_score));

            // Keep only the addresses for the remaining checks.
            let full: Vec<DocAddress> = full_with_scores
                .into_iter()
                .map(|(_score, addr)| addr)
                .collect();

            // Sanity: we actually created multiple segments and have documents.
            prop_assert!(docs_per_segment.len() >= 2);
            prop_assert!(total_docs >= 2);

            // 1) Increasing limit should preserve prefix ordering.
            for k in 1..=total_docs {
                let page: Vec<DocAddress> = searcher
                    .search(&AllQuery, &TopDocs::with_limit(k))
                    .unwrap()
                    .into_iter()
                    .map(|(_score, addr)| addr)
                    .collect();
                prop_assert_eq!(page, full[..k].to_vec());
            }

            // 2) Offset + limit pages should always match the corresponding slice.
            //    For each offset, check three representative page sizes:
            //    - first page (size 1)
            //    - a middle page (roughly half of remaining)
            //    - the last page (size = remaining)
            for offset in 0..total_docs {
                let remaining = total_docs - offset;

                let assert_page_eq = |limit: usize| -> proptest::test_runner::TestCaseResult {
                    let page: Vec<DocAddress> = searcher
                        .search(&AllQuery, &TopDocs::with_limit(limit).and_offset(offset))
                        .unwrap()
                        .into_iter()
                        .map(|(_score, addr)| addr)
                        .collect();
                    prop_assert_eq!(page, full[offset..offset + limit].to_vec());
                    Ok(())
                };

                // Smallest page.
                assert_page_eq(1)?;
                // A middle-sized page (dedupes to 1 if remaining == 1).
                assert_page_eq((remaining / 2).max(1))?;
                // Largest page for this offset.
                assert_page_eq(remaining)?;
            }

            // 3) Concatenating fixed-size pages by offset reproduces the full order.
            for page_size in 1..=total_docs.min(5) {
                let mut concat: Vec<DocAddress> = Vec::new();
                let mut offset = 0;
                while offset < total_docs {
                    let size = page_size.min(total_docs - offset);
                    let page: Vec<DocAddress> = searcher
                        .search(&AllQuery, &TopDocs::with_limit(size).and_offset(offset))
                        .unwrap()
                        .into_iter()
                        .map(|(_score, addr)| addr)
                        .collect();
                    concat.extend(page);
                    offset += size;
                }
                // Avoid moving `full` across loop iterations.
                prop_assert_eq!(concat, full.clone());
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(20))]
        /// Build multiple segments with same-scoring term matches and verify stable ordering
        /// across pages for a real scoring query (TermQuery with identical TF and fieldnorm).
        #[test]
        fn proptest_stable_ordering_across_segments_with_term_query_and_pagination(
            docs_per_segment in proptest::collection::vec(1usize..50, 2..5)
        ) {
            use crate::indexer::NoMergePolicy;
            use crate::schema::IndexRecordOption;
            use crate::query::TermQuery;
            use crate::Term;

            // Build an index with multiple segments; each doc has exactly one token "x",
            // ensuring equal BM25 scores across all matching docs (same TF=1 and fieldnorm=1).
            let mut schema_builder = Schema::builder();
            let text = schema_builder.add_text_field("text", TEXT);
            let schema = schema_builder.build();
            let index = Index::create_in_ram(schema);
            let mut writer = index.writer_for_tests().unwrap();
            writer.set_merge_policy(Box::new(NoMergePolicy));

            for num_docs in &docs_per_segment {
                for _ in 0..*num_docs {
                    writer.add_document(doc!(text => "x")).unwrap();
                }
                writer.commit().unwrap();
            }

            let reader = index.reader().unwrap();
            let searcher = reader.searcher();

            let total_docs: usize = docs_per_segment.iter().sum();
            let term = Term::from_field_text(text, "x");
            let tq = TermQuery::new(term, IndexRecordOption::WithFreqs);

            // Full result set, first assert all scores are identical across docs.
            let full_with_scores: Vec<(Score, DocAddress)> = searcher
                .search(&tq, &TopDocs::with_limit(total_docs))
                .unwrap();
            // Sanity: at least one document was returned.
            prop_assert!(!full_with_scores.is_empty());
            let first_score = full_with_scores[0].0;
            prop_assert!(full_with_scores.iter().all(|(score, _)| *score == first_score));

            // Keep only the addresses for the remaining checks.
            let full: Vec<DocAddress> = full_with_scores
                .into_iter()
                .map(|(_score, addr)| addr)
                .collect();

            // Sanity: we actually created multiple segments and have documents.
            prop_assert!(docs_per_segment.len() >= 2);
            prop_assert!(total_docs >= 2);

            // 1) Increasing limit should preserve prefix ordering.
            for k in 1..=total_docs {
                let page: Vec<DocAddress> = searcher
                    .search(&tq, &TopDocs::with_limit(k))
                    .unwrap()
                    .into_iter()
                    .map(|(_score, addr)| addr)
                    .collect();
                prop_assert_eq!(page, full[..k].to_vec());
            }

            // 2) Offset + limit pages should always match the corresponding slice.
            //    Check three representative page sizes for each offset: 1, ~half, and remaining.
            for offset in 0..total_docs {
                let remaining = total_docs - offset;

                let assert_page_eq = |limit: usize| -> proptest::test_runner::TestCaseResult {
                    let page: Vec<DocAddress> = searcher
                        .search(&tq, &TopDocs::with_limit(limit).and_offset(offset))
                        .unwrap()
                        .into_iter()
                        .map(|(_score, addr)| addr)
                        .collect();
                    prop_assert_eq!(page, full[offset..offset + limit].to_vec());
                    Ok(())
                };

                assert_page_eq(1)?;
                assert_page_eq((remaining / 2).max(1))?;
                assert_page_eq(remaining)?;
            }

            // 3) Concatenating fixed-size pages by offset reproduces the full order.
            for page_size in 1..=total_docs.min(5) {
                let mut concat: Vec<DocAddress> = Vec::new();
                let mut offset = 0;
                while offset < total_docs {
                    let size = page_size.min(total_docs - offset);
                    let page: Vec<DocAddress> = searcher
                        .search(&tq, &TopDocs::with_limit(size).and_offset(offset))
                        .unwrap()
                        .into_iter()
                        .map(|(_score, addr)| addr)
                        .collect();
                    concat.extend(page);
                    offset += size;
                }
                prop_assert_eq!(concat, full.clone());
            }
        }
    }

    #[test]
    #[should_panic]
    fn test_top_0() {
        TopDocs::with_limit(0);
    }

    const TITLE: &str = "title";
    const SIZE: &str = "size";

    #[test]
    fn test_top_field_collector_not_at_capacity() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field(TITLE, TEXT);
        let size = schema_builder.add_u64_field(SIZE, FAST);
        let schema = schema_builder.build();
        let (index, query) = index("beer", title, schema, |index_writer| {
            index_writer
                .add_document(doc!(
                    title => "bottle of beer",
                    size => 12u64,
                ))
                .unwrap();
            index_writer
                .add_document(doc!(
                    title => "growler of beer",
                    size => 64u64,
                ))
                .unwrap();
            index_writer
                .add_document(doc!(
                    title => "pint of beer",
                    size => 16u64,
                ))
                .unwrap();
        });
        let searcher = index.reader()?.searcher();

        let top_collector = TopDocs::with_limit(4).order_by_u64_field(SIZE, Order::Desc);
        let top_docs: Vec<(u64, DocAddress)> = searcher.search(&query, &top_collector)?;
        assert_eq!(
            &top_docs[..],
            &[
                (64, DocAddress::new(0, 1)),
                (16, DocAddress::new(0, 2)),
                (12, DocAddress::new(0, 0))
            ]
        );
        Ok(())
    }

    #[test]
    fn test_top_field_collector_datetime() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let name = schema_builder.add_text_field("name", TEXT);
        let birthday = schema_builder.add_date_field("birthday", FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        let pr_birthday = DateTime::from_utc(OffsetDateTime::parse(
            "1898-04-09T00:00:00+00:00",
            &Rfc3339,
        )?);
        index_writer.add_document(doc!(
            name => "Paul Robeson",
            birthday => pr_birthday,
        ))?;
        let mr_birthday = DateTime::from_utc(OffsetDateTime::parse(
            "1947-11-08T00:00:00+00:00",
            &Rfc3339,
        )?);
        index_writer.add_document(doc!(
            name => "Minnie Riperton",
            birthday => mr_birthday,
        ))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let top_collector = TopDocs::with_limit(3).order_by_fast_field("birthday", Order::Desc);
        let top_docs: Vec<(DateTime, DocAddress)> = searcher.search(&AllQuery, &top_collector)?;
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
        ))?;
        index_writer.add_document(doc!(
            city => "tokyo",
            altitude =>  40i64,
        ))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let top_collector = TopDocs::with_limit(3).order_by_fast_field("altitude", Order::Desc);
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
        ))?;
        index_writer.add_document(doc!(
            city => "tokyo",
            altitude =>  40f64,
        ))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let top_collector = TopDocs::with_limit(3).order_by_fast_field("altitude", Order::Desc);
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
    fn test_top_field_collector_string() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let city = schema_builder.add_text_field("city", TEXT | FAST);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(
                city => "austin",
        ))?;
        index_writer.add_document(doc!(
                city => "greenville",
        ))?;
        index_writer.add_document(doc!(
            city => "tokyo",
        ))?;
        index_writer.commit()?;

        fn query(
            index: &Index,
            order: Order,
            limit: usize,
            offset: usize,
        ) -> crate::Result<Vec<(String, DocAddress)>> {
            let searcher = index.reader()?.searcher();
            let top_collector = TopDocs::with_limit(limit)
                .and_offset(offset)
                .order_by_string_fast_field("city", order);
            searcher.search(&AllQuery, &top_collector)
        }

        assert_eq!(
            &query(&index, Order::Desc, 3, 0)?,
            &[
                ("tokyo".to_owned(), DocAddress::new(0, 2)),
                ("greenville".to_owned(), DocAddress::new(0, 1)),
                ("austin".to_owned(), DocAddress::new(0, 0)),
            ]
        );

        assert_eq!(
            &query(&index, Order::Desc, 2, 0)?,
            &[
                ("tokyo".to_owned(), DocAddress::new(0, 2)),
                ("greenville".to_owned(), DocAddress::new(0, 1)),
            ]
        );

        assert_eq!(&query(&index, Order::Desc, 3, 3)?, &[]);

        assert_eq!(
            &query(&index, Order::Desc, 2, 1)?,
            &[
                ("greenville".to_owned(), DocAddress::new(0, 1)),
                ("austin".to_owned(), DocAddress::new(0, 0)),
            ]
        );

        assert_eq!(
            &query(&index, Order::Asc, 3, 0)?,
            &[
                ("austin".to_owned(), DocAddress::new(0, 0)),
                ("greenville".to_owned(), DocAddress::new(0, 1)),
                ("tokyo".to_owned(), DocAddress::new(0, 2)),
            ]
        );

        assert_eq!(
            &query(&index, Order::Asc, 2, 1)?,
            &[
                ("greenville".to_owned(), DocAddress::new(0, 1)),
                ("tokyo".to_owned(), DocAddress::new(0, 2)),
            ]
        );

        assert_eq!(
            &query(&index, Order::Asc, 2, 0)?,
            &[
                ("austin".to_owned(), DocAddress::new(0, 0)),
                ("greenville".to_owned(), DocAddress::new(0, 1)),
            ]
        );

        assert_eq!(&query(&index, Order::Asc, 3, 3)?, &[]);

        Ok(())
    }

    proptest! {
        #[test]
        fn test_top_field_collect_string_prop(
          order in prop_oneof!(Just(Order::Desc), Just(Order::Asc)),
          limit in 1..256_usize,
          offset in 0..256_usize,
          segments_terms in
            proptest::collection::vec(
                proptest::collection::vec(0..32_u8, 1..32_usize),
                0..8_usize,
            )
        ) {
            let mut schema_builder = Schema::builder();
            let city = schema_builder.add_text_field("city", TEXT | FAST);
            let schema = schema_builder.build();
            let index = Index::create_in_ram(schema);
            let mut index_writer = index.writer_for_tests()?;

            // A Vec<Vec<u8>>, where the outer Vec represents segments, and the inner Vec
            // represents terms.
            for segment_terms in segments_terms.into_iter() {
                for term in segment_terms.into_iter() {
                    let term = format!("{term:0>3}");
                    index_writer.add_document(doc!(
                        city => term,
                    ))?;
                }
                index_writer.commit()?;
            }

            let searcher = index.reader()?.searcher();
            let top_n_results = searcher.search(&AllQuery, &TopDocs::with_limit(limit)
                .and_offset(offset)
                .order_by_string_fast_field("city", order.clone()))?;
            let all_results = searcher.search(&AllQuery, &DocSetCollector)?.into_iter().map(|doc_address| {
                // Get the term for this address.
                // NOTE: We can't determine the SegmentIds that will be generated for Segments
                // ahead of time, so we can't pre-compute the expected `DocAddress`es.
                let column = searcher.segment_readers()[doc_address.segment_ord as usize].fast_fields().str("city").unwrap().unwrap();
                let term_ord = column.term_ords(doc_address.doc_id).next().unwrap();
                let mut city = Vec::new();
                column.dictionary().ord_to_term(term_ord, &mut city).unwrap();
                (String::try_from(city).unwrap(), doc_address)
            });

            // Using the TopDocs collector should always be equivalent to sorting, skipping the
            // offset, and then taking the limit.
            let sorted_docs: Vec<_> = if order.is_desc() {
                let mut comparable_docs: Vec<ComparableDoc<_, _, true>> =
                    all_results.into_iter().map(|(feature, doc)| ComparableDoc { feature, doc}).collect();
                comparable_docs.sort();
                comparable_docs.into_iter().map(|cd| (cd.feature, cd.doc)).collect()
            } else {
                let mut comparable_docs: Vec<ComparableDoc<_, _, false>> =
                    all_results.into_iter().map(|(feature, doc)| ComparableDoc { feature, doc}).collect();
                comparable_docs.sort();
                comparable_docs.into_iter().map(|cd| (cd.feature, cd.doc)).collect()
            };
            let expected_docs = sorted_docs.into_iter().skip(offset).take(limit).collect::<Vec<_>>();
            prop_assert_eq!(
                expected_docs,
                top_n_results
            );
        }
    }

    #[test]
    #[should_panic]
    fn test_field_does_not_exist() {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field(TITLE, TEXT);
        let size = schema_builder.add_u64_field(SIZE, FAST);
        let schema = schema_builder.build();
        let (index, _) = index("beer", title, schema, |index_writer| {
            index_writer
                .add_document(doc!(
                    title => "bottle of beer",
                    size => 12u64,
                ))
                .unwrap();
        });
        let searcher = index.reader().unwrap().searcher();
        let top_collector = TopDocs::with_limit(4).order_by_u64_field("missing_field", Order::Desc);
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
        index_writer.add_document(doc!(size=>1u64))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let segment = searcher.segment_reader(0);
        let top_collector = TopDocs::with_limit(4).order_by_u64_field(SIZE, Order::Desc);
        let err = top_collector.for_segment(0, segment).err().unwrap();
        assert!(matches!(err, crate::TantivyError::InvalidArgument(_)));
        Ok(())
    }

    #[test]
    fn test_field_wrong_type() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let size = schema_builder.add_u64_field(SIZE, STORED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(size=>1u64))?;
        index_writer.commit()?;
        let searcher = index.reader()?.searcher();
        let segment = searcher.segment_reader(0);
        let top_collector = TopDocs::with_limit(4).order_by_fast_field::<i64>(SIZE, Order::Desc);
        let err = top_collector.for_segment(0, segment).err().unwrap();
        assert!(
            matches!(err, crate::TantivyError::SchemaError(msg) if msg == "Field \"size\" is not a fast field.")
        );
        Ok(())
    }

    #[test]
    fn test_tweak_score_top_collector_with_offset() -> crate::Result<()> {
        let index = make_index()?;
        let field = index.schema().get_field("text").unwrap();
        let query_parser = QueryParser::for_index(&index, vec![field]);
        let text_query = query_parser.parse_query("droopy tax")?;
        let collector = TopDocs::with_limit(2).and_offset(1).tweak_score(
            move |_segment_reader: &SegmentReader| move |doc: DocId, _original_score: Score| doc,
        );
        let score_docs: Vec<(u32, DocAddress)> =
            index.reader()?.searcher().search(&text_query, &collector)?;
        assert_eq!(
            score_docs,
            vec![(1, DocAddress::new(0, 1)), (0, DocAddress::new(0, 0)),]
        );
        Ok(())
    }

    #[test]
    fn test_custom_score_top_collector_with_offset() {
        let index = make_index().unwrap();
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
        mut doc_adder: impl FnMut(&mut IndexWriter),
    ) -> (Index, Box<dyn Query>) {
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
        doc_adder(&mut index_writer);
        index_writer.commit().unwrap();
        let query_parser = QueryParser::for_index(&index, vec![query_field]);
        let query = query_parser.parse_query(query).unwrap();
        (index, query)
    }
    #[test]
    fn test_fast_field_ascending_order() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field(TITLE, TEXT);
        let size = schema_builder.add_u64_field(SIZE, FAST);
        let schema = schema_builder.build();
        let (index, query) = index("beer", title, schema, |index_writer| {
            index_writer
                .add_document(doc!(
                    title => "bottle of beer",
                    size => 12u64,
                ))
                .unwrap();
            index_writer
                .add_document(doc!(
                    title => "growler of beer",
                    size => 64u64,
                ))
                .unwrap();
            index_writer
                .add_document(doc!(
                    title => "pint of beer",
                    size => 16u64,
                ))
                .unwrap();
            index_writer
                .add_document(doc!(
                    title => "empty beer",
                ))
                .unwrap();
        });
        let searcher = index.reader()?.searcher();

        let top_collector = TopDocs::with_limit(4).order_by_fast_field(SIZE, Order::Asc);
        let top_docs: Vec<(u64, DocAddress)> = searcher.search(&query, &top_collector)?;
        assert_eq!(
            &top_docs[..],
            &[
                (12, DocAddress::new(0, 0)),
                (16, DocAddress::new(0, 2)),
                (64, DocAddress::new(0, 1)),
                (18446744073709551615, DocAddress::new(0, 3)),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_topn_computer_asc() {
        let mut computer: TopNComputer<u32, u32, false> = TopNComputer::new(2);

        computer.push(1u32, 1u32);
        computer.push(2u32, 2u32);
        computer.push(3u32, 3u32);
        computer.push(2u32, 4u32);
        computer.push(4u32, 5u32);
        computer.push(1u32, 6u32);
        assert_eq!(
            computer.into_sorted_vec(),
            &[
                ComparableDoc {
                    feature: 1u32,
                    doc: 1u32,
                },
                ComparableDoc {
                    feature: 1u32,
                    doc: 6u32,
                }
            ]
        );
    }
}
