use core::fmt::Debug;

use columnar::column_index::{MultiValueIndex, OptionalIndex, Set};
use columnar::ColumnIndex;
use common::BitSet;

use super::{ConstScorer, EmptyScorer};
use crate::docset::{DocSet, SeekDangerResult, TERMINATED};
use crate::index::SegmentReader;
use crate::query::all_query::AllScorer;
use crate::query::boost_query::BoostScorer;
use crate::query::explanation::does_not_match;
use crate::query::{BitSetDocSet, EnableScoring, Explanation, Query, Scorer, Weight};
use crate::schema::Type;
use crate::{DocId, Score, TantivyError};

/// Query that matches all documents with a non-null value in the specified
/// field.
///
/// When querying inside a JSON field, "exists" queries can be executed strictly
/// on the field name or check all the subpaths. In that second case a document
/// will be matched if a non-null value exists in any subpath. For example,
/// assuming the following document where `myfield` is a JSON fast field:
/// ```json
/// {
///   "myfield": {
///     "mysubfield": "hello"
///   }
/// }
/// ```
/// With `json_subpaths` enabled queries on either `myfield` or
/// `myfield.mysubfield` will match the document. If it is set to false, only
/// `myfield.mysubfield` will match it.
///
/// All of the matched documents get the score 1.0.
#[derive(Clone, Debug)]
pub struct ExistsQuery {
    field_name: String,
    json_subpaths: bool,
}

impl ExistsQuery {
    /// Creates a new `ExistQuery` from the given field.
    ///
    /// This query matches all documents with at least one non-null value in the specified field.
    /// This constructor never fails, but executing the search with this query will return an
    /// error if the specified field doesn't exists or is not a fast field.
    #[deprecated]
    pub fn new_exists_query(field: String) -> ExistsQuery {
        ExistsQuery {
            field_name: field,
            json_subpaths: false,
        }
    }

    /// Creates a new `ExistQuery` from the given field.
    ///
    /// This query matches all documents with at least one non-null value in the
    /// specified field. If `json_subpaths` is set to true, documents with
    /// non-null values in any JSON subpath will also be matched.
    ///
    /// This constructor never fails, but executing the search with this query will
    /// return an error if the specified field doesn't exists or is not a fast
    /// field.
    pub fn new(field: String, json_subpaths: bool) -> Self {
        Self {
            field_name: field,
            json_subpaths,
        }
    }
}

impl Query for ExistsQuery {
    fn weight(&self, enable_scoring: EnableScoring) -> crate::Result<Box<dyn Weight>> {
        let schema = enable_scoring.schema();
        let Some((field, _path)) = schema.find_field(&self.field_name) else {
            return Err(TantivyError::FieldNotFound(self.field_name.clone()));
        };
        let field_type = schema.get_field_entry(field).field_type();
        if !field_type.is_fast() {
            return Err(TantivyError::SchemaError(format!(
                "Field {} is not a fast field.",
                self.field_name
            )));
        }
        Ok(Box::new(FastFieldExistsWeight {
            field_name: self.field_name.clone(),
            field_type: field_type.value_type(),
            json_subpaths: self.json_subpaths,
        }))
    }
}

/// Fast-field weight associated with the `ExistsQuery` query.
pub struct FastFieldExistsWeight {
    field_name: String,
    field_type: Type,
    json_subpaths: bool,
}

impl Weight for FastFieldExistsWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let fast_field_reader = reader.fast_fields();
        let mut column_handles = fast_field_reader.dynamic_column_handles(&self.field_name)?;
        if self.field_type == Type::Json && self.json_subpaths {
            let mut sub_columns =
                fast_field_reader.dynamic_subpath_column_handles(&self.field_name)?;
            column_handles.append(&mut sub_columns);
        }
        // An exists query only needs the column indexes. Drop the values so that the docset below
        // operates directly on the specialized optional or multivalued index.
        let column_indexes: crate::Result<Vec<ColumnIndex>> = column_handles
            .into_iter()
            .map(|handle| {
                handle
                    .open()
                    .map(|column| column.into_column_index())
                    .map_err(|io_error| io_error.into())
            })
            .collect();
        let mut non_empty_column_indexes: Vec<ColumnIndex> = column_indexes?
            .into_iter()
            .filter(|column_index| !matches!(column_index, ColumnIndex::Empty { .. }))
            .collect();
        if non_empty_column_indexes.is_empty() {
            return Ok(Box::new(EmptyScorer));
        }

        // If any column is full, all docs match.
        let max_doc = reader.max_doc();
        if non_empty_column_indexes
            .iter()
            .any(|column_index| matches!(column_index, ColumnIndex::Full))
        {
            let all_scorer = AllScorer::new(max_doc);
            if boost != 1.0f32 {
                return Ok(Box::new(BoostScorer::new(all_scorer, boost)));
            } else {
                return Ok(Box::new(all_scorer));
            }
        }

        // Starting here we are guaranteed that there is no full or empty column left.

        if non_empty_column_indexes.len() == 1 {
            return match non_empty_column_indexes.pop().unwrap() {
                ColumnIndex::Optional(optional_index) => {
                    Ok(exists_scorer(optional_index, max_doc, boost))
                }
                ColumnIndex::Multivalued(multivalued_index) => {
                    Ok(exists_scorer(multivalued_index, max_doc, boost))
                }
                ColumnIndex::Empty { .. } | ColumnIndex::Full => unreachable!(),
            };
        }

        // NOTE: A lower number may be better for very sparse columns.
        if non_empty_column_indexes.len() < 4 {
            return Ok(exists_scorer(non_empty_column_indexes, max_doc, boost));
        }

        // If we have many dynamic columns, precompute a bitset of matching docs
        let mut doc_bitset = BitSet::with_max_value(max_doc);
        for column_index in &non_empty_column_indexes {
            match column_index {
                ColumnIndex::Empty { .. } | ColumnIndex::Full => unreachable!(),
                ColumnIndex::Optional(optional_index) => {
                    for doc in optional_index.iter_non_null_docs() {
                        doc_bitset.insert(doc);
                    }
                }
                ColumnIndex::Multivalued(multi_idx) => {
                    for doc in multi_idx.iter_non_null_docs() {
                        doc_bitset.insert(doc);
                    }
                }
            }
        }
        let docset = BitSetDocSet::from(doc_bitset);
        Ok(Box::new(ConstScorer::new(docset, boost)))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        Ok(Explanation::new("ExistsQuery", 1.0))
    }
}

pub(crate) trait ExistsIndex: Send {
    fn next_doc(&self, target: DocId, max_doc: DocId) -> DocId;
    fn has_value(&self, doc: DocId) -> bool;
}

impl ExistsIndex for OptionalIndex {
    fn next_doc(&self, target: DocId, _max_doc: DocId) -> DocId {
        self.next_non_null_doc(target).unwrap_or(TERMINATED)
    }

    fn has_value(&self, doc: DocId) -> bool {
        doc < self.num_docs() && self.contains(doc)
    }
}

impl ExistsIndex for MultiValueIndex {
    fn next_doc(&self, target: DocId, _max_doc: DocId) -> DocId {
        self.next_non_null_doc(target).unwrap_or(TERMINATED)
    }

    fn has_value(&self, doc: DocId) -> bool {
        MultiValueIndex::has_value(self, doc)
    }
}

impl ExistsIndex for Vec<ColumnIndex> {
    fn next_doc(&self, mut target: DocId, max_doc: DocId) -> DocId {
        while target < max_doc {
            if self
                .iter()
                .any(|column_index| column_index.has_value(target))
            {
                return target;
            }
            target += 1;
        }
        TERMINATED
    }

    fn has_value(&self, doc: DocId) -> bool {
        self.iter().any(|column_index| column_index.has_value(doc))
    }
}

fn exists_scorer<T: ExistsIndex + 'static>(
    column_index: T,
    max_doc: DocId,
    boost: Score,
) -> Box<dyn Scorer> {
    let docset = ExistsDocSet::new(column_index, max_doc);
    Box::new(ConstScorer::new(docset, boost))
}

pub(crate) struct ExistsDocSet<T: ExistsIndex> {
    column_index: T,
    doc: DocId,
    max_doc: DocId,
}

impl<T: ExistsIndex> ExistsDocSet<T> {
    pub(crate) fn new(column_index: T, max_doc: DocId) -> Self {
        let doc = column_index.next_doc(0, max_doc);
        Self {
            column_index,
            doc,
            max_doc,
        }
    }
}

impl<T: ExistsIndex> DocSet for ExistsDocSet<T> {
    fn advance(&mut self) -> DocId {
        if self.doc != TERMINATED {
            self.doc = self.column_index.next_doc(self.doc + 1, self.max_doc);
        }
        self.doc
    }

    fn size_hint(&self) -> u32 {
        0
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    #[inline(always)]
    fn seek(&mut self, target: DocId) -> DocId {
        self.doc = self.column_index.next_doc(target, self.max_doc);
        self.doc
    }

    fn seek_danger(&mut self, target: DocId) -> SeekDangerResult {
        if target >= self.max_doc {
            return SeekDangerResult::SeekLowerBound(TERMINATED);
        }
        if self.column_index.has_value(target) {
            self.doc = target;
            SeekDangerResult::Found
        } else {
            // We only need to return a lower bound here. Avoid scanning for the next populated
            // document; intersections can continue with a cheap point lookup instead.
            SeekDangerResult::SeekLowerBound(target + 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;
    use std::ops::Bound;

    use columnar::column_index::OptionalIndex;
    use common::DateTime;
    use time::OffsetDateTime;

    use crate::collector::Count;
    use crate::docset::{DocSet, SeekDangerResult, TERMINATED};
    use crate::query::exist_query::{ExistsDocSet, ExistsQuery};
    use crate::query::{BooleanQuery, RangeQuery};
    use crate::schema::{Facet, FacetOptions, Schema, FAST, INDEXED, STRING, TEXT};
    use crate::{Index, Searcher, Term};

    #[test]
    fn test_exists_docset_seek_danger() {
        let optional_index = OptionalIndex::for_test(8, &[1, 4, 7]);
        let mut docset = ExistsDocSet::new(optional_index, 8);

        assert_eq!(docset.doc(), 1);
        assert_eq!(docset.seek_danger(2), SeekDangerResult::SeekLowerBound(3));
        assert_eq!(docset.seek_danger(4), SeekDangerResult::Found);
        assert_eq!(docset.doc(), 4);
        assert_eq!(docset.advance(), 7);
        assert_eq!(
            docset.seek_danger(TERMINATED),
            SeekDangerResult::SeekLowerBound(TERMINATED)
        );
    }

    #[test]
    fn test_exists_query_simple() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let all_field = schema_builder.add_u64_field("all", INDEXED | FAST);
        let even_field = schema_builder.add_u64_field("even", INDEXED | FAST);
        let odd_field = schema_builder.add_text_field("odd", STRING | FAST);
        let multi_field = schema_builder.add_text_field("multi", FAST);
        let _never_field = schema_builder.add_u64_field("never", INDEXED | FAST);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            for i in 0u64..100u64 {
                if i % 2 == 0 {
                    if i % 10 == 0 {
                        index_writer.add_document(doc!(all_field => i, even_field => i, multi_field => i.to_string(), multi_field => (i + 1).to_string()))?;
                    } else {
                        index_writer.add_document(doc!(all_field => i, even_field => i))?;
                    }
                } else {
                    index_writer.add_document(doc!(all_field => i, odd_field => i.to_string()))?;
                }
            }
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        assert_eq!(count_existing_fields(&searcher, "all", false)?, 100);
        assert_eq!(count_existing_fields(&searcher, "odd", false)?, 50);
        assert_eq!(count_existing_fields(&searcher, "even", false)?, 50);
        assert_eq!(count_existing_fields(&searcher, "multi", false)?, 10);
        assert_eq!(count_existing_fields(&searcher, "multi", true)?, 10);
        assert_eq!(count_existing_fields(&searcher, "never", false)?, 0);

        // exercise seek
        let query = BooleanQuery::intersection(vec![
            Box::new(RangeQuery::new(
                Bound::Included(Term::from_field_u64(all_field, 50)),
                Bound::Unbounded,
            )),
            Box::new(ExistsQuery::new("even".to_string(), false)),
        ]);
        assert_eq!(searcher.search(&query, &Count)?, 25);

        let query = BooleanQuery::intersection(vec![
            Box::new(RangeQuery::new(
                Bound::Included(Term::from_field_u64(all_field, 0)),
                Bound::Included(Term::from_field_u64(all_field, 50)),
            )),
            Box::new(ExistsQuery::new("odd".to_string(), false)),
        ]);
        assert_eq!(searcher.search(&query, &Count)?, 25);

        Ok(())
    }

    #[test]
    fn test_exists_query_json() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", TEXT | FAST);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            for i in 0u64..100u64 {
                if i % 2 == 0 {
                    index_writer.add_document(doc!(json => json!({"all": i, "even": true})))?;
                } else {
                    index_writer
                        .add_document(doc!(json => json!({"all": i.to_string(), "odd": true})))?;
                }
            }
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        assert_eq!(count_existing_fields(&searcher, "json.all", false)?, 100);
        assert_eq!(count_existing_fields(&searcher, "json.even", false)?, 50);
        assert_eq!(count_existing_fields(&searcher, "json.even", true)?, 50);
        assert_eq!(count_existing_fields(&searcher, "json.odd", false)?, 50);
        assert_eq!(count_existing_fields(&searcher, "json", false)?, 0);
        assert_eq!(count_existing_fields(&searcher, "json", true)?, 100);

        // Handling of non-existing fields:
        assert_eq!(count_existing_fields(&searcher, "json.absent", false)?, 0);
        assert_eq!(count_existing_fields(&searcher, "json.absent", true)?, 0);
        assert_does_not_exist(&searcher, "does_not_exists.absent", true);
        assert_does_not_exist(&searcher, "does_not_exists.absent", false);

        Ok(())
    }

    #[test]
    fn test_exists_query_json_union_no_single_full_subpath() -> crate::Result<()> {
        // Build docs where no single subpath exists for all docs, but the union does.
        let mut schema_builder = Schema::builder();
        let json = schema_builder.add_json_field("json", TEXT | FAST);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            for i in 0u64..100u64 {
                if i % 2 == 0 {
                    // only subpath `a`
                    index_writer.add_document(doc!(json => json!({"a": i})))?;
                } else {
                    // only subpath `b`
                    index_writer.add_document(doc!(json => json!({"b": i})))?;
                }
            }
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        // No single subpath is full
        assert_eq!(count_existing_fields(&searcher, "json.a", false)?, 50);
        assert_eq!(count_existing_fields(&searcher, "json.b", false)?, 50);

        // Root exists with subpaths disabled is zero
        assert_eq!(count_existing_fields(&searcher, "json", false)?, 0);

        // Root exists with subpaths enabled should match all docs via union
        assert_eq!(count_existing_fields(&searcher, "json", true)?, 100);

        Ok(())
    }

    #[test]
    fn test_exists_query_misc_supported_types() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let bool = schema_builder.add_bool_field("bool", FAST);
        let bytes = schema_builder.add_bytes_field("bytes", FAST);
        let date = schema_builder.add_date_field("date", FAST);
        let f64 = schema_builder.add_f64_field("f64", FAST);
        let ip_addr = schema_builder.add_ip_addr_field("ip_addr", FAST);
        let facet = schema_builder.add_facet_field("facet", FacetOptions::default());
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            let now = OffsetDateTime::now_utc().unix_timestamp();
            for i in 0u8..100u8 {
                if i % 2 == 0 {
                    let date_val = DateTime::from_utc(OffsetDateTime::from_unix_timestamp(
                        now + i as i64 * 100,
                    )?);
                    index_writer.add_document(
                        doc!(bool => i % 3 == 0, bytes => vec![i, i + 1,  i + 2], date => date_val),
                    )?;
                } else {
                    let ip_addr_v6 = Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc00a, i.into());
                    index_writer
                        .add_document(doc!(f64 => i as f64 * 0.5, ip_addr => ip_addr_v6, facet => Facet::from("/facet/foo"), facet => Facet::from("/facet/bar")))?;
                }
            }
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        assert_eq!(count_existing_fields(&searcher, "bool", false)?, 50);
        assert_eq!(count_existing_fields(&searcher, "bool", true)?, 50);
        assert_eq!(count_existing_fields(&searcher, "bytes", false)?, 50);
        assert_eq!(count_existing_fields(&searcher, "date", false)?, 50);
        assert_eq!(count_existing_fields(&searcher, "f64", false)?, 50);
        assert_eq!(count_existing_fields(&searcher, "ip_addr", false)?, 50);
        assert_eq!(count_existing_fields(&searcher, "facet", false)?, 50);

        Ok(())
    }

    #[test]
    fn test_exists_query_unsupported_types() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let not_fast = schema_builder.add_text_field("not_fast", TEXT);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        {
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(
                not_fast => "slow",
            ))?;
            index_writer.commit()?;
        }
        let reader = index.reader()?;
        let searcher = reader.searcher();

        assert_eq!(
            searcher
                .search(&ExistsQuery::new("not_fast".to_string(), false), &Count)
                .unwrap_err()
                .to_string(),
            "Schema error: 'Field not_fast is not a fast field.'"
        );

        assert_does_not_exist(&searcher, "does_not_exists", false);

        Ok(())
    }

    fn count_existing_fields(
        searcher: &Searcher,
        field: &str,
        json_subpaths: bool,
    ) -> crate::Result<usize> {
        let query = ExistsQuery::new(field.to_string(), json_subpaths);
        searcher.search(&query, &Count)
    }

    fn assert_does_not_exist(searcher: &Searcher, field: &str, json_subpaths: bool) {
        assert_eq!(
            searcher
                .search(&ExistsQuery::new(field.to_string(), json_subpaths), &Count)
                .unwrap_err()
                .to_string(),
            format!("The field does not exist: '{field}'")
        );
    }
}
