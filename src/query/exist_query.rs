use core::fmt::Debug;

use columnar::{ColumnIndex, DynamicColumn};

use super::{ConstScorer, EmptyScorer};
use crate::core::SegmentReader;
use crate::docset::{DocSet, TERMINATED};
use crate::query::explanation::does_not_match;
use crate::query::{EnableScoring, Explanation, Query, Scorer, Weight};
use crate::{DocId, Score, TantivyError};

/// Query that matches all documents with a non-null value in the specified field.
///
/// All of the matched documents get the score 1.0.
#[derive(Clone, Debug)]
pub struct ExistsQuery {
    field_name: String,
}

impl ExistsQuery {
    /// Creates a new `ExistQuery` from the given field.
    ///
    /// This query matches all documents with at least one non-null value in the specified field.
    /// This constructor never fails, but executing the search with this query will return an
    /// error if the specified field doesn't exists or is not a fast field.
    pub fn new_exists_query(field: String) -> ExistsQuery {
        ExistsQuery { field_name: field }
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
        Ok(Box::new(ExistsWeight {
            field_name: self.field_name.clone(),
        }))
    }
}

/// Weight associated with the `ExistsQuery` query.
pub struct ExistsWeight {
    field_name: String,
}

impl Weight for ExistsWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let fast_field_reader = reader.fast_fields();
        let dynamic_columns: crate::Result<Vec<DynamicColumn>> = fast_field_reader
            .dynamic_column_handles(&self.field_name)?
            .into_iter()
            .map(|handle| handle.open().map_err(|io_error| io_error.into()))
            .collect();
        let mut non_empty_columns = Vec::new();
        for column in dynamic_columns? {
            if !matches!(column.column_index(), ColumnIndex::Empty { .. }) {
                non_empty_columns.push(column)
            }
        }
        // TODO: we can optimizer more here since in most cases we will have only one index
        if !non_empty_columns.is_empty() {
            let docset = ExistsDocSet::new(non_empty_columns, reader.max_doc());
            Ok(Box::new(ConstScorer::new(docset, boost)))
        } else {
            Ok(Box::new(EmptyScorer))
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        Ok(Explanation::new("ExistsQuery", 1.0))
    }
}

pub(crate) struct ExistsDocSet {
    columns: Vec<DynamicColumn>,
    doc: DocId,
    max_doc: DocId,
}

impl ExistsDocSet {
    pub(crate) fn new(columns: Vec<DynamicColumn>, max_doc: DocId) -> Self {
        let mut set = Self {
            columns,
            doc: 0u32,
            max_doc,
        };
        set.find_next();
        set
    }

    fn find_next(&mut self) -> DocId {
        while self.doc < self.max_doc {
            if self
                .columns
                .iter()
                .any(|col| col.column_index().has_value(self.doc))
            {
                return self.doc;
            }
            self.doc += 1;
        }
        self.doc = TERMINATED;
        TERMINATED
    }
}

impl DocSet for ExistsDocSet {
    fn advance(&mut self) -> DocId {
        self.seek(self.doc + 1)
    }

    fn size_hint(&self) -> u32 {
        0
    }

    fn doc(&self) -> DocId {
        self.doc
    }

    #[inline(always)]
    fn seek(&mut self, target: DocId) -> DocId {
        self.doc = target;
        self.find_next()
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv6Addr;
    use std::ops::Bound;

    use common::DateTime;
    use time::OffsetDateTime;

    use crate::collector::Count;
    use crate::query::exist_query::ExistsQuery;
    use crate::query::{BooleanQuery, RangeQuery};
    use crate::schema::{Facet, FacetOptions, Schema, FAST, INDEXED, STRING, TEXT};
    use crate::{doc, Index, Searcher};

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

        assert_eq!(count_existing_fields(&searcher, "all")?, 100);
        assert_eq!(count_existing_fields(&searcher, "odd")?, 50);
        assert_eq!(count_existing_fields(&searcher, "even")?, 50);
        assert_eq!(count_existing_fields(&searcher, "multi")?, 10);
        assert_eq!(count_existing_fields(&searcher, "never")?, 0);

        // exercise seek
        let query = BooleanQuery::intersection(vec![
            Box::new(RangeQuery::new_u64_bounds(
                "all".to_string(),
                Bound::Included(50),
                Bound::Unbounded,
            )),
            Box::new(ExistsQuery::new_exists_query("even".to_string())),
        ]);
        assert_eq!(searcher.search(&query, &Count)?, 25);

        let query = BooleanQuery::intersection(vec![
            Box::new(RangeQuery::new_u64_bounds(
                "all".to_string(),
                Bound::Included(0),
                Bound::Excluded(50),
            )),
            Box::new(ExistsQuery::new_exists_query("odd".to_string())),
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

        assert_eq!(count_existing_fields(&searcher, "json.all")?, 100);
        assert_eq!(count_existing_fields(&searcher, "json.even")?, 50);
        assert_eq!(count_existing_fields(&searcher, "json.odd")?, 50);

        // Handling of non-existing fields:
        assert_eq!(count_existing_fields(&searcher, "json.absent")?, 0);
        assert_eq!(
            searcher
                .search(
                    &ExistsQuery::new_exists_query("does_not_exists.absent".to_string()),
                    &Count
                )
                .unwrap_err()
                .to_string(),
            "The field does not exist: 'does_not_exists.absent'"
        );

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

        assert_eq!(count_existing_fields(&searcher, "bool")?, 50);
        assert_eq!(count_existing_fields(&searcher, "bytes")?, 50);
        assert_eq!(count_existing_fields(&searcher, "date")?, 50);
        assert_eq!(count_existing_fields(&searcher, "f64")?, 50);
        assert_eq!(count_existing_fields(&searcher, "ip_addr")?, 50);
        assert_eq!(count_existing_fields(&searcher, "facet")?, 50);

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
                .search(
                    &ExistsQuery::new_exists_query("not_fast".to_string()),
                    &Count
                )
                .unwrap_err()
                .to_string(),
            "Schema error: 'Field not_fast is not a fast field.'"
        );

        assert_eq!(
            searcher
                .search(
                    &ExistsQuery::new_exists_query("does_not_exists".to_string()),
                    &Count
                )
                .unwrap_err()
                .to_string(),
            "The field does not exist: 'does_not_exists'"
        );

        Ok(())
    }

    fn count_existing_fields(searcher: &Searcher, field: &str) -> crate::Result<usize> {
        let query = ExistsQuery::new_exists_query(field.to_string());
        searcher.search(&query, &Count)
    }
}
