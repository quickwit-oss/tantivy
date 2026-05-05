use std::net::Ipv6Addr;

use columnar::{Column, ColumnType};
use rustc_hash::{FxHashMap, FxHashSet};

use super::term_set_gallop;
use super::term_set_strategy::{
    select_strategy, PlannerInputs, TermSetStrategy, TermSetStrategyConfig,
};
use crate::query::score_combiner::DoNothingCombiner;
use crate::query::{
    BooleanWeight, ConstScorer, EmptyScorer, EnableScoring, Explanation, Occur, Query, Scorer,
    Weight,
};
use crate::{DocId, DocSet, Score, SegmentReader, TantivyError, Term, TERMINATED};

// --- FastFieldTermSetQuery ---
#[derive(Debug, Clone)]
/// `FastFieldTermSetQuery` is the same as [TermSetQuery] but only uses the fast field.
pub struct FastFieldTermSetQuery {
    terms_map: FxHashMap<crate::schema::Field, Vec<Term>>,
    strategy_config: TermSetStrategyConfig,
}

impl FastFieldTermSetQuery {
    /// Create a new `FastFieldTermSetQuery`.
    pub fn new<T: IntoIterator<Item = Term>>(terms: T) -> Self {
        let mut terms_map: FxHashMap<_, Vec<_>> = FxHashMap::default();
        for term in terms {
            terms_map.entry(term.field()).or_default().push(term);
        }

        FastFieldTermSetQuery {
            terms_map,
            strategy_config: TermSetStrategyConfig::default(),
        }
    }

    /// Override the strategy thresholds and gates used when this query runs
    /// against fast fields. Consumers (paradedb) build the config from GUCs.
    pub fn with_strategy_config(mut self, cfg: TermSetStrategyConfig) -> Self {
        self.strategy_config = cfg;
        self
    }
}

impl Query for FastFieldTermSetQuery {
    fn weight(&self, _enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let mut sub_queries: Vec<(_, Box<dyn Weight>)> = Vec::with_capacity(self.terms_map.len());
        for (&field, terms) in &self.terms_map {
            sub_queries.push((
                Occur::Should,
                Box::new(FastFieldTermSetWeight::new(
                    field,
                    terms,
                    self.strategy_config.clone(),
                )?),
            ));
        }
        Ok(Box::new(BooleanWeight::new(
            sub_queries,
            false,
            Box::new(DoNothingCombiner::default),
        )))
    }
}

// --- FastFieldTermSetWeight ---

/// Validated input terms held in their post-validation type. Stored as
/// `Vec` rather than `FxHashSet` so that strategies that don't need
/// hash-set semantics (today: `Gallop`, which walks sorted ranges) don't
/// pay the HashSet build cost. The non-gallop arms in `scorer()` build a
/// `FxHashSet` locally from the Vec just before `TermSetDocSet::new`.
#[derive(Clone, Debug)]
enum TermVec {
    U64(Vec<u64>),
    Ipv6Addr(Vec<Ipv6Addr>),
}

#[derive(Clone, Debug)]
pub struct FastFieldTermSetWeight {
    field: crate::schema::Field,
    term_vec: Option<TermVec>,
    strategy_config: TermSetStrategyConfig,
}

impl FastFieldTermSetWeight {
    pub fn new<'a>(
        field: crate::schema::Field,
        terms: impl IntoIterator<Item = &'a Term>,
        strategy_config: TermSetStrategyConfig,
    ) -> crate::Result<Self> {
        let mut terms_iter = terms.into_iter().peekable();

        if terms_iter.peek().is_none() {
            return Ok(Self {
                field,
                term_vec: None,
                strategy_config,
            });
        }

        let first_term_value = terms_iter.peek().unwrap().value();
        let term_vec = if first_term_value.as_ip_addr().is_some() {
            let mut values = Vec::new();
            for term in terms_iter {
                let value = term.value();
                if let Some(val) = value.as_ip_addr() {
                    values.push(val);
                } else {
                    return Err(crate::TantivyError::InvalidArgument(format!(
                        "Expected term with ip address, but got {:?}",
                        term
                    )));
                }
            }
            TermVec::Ipv6Addr(values)
        } else {
            // Numeric types.
            //
            // NOTE: Keep in sync with `TermSetQuery::specialized_weight`.
            let mut values = Vec::new();
            for term in terms_iter {
                let Some(val_u64) = term.value().as_u64_lenient() else {
                    return Err(crate::TantivyError::InvalidArgument(format!(
                        "Expected term with u64, i64, f64, or date, but got {:?}",
                        term
                    )));
                };
                values.push(val_u64);
            }
            TermVec::U64(values)
        };

        Ok(Self {
            field,
            term_vec: Some(term_vec),
            strategy_config,
        })
    }
}

impl Weight for FastFieldTermSetWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        let Some(term_vec) = &self.term_vec else {
            return Ok(Box::new(EmptyScorer));
        };

        let field_entry = reader.schema().get_field_entry(self.field);
        let field_type = field_entry.field_type();
        let field_name = field_entry.name();

        if field_type.is_json() {
            // TODO: Handle JSON fields.
            return Err(crate::TantivyError::InvalidArgument(format!(
                "unsupported type for fast fields TermSet {field_type:?}",
            )));
        } else if field_type.is_str() {
            // TODO: Handle Str fields. They are superficially simple, because we can convert all
            // input terms to TermOrdinals, and then use the numeric codepath. But it would require
            // a batch operation for looking up many terms, because otherwise each term lookup would
            // involve decompressing a term dictionary block (some of them repeatedly). And those
            // lookups would need to happen per-segment, unlike with numeric types.
            return Err(crate::TantivyError::InvalidArgument(format!(
                "unsupported type for fast fields TermSet {field_type:?}",
            )));
        }

        match term_vec {
            TermVec::Ipv6Addr(values) => {
                if !field_type.is_ip_addr() {
                    return Err(crate::TantivyError::InvalidArgument(format!(
                        "fast fields TermSet for field `{field_name}` contains IP addresses, but \
                         the field type is {field_type:?}"
                    )));
                }
                let Some(ip_addr_column): Option<Column<Ipv6Addr>> =
                    reader.fast_fields().column_opt(field_name)?
                else {
                    return Ok(Box::new(EmptyScorer));
                };
                // IPv6 always routes through TermSetDocSet — no Gallop strategy
                // for IP — so the HashSet is needed unconditionally here.
                let term_set: FxHashSet<Ipv6Addr> = values.iter().copied().collect();
                let docset = TermSetDocSet::new(ip_addr_column, term_set);
                Ok(Box::new(ConstScorer::new(docset, boost)))
            }
            TermVec::U64(values) => {
                if field_type.is_ip_addr() {
                    return Err(crate::TantivyError::InvalidArgument(format!(
                        "fast fields TermSet for field `{field_name}` contains numeric values, \
                         but the field type is {field_type:?}"
                    )));
                }
                let fast_field_reader = reader.fast_fields();
                let Some((column, _col_type)) = fast_field_reader.u64_lenient_for_type(
                    Some(&[
                        ColumnType::U64,
                        ColumnType::I64,
                        ColumnType::F64,
                        ColumnType::DateTime,
                    ]),
                    field_name,
                )?
                else {
                    return Ok(Box::new(EmptyScorer));
                };

                let strategy = select_strategy(
                    reader,
                    &column,
                    PlannerInputs {
                        field_name,
                        candidate_size: None,
                        avg_docs_per_term: None,
                    },
                    values.as_slice(),
                    &self.strategy_config,
                );

                match strategy {
                    TermSetStrategy::Gallop {
                        sort_order,
                        sorted_terms,
                    } => {
                        // Gallop walks the column on demand via
                        // `TermSetGallopDocSet`; no HashSet build needed.
                        let cardinality = column.get_cardinality();
                        let docset = term_set_gallop::TermSetGallopDocSet::new(
                            column,
                            sort_order,
                            sorted_terms,
                            cardinality,
                        );
                        Ok(Box::new(ConstScorer::new(docset, boost)))
                    }
                    // The non-Gallop variants are planner stubs today and route
                    // to TermSetDocSet; follow-ups A and B replace these arms
                    // with real implementations without touching the planner.
                    // The HashSet is built locally here so the gallop path
                    // doesn't pay for it.
                    TermSetStrategy::LinearScan
                    | TermSetStrategy::BitsetFromPostings
                    | TermSetStrategy::PostingListDirect
                    | TermSetStrategy::HashProbe => {
                        let term_set: FxHashSet<u64> = values.iter().copied().collect();
                        let docset = TermSetDocSet::new(column, term_set);
                        Ok(Box::new(ConstScorer::new(docset, boost)))
                    }
                }
            }
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(TantivyError::InvalidArgument(format!(
                "Document #({doc}) does not match"
            )));
        }
        let explanation = Explanation::new("FastFieldTermSetScorer", scorer.score());
        Ok(explanation)
    }
}

// --- TermSetDocSet ---

#[derive(Clone)]
pub(crate) struct TermSetDocSet<T: Copy + Eq + std::hash::Hash> {
    column: Column<T>,
    values: FxHashSet<T>,
    doc_id: DocId,
    max_doc: DocId,
}

impl<T: Copy + Eq + std::hash::Hash + PartialOrd + std::fmt::Debug + Send + Sync + 'static>
    TermSetDocSet<T>
{
    pub fn new(column: Column<T>, values: FxHashSet<T>) -> Self {
        let max_doc = column.num_docs();
        let mut doc_set = Self {
            column,
            values,
            doc_id: TERMINATED,
            max_doc,
        };
        doc_set.advance();
        doc_set
    }
}

impl<T: Copy + Eq + std::hash::Hash + PartialOrd + std::fmt::Debug + Send + Sync + 'static> DocSet
    for TermSetDocSet<T>
{
    fn advance(&mut self) -> DocId {
        let mut next_doc_id = if self.doc_id == TERMINATED {
            0
        } else {
            self.doc_id + 1
        };

        while next_doc_id < self.max_doc {
            for value in self.column.values_for_doc(next_doc_id) {
                if self.values.contains(&value) {
                    self.doc_id = next_doc_id;
                    return self.doc_id;
                }
            }
            next_doc_id += 1;
        }

        self.doc_id = TERMINATED;
        TERMINATED
    }

    /// Jump the cursor directly to `target` and resume the per-doc match loop
    /// from there. The trait default would call `advance()` in a loop and pay
    /// one column read + one hash probe for every doc between the current
    /// cursor and `target`; this override skips that work entirely.
    ///
    /// Important for AND-intersection: when one column gallops and the other lands
    /// here, the intersection drives `seek(target)` calls toward the
    /// gallop-emitted DocIds. Without this override, the dumb default seek
    /// re-scans the gaps and the gallop speedup is largely diluted.
    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(target >= self.doc_id || self.doc_id == TERMINATED);
        if target >= self.max_doc {
            self.doc_id = TERMINATED;
            return TERMINATED;
        }
        let mut next_doc_id = target;
        while next_doc_id < self.max_doc {
            for value in self.column.values_for_doc(next_doc_id) {
                if self.values.contains(&value) {
                    self.doc_id = next_doc_id;
                    return next_doc_id;
                }
            }
            next_doc_id += 1;
        }
        self.doc_id = TERMINATED;
        TERMINATED
    }

    fn doc(&self) -> DocId {
        self.doc_id
    }

    fn size_hint(&self) -> u32 {
        self.max_doc - self.doc_id.saturating_add(1)
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::str::FromStr;

    use super::FastFieldTermSetQuery;
    use crate::collector::{Count, TopDocs};
    use crate::query::QueryParser;
    use crate::schema::{IntoIpv6Addr, Schema, FAST, INDEXED, STRING, TEXT};
    use crate::{Index, IndexWriter, Term};

    fn create_test_index() -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let text_field_fast = schema_builder.add_text_field("text_fast", STRING | FAST);
        let u64_field_fast = schema_builder.add_u64_field("u64_fast", FAST | INDEXED);
        let ip_field_fast = schema_builder.add_ip_addr_field("ip_fast", FAST | INDEXED);
        let bool_field_fast = schema_builder.add_bool_field("bool_fast", FAST | INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(
                text_field => "doc1",
                text_field_fast => "doc1",
                u64_field_fast => 1u64,
                ip_field_fast => IpAddr::from_str("127.0.0.1").unwrap().into_ipv6_addr(),
                bool_field_fast => true,
            ))?;
            index_writer.add_document(doc!(
                text_field => "doc2",
                text_field_fast => "doc2",
                u64_field_fast => 2u64,
                ip_field_fast => IpAddr::from_str("127.0.0.2").unwrap().into_ipv6_addr(),
                bool_field_fast => false,
            ))?;
            index_writer.add_document(doc!(
                text_field => "doc3",
                text_field_fast => "doc3",
                u64_field_fast => 3u64,
                ip_field_fast => IpAddr::from_str("::ffff:127.0.0.3").unwrap().into_ipv6_addr(),
                bool_field_fast => true,
            ))?;
            index_writer.commit()?;
        }
        Ok(index)
    }

    #[test]
    pub fn test_term_set_query_fast_field_u64() -> crate::Result<()> {
        let index = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let u64_field_fast = index.schema().get_field("u64_fast").unwrap();

        let query = FastFieldTermSetQuery::new(vec![
            Term::from_field_u64(u64_field_fast, 1),
            Term::from_field_u64(u64_field_fast, 3),
        ]);

        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 2);
        Ok(())
    }

    #[test]
    pub fn test_term_set_query_fast_field_string() -> crate::Result<()> {
        let index = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let text_field_fast = index.schema().get_field("text_fast").unwrap();

        let query = FastFieldTermSetQuery::new(vec![
            Term::from_field_text(text_field_fast, "doc1"),
            Term::from_field_text(text_field_fast, "doc3"),
        ]);

        let err = searcher.search(&query, &Count).unwrap_err();
        assert!(matches!(err, crate::TantivyError::InvalidArgument(_)));
        Ok(())
    }

    #[test]
    pub fn test_term_set_query_fast_field_ip() -> crate::Result<()> {
        let index = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let ip_field_fast = index.schema().get_field("ip_fast").unwrap();

        let query = FastFieldTermSetQuery::new(vec![
            Term::from_field_ip_addr(
                ip_field_fast,
                IpAddr::from_str("127.0.0.1").unwrap().into_ipv6_addr(),
            ),
            Term::from_field_ip_addr(
                ip_field_fast,
                IpAddr::from_str("127.0.0.3").unwrap().into_ipv6_addr(),
            ),
        ]);

        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 2);

        Ok(())
    }

    #[test]
    pub fn test_term_set_query_fast_field_no_match() -> crate::Result<()> {
        let index = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let u64_field_fast = index.schema().get_field("u64_fast").unwrap();

        let query = FastFieldTermSetQuery::new(vec![
            Term::from_field_u64(u64_field_fast, 4),
            Term::from_field_u64(u64_field_fast, 5),
        ]);

        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[test]
    pub fn test_term_set_query_fast_field_empty() -> crate::Result<()> {
        let index = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let query = FastFieldTermSetQuery::new(Vec::<Term>::new());

        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[test]
    pub fn test_term_set_query_parser_fast_field() -> crate::Result<()> {
        let index = create_test_index()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&index, vec![]);

        let query = query_parser.parse_query("u64_fast: IN [1 3]")?;
        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 2);

        let query = query_parser.parse_query("text_fast: IN [doc1 doc3]")?;
        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 2);

        let query = query_parser.parse_query("ip_fast: IN [127.0.0.1 127.0.0.3]")?;
        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 2);

        let query = query_parser.parse_query("bool_fast: IN [true]")?;
        let count = searcher.search(&query, &Count)?;
        assert_eq!(count, 2);

        Ok(())
    }

    fn create_test_index_with_missing_values() -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let u64_field_fast = schema_builder.add_u64_field("u64_fast", FAST | INDEXED);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            // doc 0
            index_writer.add_document(doc!(
                u64_field_fast => 1u64,
            ))?;
            // doc 1
            index_writer.add_document(doc!(
                text_field => "doc2"
            ))?;
            // doc 2
            index_writer.add_document(doc!(
                u64_field_fast => 3u64,
            ))?;
            index_writer.commit()?;
        }
        Ok(index)
    }

    #[test]
    pub fn test_term_set_query_fast_field_missing_value() -> crate::Result<()> {
        let index = create_test_index_with_missing_values()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let u64_field_fast = index.schema().get_field("u64_fast").unwrap();

        // A document with a missing value won't be found by a term query searching for 0.
        let query = FastFieldTermSetQuery::new(vec![Term::from_field_u64(u64_field_fast, 0)]);

        let top_docs = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;
        assert_eq!(top_docs.len(), 0);

        // Check that it doesn't match other values
        let query = FastFieldTermSetQuery::new(vec![Term::from_field_u64(u64_field_fast, 1)]);
        let top_docs = searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;
        assert_eq!(top_docs.len(), 1);
        let (_, doc_address) = top_docs[0];
        assert_eq!(doc_address.doc_id, 0);

        Ok(())
    }
}
