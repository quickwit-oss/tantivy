use std::fmt;
use std::sync::Arc;

use crate::core::searcher::Searcher;
use crate::query::{
    block_join_query::{ParentBitSetProducer, ScoreMode as BJScoreMode, ToParentBlockJoinQuery},
    EnableScoring, Explanation, Query, QueryClone, Scorer, Weight,
};
use crate::schema::{Field, IndexRecordOption, Term};
use crate::{DocAddress, DocId, DocSet, Score, SegmentReader, TantivyError, TERMINATED};

/// Our smaller enum for nested query's score_mode
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NestedScoreMode {
    None,
    Avg,
    Max,
    Min,
    Sum,
}

impl NestedScoreMode {
    /// Convert the user input like "none", "min", etc. into `NestedScoreMode`.
    pub fn from_str(mode: &str) -> Result<NestedScoreMode, String> {
        match mode.to_lowercase().as_str() {
            "none" => Ok(NestedScoreMode::None),
            "avg" => Ok(NestedScoreMode::Avg),
            "max" => Ok(NestedScoreMode::Max),
            "min" => Ok(NestedScoreMode::Min),
            "sum" => Ok(NestedScoreMode::Sum),
            other => Err(format!("Unrecognized nested score_mode: {}", other)),
        }
    }

    /// Convert `NestedScoreMode` into block_join’s `ScoreMode`.
    fn to_block_join_score_mode(&self) -> BJScoreMode {
        match self {
            NestedScoreMode::None => BJScoreMode::None,
            NestedScoreMode::Avg => BJScoreMode::Avg,
            NestedScoreMode::Max => BJScoreMode::Max,
            NestedScoreMode::Min => BJScoreMode::Min,
            NestedScoreMode::Sum => BJScoreMode::Total,
        }
    }
}

/// The `NestedQuery` struct, analogous to Elasticsearch's `NestedQueryBuilder`.
///
/// - `path`: the nested path name (e.g. `"user"`).
/// - `child_query`: the query to match child docs.
/// - `score_mode`: how child scores get aggregated.
/// - `ignore_unmapped`: if `true`, we produce a no-match scorer if the path is unmapped.
pub struct NestedQuery {
    path: String,
    child_query: Box<dyn Query>,
    score_mode: NestedScoreMode,
    ignore_unmapped: bool,
}

impl NestedQuery {
    pub fn new(
        path: String,
        child_query: Box<dyn Query>,
        score_mode: NestedScoreMode,
        ignore_unmapped: bool,
    ) -> Self {
        Self {
            path,
            child_query,
            score_mode,
            ignore_unmapped,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn child_query(&self) -> &dyn Query {
        self.child_query.as_ref()
    }
    pub fn score_mode(&self) -> NestedScoreMode {
        self.score_mode
    }
    pub fn ignore_unmapped(&self) -> bool {
        self.ignore_unmapped
    }
}

impl fmt::Debug for NestedQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NestedQuery")
            .field("path", &self.path)
            .field("score_mode", &self.score_mode)
            .field("ignore_unmapped", &self.ignore_unmapped)
            .finish()
    }
}

impl QueryClone for NestedQuery {
    fn box_clone(&self) -> Box<dyn Query> {
        Box::new(NestedQuery {
            path: self.path.clone(),
            child_query: self.child_query.box_clone(),
            score_mode: self.score_mode,
            ignore_unmapped: self.ignore_unmapped,
        })
    }
}

impl Query for NestedQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        // 1. Build the child's weight:
        let _child_weight = self.child_query.weight(enable_scoring)?;

        // 2. Lookup "_is_parent_<path>" from schema.nested_paths()
        let schema = enable_scoring.schema();
        let Some(parent_name) = schema.nested_paths().get(&self.path) else {
            // Path not found in nested_paths
            if self.ignore_unmapped {
                // produce an empty (no-match) weight
                return Ok(Box::new(NoMatchWeight));
            } else {
                return Err(TantivyError::SchemaError(format!(
                    "NestedQuery path '{}' not mapped, and ignore_unmapped=false",
                    self.path
                )));
            }
        };

        // 3. Check that parent_name maps to a real Field in the schema
        let maybe_parent_field = schema.get_field(parent_name).ok();
        if maybe_parent_field.is_none() {
            if self.ignore_unmapped {
                return Ok(Box::new(NoMatchWeight));
            } else {
                return Err(TantivyError::SchemaError(format!(
                    "NestedQuery path '{}' not mapped, and ignore_unmapped=false",
                    self.path
                )));
            }
        }
        let parent_field = maybe_parent_field.unwrap();

        // 4. Create the ParentBitSetProducer
        let parent_bitset_producer = Arc::new(NestedParentBitSetProducer::new(parent_field));

        // 5. Convert NestedScoreMode => block_join::ScoreMode
        let bj_score_mode = self.score_mode.to_block_join_score_mode();

        // 6. Build and return a ToParentBlockJoinQuery
        let block_join_query = ToParentBlockJoinQuery::new(
            self.child_query.box_clone(),
            parent_bitset_producer,
            bj_score_mode,
        );
        block_join_query.weight(enable_scoring)
    }

    fn explain(&self, searcher: &Searcher, doc_address: DocAddress) -> crate::Result<Explanation> {
        let w = self.weight(EnableScoring::enabled_from_searcher(searcher))?;
        w.explain(
            searcher.segment_reader(doc_address.segment_ord),
            doc_address.doc_id,
        )
    }

    fn count(&self, searcher: &Searcher) -> crate::Result<usize> {
        let w = self.weight(EnableScoring::disabled_from_searcher(searcher))?;
        let mut sum = 0usize;
        for seg_reader in searcher.segment_readers() {
            sum += w.count(seg_reader)? as usize;
        }
        Ok(sum)
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        // pass down to child query
        self.child_query.query_terms(visitor);
    }
}

/// A trivial “NoMatchWeight” => no docs
pub struct NoMatchWeight;

impl Weight for NoMatchWeight {
    fn scorer(&self, _reader: &SegmentReader, _boost: Score) -> crate::Result<Box<dyn Scorer>> {
        Ok(Box::new(NoMatchScorer))
    }
    fn explain(&self, _reader: &SegmentReader, _doc: DocId) -> crate::Result<Explanation> {
        Ok(Explanation::new("No-match query", 0.0))
    }
    fn count(&self, _reader: &SegmentReader) -> crate::Result<u32> {
        Ok(0)
    }
    fn for_each_pruning(
        &self,
        _threshold: Score,
        _reader: &SegmentReader,
        _callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) -> crate::Result<()> {
        Ok(())
    }
}

/// A trivial “NoMatchScorer” => always TERMINATED
pub struct NoMatchScorer;

impl crate::docset::DocSet for NoMatchScorer {
    fn advance(&mut self) -> DocId {
        TERMINATED
    }
    fn doc(&self) -> DocId {
        TERMINATED
    }
    fn size_hint(&self) -> u32 {
        0
    }
}

impl Scorer for NoMatchScorer {
    fn score(&mut self) -> Score {
        0.0
    }
}

/// Example `NestedParentBitSetProducer` to find docs where a `_is_parent_<path>` bool field = true
pub struct NestedParentBitSetProducer {
    parent_field: Field,
}

impl NestedParentBitSetProducer {
    pub fn new(parent_field: Field) -> Self {
        Self { parent_field }
    }
}

impl ParentBitSetProducer for NestedParentBitSetProducer {
    fn produce(&self, reader: &SegmentReader) -> crate::Result<common::BitSet> {
        let max_doc = reader.max_doc();
        let mut bitset = common::BitSet::with_max_value(max_doc);

        // For example, if the parent_field is a boolean field, you read all postings for “true”.
        let inverted = reader.inverted_index(self.parent_field)?;
        let term_true = Term::from_field_bool(self.parent_field, true);
        if let Some(mut postings) = inverted.read_postings(&term_true, IndexRecordOption::Basic)? {
            let mut d = postings.doc();
            while d != TERMINATED {
                bitset.insert(d);
                d = postings.advance();
            }
        }
        Ok(bitset)
    }
}

#[cfg(test)]
mod nested_query_tests {
    use super::*; // or import your nested_query, NestedQuery, etc. explicitly
    use crate::collector::TopDocs;
    use crate::index::Index;
    use crate::query::EnableScoring;
    use crate::query::{
        nested_query::{NestedQuery, NestedScoreMode},
        Query, TermQuery,
    };
    use crate::schema::{
        DocParsingError, Field, FieldType, IndexRecordOption, NestedOptions, Schema, SchemaBuilder,
        TantivyDocument, TextOptions, Value, STRING, TEXT,
    };
    use crate::IndexWriter;
    use crate::Term;
    use serde_json::json;

    /// A small helper to build a nested schema:
    /// - `user` is a nested field (with `include_in_parent=true` for demonstration).
    /// - We'll also add "user.first" and "user.last" fields as TEXT,
    ///   plus a top-level "group" field as STRING, etc.
    fn make_nested_schema() -> (
        Schema,
        Field, /* user field */
        Field, /* first */
        Field, /* last */
        Field, /* group */
    ) {
        let mut builder = Schema::builder();

        // normal top-level field
        let group_field = builder.add_text_field("group", STRING);

        // define `NestedOptions`
        let nested_opts = NestedOptions::new()
            .set_include_in_parent(true) // auto-copy child fields up to parent
            .set_store_parent_flag(true); // auto-add `_is_parent_user` bool

        // the "user" nested field
        let user_nested_field = builder.add_nested_field("user", nested_opts);

        // define child fields user.first and user.last as text
        // For a real schema, you'd typically name them "first" etc. then store them inside the JSON objects.
        let first_field = builder.add_text_field("first", TEXT);
        let last_field = builder.add_text_field("last", TEXT);

        let schema = builder.build();
        (
            schema,
            user_nested_field,
            first_field,
            last_field,
            group_field,
        )
    }

    /// Index a single JSON doc that has nested `user` array-of-objects.
    /// Uses your parse_json_for_nested(...) method to produce child docs + parent doc.
    fn index_test_document(
        index_writer: &mut IndexWriter,
        schema: &Schema,
        group_val: &str,
        users: serde_json::Value,
    ) -> Result<(), DocParsingError> {
        // Build up a single top-level JSON object
        let full_doc = json!({
            "group": group_val,
            "user": users, // e.g. an array of { "first": "...", "last": "..." }
        });
        let doc_str = serde_json::to_string(&full_doc).unwrap();

        // Expand into multiple docs
        let expanded_docs = TantivyDocument::parse_json_for_nested(schema, &doc_str)?;

        // Add them as a block using add_documents
        let docs: Vec<_> = expanded_docs.into_iter().map(|d| d.into()).collect();
        index_writer.add_documents(docs).unwrap();

        Ok(())
    }

    /// A trivial lookup function for `_is_parent_<path>` that your NestedQuery calls.
    /// In real code, you might do something more robust. Here we hard-code `_is_parent_user`.
    ///
    /// This must be *hooked* into your `NestedQuery::lookup_parent_field_for_path`
    /// either by rewriting that function or making it pick from a static map.
    fn test_lookup_parent_field(path: &str, schema: &Schema) -> Option<Field> {
        // if path == "user", we expect a field named `_is_parent_user`
        let maybe_name = format!("_is_parent_{}", path);
        schema.get_field(&maybe_name).ok()
    }

    #[test]
    fn test_nested_query_single_level() -> crate::Result<()> {
        // 1) Build the nested schema
        let (schema, _user_nested_field, first_field, last_field, group_field) =
            make_nested_schema();
        let index = Index::create_in_ram(schema.clone());

        // 2) Create an index writer & add docs
        {
            let mut writer = index.writer_for_tests()?;

            // Document #1: group="fans", user => [ {"first":"John","last":"Smith"}, {"first":"Alice","last":"White"} ]
            index_test_document(
                &mut writer,
                &schema,
                "fans",
                json!([
                    { "first": "John", "last": "Smith" },
                    { "first": "Alice", "last": "White" }
                ]),
            )?;

            // Document #2: group="boring", user => [ {"first":"Bob","last":"Marley"} ]
            index_test_document(
                &mut writer,
                &schema,
                "boring",
                json!([
                    { "first": "Bob", "last": "Marley" }
                ]),
            )?;

            writer.commit()?;
        }

        // 4) Search
        let reader = index.reader()?;
        let searcher = reader.searcher();

        // We'll query for child docs whose `first=="Alice"`
        let child_term = Term::from_field_text(first_field, "Alice");
        let child_query = TermQuery::new(child_term, IndexRecordOption::Basic);

        // Build the NestedQuery
        let nested_query = NestedQuery::new(
            "user".to_string(),
            Box::new(child_query),
            NestedScoreMode::Avg,
            false, // ignore_unmapped
        );

        // Execute search
        let top_docs = searcher.search(&nested_query, &TopDocs::with_limit(10))?;
        // We expect 1 doc: the parent doc that had "Alice" in user array
        assert_eq!(1, top_docs.len(), "Should match exactly one parent doc");

        // Fetch that doc & check "group= fans"
        let (score, doc_address) = top_docs[0];
        let retrieved_doc: TantivyDocument = searcher.doc(doc_address)?;
        let group_vals = retrieved_doc
            .get_all(group_field)
            .map(|v| v.as_str())
            .collect::<Vec<_>>();
        assert_eq!(group_vals, vec![Some("fans")]);

        Ok(())
    }

    #[test]
    fn test_nested_query_no_match() -> crate::Result<()> {
        // This time we’ll test a nested query that doesn’t match any child => no parent docs.
        let (schema, _user_nested_field, first_field, _last_field, _group_field) =
            make_nested_schema();
        let index = Index::create_in_ram(schema.clone());

        {
            let mut writer = index.writer_for_tests()?;
            // Insert one doc => user => [ { first:"John"}, { first:"Alice"} ] ...
            index_test_document(
                &mut writer,
                &schema,
                "groupVal",
                json!([
                    {"first":"John"},
                    {"first":"Alice"}
                ]),
            )?;
            writer.commit()?;
        }

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // child query => "first=NoSuchName"
        let child_query = TermQuery::new(
            Term::from_field_text(first_field, "NoSuchName"),
            IndexRecordOption::Basic,
        );
        let nested_query = NestedQuery::new(
            "user".into(),
            Box::new(child_query),
            NestedScoreMode::None,
            false,
        );

        let top_docs = searcher.search(&nested_query, &TopDocs::with_limit(10))?;
        assert_eq!(0, top_docs.len(), "No matches expected");

        Ok(())
    }

    #[test]
    fn test_nested_query_ignore_unmapped() -> crate::Result<()> {
        // Demonstrates that if `path="badPath"` is not recognized and `ignore_unmapped=true`,
        // we get no matches instead of an error.
        let (schema, _ufield, first_field, _last_field, _group_field) = make_nested_schema();
        let index = Index::create_in_ram(schema.clone());

        {
            let mut writer = index.writer_for_tests()?;
            // Insert doc
            index_test_document(
                &mut writer,
                &schema,
                "unmappedTest",
                json!([
                    {"first":"SomeName"}
                ]),
            )?;
            writer.commit()?;
        }

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // We do a NestedQuery with path="someUnknownPath", but ignore_unmapped=true => no error => no docs
        let child_query = TermQuery::new(
            Term::from_field_text(first_field, "SomeName"),
            IndexRecordOption::Basic,
        );
        let nested_query = NestedQuery::new(
            "someUnknownPath".to_string(),
            Box::new(child_query),
            NestedScoreMode::Sum,
            true, // ignore_unmapped
        );

        let top_docs = searcher.search(&nested_query, &TopDocs::with_limit(10))?;
        assert_eq!(0, top_docs.len(), "No docs returned, but no error either");

        Ok(())
    }

    #[test]
    fn test_nested_query_unmapped_error() {
        // If ignore_unmapped=false, we expect an error instead.
        let (schema, _ufield, first_field, _last_field, _group_field) = make_nested_schema();
        let index = Index::create_in_ram(schema.clone());

        // We won't even index anything for this example. We'll just do the query.
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let child_query = TermQuery::new(
            Term::from_field_text(first_field, "X"),
            IndexRecordOption::Basic,
        );
        let nested_query = NestedQuery::new(
            "badPath".into(),
            Box::new(child_query),
            NestedScoreMode::None,
            false, // ignore_unmapped=false => expect error
        );

        let res = searcher.search(&nested_query, &TopDocs::with_limit(10));
        match res {
            Err(e) => {
                // Should be a schema error about path unmapped
                let msg = format!("{:?}", e);
                assert!(
                    msg.contains("NestedQuery path 'badPath' not mapped")
                        && !msg.contains("ignore_unmapped=true"),
                    "Expected schema error complaining about unmapped path"
                );
            }
            Ok(_) => panic!("Expected an error for unmapped path with ignore_unmapped=false"),
        }
    }
}
