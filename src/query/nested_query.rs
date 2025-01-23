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
        TantivyDocument, TextOptions, Value, STORED, STRING, TEXT,
    };
    use crate::IndexWriter;
    use crate::Term;
    use serde_json::json;

    /// A small helper to build a nested schema:
    /// - `user` is a nested field (with `include_in_parent=true` for demonstration).
    /// - We'll also add "user.first" and "user.last" fields as TEXT,
    ///   plus a top-level "group" field as STRING, etc.
    fn make_nested_schema() -> (Schema, Field, Field, Field, Field) {
        let mut builder = Schema::builder();

        // normal top-level field
        let group_field = builder.add_text_field("group", STRING | STORED);

        // nested field
        let nested_opts = NestedOptions::new()
            .set_include_in_parent(true) // or false as you need
            .set_store_parent_flag(true);
        let user_nested_field = builder.add_nested_field("user", nested_opts);

        // child fields "first" and "last"
        // CHANGED: Use STRING, so the exact token "Alice" is indexed.
        let first_field = builder.add_text_field("first", STRING);
        let last_field = builder.add_text_field("last", STRING);

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

#[cfg(test)]
mod nested_query_equiv_tests {
    use super::*;
    use crate::collector::TopDocs;
    use crate::query::{
        nested_query::{NestedQuery, NestedScoreMode},
        Query, QueryClone, TermQuery,
    };
    use crate::query::{EnableScoring, Explanation, QueryParserError};
    use crate::schema::{
        Field, FieldEntry, FieldType, IndexRecordOption, NestedOptions, Schema, SchemaBuilder,
        TantivyDocument, TextOptions, Value, STORED, STRING,
    };
    use crate::{doc, DocAddress, DocId, Index, ReloadPolicy, Term, TERMINATED};
    use serde_json::json;

    // A small helper that sets up a nested schema with a `user` nested field.
    fn make_schema_for_eq_tests() -> (Schema, Field, Field, Field, Field) {
        let mut builder = SchemaBuilder::default();

        // A top-level string field, stored so we can retrieve it.
        let group_f = builder.add_text_field("group", STRING | STORED);

        // Create a nested field named `user`
        let nested_opts = NestedOptions::new()
            .set_include_in_parent(true)
            .set_store_parent_flag(true);
        let user_nested_f = builder.add_nested_field("user", nested_opts);

        // Child fields: "first" and "last"
        let first_f = builder.add_text_field("first", STRING);
        let last_f = builder.add_text_field("last", STRING);

        let schema = builder.build();
        (schema, user_nested_f, first_f, last_f, group_f)
    }

    // Helper: indexes a doc with user: [{first,... last,...}] array
    fn index_doc_for_eq_tests(
        index_writer: &mut crate::indexer::IndexWriter,
        schema: &Schema,
        group_val: &str,
        user_array: serde_json::Value,
    ) {
        let top_obj = json!({
            "group": group_val,
            "user": user_array
        });
        let doc_str = serde_json::to_string(&top_obj).unwrap();
        let expanded_docs =
            TantivyDocument::parse_json_for_nested(schema, &doc_str).expect("parse nested doc");

        let docs = expanded_docs
            .into_iter()
            .map(|d| d.into())
            .collect::<Vec<_>>();
        index_writer.add_documents(docs).unwrap();
    }

    #[test]
    fn test_ignore_unmapped_true() {
        // If we specify path="unmapped" but ignore_unmapped=true => no error => no hits
        let (schema, _user_nested_f, first_f, last_f, group_f) = make_schema_for_eq_tests();
        let index = Index::create_in_ram(schema.clone());

        // Index one doc
        {
            let mut writer = index.writer_for_tests().unwrap();
            index_doc_for_eq_tests(
                &mut writer,
                &schema,
                "someGroup",
                json!([
                    { "first": "Bob", "last": "Smith" }
                ]),
            );
            writer.commit().unwrap();
        }

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        // child query => first=Bob
        let child_q = TermQuery::new(
            Term::from_field_text(first_f, "Bob"),
            IndexRecordOption::Basic,
        );

        let nested_q = NestedQuery::new(
            "unmapped".into(), // does not exist in nested_paths
            Box::new(child_q),
            NestedScoreMode::None,
            true, // ignore_unmapped
        );

        // => no error => zero hits
        let top_docs = searcher
            .search(&nested_q, &TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(
            top_docs.len(),
            0,
            "Expected zero hits for ignore_unmapped=true + unknown path"
        );
    }

    #[test]
    fn test_ignore_unmapped_false_error() {
        // If we specify path="unmapped" but ignore_unmapped=false => expect an error
        let (schema, _user_nested_f, first_f, _last_f, _group_f) = make_schema_for_eq_tests();
        let index = Index::create_in_ram(schema.clone());

        // no docs needed
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let child_q = TermQuery::new(
            Term::from_field_text(first_f, "Anything"),
            IndexRecordOption::Basic,
        );
        let nested_q = NestedQuery::new(
            "unmapped".into(),
            Box::new(child_q),
            NestedScoreMode::None,
            false,
        );

        let result = searcher.search(&nested_q, &TopDocs::with_limit(10));
        match result {
            Ok(_) => panic!("Expected an error for path=unmapped + ignore_unmapped=false"),
            Err(e) => {
                let msg = format!("{:?}", e);
                assert!(msg.contains("not mapped") && !msg.contains("ignore_unmapped=true"));
            }
        }
    }

    #[test]
    fn test_nested_query_some_match() -> crate::Result<()> {
        // If path="user" is found, we match doc #1 but not doc #2
        let (schema, _user_nested_f, first_f, last_f, group_f) = make_schema_for_eq_tests();
        let index = Index::create_in_ram(schema.clone());

        {
            let mut writer = index.writer_for_tests()?;
            // doc1 => group="fans", user => (Bob, Alice)
            index_doc_for_eq_tests(
                &mut writer,
                &schema,
                "fans",
                json!([
                    {"first":"Bob","last":"Smith"},
                    {"first":"Alice","last":"Branson"}
                ]),
            );
            // doc2 => group="boring", user => (John)
            index_doc_for_eq_tests(
                &mut writer,
                &schema,
                "boring",
                json!([
                    {"first":"John","last":"Legend"}
                ]),
            );
            writer.commit()?;
        }

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // child query => first=Alice
        let tq = TermQuery::new(
            Term::from_field_text(first_f, "Alice"),
            IndexRecordOption::Basic,
        );
        let nested_q = NestedQuery::new("user".into(), Box::new(tq), NestedScoreMode::Avg, false);

        let top_docs = searcher.search(&nested_q, &TopDocs::with_limit(10))?;
        // => doc1 is matched because user array has (Alice)
        // => doc2 no match
        assert_eq!(1, top_docs.len());
        let (score, addr) = top_docs[0];
        let stored_doc: TantivyDocument = searcher.doc(addr)?;
        let group_vals = stored_doc
            .get_all(group_f)
            .map(|v| v.as_str())
            .collect::<Vec<_>>();
        assert_eq!(group_vals, vec![Some("fans")]);
        Ok(())
    }

    #[test]
    fn test_no_child_match() -> crate::Result<()> {
        // No child match => zero parents
        let (schema, _user_nested_f, first_f, _last_f, _group_f) = make_schema_for_eq_tests();
        let index = Index::create_in_ram(schema.clone());

        // doc => user => Alice
        {
            let mut writer = index.writer_for_tests()?;
            index_doc_for_eq_tests(
                &mut writer,
                &schema,
                "someGroup",
                json!([{ "first":"Alice"}]),
            );
            writer.commit()?;
        }

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // but we search for first=Nope
        let child_q = TermQuery::new(
            Term::from_field_text(first_f, "Nope"),
            IndexRecordOption::Basic,
        );
        let nested_q = NestedQuery::new(
            "user".into(),
            Box::new(child_q),
            NestedScoreMode::None,
            false,
        );
        let hits = searcher.search(&nested_q, &TopDocs::with_limit(10))?;
        assert_eq!(hits.len(), 0);
        Ok(())
    }

    #[test]
    fn test_nested_score_modes() -> crate::Result<()> {
        // We'll ensure that if there are multiple children for a single parent,
        // we can see that the aggregator is properly used.

        // In Tantivy, you can’t trivially see child doc scores unless your child query has e.g. a TF-based or custom scorer.
        // We’ll do a contrived example with one child having a term that has higher IDF than the other.

        // For simplicity, we’ll just confirm that we got 1 parent, and let the aggregator do something
        // minimal. If you want to truly test sum/avg, you'd need to re-check parent doc’s actual score.
        let (schema, _nested_f, first_f, _last_f, group_f) = make_schema_for_eq_tests();
        let index = Index::create_in_ram(schema.clone());

        {
            let mut writer = index.writer_for_tests()?;
            // doc => user => child0 => first=java, child1 => first=java, child2 => first=rust
            // so we have multiple child docs for the same parent
            index_doc_for_eq_tests(
                &mut writer,
                &schema,
                "someGroup",
                json!([
                    {"first":"java"},
                    {"first":"java"},
                    {"first":"rust"}
                ]),
            );
            writer.commit()?;
        }

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // We match children with first=java
        let tq = TermQuery::new(
            Term::from_field_text(first_f, "java"),
            IndexRecordOption::Basic,
        );
        for &mode in &[
            NestedScoreMode::None,
            NestedScoreMode::Sum,
            NestedScoreMode::Avg,
            NestedScoreMode::Max,
            NestedScoreMode::Min,
        ] {
            let nested_q = NestedQuery::new("user".into(), Box::new(tq.box_clone()), mode, false);
            let hits = searcher.search(&nested_q, &TopDocs::with_limit(10))?;
            assert_eq!(1, hits.len());
            // We won't compare the parent's actual final score, but you can check:
            let (score, addr) = hits[0];
            let doc: TantivyDocument = searcher.doc(addr)?;
            let group_vals = doc.get_all(group_f).map(|v| v.as_str()).collect::<Vec<_>>();
            assert_eq!(group_vals, vec![Some("someGroup")]);
        }
        Ok(())
    }

    // Additional tests can replicate ES’s
    // “testMinFromString/testMaxFromString/testAvgFromString/testSumFromString/testNoneFromString”
    // to confirm NestedScoreMode::from_str(...) works.
    #[test]
    fn test_nested_score_mode_parsing() {
        assert_eq!(
            NestedScoreMode::from_str("none").unwrap(),
            NestedScoreMode::None
        );
        assert_eq!(
            NestedScoreMode::from_str("avg").unwrap(),
            NestedScoreMode::Avg
        );
        assert_eq!(
            NestedScoreMode::from_str("max").unwrap(),
            NestedScoreMode::Max
        );
        assert_eq!(
            NestedScoreMode::from_str("min").unwrap(),
            NestedScoreMode::Min
        );
        assert_eq!(
            NestedScoreMode::from_str("sum").unwrap(),
            NestedScoreMode::Sum
        );

        // unknown => error
        let err = NestedScoreMode::from_str("garbage").unwrap_err();
        assert!(err.contains("Unrecognized nested score_mode"));
    }
}

#[cfg(test)]
mod nested_query_extended_examples {
    use super::*;
    use crate::collector::TopDocs;
    use crate::query::{
        nested_query::{NestedQuery, NestedScoreMode},
        Query, TermQuery,
    };
    use crate::schema::{
        DocParsingError, Field, FieldType, IndexRecordOption, NestedOptions, Schema, SchemaBuilder,
        TantivyDocument, TextOptions, Value, STORED, STRING,
    };
    use crate::{Index, IndexWriter, Term};
    use serde_json::json;

    // --------------------------------------------------------------------------
    // 1) Multi-level nested queries example (similar to "drivers" in the ES docs)
    // --------------------------------------------------------------------------

    /// Builds an index schema like:
    ///
    ///  driver: {
    ///    type: nested,
    ///    properties: {
    ///       last_name: text
    ///       vehicle: { type: nested
    ///          properties: { make: text, model: text }
    ///       }
    ///    }
    ///  }
    ///
    /// We'll also add a top-level "misc" field or something, if we want.
    fn make_multi_level_schema() -> (Schema, Field, Field, Field, Field, Field, Field) {
        let mut builder = Schema::builder();

        // A top-level stored field, just for demonstration
        let doc_tag_field = builder.add_text_field("doc_tag", STRING | STORED);

        // 1) The first nested field => "driver"
        let driver_nested_opts = NestedOptions::new()
            .set_include_in_parent(false)
            .set_store_parent_flag(true);
        let driver_field = builder.add_nested_field("driver", driver_nested_opts);

        // If you want "driver.last_name" to exist as a child field,
        // just add a text field named "driver.last_name" (not a nested field):
        let last_name_field = builder.add_text_field("driver.last_name", STRING);

        // 2) The second nested field => literally "driver.vehicle"
        // so the path is "driver.vehicle" in the queries
        let vehicle_nested_opts = NestedOptions::new()
            .set_include_in_parent(false)
            .set_store_parent_flag(true);
        let vehicle_field = builder.add_nested_field("driver.vehicle", vehicle_nested_opts);

        // Then the subfields for "driver.vehicle.make" and "driver.vehicle.model":
        let make_field = builder.add_text_field("driver.vehicle.make", STRING);
        let model_field = builder.add_text_field("driver.vehicle.model", STRING);

        let schema = builder.build();
        (
            schema,
            doc_tag_field,
            driver_field,
            last_name_field,
            vehicle_field,
            make_field,
            model_field,
        )
    }

    /// Expands JSON like:
    /// {
    ///   "doc_tag": "DocA",
    ///   "driver": {
    ///       "last_name": "McQueen",
    ///       "vehicle": [
    ///          {"make":"Powell Motors","model":"Canyonero"},
    ///          {"make":"Miller-Meteor","model":"Ecto-1"}
    ///       ]
    ///   }
    /// }
    ///
    /// into multiple doc blocks. We then add them all at once.
    fn index_doc_multi_level(
        writer: &mut IndexWriter,
        schema: &Schema,
        doc_tag: &str,
        last_name: &str,
        vehicles: serde_json::Value,
    ) -> Result<(), DocParsingError> {
        let doc_obj = json!({
            "doc_tag": doc_tag,
            "driver": {
                "last_name": last_name,
                "vehicle": vehicles
            }
        });
        let doc_str = serde_json::to_string(&doc_obj).unwrap();
        let expanded_docs = TantivyDocument::parse_json_for_nested(schema, &doc_str)?;
        let docs: Vec<_> = expanded_docs.into_iter().map(|d| d.into()).collect();
        writer.add_documents(docs).unwrap();
        Ok(())
    }

    /// Test that we can do multi-level nested queries:
    /// Path=driver => child query => "nested => path=driver.vehicle => must => { ... }"
    /// We'll match the doc that has vehicle=Powell Motors => model=Canyonero
    #[test]
    fn test_multi_level_nested_query() -> crate::Result<()> {
        let (
            schema,
            doc_tag_field,
            driver_field,
            last_name_field,
            vehicle_field,
            make_field,
            model_field,
        ) = make_multi_level_schema();

        let index = Index::create_in_ram(schema.clone());
        {
            let mut writer = index.writer_for_tests()?;

            // doc #1 => doc_tag="Doc1", driver.last_name="McQueen"
            //  driver.vehicle => [ {make=Powell Motors, model=Canyonero}, {make=Miller-Meteor, model=Ecto-1} ]
            index_doc_multi_level(
                &mut writer,
                &schema,
                "Doc1",
                "McQueen",
                json!([
                    { "make":"Powell Motors", "model":"Canyonero"},
                    { "make":"Miller-Meteor", "model":"Ecto-1"}
                ]),
            )?;

            // doc #2 => doc_tag="Doc2", driver.last_name="Hudson"
            //  driver.vehicle => [ {make=Mifune, model=Mach Five}, {make=Miller-Meteor, model=Ecto-1} ]
            index_doc_multi_level(
                &mut writer,
                &schema,
                "Doc2",
                "Hudson",
                json!([
                    { "make":"Mifune", "model":"Mach Five" },
                    { "make":"Miller-Meteor", "model":"Ecto-1" }
                ]),
            )?;

            writer.commit()?;
        }

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // We'll do a multi-level nested query:
        //   nested(path="driver") -> child = nested(path="driver.vehicle") -> child= bool must => match make=Powell Motors, model=Canyonero
        use crate::query::{BooleanQuery, Occur};

        let tq_make = TermQuery::new(
            Term::from_field_text(make_field, "Powell Motors"),
            IndexRecordOption::Basic,
        );
        let tq_model = TermQuery::new(
            Term::from_field_text(model_field, "Canyonero"),
            IndexRecordOption::Basic,
        );
        let bool_sub = BooleanQuery::new(vec![
            (Occur::Must, Box::new(tq_make)),
            (Occur::Must, Box::new(tq_model)),
        ]);

        let vehicle_nested = NestedQuery::new(
            "driver.vehicle".into(),
            Box::new(bool_sub),
            NestedScoreMode::None,
            false,
        );
        let driver_nested = NestedQuery::new(
            "driver".into(),
            Box::new(vehicle_nested),
            NestedScoreMode::None,
            false,
        );

        let hits = searcher.search(&driver_nested, &TopDocs::with_limit(10))?;
        assert_eq!(1, hits.len(), "Only doc #1 should match this criteria");
        let (score, addr) = hits[0];

        //  -- Type annotation fix for E0282:
        let stored: TantivyDocument = searcher.doc(addr)?;
        let doc_tags = stored
            .get_all(doc_tag_field)
            .map(|v| v.as_str())
            .collect::<Vec<_>>();
        assert_eq!(doc_tags, vec![Some("Doc1")]);

        Ok(())
    }

    // --------------------------------------------------------------------------
    // 2) must_not clauses and nested queries example (the "comments" scenario)
    // --------------------------------------------------------------------------

    /// Build a schema with "comments" => nested => properties: author => text
    /// We'll also have a top-level "doc_num" so we can identify the doc easily in test results.
    fn make_comments_schema() -> (Schema, Field, Field, Field) {
        let mut builder = Schema::builder();

        // doc_num => top-level STORED
        let doc_num_field = builder.add_text_field("doc_num", STRING | STORED);

        // "comments" => nested
        let nested_opts = NestedOptions::new()
            .set_include_in_parent(false)
            .set_store_parent_flag(true);
        let comments_field = builder.add_nested_field("comments", nested_opts);

        // "comments.author" => text
        let author_field = builder.add_text_field("author", STRING);

        let schema = builder.build();
        (schema, doc_num_field, comments_field, author_field)
    }

    fn index_doc_with_comments(
        writer: &mut IndexWriter,
        schema: &Schema,
        doc_num: &str,
        comments: serde_json::Value,
    ) -> Result<(), DocParsingError> {
        let doc_obj = json!({
            "doc_num": doc_num,
            "comments": comments
        });
        let doc_str = serde_json::to_string(&doc_obj).unwrap();
        let expanded = TantivyDocument::parse_json_for_nested(schema, &doc_str)?;
        let docs: Vec<_> = expanded.into_iter().map(|d| d.into()).collect();
        writer.add_documents(docs).unwrap();
        Ok(())
    }

    // /// Reproduce the "must_not" clauses example from the docs:
    // ///
    // /// doc #1 => comments=[{author=kimchy}]
    // /// doc #2 => comments=[{author=kimchy},{author=nik9000}]
    // /// doc #3 => comments=[{author=nik9000}]
    // ///
    // /// Then a nested query => must_not => term => "comments.author=nik9000"
    // /// => returns doc1 + doc2, because doc2 has a child doc= kimchy that doesn’t match must_not => we ignore the nik9000 child that does match the must_not.
    // /// doc3 is not returned => because the single child doc is nik9000, which is disallowed => so doc3 fails the nested query.
    // ///
    // /// Then we do the second approach: an outer must_not => nested => term => ...
    // /// => that excludes any doc that has *any* child doc with "nik9000."
    // #[test]
    // fn test_comments_must_not_nested() -> crate::Result<()> {
    //     let (schema, doc_num_f, comments_f, author_f) = make_comments_schema();
    //     let index = Index::create_in_ram(schema.clone());

    //     // Build docs
    //     {
    //         let mut writer = index.writer_for_tests()?;
    //         // doc #1 => doc_num=1 => comments=[kimchy]
    //         index_doc_with_comments(
    //             &mut writer,
    //             &schema,
    //             "1",
    //             json!([
    //                 {"author":"kimchy"}
    //             ]),
    //         )?;
    //         // doc #2 => doc_num=2 => comments=[kimchy, nik9000]
    //         index_doc_with_comments(
    //             &mut writer,
    //             &schema,
    //             "2",
    //             json!([
    //                 {"author":"kimchy"},
    //                 {"author":"nik9000"}
    //             ]),
    //         )?;
    //         // doc #3 => doc_num=3 => comments=[nik9000]
    //         index_doc_with_comments(
    //             &mut writer,
    //             &schema,
    //             "3",
    //             json!([
    //                 {"author":"nik9000"}
    //             ]),
    //         )?;
    //         writer.commit()?;
    //     }

    //     let reader = index.reader()?;
    //     let searcher = reader.searcher();

    //     // 1) The "outer" query is nested => path=comments => bool => must_not => [term => comments.author=nik9000]
    //     // Because it's "nested => must_not => childTerm," doc #2 is STILL included if it has *some child* that doesn't match must_not
    //     // doc #3 fails because its single child is "nik9000" => that child triggers must_not, so doc #3 is not included
    //     use crate::query::{BooleanQuery, Occur};

    //     let tq_nik = TermQuery::new(
    //         Term::from_field_text(author_f, "nik9000"),
    //         IndexRecordOption::Basic,
    //     );
    //     let mut_not = BooleanQuery::new(vec![(Occur::MustNot, Box::new(tq_nik))]);
    //     let nested_query = NestedQuery::new(
    //         "comments".into(),
    //         Box::new(mut_not),
    //         NestedScoreMode::None,
    //         false,
    //     );

    //     let hits = searcher.search(&nested_query, &TopDocs::with_limit(10))?;
    //     // => doc #1 => has child=kimchy => doesn't violate must_not => included
    //     // => doc #2 => has child=kimchy and child=nik9000 => the child with nik9000 violates must_not, but the child with kimchy "matches"? Actually in a "nested => must_not" scenario, if ANY child doc triggers must_not, you might think doc fails. But the example from the ES docs says doc #2 is STILL returned because the child with nik9000 doesn't match the must query. In short, each child doc is tested. The doc #2 has child #0 => kimchy => passes must_not??? Actually let's see.
    //     // The official example says doc #2 is included because doc #2 has at least one child doc that doesn't match the must_not.
    //     // => doc #3 => only child= nik9000 => triggers must_not => doc #3 is excluded
    //     assert_eq!(2, hits.len(), "We expect doc #1, doc #2 => #3 excluded");

    //     // Let's confirm the doc_nums
    //     // Sort them just to be consistent
    //     let doc_nums: Vec<String> = hits
    //         .iter()
    //         .map(|(_score, addr)| {
    //             let stored: TantivyDocument = searcher.doc(*addr).unwrap();
    //             stored
    //                 .get_first(doc_num_f)
    //                 .unwrap()
    //                 .as_str()
    //                 .unwrap()
    //                 .to_string()
    //         })
    //         .collect();

    //     assert!(
    //         doc_nums.contains(&"1".to_string()) && doc_nums.contains(&"2".to_string()),
    //         "Doc #1 and #2 were matched, doc #3 was excluded"
    //     );

    //     // 2) The second approach: use an outer bool => must_not => [ nested => path=comments => term => comments.author=nik9000].
    //     // => that excludes ANY doc that has a child with "nik9000."
    //     let tq_nik2 = TermQuery::new(
    //         Term::from_field_text(author_f, "nik9000"),
    //         IndexRecordOption::Basic,
    //     );
    //     let nested2 = NestedQuery::new(
    //         "comments".into(),
    //         Box::new(tq_nik2),
    //         NestedScoreMode::None,
    //         false,
    //     );
    //     let bool_q = BooleanQuery::new(vec![(Occur::MustNot, Box::new(nested2))]);

    //     let hits2 = searcher.search(&bool_q, &TopDocs::with_limit(10))?;
    //     // => doc #1 => no child with nik => included
    //     // => doc #2 => has child with nik => excluded
    //     // => doc #3 => has child with nik => excluded
    //     assert_eq!(1, hits2.len(), "Only doc #1 remains");
    //     let (score, addr) = hits2[0];
    //     let doc_stored: TantivyDocument = searcher.doc(addr)?;
    //     let doc_num = doc_stored
    //         .get_first(doc_num_f)
    //         .map(|v| v.as_str().unwrap().to_string())
    //         .unwrap();
    //     assert_eq!("1", doc_num);
    //     Ok(())
    // }
}
