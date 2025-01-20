// src/query/nested_query.rs

use crate::{
    doc,
    query::{EnableScoring, Explanation, Query, QueryClone, Scorer, TermQuery, Weight},
    schema::{Field, FieldType, Term},
    DocAddress, DocId, DocSet, Score, Searcher, SegmentReader, TERMINATED,
};
use common::BitSet;
use std::fmt;
use std::sync::Arc;

/// Score modes for how to compute the parent's score from matched children
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NestedScoreMode {
    Avg,
    Max,
    Sum,
    None,
}

/// A struct to hold `inner_hits` data. In a real system, you might store more details.
#[derive(Clone, Debug, Default)]
pub struct InnerHits {
    pub child_doc_ids: Vec<DocId>,
    // Could store child scores, fields, highlights, etc.
}

impl InnerHits {
    pub fn new() -> Self {
        InnerHits {
            child_doc_ids: vec![],
        }
    }
}

/// A “production ready” NestedQuery that:
/// - identifies parents via a hidden boolean field
/// - runs a child query
/// - aggregates child matches in a block up to the parent doc
/// - optionally collects `inner_hits` for each matched parent
pub struct NestedQuery {
    /// The child-level query
    child_query: Box<dyn Query>,

    /// The Field that indicates if doc is a parent (bool).
    parent_flag_field: Field,

    /// The chosen scoring mode
    score_mode: NestedScoreMode,

    /// Collect the child doc IDs in `inner_hits`?
    collect_inner_hits: bool,
}

impl NestedQuery {
    pub fn new(
        child_query: Box<dyn Query>,
        parent_flag_field: Field,
        score_mode: NestedScoreMode,
        collect_inner_hits: bool,
    ) -> Self {
        println!(
            "NestedQuery::new => child_query={:?}, parent_flag_field={:?}, score_mode={:?}, collect_inner_hits={}",
            child_query, parent_flag_field, score_mode, collect_inner_hits
        );
        NestedQuery {
            child_query,
            parent_flag_field,
            score_mode,
            collect_inner_hits,
        }
    }
}

impl fmt::Debug for NestedQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NestedQuery")
            .field("child_query", &"...")
            .field("parent_flag_field", &self.parent_flag_field.field_id())
            .field("score_mode", &self.score_mode)
            .field("collect_inner_hits", &self.collect_inner_hits)
            .finish()
    }
}

impl Clone for NestedQuery {
    fn clone(&self) -> Self {
        NestedQuery {
            child_query: self.child_query.box_clone(),
            parent_flag_field: self.parent_flag_field,
            score_mode: self.score_mode,
            collect_inner_hits: self.collect_inner_hits,
        }
    }
}

impl Query for NestedQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let child_weight = self.child_query.weight(enable_scoring)?;
        // We also build a TermQuery or a specialized query for `parent_flag_field == true`
        // so we can identify all parent docs. The field must be typed boolean in your schema.
        let term = Term::from_field_bool(self.parent_flag_field, true);
        let parents_filter = Box::new(TermQuery::new(
            term,
            crate::schema::IndexRecordOption::Basic,
        ));
        let parents_weight = parents_filter.weight(enable_scoring)?;

        Ok(Box::new(NestedWeight {
            child_weight,
            parents_weight,
            parent_flag_field: self.parent_flag_field,
            score_mode: self.score_mode,
            collect_inner_hits: self.collect_inner_hits,
        }))
    }

    fn explain(&self, searcher: &Searcher, doc_address: DocAddress) -> crate::Result<Explanation> {
        // We can create a scorer and see if doc_address is matched.
        let reader = searcher.segment_reader(doc_address.segment_ord);
        let mut scorer = self
            .weight(EnableScoring::enabled_from_searcher(searcher))?
            .scorer(reader, 1.0)?;

        let mut doc_id = scorer.doc();
        while doc_id != TERMINATED && doc_id < doc_address.doc_id {
            doc_id = scorer.advance();
        }

        let score = if doc_id == doc_address.doc_id {
            scorer.score()
        } else {
            0.0
        };
        Ok(Explanation::new("NestedQuery Explanation", score))
    }

    fn count(&self, searcher: &Searcher) -> crate::Result<usize> {
        let weight = self.weight(EnableScoring::disabled_from_searcher(searcher))?;
        let mut total_count = 0;
        for reader in searcher.segment_readers() {
            total_count += weight.count(reader)? as usize;
        }
        Ok(total_count)
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        self.child_query.query_terms(visitor);
        // We also have a hidden parent query we built, if you want to reveal it:
        // but typically we skip that for user-level query terms
    }
}

/// The Weighted structure for our NestedQuery
pub struct NestedWeight {
    child_weight: Box<dyn Weight>,
    parents_weight: Box<dyn Weight>,
    parent_flag_field: Field,
    score_mode: NestedScoreMode,
    collect_inner_hits: bool,
}

impl Weight for NestedWeight {
    fn scorer(&self, reader: &SegmentReader, boost: f32) -> crate::Result<Box<dyn Scorer>> {
        // 1) identify parents (docs where parent_flag_field == true)
        let max_doc = reader.max_doc();
        let mut parent_bitset = BitSet::with_max_value(max_doc);
        println!("NestedWeight::scorer => building parent_bitset, max_doc={}", max_doc);

        let mut parent_scorer = self.parents_weight.scorer(reader, boost)?;
        let mut found_parent = false;

        while parent_scorer.doc() != TERMINATED {
            let doc_id = parent_scorer.doc();
            println!("Found parent doc_id={}", doc_id);
            parent_bitset.insert(doc_id);
            parent_scorer.advance();
            found_parent = true;
        }

        if !found_parent {
            println!("No parents => returning EmptyScorer");
            // No parent => no matches
            return Ok(Box::new(EmptyScorer));
        }

        // 2) build the child scorer
        let child_scorer = self.child_weight.scorer(reader, boost)?;

        // 3) build a NestedScorer
        let scorer = NestedScorer {
            parent_docs: parent_bitset,
            child_scorer,
            score_mode: self.score_mode,
            collect_inner_hits: self.collect_inner_hits,
            current_parent: TERMINATED,
            doc_has_more: true,
            initialized: false,
            last_collected_hits: InnerHits::new(),
            current_score: 0.0,
        };

        Ok(Box::new(scorer))
    }

    fn explain(&self, _reader: &SegmentReader, _doc: DocId) -> crate::Result<Explanation> {
        Ok(Explanation::new(
            "NestedWeight: no explicit explanation",
            0.0,
        ))
    }

    fn count(&self, reader: &SegmentReader) -> crate::Result<u32> {
        let mut scorer = self.scorer(reader, 1.0)?;
        let mut count = 0u32;
        while scorer.doc() != TERMINATED {
            count += 1;
            scorer.advance();
        }
        Ok(count)
    }
}

struct EmptyScorer;
impl DocSet for EmptyScorer {
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
impl Scorer for EmptyScorer {
    fn score(&mut self) -> Score {
        0.0
    }
}

/// The main scorer for NestedQuery
pub struct NestedScorer {
    parent_docs: BitSet,
    child_scorer: Box<dyn Scorer>,
    score_mode: NestedScoreMode,
    collect_inner_hits: bool,

    /// The docID of the current matched parent
    current_parent: DocId,

    doc_has_more: bool,
    initialized: bool,

    /// For highlighting or debugging
    last_collected_hits: InnerHits,

    /// Current parent's aggregated score
    current_score: f32,
}

impl DocSet for NestedScorer {
    fn advance(&mut self) -> DocId {
        println!("NestedScorer::advance called, current_parent={}, doc_has_more={}, initialized={}", 
                self.current_parent, self.doc_has_more, self.initialized);
        if !self.doc_has_more {
            println!("No more docs, returning TERMINATED");
            return TERMINATED;
        }
        if !self.initialized {
            println!("First advance() call, initializing");
            self.initialized = true;
            // child_scorer at first doc
            self.child_scorer.advance();
        }
        // find the next parent doc
        let next_parent = self.find_next_parent(match self.current_parent {
            TERMINATED => 0,
            _ => self.current_parent + 1,
        });
        if next_parent == TERMINATED {
            self.doc_has_more = false;
            self.current_parent = TERMINATED;
            return TERMINATED;
        }
        self.current_parent = next_parent;
        self.collect_children_for_parent();
        self.current_parent
    }

    fn doc(&self) -> DocId {
        if self.doc_has_more {
            self.current_parent
        } else {
            TERMINATED
        }
    }

    fn size_hint(&self) -> u32 {
        self.parent_docs.len() as u32
    }
}

impl Scorer for NestedScorer {
    fn score(&mut self) -> Score {
        self.current_score
    }
}

impl NestedScorer {
    fn find_next_parent(&self, start_doc: DocId) -> DocId {
        println!("find_next_parent: searching from start_doc={}", start_doc);
        let max_val = self.parent_docs.max_value();
        let mut d = start_doc;
        while d <= max_val {
            if self.parent_docs.contains(d) {
                println!("find_next_parent: found parent at doc_id={}", d);
                return d;
            }
            d += 1;
        }
        println!("find_next_parent: no more parents found, returning TERMINATED");
        TERMINATED
    }

    fn collect_children_for_parent(&mut self) {
        println!("collect_children_for_parent: starting collection for parent={}", self.current_parent);
        self.last_collected_hits.child_doc_ids.clear();

        let start_doc = if self.current_parent == 0 {
            0
        } else {
            // We want child docs in the range (prev_parent+1 .. current_parent).
            // But for simplicity, we just keep advancing the child_scorer while child_scorer.doc < current_parent
            0
        };
        println!("collect_children_for_parent: searching from start_doc={} to parent={}", start_doc, self.current_parent);

        // gather all child docs in [start_doc .. parent_doc)
        let mut child_scores = vec![];

        while self.child_scorer.doc() != TERMINATED && self.child_scorer.doc() < self.current_parent
        {
            let cd = self.child_scorer.doc();
            println!("Examining potential child doc_id={}", cd);
            // But skip if there's an intervening parent doc
            if !self.is_intervening_parent(cd, self.current_parent) {
                let s = self.child_scorer.score();
                println!("Found valid child doc_id={} with score={}", cd, s);
                child_scores.push(s);
                if self.collect_inner_hits {
                    self.last_collected_hits.child_doc_ids.push(cd);
                    println!("Added to inner_hits, current child_doc_ids={:?}", self.last_collected_hits.child_doc_ids);
                }
            } else {
                println!("Skipping doc_id={} due to intervening parent", cd);
            }
            self.child_scorer.advance();
        }

        if child_scores.is_empty() {
            println!("No valid children found, setting score=0.0");
            self.current_score = 0.0;
        } else {
            println!("Computing final score from child_scores={:?}", child_scores);
            self.current_score = match self.score_mode {
                NestedScoreMode::Avg => {
                    let sum: f32 = child_scores.iter().sum();
                    sum / (child_scores.len() as f32)
                }
                NestedScoreMode::Max => child_scores.into_iter().fold(std::f32::MIN, f32::max),
                NestedScoreMode::Sum => child_scores.into_iter().sum(),
                NestedScoreMode::None => 1.0,
            };
        }
    }

    /// Returns true if there's another parent doc p where cd < p < current_parent.
    fn is_intervening_parent(&self, cd: DocId, current_parent: DocId) -> bool {
        // This is the “block boundary” check
        for doc_id in (cd + 1)..current_parent {
            if self.parent_docs.contains(doc_id) {
                return true;
            }
        }
        false
    }
}

// tests/test_nested_query.rs
// Example standalone test file or test mod.

#[cfg(test)]
mod test_nested_query {
    use crate::{
        collector::TopDocs,
        doc,
        query::{BooleanQuery, EnableScoring, Occur, Query, TermQuery},
        schema::{
            Field, IndexRecordOption, NestedOptions, Schema, SchemaBuilder, Value, STORED, STRING,
            TEXT,
        },
        DocAddress, DocId, Index, IndexWriter, TantivyDocument, Term, TERMINATED,
    };

    use crate::query::nested_query::{NestedQuery, NestedScoreMode};

    /// Helper to create an index with a single "user" nested field (which automatically
    /// generates "_is_parent_user"). Also returns the `Field` for `_is_parent_user`.
    fn create_user_nested_index() -> (Index, Field, Field) {
        let mut builder = Schema::builder();
        // Normal field:
        let group_field = builder.add_text_field("group", STRING | STORED);

        // Add a nested field "user"
        // => auto-creates hidden bool field `_is_parent_user`.
        let nested_opts = NestedOptions::new()
            .set_include_in_parent(true)
            .set_store_parent_flag(true);
        builder.add_nested_field("user", nested_opts);

        // Build schema, now `_is_parent_user` was created automatically
        let schema = builder.build();
        let index = Index::create_in_ram(schema.clone());

        // retrieve the field IDs we need
        let group_field_id = schema.get_field("group").unwrap();
        let is_parent_user_field = schema.get_field("_is_parent_user").unwrap();

        (index, group_field_id, is_parent_user_field)
    }

    #[test]
    fn test_single_nested_block_multiple_children() -> crate::Result<()> {
        // Single doc with 2 child "user" objects + 1 parent doc block
        let (index, group_field, is_parent_user_field) = create_user_nested_index();
        let mut writer = index.writer_for_tests()?;

        // Instead of storing is_parent_user_field => "false", store bool => false
        let child1 = doc!(is_parent_user_field => false);
        let child2 = doc!(is_parent_user_field => false);
        let parent = doc!(
            group_field => "fans",
            is_parent_user_field => true
        );

        writer.add_documents(vec![child1, child2, parent])?;
        writer.commit()?;

        // Child query => "is_parent_user:false"
        let child_query = Box::new(TermQuery::new(
            Term::from_field_text(is_parent_user_field, "false"),
            IndexRecordOption::Basic,
        ));

        let nested_query = NestedQuery::new(
            child_query,
            is_parent_user_field,
            NestedScoreMode::None,
            true, // collect inner hits
        );

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let top_docs = searcher.search(&nested_query, &TopDocs::with_limit(10))?;
        // Expect 1 parent doc
        assert_eq!(top_docs.len(), 1);
        // Check group="fans"
        let (score, doc_addr) = top_docs[0];
        assert_eq!(score, 1.0);
        let doc_retrieved: TantivyDocument = searcher.doc(doc_addr)?;
        let group_val = doc_retrieved
            .get_first(group_field)
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(group_val, "fans");

        Ok(())
    }

    #[test]
    fn test_nested_must_clause_not_matching() -> crate::Result<()> {
        // We'll define a simple schema with group + "user" nested, plus "user.first", "user.last".
        let mut builder = Schema::builder();
        let group_field = builder.add_text_field("group", STRING | STORED);

        let nested_opts = NestedOptions::new().set_store_parent_flag(true);
        builder.add_nested_field("user", nested_opts);

        let first_field = builder.add_text_field("user.first", STRING);
        let last_field = builder.add_text_field("user.last", STRING);

        let schema = builder.build();
        let index = Index::create_in_ram(schema.clone());

        let is_parent_user_field = schema
            .get_field("_is_parent_user")
            .expect("auto-created by add_nested_field");
        let mut writer = index.writer_for_tests()?;

        // child doc #1 => user.first=John, user.last=Smith
        // child doc #2 => user.first=Alice, user.last=White
        // parent => group="fans"
        let child1 = doc!(
            is_parent_user_field => false,
            first_field => "John",
            last_field => "Smith"
        );
        let child2 = doc!(
            is_parent_user_field => false,
            first_field => "Alice",
            last_field => "White"
        );
        let parent = doc!(
            group_field => "fans",
            is_parent_user_field => true
        );
        writer.add_documents(vec![child1, child2, parent])?;
        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // must => first=Alice AND last=Smith => no single child doc has both => no match
        let child_bool_query_fail = {
            let first_term = Box::new(TermQuery::new(
                Term::from_field_text(first_field, "Alice"),
                IndexRecordOption::Basic,
            ));
            let last_term = Box::new(TermQuery::new(
                Term::from_field_text(last_field, "Smith"),
                IndexRecordOption::Basic,
            ));
            let mut bool_q = BooleanQuery::new(vec![]);
            bool_q.add_clause(Occur::Must, first_term);
            bool_q.add_clause(Occur::Must, last_term);
            bool_q
        };
        let nested_query_fail = NestedQuery::new(
            Box::new(child_bool_query_fail),
            is_parent_user_field,
            NestedScoreMode::None,
            false,
        );

        let top_docs_fail = searcher.search(&nested_query_fail, &TopDocs::with_limit(10))?;
        assert_eq!(top_docs_fail.len(), 0);

        // must => first=Alice AND last=White => matches child2 => so parent matches
        let child_bool_query_ok = {
            let first_term = Box::new(TermQuery::new(
                Term::from_field_text(first_field, "Alice"),
                IndexRecordOption::Basic,
            ));
            let last_term = Box::new(TermQuery::new(
                Term::from_field_text(last_field, "White"),
                IndexRecordOption::Basic,
            ));
            let mut bool_q = BooleanQuery::new(vec![]);
            bool_q.add_clause(Occur::Must, first_term);
            bool_q.add_clause(Occur::Must, last_term);
            bool_q
        };
        let nested_query_ok = NestedQuery::new(
            Box::new(child_bool_query_ok),
            is_parent_user_field,
            NestedScoreMode::None,
            false,
        );

        let top_docs_ok = searcher.search(&nested_query_ok, &TopDocs::with_limit(10))?;
        assert_eq!(top_docs_ok.len(), 1);
        let (_score, docaddr) = top_docs_ok[0];
        let doc_retrieved: TantivyDocument = searcher.doc(docaddr)?;
        let group_val = doc_retrieved
            .get_first(group_field)
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(group_val, "fans");

        Ok(())
    }

    #[test]
    fn test_nested_must_not_clause() -> crate::Result<()> {
        // block #1 => child with author=kimchy, child with author=nik9000, parent => is_parent_user=true
        let mut builder = Schema::builder();
        builder.add_nested_field("user", NestedOptions::new().set_store_parent_flag(true));
        let author_field = builder.add_text_field("user.author", STRING);
        let schema = builder.build();
        let index = Index::create_in_ram(schema.clone());
        let is_parent_user_field = schema.get_field("_is_parent_user").unwrap();

        let mut writer = index.writer_for_tests()?;
        let child1 = doc!(
            is_parent_user_field => false,
            author_field => "kimchy"
        );
        let child2 = doc!(
            is_parent_user_field => false,
            author_field => "nik9000"
        );
        let parent = doc!(
            is_parent_user_field => true
        );
        writer.add_documents(vec![child1, child2, parent])?;
        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // child query => must_not => author=nik9000 => child1 matches => parent => returned
        let must_not_nik = {
            let t = Box::new(TermQuery::new(
                Term::from_field_text(author_field, "nik9000"),
                IndexRecordOption::Basic,
            ));
            let mut bq = BooleanQuery::new(vec![]);
            bq.add_clause(Occur::MustNot, t);
            bq
        };
        let nested_must_not = NestedQuery::new(
            Box::new(must_not_nik),
            is_parent_user_field,
            NestedScoreMode::None,
            false,
        );
        let top_docs = searcher.search(&nested_must_not, &TopDocs::with_limit(10))?;
        assert_eq!(top_docs.len(), 1);

        // If we want to exclude ANY doc that has a child "nik9000", do an outer must_not
        let exclude_any_nik = BooleanQuery::new(vec![(
            Occur::MustNot,
            Box::new(NestedQuery::new(
                Box::new(TermQuery::new(
                    Term::from_field_text(author_field, "nik9000"),
                    IndexRecordOption::Basic,
                )),
                is_parent_user_field,
                NestedScoreMode::None,
                false,
            )),
        )]);
        let top_docs_excl_nik = searcher.search(&exclude_any_nik, &TopDocs::with_limit(10))?;
        assert_eq!(top_docs_excl_nik.len(), 0);

        Ok(())
    }

    #[test]
    fn test_multi_level_nesting() -> crate::Result<()> {
        let mut builder = Schema::builder();
        builder.add_nested_field("user", NestedOptions::new().set_store_parent_flag(true));
        builder.add_nested_field(
            "user.vehicle",
            NestedOptions::new().set_store_parent_flag(true),
        );
        let top_field = builder.add_text_field("description", STORED | STRING);
        let schema = builder.build();
        let index = Index::create_in_ram(schema.clone());

        let is_parent_user_field = schema.get_field("_is_parent_user").unwrap();
        let is_parent_user_dot_vehicle_field = schema.get_field("_is_parent_user.vehicle").unwrap();

        let mut writer = index.writer_for_tests()?;

        // doc0 => user-child => is_parent_user=false
        // doc1 => vehicle-child => is_parent_user.vehicle=false
        // doc2 => user parent => is_parent_user=true, is_parent_user.vehicle=false
        // doc3 => top parent => is_parent_user.vehicle=true => "top-level doc"
        let child_user = doc!(is_parent_user_field => false);
        let child_vehicle = doc!(is_parent_user_dot_vehicle_field => false);
        let parent_user = doc!(
            is_parent_user_field => true,
            is_parent_user_dot_vehicle_field => false
        );
        let parent_top = doc!(
            is_parent_user_dot_vehicle_field => true,
            top_field => "top-level doc"
        );
        writer.add_documents(vec![child_user, child_vehicle, parent_user, parent_top])?;
        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // Step1: nested against user => doc with is_parent_user_field=false => that yields doc2
        let child_of_user = Box::new(TermQuery::new(
            Term::from_field_text(is_parent_user_field, "false"),
            IndexRecordOption::Basic,
        ));
        let nested_user = NestedQuery::new(
            child_of_user,
            is_parent_user_field,
            NestedScoreMode::None,
            false,
        );

        // Step2: that doc is itself "parent" for vehicle => is_parent_user.vehicle
        let nested_vehicle = NestedQuery::new(
            Box::new(nested_user),
            is_parent_user_dot_vehicle_field,
            NestedScoreMode::None,
            true,
        );

        let top_docs = searcher.search(&nested_vehicle, &TopDocs::with_limit(10))?;
        // Should match doc3 => "top-level doc"
        assert_eq!(top_docs.len(), 1);
        let (score, docaddr) = top_docs[0];
        let retrieved: TantivyDocument = searcher.doc(docaddr)?;
        assert_eq!(
            retrieved.get_first(top_field).unwrap().as_str().unwrap(),
            "top-level doc"
        );
        assert_eq!(score, 1.0);

        Ok(())
    }

    #[test]
    fn test_inner_hits_highlighting() -> crate::Result<()> {
        let mut builder = Schema::builder();
        let msg_field = builder.add_text_field("msg", STRING | STORED);
        builder.add_nested_field("comment", NestedOptions::new().set_store_parent_flag(true));
        let schema = builder.build();
        let index = Index::create_in_ram(schema.clone());

        let is_parent_comment_field = schema.get_field("_is_parent_comment").unwrap();

        let mut writer = index.writer_for_tests()?;

        let child1 = doc!(msg_field => "Hello", is_parent_comment_field => false);
        let child2 = doc!(msg_field => "World", is_parent_comment_field => false);
        let parent = doc!(is_parent_comment_field => true);
        writer.add_documents(vec![child1, child2, parent])?;
        writer.commit()?;

        let child_query = Box::new(TermQuery::new(
            Term::from_field_text(msg_field, "World"),
            IndexRecordOption::Basic,
        ));
        let nested_query = NestedQuery::new(
            child_query,
            is_parent_comment_field,
            NestedScoreMode::None,
            true,
        );

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let top_docs = searcher.search(&nested_query, &TopDocs::with_limit(10))?;
        assert_eq!(top_docs.len(), 1);
        let (score, docaddr) = top_docs[0];
        assert_eq!(score, 1.0);

        // Demonstrate manual iteration w/ scorer for "inner hits"
        use crate::query::EnableScoring;
        let weight = nested_query.weight(EnableScoring::disabled_from_searcher(&searcher))?;
        let segment_reader = searcher.segment_reader(docaddr.segment_ord);
        let mut scorer = weight.scorer(segment_reader, 1.0)?;

        let mut parents_found = 0;
        while scorer.doc() != TERMINATED {
            let doc_id = scorer.doc();
            if doc_id == docaddr.doc_id {
                parents_found += 1;
            }
            scorer.advance();
        }
        assert_eq!(parents_found, 1);

        Ok(())
    }

    #[test]
    fn test_nested_many_children() -> crate::Result<()> {
        let mut builder = Schema::builder();
        builder.add_text_field("group", STRING | STORED);
        builder.add_nested_field("user", NestedOptions::new().set_store_parent_flag(true));
        let schema = builder.build();
        let index = Index::create_in_ram(schema.clone());

        let is_parent_user_field = schema.get_field("_is_parent_user").unwrap();
        let group_field = schema.get_field("group").unwrap();

        let mut writer = index.writer_for_tests()?;

        let mut docs = Vec::new();
        // 100 child docs => is_parent_user=false
        for _i in 0..100 {
            let child_doc = doc!(is_parent_user_field => false);
            docs.push(child_doc);
        }
        let parent_doc = doc!(
            group_field => "OneParent",
            is_parent_user_field => true
        );
        docs.push(parent_doc);
        writer.add_documents(docs)?;
        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        // We'll just do "child query => is_parent_user:false" => confirm we find 1 parent
        let child_query = Box::new(TermQuery::new(
            Term::from_field_text(is_parent_user_field, "false"),
            IndexRecordOption::Basic,
        ));
        let nested_q = NestedQuery::new(
            child_query,
            is_parent_user_field,
            NestedScoreMode::None,
            false,
        );
        let top_docs = searcher.search(&nested_q, &TopDocs::with_limit(10))?;
        assert_eq!(top_docs.len(), 1, "only one parent");
        let (_score, docaddr) = top_docs[0];
        let docret: TantivyDocument = searcher.doc(docaddr)?;
        let group_val = docret.get_first(group_field).unwrap().as_str().unwrap();
        assert_eq!(group_val, "OneParent");

        Ok(())
    }
}
