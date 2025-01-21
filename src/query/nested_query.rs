// // src/query/nested_query.rs

// use std::fmt;

// use crate::core::searcher::Searcher;
// use crate::query::{
//     BlockJoinQuery, BlockJoinScoreMode, EnableScoring, Explanation, Query, QueryClone, Scorer,
//     TermQuery, Weight,
// };
// use crate::schema::{Field, IndexRecordOption, Term};
// use crate::{DocAddress, DocId, Result, Score, SegmentReader, TERMINATED};

// use crate::query::block_join_query::{BlockJoinScorer, BlockJoinWeight};
// use crate::schema::Schema;
// use crate::{DocSet, TantivyError};

// /// How we score from the child docs up to the parent.
// #[derive(Clone, Copy, Debug, PartialEq)]
// pub enum NestedScoreMode {
//     /// Average all matched child doc scores.
//     Avg,
//     /// Take the maximum child doc score.
//     Max,
//     /// Sum the child doc scores.
//     Sum,
//     /// Don’t incorporate child scores—just use a fixed score for the matched parent.
//     None,
// }

// /// A struct to hold child doc IDs for "inner_hits".
// /// Expand or store more data if desired (scores, highlights, etc.).
// #[derive(Clone, Debug, Default)]
// pub struct NestedInnerHits {
//     pub child_doc_ids: Vec<DocId>,
// }

// /// A production-ready `NestedQuery` that:
// ///
// /// - Identifies parent docs via a hidden boolean field `_is_parent_<path>`.
// /// - Runs a `child_query` on child documents (those with `_is_parent_<path> = false`).
// /// - Aggregates child scores in the parent according to `NestedScoreMode`.
// /// - Optionally collects "inner hits" (child DocIDs) if `collect_inner_hits` is true.
// pub struct NestedQuery {
//     /// The name of the nested path (e.g. `"user"`), used only for debug or reference.
//     path: String,

//     /// The hidden boolean field used to identify parent docs for this nested path.
//     parent_flag_field: Field,

//     /// The query used to match child documents.
//     child_query: Box<dyn Query>,

//     /// Score mode describing how to aggregate scores from children into the parent.
//     score_mode: NestedScoreMode,

//     /// If true, we gather child doc IDs in a `NestedInnerHits` structure (not fully implemented).
//     collect_inner_hits: bool,
// }

// impl fmt::Debug for NestedQuery {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.debug_struct("NestedQuery")
//             .field("path", &self.path)
//             .field("child_query", &"...")
//             .field("score_mode", &self.score_mode)
//             .field("collect_inner_hits", &self.collect_inner_hits)
//             .finish()
//     }
// }

// impl NestedQuery {
//     /// Creates a new `NestedQuery`.
//     ///
//     /// - `schema`: The index schema, which must contain a boolean field named `"_is_parent_{path}"`.
//     /// - `path`: A string such as `"user"`.
//     /// - `child_query`: The query for matching child docs.
//     /// - `score_mode`: How child scores are aggregated.
//     /// - `collect_inner_hits`: If true, gather child doc IDs in the scorer (stub logic).
//     ///
//     /// Fails if the schema does not have the parent-flag field.
//     pub fn new(
//         schema: &Schema,
//         path: String,
//         child_query: Box<dyn Query>,
//         score_mode: NestedScoreMode,
//         collect_inner_hits: bool,
//     ) -> Result<NestedQuery> {
//         let parent_flag_name = format!("_is_parent_{}", path);
//         let parent_flag_field = schema.get_field(&parent_flag_name).map_err(|_err| {
//             TantivyError::SchemaError(format!(
//                 "Missing nested parent flag field: {parent_flag_name}"
//             ))
//         })?;

//         Ok(NestedQuery {
//             path,
//             parent_flag_field,
//             child_query,
//             score_mode,
//             collect_inner_hits,
//         })
//     }

//     /// Converts `NestedScoreMode` to a `BlockJoinScoreMode`.
//     fn to_block_join_score_mode(mode: NestedScoreMode) -> BlockJoinScoreMode {
//         match mode {
//             NestedScoreMode::Avg => BlockJoinScoreMode::Avg,
//             NestedScoreMode::Max => BlockJoinScoreMode::Max,
//             NestedScoreMode::Sum => BlockJoinScoreMode::Sum,
//             NestedScoreMode::None => BlockJoinScoreMode::None,
//         }
//     }
// }

// impl Clone for NestedQuery {
//     fn clone(&self) -> Self {
//         NestedQuery {
//             path: self.path.clone(),
//             parent_flag_field: self.parent_flag_field,
//             child_query: self.child_query.box_clone(),
//             score_mode: self.score_mode,
//             collect_inner_hits: self.collect_inner_hits,
//         }
//     }
// }

// impl Query for NestedQuery {
//     fn weight(&self, enable_scoring: EnableScoring<'_>) -> Result<Box<dyn Weight>> {
//         // Build a term for parent docs: _is_parent_<path> = true
//         let parent_flag_term = Term::from_field_bool(self.parent_flag_field, true);
//         let parent_query = TermQuery::new(parent_flag_term, IndexRecordOption::Basic);

//         // Wrap the child query in a `BlockJoinQuery`, filtering by the parent docs.
//         let block_join_query = BlockJoinQuery::new(
//             self.child_query.box_clone(),
//             Box::new(parent_query),
//             Self::to_block_join_score_mode(self.score_mode),
//         );

//         let block_join_weight = block_join_query.weight(enable_scoring)?;
//         Ok(Box::new(NestedWeight {
//             path: self.path.clone(),
//             block_join_weight,
//             nested_score_mode: self.score_mode,
//             collect_inner_hits: self.collect_inner_hits,
//         }))
//     }

//     fn explain(&self, searcher: &Searcher, doc_address: DocAddress) -> Result<Explanation> {
//         let w = self.weight(EnableScoring::enabled_from_searcher(searcher))?;
//         w.explain(
//             searcher.segment_reader(doc_address.segment_ord),
//             doc_address.doc_id,
//         )
//     }

//     fn count(&self, searcher: &Searcher) -> Result<usize> {
//         let w = self.weight(EnableScoring::disabled_from_searcher(searcher))?;
//         let mut total = 0;
//         for reader in searcher.segment_readers() {
//             total += w.count(reader)? as usize;
//         }
//         Ok(total)
//     }

//     fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
//         // Only expose child-query terms, not the parent-flag filter.
//         self.child_query.query_terms(visitor);
//     }
// }

// /// Weight for `NestedQuery`.
// struct NestedWeight {
//     path: String,
//     block_join_weight: Box<dyn Weight>,
//     nested_score_mode: NestedScoreMode,
//     collect_inner_hits: bool,
// }

// impl Weight for NestedWeight {
//     fn scorer(&self, reader: &SegmentReader, boost: Score) -> Result<Box<dyn Scorer>> {
//         let bj_scorer = self.block_join_weight.scorer(reader, boost)?;
//         Ok(Box::new(NestedScorer {
//             path: self.path.clone(),
//             bj_scorer,
//             collect_inner_hits: self.collect_inner_hits,
//             inner_hits_buffer: NestedInnerHits::default(),
//         }))
//     }

//     fn explain(&self, reader: &SegmentReader, doc: DocId) -> Result<Explanation> {
//         self.block_join_weight.explain(reader, doc)
//     }

//     fn count(&self, reader: &SegmentReader) -> Result<u32> {
//         self.block_join_weight.count(reader)
//     }
// }

// /// `NestedScorer` wraps a `BlockJoinScorer` and optionally collects child doc IDs.
// struct NestedScorer {
//     path: String,
//     bj_scorer: Box<dyn Scorer>,
//     collect_inner_hits: bool,
//     inner_hits_buffer: NestedInnerHits,
// }

// impl DocSet for NestedScorer {
//     fn advance(&mut self) -> DocId {
//         let parent_doc = self.bj_scorer.advance();
//         if parent_doc == TERMINATED {
//             return TERMINATED;
//         }

//         // If `collect_inner_hits` is false, we do nothing else.
//         if !self.collect_inner_hits {
//             return parent_doc;
//         }

//         // Here you'd gather child doc IDs from the block-join logic if we had it.
//         self.inner_hits_buffer.child_doc_ids.clear();

//         parent_doc
//     }

//     fn doc(&self) -> DocId {
//         self.bj_scorer.doc()
//     }

//     fn size_hint(&self) -> u32 {
//         self.bj_scorer.size_hint()
//     }
// }

// impl Scorer for NestedScorer {
//     fn score(&mut self) -> Score {
//         self.bj_scorer.score()
//     }
// }

// //
// // -----------------------------------------------------
// // Extensive tests for NestedQuery
// // -----------------------------------------------------
// //
// #[cfg(test)]
// mod tests {
//     use super::{NestedInnerHits, NestedQuery, NestedScoreMode};
//     use crate::collector::TopDocs;
//     use crate::query::nested_query::NestedScorer;
//     use crate::query::{BlockJoinScoreMode, BooleanQuery, EnableScoring, Occur, Query, TermQuery};
//     use crate::schema::{
//         Field, IndexRecordOption, Schema, SchemaBuilder, TantivyDocument, Term, Value, STRING,
//     };
//     use crate::DocSet;
//     use crate::{DocAddress, Index, IndexWriter, Score, TERMINATED};

//     /// Creates a schema with a nested field "user" => `_is_parent_user` as a bool field.
//     /// Also includes a normal text field "group".
//     ///
//     /// In production, you'd often do:
//     ///   builder.add_nested_field("user", NestedOptions::...)
//     /// But here we show a simpler demonstration: a bool field named "_is_parent_user".
//     fn create_nested_schema() -> (Schema, Field, Field) {
//         let mut builder = SchemaBuilder::default();
//         let group_field = builder.add_text_field("group", STRING);
//         // Fix #1: make this a real boolean field, not a text field
//         let parent_flag_field = builder.add_bool_field("_is_parent_user", crate::schema::INDEXED);

//         let schema = builder.build();
//         (schema, group_field, parent_flag_field)
//     }

//     #[test]
//     fn test_nested_query_simple() -> crate::Result<()> {
//         // We'll mimic a single "block": 2 child docs + 1 parent doc:
//         //   doc0 => child1 => `_is_parent_user=false`
//         //   doc1 => child2 => `_is_parent_user=false`
//         //   doc2 => parent => `_is_parent_user=true`, group="fans"
//         let (schema, group_field, parent_flag_field) = create_nested_schema();
//         let index = Index::create_in_ram(schema.clone());

//         {
//             let mut writer = index.writer_for_tests()?;
//             // Use actual booleans rather than "false"/"true" strings
//             let child_doc1 = doc!( parent_flag_field => false );
//             let child_doc2 = doc!( parent_flag_field => false );
//             let parent_doc = doc!( parent_flag_field => true, group_field => "fans" );
//             writer.add_documents(vec![child_doc1, child_doc2, parent_doc])?;
//             writer.commit()?;
//         }

//         let reader = index.reader()?;
//         let searcher = reader.searcher();

//         // The child docs have _is_parent_user=false
//         // => so the child query is a bool term
//         let child_term = Term::from_field_bool(parent_flag_field, false);
//         let child_query = TermQuery::new(child_term, IndexRecordOption::Basic);

//         // Create our NestedQuery
//         // The `_is_parent_user` field is in the schema, so pass &schema + path="user"
//         let nested_query = NestedQuery::new(
//             &schema,
//             "user".to_string(),
//             Box::new(child_query),
//             NestedScoreMode::None,
//             false,
//         )?;

//         let top_docs = searcher.search(&nested_query, &TopDocs::with_limit(10))?;
//         assert_eq!(top_docs.len(), 1, "Expect 1 parent doc with group=fans");
//         let (score, doc_addr) = top_docs[0];
//         assert_eq!(
//             score, 1.0,
//             "Mode=None => parent doc gets a fixed score of 1.0"
//         );

//         let retrieved_doc: TantivyDocument = searcher.doc(doc_addr)?;
//         let group_val = retrieved_doc
//             .get_first(group_field)
//             .unwrap()
//             .as_str()
//             .unwrap();
//         assert_eq!(group_val, "fans");
//         Ok(())
//     }

//     #[test]
//     fn test_nested_query_with_scoring_avg() -> crate::Result<()> {
//         // Show how child frequency can influence parent's aggregated score
//         // We'll store "tag=java" multiple times in one child doc vs once in another.
//         let mut builder = SchemaBuilder::default();
//         let group_field = builder.add_text_field("group", STRING);
//         // We'll do a bool field named "_is_parent_foo"
//         let parent_flag_field = builder.add_bool_field("_is_parent_foo", crate::schema::INDEXED);
//         // Each child doc can store multiple "tag" values => changes freq => changes score
//         let tag_field = builder.add_text_field("tag", STRING);
//         let schema = builder.build();
//         let index = Index::create_in_ram(schema.clone());

//         {
//             let mut writer = index.writer_for_tests()?;
//             // child0 => "java" repeated 2x => higher freq => bigger score
//             let child0 = doc!(
//                 parent_flag_field => false,
//                 tag_field => "java",
//                 tag_field => "java"
//             );
//             // child1 => "java" repeated 1x => lower freq => smaller score
//             let child1 = doc!( parent_flag_field => false, tag_field => "java" );
//             let parent_doc = doc!( parent_flag_field => true, group_field => "baz" );

//             writer.add_documents(vec![child0, child1, parent_doc])?;
//             writer.commit()?;
//         }

//         let reader = index.reader()?;
//         let searcher = reader.searcher();

//         // The child query is "java" with WithFreqs => the doc that has 2 freq will get a higher child score
//         let child_term = Term::from_field_text(tag_field, "java");
//         let child_query = TermQuery::new(child_term, IndexRecordOption::WithFreqs);

//         // The nested path is `_is_parent_foo`, so path="foo"
//         // => internally, the parent filter is `_is_parent_foo==true`
//         let nested_query = NestedQuery::new(
//             &schema,
//             "foo".to_string(),
//             Box::new(child_query),
//             NestedScoreMode::Avg,
//             false,
//         )?;

//         let top_docs = searcher.search(&nested_query, &TopDocs::with_limit(1))?;
//         assert_eq!(top_docs.len(), 1);

//         let (score, doc_addr) = top_docs[0];
//         // The child's average should be > 1.0 if freq-based scoring is used.
//         assert!(
//             score > 1.0,
//             "Expected average child doc score above 1.0, got {score}"
//         );

//         let doc: TantivyDocument = searcher.doc(doc_addr)?;
//         let group_val = doc.get_first(group_field).unwrap().as_str().unwrap();
//         assert_eq!(group_val, "baz");
//         Ok(())
//     }

//     #[test]
//     fn test_nested_query_with_inner_hits() -> crate::Result<()> {
//         // Demonstrate usage with collect_inner_hits=true. The actual child doc IDs are not
//         // fully implemented in the code, but we show the pattern.
//         let (schema, group_field, parent_flag_field) = create_nested_schema();
//         let index = Index::create_in_ram(schema.clone());

//         {
//             let mut writer = index.writer_for_tests()?;
//             // doc0 => child=false, doc1 => child=false, doc2 => parent=true
//             writer.add_documents(vec![
//                 doc!(parent_flag_field => false),
//                 doc!(parent_flag_field => false),
//                 doc!(parent_flag_field => true, group_field => "fans"),
//             ])?;
//             writer.commit()?;
//         }

//         let reader = index.reader()?;
//         let searcher = reader.searcher();

//         // This child query matches docs with `_is_parent_user=false`
//         let child_term = Term::from_field_bool(parent_flag_field, false);
//         let child_query = TermQuery::new(child_term, IndexRecordOption::Basic);

//         // collect_inner_hits = true
//         let nested_query = NestedQuery::new(
//             &schema,
//             "user".to_string(),
//             Box::new(child_query),
//             NestedScoreMode::None,
//             true,
//         )?;

//         let weight = nested_query.weight(EnableScoring::disabled_from_searcher(&searcher))?;
//         let segment_reader = searcher.segment_reader(0);
//         let mut scorer = weight.scorer(segment_reader, 1.0)?;

//         let mut matched_parents = Vec::new();
//         while scorer.doc() != TERMINATED {
//             matched_parents.push(scorer.doc());
//             scorer.advance();
//         }
//         // The single parent doc is doc2
//         assert_eq!(matched_parents, vec![2]);
//         Ok(())
//     }

//     #[test]
//     fn test_nested_query_multi_block() -> crate::Result<()> {
//         // Two blocks of child docs, each ending in a parent doc:
//         //  doc0 => child=false
//         //  doc1 => child=false
//         //  doc2 => parent=true, group=alpha
//         //  doc3 => child=false
//         //  doc4 => parent=true, group=beta
//         let (schema, group_field, parent_flag_field) = create_nested_schema();
//         let index = Index::create_in_ram(schema.clone());

//         {
//             let mut writer = index.writer_for_tests()?;
//             // block1
//             writer.add_documents(vec![
//                 doc!(parent_flag_field=>false),
//                 doc!(parent_flag_field=>false),
//                 doc!(parent_flag_field=>true, group_field=>"alpha"),
//             ])?;
//             // block2
//             writer.add_documents(vec![
//                 doc!(parent_flag_field=>false),
//                 doc!(parent_flag_field=>true, group_field=>"beta"),
//             ])?;
//             writer.commit()?;
//         }
//         let reader = index.reader()?;
//         let searcher = reader.searcher();

//         let child_term = Term::from_field_bool(parent_flag_field, false);
//         let child_query = TermQuery::new(child_term, IndexRecordOption::Basic);

//         let nested_query = NestedQuery::new(
//             &schema,
//             "user".to_string(),
//             Box::new(child_query),
//             NestedScoreMode::None,
//             false,
//         )?;

//         let top_docs = searcher.search(&nested_query, &TopDocs::with_limit(10))?;
//         // 2 parent docs => alpha and beta
//         assert_eq!(top_docs.len(), 2);

//         let mut group_strings = Vec::new();
//         for (score, addr) in top_docs {
//             let doc = searcher.doc::<TantivyDocument>(addr)?;
//             let group_val = doc.get_first(group_field).unwrap().as_str().unwrap();
//             group_strings.push((score, group_val.to_owned()));
//         }

//         // We expect alpha and beta in some order
//         let groups_only: Vec<_> = group_strings.iter().map(|(_score, grp)| grp).collect();
//         assert!(groups_only.contains(&&"alpha".to_string()));
//         assert!(groups_only.contains(&&"beta".to_string()));
//         Ok(())
//     }

//     #[test]
//     fn test_nested_query_with_boolean_child() -> crate::Result<()> {
//         // Example: must have skill=python AND skill=ruby in the same child block => parent matches
//         // but we won't do any actual doc indexing in this snippet. It's just a demonstration.
//         let (schema, _group_field, _parent_flag_field) = create_nested_schema();
//         // Suppose we also add "skill" as text:
//         let skill_field = schema
//             .get_field("skill")
//             .unwrap_or_else(|_| panic!("Add skill field if needed."));

//         // Then a boolean child query:
//         let must_python = TermQuery::new(
//             Term::from_field_text(skill_field, "python"),
//             IndexRecordOption::Basic,
//         );
//         let must_ruby = TermQuery::new(
//             Term::from_field_text(skill_field, "ruby"),
//             IndexRecordOption::Basic,
//         );
//         let mut bool_q = BooleanQuery::new(vec![]);
//         bool_q.add_clause(Occur::Must, Box::new(must_python));
//         bool_q.add_clause(Occur::Must, Box::new(must_ruby));

//         // In real usage, you'd index docs accordingly. We'll just skip that here.
//         let nested_q = NestedQuery::new(
//             &schema,
//             "user".to_string(),
//             Box::new(bool_q),
//             NestedScoreMode::Max,
//             false,
//         )?;

//         // We won't actually run it, but:
//         assert!(true);
//         Ok(())
//     }
// }
