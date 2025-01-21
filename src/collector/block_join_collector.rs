use crate::collector::{Collector, Fruit, SegmentCollector};
use crate::query::{AllQuery, Query, Scorer, TermQuery};
use crate::schema::{Field, Schema, SchemaBuilder, INDEXED, STORED, TEXT};
use crate::schema::{IndexRecordOption, Term};
use crate::{
    doc, DocId, Index, IndexWriter, Result, Score, SegmentOrdinal, SegmentReader, TantivyError,
};
use common::BitSet;
use std::sync::Arc;

/// A conceptual `BlockJoinCollector` for Tantivy.
///
/// It collects "parent" documents and, for each parent, stores all "child" docs (and scores) that
/// matched a given query. The notion of which docs are parents is customizable via the
/// `parent_bitset_fn` closure passed at creation time.
pub struct BlockJoinCollector {
    /// A user-provided closure/adapter that, for each segment, returns a `BitSet` marking which docs
    /// are “parent” docs in that segment.
    ///
    /// Typically you'd build this using a separate query or a known `_is_parent` boolean field, but
    /// we leave the logic up to you. The collector will then store children in the last parent doc
    /// encountered.
    parent_bitset_fn: Arc<dyn Fn(&SegmentReader) -> Result<BitSet> + Send + Sync>,
}

/// The final fruit: a Vec of `(parent_local_doc, child_local_docs, child_scores)`.
///
/// - `parent_local_doc` is the segment-local doc ID for the parent  
/// - `child_local_docs` are the segment-local doc IDs of all matching child docs after that parent  
/// - `child_scores` are the scores for each child (if `requires_scoring() == true`)
pub type BlockJoinFruit = Vec<(DocId, Vec<DocId>, Vec<Score>)>;

impl BlockJoinCollector {
    /// Create a new `BlockJoinCollector` with a parent filter function. That function
    /// must build a `BitSet` of parent docs for each segment, marking which doc IDs are parents.
    ///
    /// ```no_run
    /// # use tantivy::SegmentReader;
    /// # use common::BitSet;
    /// # use tantivy::Result;
    /// #
    /// let c = BlockJoinCollector::new(|segment_reader: &SegmentReader| {
    ///     // Build or load a BitSet for parent docs in this segment:
    ///     // e.g., run a query that matches `is_parent==true`
    ///     Ok(BitSet::with_max_value(segment_reader.max_doc()))
    /// });
    /// ```
    pub fn new<F>(parent_filter_fn: F) -> Self
    where
        F: Fn(&SegmentReader) -> Result<BitSet> + Send + Sync + 'static,
    {
        BlockJoinCollector {
            parent_bitset_fn: Arc::new(parent_filter_fn),
        }
    }
}

impl Collector for BlockJoinCollector {
    type Fruit = BlockJoinFruit;
    type Child = BlockJoinSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: SegmentOrdinal,
        reader: &SegmentReader,
    ) -> Result<Self::Child> {
        let bitset = (self.parent_bitset_fn)(reader)?;
        Ok(BlockJoinSegmentCollector {
            parent_bitset: bitset,
            groups: Vec::new(),
            _segment_id: segment_local_id,
        })
    }

    /// We want to collect scores for child docs, so we return `true`.
    fn requires_scoring(&self) -> bool {
        true
    }

    /// Once all segments have been processed, we merge their results (Vecs) into one.
    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> Result<Self::Fruit> {
        let mut all = Vec::new();
        for mut seg_groups in segment_fruits {
            all.append(&mut seg_groups);
        }
        Ok(all)
    }
}

/// Per-segment collector that holds a `BitSet` of which docs are parents, and a growable list
/// of `(parent, child_docs, child_scores)` results.
pub struct BlockJoinSegmentCollector {
    pub parent_bitset: BitSet,
    pub groups: Vec<(DocId, Vec<DocId>, Vec<Score>)>,
    _segment_id: SegmentOrdinal,
}

impl SegmentCollector for BlockJoinSegmentCollector {
    type Fruit = BlockJoinFruit;

    /// Called for each matching doc (parent or child) in *segment-local* doc ID space.
    fn collect(&mut self, doc_id: DocId, score: Score) {
        // If this doc is a parent, start a new group
        if self.parent_bitset.contains(doc_id) {
            self.groups.push((doc_id, Vec::new(), Vec::new()));
        } else {
            // It's a child doc => append to the last parent group (if any)
            if let Some((_, ref mut children, ref mut scores)) = self.groups.last_mut() {
                children.push(doc_id);
                scores.push(score);
            } else {
                // No parent group yet => child is ignored or log a warning
            }
        }
    }

    /// For “no-score” collecting, Tantivy may call `collect_block(docs)`.
    /// We'll rely on the default, which calls `collect(doc, 0.0)` for each doc.
    ///
    /// fn collect_block(&mut self, docs: &[DocId]) { ... } // default is fine

    fn harvest(self) -> Self::Fruit {
        self.groups
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::RamDirectory;
    use crate::query::{AllQuery, TermQuery};
    use crate::schema::{IndexRecordOption, STORED, TEXT};
    use crate::{doc, DocSet, Index, Score, TantivyDocument, Term};

    /// Create a simple schema with is_parent:bool + title:str
    fn make_schema() -> (Schema, Field, Field) {
        let mut builder = SchemaBuilder::new();
        let title_field = builder.add_text_field("title", TEXT | STORED);
        let parent_field = builder.add_bool_field("is_parent", INDEXED);
        (builder.build(), title_field, parent_field)
    }

    /// Build an index with parent + child docs in one segment
    fn build_sample_index() -> (Index, Field, Field) {
        let (schema, title_f, parent_f) = make_schema();
        let index = Index::create_in_ram(schema.clone());
        {
            let mut writer = index.writer_for_tests().unwrap();
            writer.add_document(doc!( parent_f=>true,  title_f=>"ParentA" ));
            writer.add_document(doc!( parent_f=>false, title_f=>"child a1" ));
            writer.add_document(doc!( parent_f=>false, title_f=>"child a2" ));
            writer.add_document(doc!( parent_f=>true,  title_f=>"ParentB" ));
            writer.add_document(doc!( parent_f=>false, title_f=>"child b1" ));
            writer.commit().unwrap();
        }
        (index, title_f, parent_f)
    }

    /// A helper that builds a `BlockJoinCollector` by scanning `is_parent==true`.
    fn make_block_join_collector(parent_field: Field) -> BlockJoinCollector {
        BlockJoinCollector::new(move |reader: &SegmentReader| {
            let max_doc = reader.max_doc();
            let mut bitset = BitSet::with_max_value(max_doc);

            let inverted = reader.inverted_index(parent_field)?;
            // Use from_field_bool(parent_field, true) => correct for bool fields
            if let Some(mut block_postings) = inverted.read_block_postings(
                &Term::from_field_bool(parent_field, true),
                IndexRecordOption::Basic,
            )? {
                while block_postings.block_len() > 0 {
                    let len = block_postings.block_len();
                    for i in 0..len {
                        let d = block_postings.doc(i);
                        if d == crate::TERMINATED {
                            break;
                        }
                        bitset.insert(d);
                    }
                    block_postings.advance();
                }
            }
            Ok(bitset)
        })
    }

    #[test]
    fn test_block_join_no_parents() -> Result<()> {
        // Index docs that have is_parent=false => so no doc is actually a parent
        let (schema, title_f, parent_f) = make_schema();
        let index = Index::create_in_ram(schema);

        {
            let mut writer = index.writer_for_tests()?;
            writer.add_document(doc!( parent_f=>false, title_f=>"foo" ));
            writer.add_document(doc!( parent_f=>false, title_f=>"bar" ));
            writer.add_document(doc!( parent_f=>false, title_f=>"baz" ));
            writer.commit()?;
        }

        // We'll produce a bitset the size of max_doc, but never set any bits => no parents
        let collector = BlockJoinCollector::new(move |seg_reader| {
            let max_doc = seg_reader.max_doc();
            let bitset = BitSet::with_max_value(max_doc);
            // do not insert anything => no parent docs
            Ok(bitset)
        });

        let reader = index.reader()?;
        let searcher = reader.searcher();
        let fruit = searcher.search(&AllQuery, &collector)?;

        // We expect zero parent groups
        assert!(fruit.is_empty(), "We found some parents but expected none.");
        Ok(())
    }

    #[test]
    fn test_block_join_no_children() -> Result<()> {
        // Only parent docs => each group has zero children
        let (schema, title_f, parent_f) = make_schema();
        let index = Index::create_in_ram(schema);

        {
            let mut writer = index.writer_for_tests()?;
            writer.add_document(doc!( parent_f=>true, title_f=>"Alpha" ));
            writer.add_document(doc!( parent_f=>true, title_f=>"Beta" ));
            writer.add_document(doc!( parent_f=>true, title_f=>"Gamma" ));
            writer.commit()?;
        }

        let collector = make_block_join_collector(parent_f);
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let fruit = searcher.search(&AllQuery, &collector)?;

        // We have 3 parents, each with 0 children
        assert_eq!(fruit.len(), 3);
        for (parent_doc, kids, scores) in &fruit {
            assert!(kids.is_empty());
            assert!(scores.is_empty());
        }
        Ok(())
    }

    #[test]
    fn test_block_join_basic() -> Result<()> {
        let (index, title_f, parent_f) = build_sample_index();
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let collector = make_block_join_collector(parent_f);
        let fruit = searcher.search(&AllQuery, &collector)?;

        // doc0=>[1,2], doc3=>[4]
        assert_eq!(fruit.len(), 2);

        let (p0, kids0, scores0) = &fruit[0];
        assert_eq!(*p0, 0);
        assert_eq!(&kids0[..], &[1, 2]);
        assert_eq!(scores0.len(), 2);

        let (p1, kids1, scores1) = &fruit[1];
        assert_eq!(*p1, 3);
        assert_eq!(&kids1[..], &[4]);
        assert_eq!(scores1.len(), 1);

        Ok(())
    }

    #[test]
    fn test_block_join_some_children_unmatched() -> Result<()> {
        // We must ensure that the parent doc also has "interesting" in its text,
        // otherwise the parent doc won't match the TermQuery and won't even be "seen."
        // So let's put "interesting" in the parent's title, plus the child's title.
        // doc0 => parent => "ParentX interesting"
        // doc1 => child => "interesting child"
        // doc2 => child => "boring child"
        let (schema, title_f, parent_f) = make_schema();
        let index = Index::create_in_ram(schema);

        {
            let mut writer = index.writer_for_tests()?;
            writer.add_document(doc!( parent_f=>true,  title_f=>"ParentX interesting" ));
            writer.add_document(doc!( parent_f=>false, title_f=>"interesting child" ));
            writer.add_document(doc!( parent_f=>false, title_f=>"boring child" ));
            writer.commit()?;
        }

        let collector = make_block_join_collector(parent_f);
        let reader = index.reader()?;
        let searcher = reader.searcher();

        // We only match docs containing the token "interesting"
        // Because the parent doc also has "interesting" in its text,
        // doc0 is matched -> new group. doc1 is matched -> child in that group.
        // doc2 is not matched -> child doc2 is excluded.
        let interesting_term = Term::from_field_text(title_f, "interesting");
        let term_query = TermQuery::new(interesting_term, IndexRecordOption::WithFreqs);

        let fruit = searcher.search(&term_query, &collector)?;

        // => 1 parent group => doc0 => child doc1
        assert_eq!(fruit.len(), 1, "We expected exactly 1 parent group");
        let (parent_doc, child_docs, scores) = &fruit[0];
        assert_eq!(*parent_doc, 0);
        assert_eq!(&child_docs[..], &[1]);
        assert_eq!(scores.len(), 1);

        Ok(())
    }

    #[test]
    fn test_block_join_multiple_segments() -> Result<()> {
        // We'll create 2 commits => each has 1 parent + 2 children => total 2 segments
        // We'll see parent doc=0 + child docs=1,2 in first segment => parent doc=0 + child=1,2 in second seg
        let (schema, title_f, parent_f) = make_schema();
        let index = Index::create_in_ram(schema);

        {
            let mut writer = index.writer_for_tests()?;
            writer.add_document(doc!( parent_f=>true,  title_f=>"S0P0" ));
            writer.add_document(doc!( parent_f=>false, title_f=>"s0 child1" ));
            writer.add_document(doc!( parent_f=>false, title_f=>"s0 child2" ));
            writer.commit()?;
        }
        {
            let mut writer = index.writer_for_tests()?;
            writer.add_document(doc!( parent_f=>true,  title_f=>"S1P0" ));
            writer.add_document(doc!( parent_f=>false, title_f=>"s1 child1" ));
            writer.add_document(doc!( parent_f=>false, title_f=>"s1 child2" ));
            writer.commit()?;
        }

        let reader = index.reader()?;
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 2);

        let collector = make_block_join_collector(parent_f);
        let fruit = searcher.search(&AllQuery, &collector)?;

        // 2 segments => each yields 1 parent group => total 2 groups
        assert_eq!(fruit.len(), 2);

        // Segment0 => doc0 => children [1,2]
        let (p0, c0, s0) = &fruit[0];
        assert_eq!(*p0, 0);
        assert_eq!(c0, &[1, 2]);
        assert_eq!(s0.len(), 2);

        // Segment1 => doc0 => children [1,2]
        let (p1, c1, s1) = &fruit[1];
        assert_eq!(*p1, 0);
        assert_eq!(c1, &[1, 2]);
        assert_eq!(s1.len(), 2);

        Ok(())
    }
}
