use crate::query::{BooleanWeight, DisjunctionMaxCombiner, EnableScoring, Occur, Query, Weight};
use crate::{Score, Term};

/// The disjunction max query returns documents matching one or more wrapped queries,
/// called query clauses or clauses.
///
/// If a returned document matches multiple query clauses,
/// the `DisjunctionMaxQuery` assigns the document the highest relevance score from any matching
/// clause, plus a tie breaking increment for any additional matching subqueries.
///
/// ```rust
/// use tantivy::collector::TopDocs;
/// use tantivy::doc;
/// use tantivy::query::{DisjunctionMaxQuery, Query, QueryClone, TermQuery};
/// use tantivy::schema::{IndexRecordOption, Schema, TEXT};
/// use tantivy::Term;
/// use tantivy::Index;
/// use tantivy::IndexWriter;
///
/// fn main() -> tantivy::Result<()> {
///    let mut schema_builder = Schema::builder();
///    let title = schema_builder.add_text_field("title", TEXT);
///    let body = schema_builder.add_text_field("body", TEXT);
///    let schema = schema_builder.build();
///    let index = Index::create_in_ram(schema);
///    {
///        let mut index_writer: IndexWriter = index.writer(15_000_000)?;
///        index_writer.add_document(doc!(
///            title => "The Name of Girl",
///        ))?;
///        index_writer.add_document(doc!(
///            title => "The Diary of Muadib",
///        ))?;
///        index_writer.add_document(doc!(
///            title => "The Diary of Girl",
///        ))?;
///        index_writer.commit()?;
///    }
///
///    let reader = index.reader()?;
///    let searcher = reader.searcher();
///
///    // Make TermQuery's for "girl" and "diary" in the title
///    let girl_term_query: Box<dyn Query> = Box::new(TermQuery::new(
///        Term::from_field_text(title, "girl"),
///        IndexRecordOption::Basic,
///    ));
///    let diary_term_query: Box<dyn Query> = Box::new(TermQuery::new(
///        Term::from_field_text(title, "diary"),
///        IndexRecordOption::Basic,
///    ));
///
///    // TermQuery "diary" and "girl" should be present and only one should be accounted in score
///    let queries1 = vec![diary_term_query.box_clone(), girl_term_query.box_clone()];
///    let diary_and_girl = DisjunctionMaxQuery::new(queries1);
///    let documents = searcher.search(&diary_and_girl, &TopDocs::with_limit(3).order_by_score())?;
///    assert_eq!(documents[0].0, documents[1].0);
///    assert_eq!(documents[1].0, documents[2].0);
///
///    // TermQuery "diary" and "girl" should be present
///    // and one should be accounted with multiplier 0.7
///    let queries2 = vec![diary_term_query.box_clone(), girl_term_query.box_clone()];
///    let tie_breaker = 0.7;
///    let diary_and_girl_with_tie_breaker = DisjunctionMaxQuery::with_tie_breaker(queries2, tie_breaker);
///    let documents = searcher.search(&diary_and_girl_with_tie_breaker, &TopDocs::with_limit(3).order_by_score())?;
///    assert_eq!(documents[1].0, documents[2].0);
///    // For this test all terms brings the same score. So we can do easy math and assume that
///    // `DisjunctionMaxQuery` with tie breakers score should be equal
///    // to term1 score + `tie_breaker` * term2 score or (1.0 + tie_breaker) * term score
///    assert!(f32::abs(documents[0].0 - documents[1].0 * (1.0 + tie_breaker)) < 0.001);
///    Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct DisjunctionMaxQuery {
    disjuncts: Vec<Box<dyn Query>>,
    tie_breaker: Score,
}

impl Clone for DisjunctionMaxQuery {
    fn clone(&self) -> Self {
        DisjunctionMaxQuery::with_tie_breaker(
            self.disjuncts
                .iter()
                .map(|disjunct| disjunct.box_clone())
                .collect::<Vec<_>>(),
            self.tie_breaker,
        )
    }
}

impl Query for DisjunctionMaxQuery {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let disjuncts = self
            .disjuncts
            .iter()
            .map(|disjunct| Ok((Occur::Should, disjunct.weight(enable_scoring)?)))
            .collect::<crate::Result<_>>()?;
        let tie_breaker = self.tie_breaker;
        Ok(Box::new(BooleanWeight::new(
            disjuncts,
            enable_scoring.is_scoring_enabled(),
            Box::new(move || DisjunctionMaxCombiner::with_tie_breaker(tie_breaker)),
        )))
    }

    fn query_terms<'a>(&'a self, visitor: &mut dyn FnMut(&'a Term, bool)) {
        for disjunct in &self.disjuncts {
            disjunct.query_terms(visitor);
        }
    }
}

impl DisjunctionMaxQuery {
    /// Creates a new `DisjunctionMaxQuery` with tie breaker.
    pub fn with_tie_breaker(
        disjuncts: Vec<Box<dyn Query>>,
        tie_breaker: Score,
    ) -> DisjunctionMaxQuery {
        DisjunctionMaxQuery {
            disjuncts,
            tie_breaker,
        }
    }

    /// Creates a new `DisjunctionMaxQuery` with no tie breaker.
    pub fn new(disjuncts: Vec<Box<dyn Query>>) -> DisjunctionMaxQuery {
        DisjunctionMaxQuery::with_tie_breaker(disjuncts, 0.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::collector::TopDocs;
    use crate::query::{DisjunctionMaxQuery, Query, QueryParser, TermQuery};
    use crate::schema::{IndexRecordOption, Schema, TEXT};
    use crate::{Index, Term};

    /// `TopDocs` prunes through `Weight::for_each_pruning`, which hands a union
    /// of term scorers to Block-WAND. Block-WAND sums the matching terms, so
    /// driving a dis_max query that way scored a document matching in two
    /// fields as the sum of both instead of the better of the two.
    #[test]
    fn test_dismax_is_not_summed_by_block_wand() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let title = schema_builder.add_text_field("title", TEXT);
        let body = schema_builder.add_text_field("body", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests()?;
        // "alpha" appears in two titles and two bodies, so it scores the same
        // in either field, and doc 1 is the one matching in both
        writer.add_document(doc!(title => "alpha", body => "beta"))?;
        writer.add_document(doc!(title => "alpha", body => "alpha"))?;
        writer.add_document(doc!(title => "beta", body => "alpha"))?;
        writer.commit()?;
        let searcher = index.reader()?.searcher();

        let clause = |field| {
            Box::new(TermQuery::new(
                Term::from_field_text(field, "alpha"),
                IndexRecordOption::WithFreqs,
            )) as Box<dyn Query>
        };
        let query = DisjunctionMaxQuery::new(vec![clause(title), clause(body)]);
        let top = searcher.search(&query, &TopDocs::with_limit(3).order_by_score())?;
        assert_eq!(top.len(), 3);
        // every document scores its single best field, so all three tie
        for (score, _) in &top {
            assert!(
                (score - top[0].0).abs() < 1e-5,
                "dis_max must take the best field, got {top:?}"
            );
        }

        // and the tie breaker adds exactly its fraction of the other field
        let query = DisjunctionMaxQuery::with_tie_breaker(vec![clause(title), clause(body)], 0.3);
        let top = searcher.search(&query, &TopDocs::with_limit(3).order_by_score())?;
        assert!(
            (top[0].0 - top[2].0 * 1.3).abs() < 1e-5,
            "tie_breaker must add 30% of the other field, got {top:?}"
        );
        let _ = QueryParser::for_index(&index, vec![title]);
        Ok(())
    }

    /// A `TermIntersection` (all clauses required) sums its term scores on the
    /// unpruned path whatever the combiner — `Intersection::score` hard-codes
    /// the sum; the combiner only shapes how *should* clauses combine. The
    /// pruning path must produce identical scores, which is why
    /// `block_wand_intersection` may drive a non-summing combiner too.
    #[test]
    fn test_term_intersection_pruning_matches_unpruned_scorer() -> crate::Result<()> {
        use crate::query::score_combiner::DisjunctionMaxCombiner;
        use crate::query::{BooleanWeight, EnableScoring, Occur, Weight};
        use crate::schema::Field;
        use crate::{DocSet, Score, TERMINATED};

        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests()?;
        // both terms in every doc, with varying frequencies so scores differ
        for i in 0..100 {
            let mut doc_text = String::from("alpha beta");
            for _ in 0..i % 7 {
                doc_text.push_str(" alpha");
            }
            for _ in 0..i % 3 {
                doc_text.push_str(" beta");
            }
            writer.add_document(doc!(text => doc_text))?;
        }
        writer.commit()?;
        let searcher = index.reader()?.searcher();
        let reader = searcher.segment_reader(0);

        let clause = |field: Field, term: &str| -> crate::Result<(Occur, Box<dyn Weight>)> {
            let query = TermQuery::new(
                Term::from_field_text(field, term),
                IndexRecordOption::WithFreqs,
            );
            Ok((
                Occur::Must,
                query.weight(EnableScoring::enabled_from_searcher(&searcher))?,
            ))
        };
        let weight = BooleanWeight::with_minimum_number_should_match(
            vec![clause(text, "alpha")?, clause(text, "beta")?],
            0,
            true,
            Box::new(|| DisjunctionMaxCombiner::with_tie_breaker(0.3)),
        );

        let mut unpruned: Vec<(u32, Score)> = Vec::new();
        let mut scorer = weight.scorer(reader, 1.0)?;
        while scorer.doc() != TERMINATED {
            unpruned.push((scorer.doc(), scorer.score()));
            scorer.advance();
        }
        assert_eq!(unpruned.len(), 100);

        let mut pruned: Vec<(u32, Score)> = Vec::new();
        weight.for_each_pruning(Score::MIN, reader, &mut |doc, score| {
            pruned.push((doc, score));
            Score::MIN
        })?;

        assert_eq!(pruned.len(), unpruned.len());
        for (&(pruned_doc, pruned_score), &(doc, score)) in pruned.iter().zip(unpruned.iter()) {
            assert_eq!(pruned_doc, doc);
            assert!(
                (pruned_score - score).abs() < 1e-5,
                "doc {doc}: pruning scored {pruned_score}, plain scorer {score}"
            );
        }
        Ok(())
    }
}
