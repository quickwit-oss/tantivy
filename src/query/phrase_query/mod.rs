mod phrase_query;
mod phrase_scorer;
mod phrase_weight;
pub mod regex_phrase_query;
mod regex_phrase_weight;

pub use self::phrase_query::PhraseQuery;
pub(crate) use self::phrase_scorer::intersection_count;
pub use self::phrase_scorer::PhraseScorer;
pub use self::phrase_weight::PhraseWeight;

#[cfg(test)]
pub(crate) mod tests {

    use serde_json::json;

    use super::*;
    use crate::collector::tests::{TEST_COLLECTOR_WITHOUT_SCORE, TEST_COLLECTOR_WITH_SCORE};
    use crate::index::Index;
    use crate::query::{EnableScoring, QueryParser, Weight};
    use crate::schema::{Schema, Term, TEXT};
    use crate::{assert_nearly_equals, DocAddress, DocId, IndexWriter, TERMINATED};

    pub fn create_index<S: AsRef<str>>(texts: &[S]) -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            for text in texts {
                let doc = doc!(text_field=>text.as_ref());
                index_writer.add_document(doc)?;
            }
            index_writer.commit()?;
        }
        Ok(index)
    }

    #[test]
    pub fn test_phrase_query() -> crate::Result<()> {
        let index = create_index(&[
            "b b b d c g c",
            "a b b d c g c",
            "a b a b c",
            "c a b a d ga a",
            "a b c",
        ])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let test_query = |texts: Vec<&str>| {
            let terms: Vec<Term> = texts
                .iter()
                .map(|text| Term::from_field_text(text_field, text))
                .collect();
            let phrase_query = PhraseQuery::new(terms);
            let test_fruits = searcher
                .search(&phrase_query, &TEST_COLLECTOR_WITH_SCORE)
                .unwrap();
            test_fruits
                .docs()
                .iter()
                .map(|docaddr| docaddr.doc_id)
                .collect::<Vec<_>>()
        };
        assert_eq!(test_query(vec!["a", "b"]), vec![1, 2, 3, 4]);
        assert_eq!(test_query(vec!["a", "b", "c"]), vec![2, 4]);
        assert_eq!(test_query(vec!["b", "b"]), vec![0, 1]);
        assert!(test_query(vec!["g", "ewrwer"]).is_empty());
        assert!(test_query(vec!["g", "a"]).is_empty());
        Ok(())
    }

    #[test]
    pub fn test_phrase_query_simple() -> crate::Result<()> {
        let index = create_index(&["a b b d c g c", "a b a b c"])?;
        let text_field = index.schema().get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let terms: Vec<Term> = ["a", "b", "c"]
            .iter()
            .map(|text| Term::from_field_text(text_field, text))
            .collect();
        let phrase_query = PhraseQuery::new(terms);
        let phrase_weight =
            phrase_query.phrase_weight(EnableScoring::disabled_from_schema(searcher.schema()))?;
        let mut phrase_scorer = phrase_weight.scorer(searcher.segment_reader(0), 1.0)?;
        assert_eq!(phrase_scorer.doc(), 1);
        assert_eq!(phrase_scorer.advance(), TERMINATED);
        Ok(())
    }

    #[test]
    pub fn test_phrase_query_no_score() -> crate::Result<()> {
        let index = create_index(&[
            "b b b d c g c",
            "a b b d c g c",
            "a b a b c",
            "c a b a d ga a",
            "a b c",
        ])?;
        let schema = index.schema();
        let text_field = schema.get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let test_query = |texts: Vec<&str>| {
            let terms: Vec<Term> = texts
                .iter()
                .map(|text| Term::from_field_text(text_field, text))
                .collect();
            let phrase_query = PhraseQuery::new(terms);
            let test_fruits = searcher
                .search(&phrase_query, &TEST_COLLECTOR_WITHOUT_SCORE)
                .unwrap();
            test_fruits
                .docs()
                .iter()
                .map(|docaddr| docaddr.doc_id)
                .collect::<Vec<_>>()
        };
        assert_eq!(test_query(vec!["a", "b", "c"]), vec![2, 4]);
        assert_eq!(test_query(vec!["a", "b"]), vec![1, 2, 3, 4]);
        assert_eq!(test_query(vec!["b", "b"]), vec![0, 1]);
        assert!(test_query(vec!["g", "ewrwer"]).is_empty());
        assert!(test_query(vec!["g", "a"]).is_empty());
        Ok(())
    }

    #[test]
    pub fn test_phrase_query_no_positions() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        use crate::schema::{IndexRecordOption, TextFieldIndexing, TextOptions};
        let no_positions = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
        );

        let text_field = schema_builder.add_text_field("text", no_positions);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field=>"a b c"))?;
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        let phrase_query = PhraseQuery::new(vec![
            Term::from_field_text(text_field, "a"),
            Term::from_field_text(text_field, "b"),
        ]);

        let search_error = searcher
            .search(&phrase_query, &TEST_COLLECTOR_WITH_SCORE)
            .err();
        assert!(matches!(
            search_error,
            Some(crate::TantivyError::SchemaError(msg))
            if msg == "Applied phrase query on field \"text\", which does not have positions \
            indexed"
        ));
        Ok(())
    }

    #[test]
    pub fn test_phrase_score() -> crate::Result<()> {
        let index = create_index(&["a b c", "a b c a b"])?;
        let scores = test_query(0, &index, vec!["a", "b"]);
        assert_nearly_equals!(scores[0], 0.40618482);
        assert_nearly_equals!(scores[1], 0.46844664);
        Ok(())
    }

    #[ignore]
    #[test]
    pub fn test_phrase_score_with_slop() -> crate::Result<()> {
        let index = create_index(&["a c b", "a b c a b"])?;
        let scores = test_query(1, &index, vec!["a", "b"]);
        assert_nearly_equals!(scores[0], 0.40618482);
        assert_nearly_equals!(scores[1], 0.46844664);
        Ok(())
    }

    #[test]
    pub fn test_phrase_score_with_slop_bug() -> crate::Result<()> {
        let index = create_index(&["asdf asdf Captain Subject Wendy", "Captain"])?;
        let scores = test_query(1, &index, vec!["captain", "wendy"]);
        assert_eq!(scores.len(), 1);
        Ok(())
    }

    #[test]
    pub fn test_phrase_score_with_slop_bug_2() -> crate::Result<()> {
        // fails
        let index = create_index(&["a x b x c", "a a c"])?;
        let scores = test_query(2, &index, vec!["a", "b", "c"]);
        assert_eq!(scores.len(), 1);

        let index = create_index(&["a x b x c", "b c c"])?;
        let scores = test_query(2, &index, vec!["a", "b", "c"]);
        assert_eq!(scores.len(), 1);

        Ok(())
    }

    fn test_query(slop: u32, index: &Index, texts: Vec<&str>) -> Vec<f32> {
        let text_field = index.schema().get_field("text").unwrap();
        let searcher = index.reader().unwrap().searcher();
        let terms: Vec<Term> = texts
            .iter()
            .map(|text| Term::from_field_text(text_field, text))
            .collect();
        let mut phrase_query = PhraseQuery::new(terms);
        phrase_query.set_slop(slop);
        searcher
            .search(&phrase_query, &TEST_COLLECTOR_WITH_SCORE)
            .expect("search should succeed")
            .scores()
            .to_vec()
    }

    #[test]
    pub fn test_phrase_score_with_slop_repeating() -> crate::Result<()> {
        let index = create_index(&["wendy subject subject captain", "Captain"])?;
        let scores = test_query(1, &index, vec!["wendy", "subject", "captain"]);
        assert_eq!(scores.len(), 1);
        Ok(())
    }

    #[test]
    pub fn test_phrase_score_with_slop_size() -> crate::Result<()> {
        let index = create_index(&["a b e c", "a e e e c", "a e e e e c"])?;
        let scores = test_query(3, &index, vec!["a", "c"]);
        assert_eq!(scores.len(), 2);
        assert_nearly_equals!(scores[0], 0.29086056);
        assert_nearly_equals!(scores[1], 0.26706287);
        Ok(())
    }

    #[test]
    pub fn test_phrase_slop() -> crate::Result<()> {
        let index = create_index(&["a x b c"])?;
        let scores = test_query(1, &index, vec!["a", "b", "c"]);
        assert_eq!(scores.len(), 1);

        let index = create_index(&["a x b x c"])?;
        let scores = test_query(1, &index, vec!["a", "b", "c"]);
        assert_eq!(scores.len(), 0);

        let index = create_index(&["a b"])?;
        let scores = test_query(1, &index, vec!["b", "a"]);
        assert_eq!(scores.len(), 0);

        let index = create_index(&["a b"])?;
        let scores = test_query(2, &index, vec!["b", "a"]);
        assert_eq!(scores.len(), 1);

        Ok(())
    }

    #[test]
    pub fn test_phrase_slop_matches_same_docs_with_and_without_scoring() -> crate::Result<()> {
        // The slop spent between earlier terms of the phrase has to be accounted for
        // when scoring is disabled too. Otherwise `Count` (or a filter) matches
        // documents that `TopDocs` never returns.
        let index = create_index(&["a x b x c", "a b c", "a h d", "a x b c"])?;
        let text_field = index.schema().get_field("text").unwrap();
        let searcher = index.reader()?.searcher();
        let matching_docs = |slop: u32, texts: &[&str], with_scoring: bool| -> Vec<DocId> {
            let terms: Vec<Term> = texts
                .iter()
                .map(|text| Term::from_field_text(text_field, text))
                .collect();
            let mut phrase_query = PhraseQuery::new(terms);
            phrase_query.set_slop(slop);
            let collector = if with_scoring {
                &TEST_COLLECTOR_WITH_SCORE
            } else {
                &TEST_COLLECTOR_WITHOUT_SCORE
            };
            searcher
                .search(&phrase_query, collector)
                .unwrap()
                .docs()
                .iter()
                .map(|doc_address| doc_address.doc_id)
                .collect()
        };
        for (slop, texts, expected) in [
            (1, &["a", "b", "c"][..], vec![1, 3]),
            (2, &["a", "b", "c"][..], vec![0, 1, 3]),
            (1, &["a", "d", "h"][..], vec![]),
        ] {
            assert_eq!(matching_docs(slop, texts, true), expected);
            assert_eq!(matching_docs(slop, texts, false), expected);
        }
        Ok(())
    }

    #[test]
    pub fn test_phrase_slop_match_missed_by_greedy_count() -> crate::Result<()> {
        // `c a b` matches `"a b c"~3`, but the greedy carrying-slop count skips it.
        let index = create_index(&["b x a c a b"])?;
        let scores = test_query(3, &index, vec!["a", "b", "c"]);
        assert_eq!(scores.len(), 1);
        let text_field = index.schema().get_field("text").unwrap();
        let mut phrase_query = PhraseQuery::new(
            ["a", "b", "c"]
                .iter()
                .map(|text| Term::from_field_text(text_field, text))
                .collect(),
        );
        phrase_query.set_slop(3);
        let searcher = index.reader()?.searcher();
        let docs = searcher.search(&phrase_query, &TEST_COLLECTOR_WITHOUT_SCORE)?;
        assert_eq!(docs.docs().len(), 1);
        Ok(())
    }

    /// Doc ids matching the sloppy phrase, with or without scoring.
    fn phrase_matching_docs(
        index: &Index,
        slop: u32,
        texts: &[&str],
        with_scoring: bool,
    ) -> Vec<DocId> {
        let text_field = index.schema().get_field("text").unwrap();
        let searcher = index.reader().unwrap().searcher();
        let terms: Vec<Term> = texts
            .iter()
            .map(|text| Term::from_field_text(text_field, text))
            .collect();
        let mut phrase_query = PhraseQuery::new(terms);
        phrase_query.set_slop(slop);
        let collector = if with_scoring {
            &TEST_COLLECTOR_WITH_SCORE
        } else {
            &TEST_COLLECTOR_WITHOUT_SCORE
        };
        let mut docs: Vec<DocId> = searcher
            .search(&phrase_query, collector)
            .unwrap()
            .docs()
            .iter()
            .map(|doc_address| doc_address.doc_id)
            .collect();
        docs.sort_unstable();
        docs
    }

    #[test]
    pub fn test_phrase_slop_interior_alignment_point() -> crate::Result<()> {
        // `"a b c"~3` on `a x x c b`: the adjusted positions are a=2, b=5, c=3. Aligning
        // all terms at 3 costs 1 + 2 + 0 = 3, so the document matches. The intermediate
        // merge of `a` and `b` must not hide the interior point 3 from the last term.
        let index = create_index(&["a x x c b"])?;
        for with_scoring in [false, true] {
            assert_eq!(
                phrase_matching_docs(&index, 3, &["a", "b", "c"], with_scoring),
                vec![0],
                "with_scoring={with_scoring}"
            );
            assert!(phrase_matching_docs(&index, 2, &["a", "b", "c"], with_scoring).is_empty());
        }
        Ok(())
    }

    #[test]
    pub fn test_phrase_slop_does_not_promote_non_match() -> crate::Result<()> {
        // `"a b c d"~6` on `x x b d c c a b`: the cheapest choice of one position per term
        // (b=2, a=6, c=4, d=3) costs 7, which is above the slop. Carried slop of the
        // intermediate merges understates it, but the document must not match.
        let index = create_index(&["x x b d c c a b"])?;
        for with_scoring in [false, true] {
            assert!(
                phrase_matching_docs(&index, 6, &["a", "b", "c", "d"], with_scoring).is_empty(),
                "with_scoring={with_scoring}"
            );
            assert_eq!(
                phrase_matching_docs(&index, 7, &["a", "b", "c", "d"], with_scoring),
                vec![0],
                "with_scoring={with_scoring}"
            );
        }
        Ok(())
    }

    /// Reference for sloppy phrase matching: the minimal total cost over all choices of one
    /// position per term, where the cost of a choice is the sum of the distances from each
    /// (offset adjusted) position to the best common alignment point.
    fn naive_phrase_matches(doc_tokens: &[&str], phrase: &[&str], slop: u32) -> bool {
        let positions: Vec<Vec<i64>> = phrase
            .iter()
            .enumerate()
            .map(|(term_idx, term)| {
                doc_tokens
                    .iter()
                    .enumerate()
                    .filter(|(_, token)| *token == term)
                    .map(|(pos, _)| pos as i64 - term_idx as i64)
                    .collect()
            })
            .collect();
        if positions
            .iter()
            .any(|term_positions| term_positions.is_empty())
        {
            return false;
        }
        let mut choice = vec![0usize; phrase.len()];
        loop {
            let mut chosen: Vec<i64> = choice
                .iter()
                .enumerate()
                .map(|(term_idx, &i)| positions[term_idx][i])
                .collect();
            chosen.sort_unstable();
            let median = chosen[chosen.len() / 2];
            let cost: i64 = chosen.iter().map(|pos| (pos - median).abs()).sum();
            if cost <= slop as i64 {
                return true;
            }
            // Advance the mixed-radix counter.
            let mut term_idx = 0;
            loop {
                if term_idx == choice.len() {
                    return false;
                }
                choice[term_idx] += 1;
                if choice[term_idx] < positions[term_idx].len() {
                    break;
                }
                choice[term_idx] = 0;
                term_idx += 1;
            }
        }
    }

    #[test]
    pub fn test_sloppy_phrase_matches_naive_reference() -> crate::Result<()> {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};

        const ALPHABET: [&str; 4] = ["a", "b", "c", "d"];
        let mut rng = StdRng::seed_from_u64(0x5107);
        let texts: Vec<String> = (0..400)
            .map(|_| {
                let len = rng.random_range(2..=12);
                (0..len)
                    .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())])
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect();
        let index = create_index(&texts)?;
        for _ in 0..60 {
            let num_terms = rng.random_range(2..=5);
            let phrase: Vec<&str> = (0..num_terms)
                .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())])
                .collect();
            let slop = rng.random_range(0..=8);
            let expected: Vec<DocId> = texts
                .iter()
                .enumerate()
                .filter(|(_, text)| {
                    let tokens: Vec<&str> = text.split(' ').collect();
                    naive_phrase_matches(&tokens, &phrase, slop)
                })
                .map(|(doc_id, _)| doc_id as DocId)
                .collect();
            for with_scoring in [false, true] {
                assert_eq!(
                    phrase_matching_docs(&index, slop, &phrase, with_scoring),
                    expected,
                    "phrase={phrase:?} slop={slop} with_scoring={with_scoring}"
                );
            }
        }
        Ok(())
    }

    #[test]
    pub fn test_phrase_score_with_slop_ordering() -> crate::Result<()> {
        let index = create_index(&[
            "a e b e c",
            "a e e e e e b e e e e c",
            "a c b", // also matches
            "a c e b e",
            "a e c b",
            "a e b c",
        ])?;
        let scores = test_query(3, &index, vec!["a", "b", "c"]);
        // The first and last matches.
        assert_nearly_equals!(scores[0], 0.23091172);
        assert_nearly_equals!(scores[1], 0.27310878);
        assert_nearly_equals!(scores[3], 0.25024384);
        Ok(())
    }

    #[test] // motivated by #234
    pub fn test_phrase_query_docfreq_order() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field=>"b"))?;
            index_writer.add_document(doc!(text_field=>"a b"))?;
            index_writer.add_document(doc!(text_field=>"b a"))?;
            index_writer.commit()?;
        }

        let searcher = index.reader()?.searcher();
        let test_query = |texts: Vec<&str>| {
            let terms: Vec<Term> = texts
                .iter()
                .map(|text| Term::from_field_text(text_field, text))
                .collect();
            let phrase_query = PhraseQuery::new(terms);
            searcher
                .search(&phrase_query, &TEST_COLLECTOR_WITH_SCORE)
                .expect("search should succeed")
                .docs()
                .to_vec()
        };
        assert_eq!(test_query(vec!["a", "b"]), vec![DocAddress::new(0, 1)]);
        assert_eq!(test_query(vec!["b", "a"]), vec![DocAddress::new(0, 2)]);
        Ok(())
    }

    #[test] // motivated by #234
    pub fn test_phrase_query_non_trivial_offsets() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field=>"a b c d e f g h"))?;
            index_writer.commit()?;
        }
        let searcher = index.reader().unwrap().searcher();
        let test_query = |texts: Vec<(usize, &str)>| {
            let terms: Vec<(usize, Term)> = texts
                .iter()
                .map(|(offset, text)| (*offset, Term::from_field_text(text_field, text)))
                .collect();
            let phrase_query = PhraseQuery::new_with_offset(terms);
            searcher
                .search(&phrase_query, &TEST_COLLECTOR_WITH_SCORE)
                .expect("search should succeed")
                .docs()
                .iter()
                .map(|doc_address| doc_address.doc_id)
                .collect::<Vec<DocId>>()
        };
        assert_eq!(test_query(vec![(0, "a"), (1, "b")]), vec![0]);
        assert_eq!(test_query(vec![(1, "b"), (0, "a")]), vec![0]);
        assert!(test_query(vec![(0, "a"), (2, "b")]).is_empty());
        assert_eq!(test_query(vec![(0, "a"), (2, "c")]), vec![0]);
        assert_eq!(test_query(vec![(0, "a"), (2, "c"), (3, "d")]), vec![0]);
        assert_eq!(test_query(vec![(0, "a"), (2, "c"), (4, "e")]), vec![0]);
        assert_eq!(test_query(vec![(4, "e"), (0, "a"), (2, "c")]), vec![0]);
        assert!(test_query(vec![(0, "a"), (2, "d")]).is_empty());
        assert_eq!(test_query(vec![(1, "a"), (3, "c")]), vec![0]);
        Ok(())
    }

    #[test]
    pub fn test_phrase_query_on_json() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        {
            let mut index_writer: IndexWriter = index.writer_for_tests()?;
            index_writer.add_document(doc!(json_field=>json!({
                "text": "elliot smith the happy who"
            })))?;
            index_writer.add_document(doc!(json_field=>json!({
                "text": "the who elliot smith"
            })))?;
            index_writer.add_document(doc!(json_field=>json!({
                "arr": [{"text":"the who"}, {"text":"elliot smith"}]
            })))?;
            index_writer.add_document(doc!(json_field=>json!({
                "text2": "the smith"
            })))?;
            index_writer.commit()?;
        }
        let searcher = index.reader()?.searcher();
        let matching_docs = |query: &str| {
            let query_parser = QueryParser::for_index(&index, vec![json_field]);
            let phrase_query = query_parser.parse_query(query).unwrap();
            let phrase_weight = phrase_query
                .weight(EnableScoring::disabled_from_schema(searcher.schema()))
                .unwrap();
            let mut phrase_scorer = phrase_weight
                .scorer(searcher.segment_reader(0), 1.0f32)
                .unwrap();
            let mut docs = Vec::new();
            loop {
                let doc = phrase_scorer.doc();
                if doc == TERMINATED {
                    break;
                }
                docs.push(doc);
                phrase_scorer.advance();
            }
            docs
        };
        assert!(matching_docs(r#"text:"the smith""#).is_empty());
        assert_eq!(&matching_docs(r#"text:the"#), &[0u32, 1u32]);
        assert_eq!(&matching_docs(r#"text:"the""#), &[0u32, 1u32]);
        assert_eq!(&matching_docs(r#"text:"smith""#), &[0u32, 1u32]);
        assert_eq!(&matching_docs(r#"text:"elliot smith""#), &[0u32, 1u32]);
        assert_eq!(&matching_docs(r#"text2:"the smith""#), &[3u32]);
        assert!(&matching_docs(r#"arr.text:"the smith""#).is_empty());
        assert_eq!(&matching_docs(r#"arr.text:"elliot smith""#), &[2]);
        Ok(())
    }
}
