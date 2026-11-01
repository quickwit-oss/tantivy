use std::collections::HashMap;

use crate::docset::DocSet;
use crate::fieldnorm::FieldNormReader;
use crate::postings::{BlockSegmentPostings, FreqReadingOption, Postings, SegmentPostings};
use crate::query::bm25::{Bm25BlockMax, Bm25Weight};
use crate::query::{Explanation, Scorer};
use crate::{DocId, Score};

#[derive(Clone)]
pub struct TermScorer {
    postings: SegmentPostings,
    fieldnorm_reader: FieldNormReader,
    similarity_weight: Bm25Weight,
    segment_average_fieldnorm: Option<Score>,
}

pub(crate) struct TermPruningBounds {
    pub global: Score,
    correction: Option<Option<Bm25BlockMax>>,
    block: Option<(DocId, Score)>,
}

impl TermScorer {
    pub fn new(
        postings: SegmentPostings,
        fieldnorm_reader: FieldNormReader,
        similarity_weight: Bm25Weight,
    ) -> TermScorer {
        TermScorer {
            postings,
            fieldnorm_reader,
            similarity_weight,
            segment_average_fieldnorm: None,
        }
    }

    pub(crate) fn with_segment_average_fieldnorm(mut self, average: Score) -> Self {
        self.segment_average_fieldnorm = Some(average);
        self
    }

    pub(crate) fn pruning_score_upper_bound(&self) -> Option<Score> {
        self.similarity_weight.pruning_score_upper_bound()
    }

    pub(crate) fn pruning_bounds(&self) -> Option<TermPruningBounds> {
        Some(TermPruningBounds {
            global: self.pruning_score_upper_bound()?,
            correction: None,
            block: None,
        })
    }

    pub(crate) fn pruning_block_max_score(
        &mut self,
        bounds: &mut TermPruningBounds,
        shared: &mut HashMap<(u32, u32), Option<Bm25BlockMax>>,
    ) -> Score {
        let block = &self.postings.block_cursor;
        match block.skip_reader().block_info() {
            crate::postings::BlockInfo::VInt { .. } => {
                if block.block_is_loaded() {
                    self.block_max_score()
                } else {
                    bounds.global
                }
            }
            crate::postings::BlockInfo::BitPacked {
                block_wand_fieldnorm_id,
                block_wand_term_freq,
                ..
            } => {
                let end = self.last_doc_in_block();
                if let Some((cached_end, score)) = bounds.block {
                    if cached_end == end {
                        return score;
                    }
                }
                if !(1..255).contains(&block_wand_term_freq) {
                    return bounds.global;
                }
                let score = if let Some(average) = self.segment_average_fieldnorm {
                    let mode = *bounds.correction.get_or_insert_with(|| {
                        let key = (
                            self.similarity_weight.average_fieldnorm().to_bits(),
                            average.to_bits(),
                        );
                        if key.0 == key.1 {
                            return self.similarity_weight.prepare_block_max(average);
                        }
                        *shared
                            .entry(key)
                            .or_insert_with(|| self.similarity_weight.prepare_block_max(average))
                    });
                    mode.and_then(|mode| {
                        self.similarity_weight.pruning_block_score(
                            mode,
                            block_wand_fieldnorm_id,
                            block_wand_term_freq,
                        )
                    })
                    .unwrap_or(bounds.global)
                } else {
                    bounds.global
                };
                bounds.block = Some((end, score));
                score
            }
        }
    }

    pub(crate) fn seek_block(&mut self, target_doc: DocId) {
        self.postings.block_cursor.seek_block(target_doc);
    }

    #[cfg(test)]
    pub fn create_for_test(
        doc_and_tfs: &[(DocId, u32)],
        fieldnorms: &[u32],
        similarity_weight: Bm25Weight,
    ) -> TermScorer {
        assert!(!doc_and_tfs.is_empty());
        assert!(
            doc_and_tfs
                .iter()
                .map(|(doc, _tf)| *doc)
                .max()
                .unwrap_or(0u32)
                < fieldnorms.len() as u32
        );
        let segment_postings =
            SegmentPostings::create_from_docs_and_tfs(doc_and_tfs, Some(fieldnorms));
        let fieldnorm_reader = FieldNormReader::for_test(fieldnorms);
        let total_num_tokens = fieldnorms.iter().map(|&norm| u64::from(norm)).sum::<u64>();
        let average = crate::query::bm25::segment_average_fieldnorm(
            total_num_tokens,
            fieldnorms.len() as u32,
        );
        TermScorer::new(segment_postings, fieldnorm_reader, similarity_weight)
            .with_segment_average_fieldnorm(average)
    }

    /// See `FreqReadingOption`.
    pub(crate) fn freq_reading_option(&self) -> FreqReadingOption {
        self.postings.block_cursor.freq_reading_option()
    }

    /// Returns the maximum score for the current block.
    ///
    /// In some rare case, the result may not be exact. In this case a lower value is returned,
    /// (and may lead us to return a lesser document).
    ///
    /// At index time, we store the (fieldnorm_id, term frequency) pair that maximizes the
    /// score assuming the average fieldnorm computed on this segment.
    ///
    /// Though extremely rare, it is theoretically possible that the actual average fieldnorm
    /// is different enough from the current segment average fieldnorm that the maximum over a
    /// specific is achieved on a different document.
    ///
    /// Stored full-block maxima require recorded fieldnorms, the same average used by the
    /// serializer, and an unquantized term frequency. An unloaded final block can also
    /// fall back to an underestimated bound.
    pub fn block_max_score(&mut self) -> Score {
        self.postings
            .block_cursor
            .block_max_score(&self.fieldnorm_reader, &self.similarity_weight)
    }

    pub fn term_freq(&self) -> u32 {
        self.postings.term_freq()
    }

    pub fn fieldnorm_id(&self) -> u8 {
        self.fieldnorm_reader.fieldnorm_id(self.doc())
    }

    pub fn explain(&self) -> Explanation {
        let fieldnorm_id = self.fieldnorm_id();
        let term_freq = self.term_freq();
        self.similarity_weight.explain(fieldnorm_id, term_freq)
    }

    pub fn max_score(&self) -> Score {
        self.similarity_weight.max_score()
    }

    pub fn last_doc_in_block(&self) -> DocId {
        self.postings.block_cursor.skip_reader().last_doc_in_block()
    }

    /// Returns a mutable reference to the underlying block cursor.
    pub(crate) fn block_cursor(&mut self) -> &mut BlockSegmentPostings {
        &mut self.postings.block_cursor
    }

    /// Returns a reference to the fieldnorm reader for batch lookups.
    pub(crate) fn fieldnorm_reader(&self) -> &FieldNormReader {
        &self.fieldnorm_reader
    }

    /// Returns a reference to the BM25 weight for batch score computation.
    pub(crate) fn bm25_weight(&self) -> &Bm25Weight {
        &self.similarity_weight
    }
}

impl DocSet for TermScorer {
    #[inline]
    fn advance(&mut self) -> DocId {
        self.postings.advance()
    }

    #[inline]
    fn seek(&mut self, target: DocId) -> DocId {
        debug_assert!(target >= self.doc());
        self.postings.seek(target)
    }

    #[inline]
    fn doc(&self) -> DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }

    // TODO
    // It is probably possible to optimize fill_bitset_block for TermScorer,
    // working directly with the blocks, enabling vectorization.
    // I did not manage to get a performance improvement on Mac ARM,
    // and do not have access to x86 to investigate.
}

impl Scorer for TermScorer {
    #[inline]
    fn score(&mut self) -> Score {
        let fieldnorm_id = self.fieldnorm_id();
        let term_freq = self.term_freq();
        self.similarity_weight.score(fieldnorm_id, term_freq)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use crate::index::SegmentId;
    use crate::indexer::index_writer::MEMORY_BUDGET_NUM_BYTES_MIN;
    use crate::merge_policy::NoMergePolicy;
    use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
    use crate::query::term_query::TermScorer;
    use crate::query::{Bm25Weight, EnableScoring, Scorer, TermQuery};
    use crate::schema::{IndexRecordOption, Schema, TEXT};
    use crate::{
        assert_nearly_equals, DocId, DocSet, Index, IndexWriter, Score, Searcher, Term, TERMINATED,
    };

    fn test_block_bound(scorer: &mut TermScorer, global: Score) -> Score {
        let mut bounds = scorer.pruning_bounds().unwrap();
        assert_eq!(bounds.global, global);
        scorer.pruning_block_max_score(&mut bounds, &mut std::collections::HashMap::new())
    }

    #[test]
    fn pruning_block_bounds_require_matching_average_and_available_block() {
        let postings = (0..259).map(|doc| (doc, 1)).collect::<Vec<_>>();
        let norms = vec![100; 259];
        let weight = Bm25Weight::for_one_term(259, 1000, 100.0);
        let mut scorer = TermScorer::create_for_test(&postings, &norms, weight.clone());
        let global = scorer.pruning_score_upper_bound().unwrap();
        let exact = scorer.block_max_score();
        assert!(exact < global);
        assert_eq!(
            test_block_bound(&mut scorer, global).to_bits(),
            exact.to_bits()
        );
        let mut mismatched = TermScorer::create_for_test(
            &postings,
            &norms,
            Bm25Weight::for_one_term(259, 1000, 100.0f32.next_up()),
        );
        let mismatch_bound = mismatched.pruning_score_upper_bound().unwrap();
        let corrected = test_block_bound(&mut mismatched, mismatch_bound);
        assert!(corrected < mismatch_bound);
        assert!(corrected >= mismatched.score());
        scorer.seek_block(258);
        assert!(!scorer.postings.block_cursor.block_is_loaded());
        assert_eq!(test_block_bound(&mut scorer, global), global);
        scorer.seek(258);
        assert!(test_block_bound(&mut scorer, global) < global);
        let mut ungated = TermScorer::new(
            crate::postings::SegmentPostings::create_from_docs_and_tfs(&postings, Some(&norms)),
            crate::fieldnorm::FieldNormReader::for_test(&norms),
            weight,
        );
        assert_eq!(test_block_bound(&mut ungated, global), global);
    }

    #[test]
    fn production_term_weight_gates_stored_bounds_on_real_fieldnorms() -> crate::Result<()> {
        use crate::schema::{TextFieldIndexing, TextOptions};
        for fieldnorms in [true, false] {
            let mut schema = Schema::builder();
            let field = schema.add_text_field(
                "text",
                TextOptions::default().set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("default")
                        .set_index_option(IndexRecordOption::WithFreqs)
                        .set_fieldnorms(fieldnorms),
                ),
            );
            let index = Index::create_in_ram(schema.build());
            let mut writer = index.writer::<crate::TantivyDocument>(20_000_000)?;
            for _ in 0..259 {
                writer.add_document(doc!(field => "amber stone"))?;
            }
            writer.commit()?;
            let searcher = index.reader()?.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            let query = TermQuery::new(
                Term::from_field_text(field, "amber"),
                IndexRecordOption::WithFreqs,
            );
            let weight =
                query.specialized_weight(EnableScoring::enabled_from_searcher(&searcher))?;
            let mut scorer = weight
                .term_scorer_for_test(searcher.segment_reader(0), 1.0)?
                .unwrap();
            assert_eq!(scorer.segment_average_fieldnorm.is_some(), fieldnorms);
            let global = scorer.pruning_score_upper_bound().unwrap();
            let block = test_block_bound(&mut scorer, global);
            if fieldnorms {
                assert!(block < global);
                assert_eq!(block.to_bits(), scorer.block_max_score().to_bits());
            } else {
                assert_eq!(block, global);
            }
        }
        Ok(())
    }

    #[test]
    fn test_term_scorer_max_score() -> crate::Result<()> {
        let bm25_weight = Bm25Weight::for_one_term(3, 6, 10.0);
        let mut term_scorer = TermScorer::create_for_test(
            &[(2, 3), (3, 12), (7, 8)],
            &[0, 0, 10, 12, 0, 0, 0, 100],
            bm25_weight,
        );
        let max_scorer = term_scorer.max_score();
        crate::assert_nearly_equals!(max_scorer, 1.3990127);
        assert_eq!(term_scorer.doc(), 2);
        assert_eq!(term_scorer.term_freq(), 3);
        assert_nearly_equals!(term_scorer.block_max_score(), 1.3676447);
        assert_nearly_equals!(term_scorer.score(), 1.0892314);
        assert_eq!(term_scorer.advance(), 3);
        assert_eq!(term_scorer.doc(), 3);
        assert_eq!(term_scorer.term_freq(), 12);
        assert_nearly_equals!(term_scorer.score(), 1.3676447);
        assert_eq!(term_scorer.advance(), 7);
        assert_eq!(term_scorer.doc(), 7);
        assert_eq!(term_scorer.term_freq(), 8);
        assert_nearly_equals!(term_scorer.score(), 0.72015285);
        assert_eq!(term_scorer.advance(), TERMINATED);
        Ok(())
    }

    #[test]
    fn test_term_scorer_shallow_advance() -> crate::Result<()> {
        let bm25_weight = Bm25Weight::for_one_term(300, 1024, 10.0);
        let mut doc_and_tfs = vec![];
        for i in 0u32..300u32 {
            let doc = i * 10;
            doc_and_tfs.push((doc, 1u32 + doc % 3u32));
        }
        let fieldnorms: Vec<u32> = std::iter::repeat_n(10u32, 3_000).collect();
        let mut term_scorer = TermScorer::create_for_test(&doc_and_tfs, &fieldnorms, bm25_weight);
        assert_eq!(term_scorer.doc(), 0u32);
        term_scorer.seek_block(1289);
        assert_eq!(term_scorer.doc(), 0u32);
        term_scorer.seek(1289);
        assert_eq!(term_scorer.doc(), 1290);
        Ok(())
    }

    proptest! {
        #[test]
        fn test_term_scorer_block_max_score(term_freqs_fieldnorms in proptest::collection::vec((1u32..10u32, 0u32..100u32), 80..300)) {
        let term_doc_freq = term_freqs_fieldnorms.len();
         let doc_tfs: Vec<(u32, u32)> = term_freqs_fieldnorms.iter()
                   .cloned()
                  .enumerate()
                  .map(|(doc, (tf, _))| (doc as u32, tf))
                  .collect();

         let mut fieldnorms: Vec<u32> = vec![];
         for item in term_freqs_fieldnorms.iter().take(term_doc_freq) {
             let (tf, num_extra_terms) = item;
             fieldnorms.push(tf + num_extra_terms);
         }
         let average_fieldnorm = fieldnorms
             .iter()
             .cloned()
             .sum::<u32>() as Score / term_doc_freq as Score;
             // Average fieldnorm is over the entire index,
             // not necessarily the docs that are in the posting list.
             // For this reason we multiply by 1.1 to make a realistic value.
         let bm25_weight = Bm25Weight::for_one_term(term_doc_freq as u64,
            term_doc_freq as u64 * 10u64,
            average_fieldnorm);

         let mut term_scorer =
              TermScorer::create_for_test(&doc_tfs[..], &fieldnorms[..], bm25_weight);

         let docs: Vec<DocId> = (0..term_doc_freq).map(|doc| doc as DocId).collect();
         for block in docs.chunks(COMPRESSION_BLOCK_SIZE) {
             let block_max_score: Score = term_scorer.block_max_score();
             let mut block_max_score_computed: Score = 0.0;
             for &doc in block {
                assert_eq!(term_scorer.doc(), doc);
                block_max_score_computed = block_max_score_computed.max(term_scorer.score());
                term_scorer.advance();
             }
             assert_nearly_equals!(block_max_score_computed, block_max_score);
         }
        }
    }

    #[test]
    fn test_block_wand() {
        let mut doc_tfs: Vec<(u32, u32)> = vec![];
        for doc in 0u32..128u32 {
            doc_tfs.push((doc, 1u32));
        }
        for doc in 128u32..256u32 {
            doc_tfs.push((doc, if doc == 200 { 2u32 } else { 1u32 }));
        }
        doc_tfs.push((256, 1u32));
        doc_tfs.push((257, 3u32));
        doc_tfs.push((258, 1u32));

        let fieldnorms: Vec<u32> = std::iter::repeat_n(20u32, 300).collect();
        let bm25_weight = Bm25Weight::for_one_term(10, 129, 20.0);
        let mut docs = TermScorer::create_for_test(&doc_tfs[..], &fieldnorms[..], bm25_weight);
        assert_nearly_equals!(docs.block_max_score(), 2.5161593);
        docs.seek_block(135);
        assert_nearly_equals!(docs.block_max_score(), 3.4597192);
        docs.seek_block(256);
        // the block is not loaded yet.
        assert_nearly_equals!(docs.block_max_score(), 5.2971773);
        assert_eq!(256, docs.seek(256));
        assert_nearly_equals!(docs.block_max_score(), 3.9539647);
    }

    fn test_block_wand_aux(term_query: &TermQuery, searcher: &Searcher) -> crate::Result<()> {
        let term_weight =
            term_query.specialized_weight(EnableScoring::enabled_from_searcher(searcher))?;
        for reader in searcher.segment_readers() {
            let mut block_max_scores = vec![];
            let mut block_max_scores_b = vec![];
            let mut docs = vec![];
            {
                let mut term_scorer = term_weight.term_scorer_for_test(reader, 1.0)?.unwrap();
                while term_scorer.doc() != TERMINATED {
                    let mut score = term_scorer.score();
                    docs.push(term_scorer.doc());
                    for _ in 0..128 {
                        score = score.max(term_scorer.score());
                        if term_scorer.advance() == TERMINATED {
                            break;
                        }
                    }
                    block_max_scores.push(score);
                }
            }
            {
                let mut term_scorer = term_weight.term_scorer_for_test(reader, 1.0)?.unwrap();
                for d in docs {
                    term_scorer.seek_block(d);
                    block_max_scores_b.push(term_scorer.block_max_score());
                }
            }
            for (l, r) in block_max_scores
                .iter()
                .cloned()
                .zip(block_max_scores_b.iter().cloned())
            {
                assert_nearly_equals!(l, r);
            }
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn test_block_wand_long_test() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer: IndexWriter =
            index.writer_with_num_threads(3, 3 * MEMORY_BUDGET_NUM_BYTES_MIN)?;
        use rand::Rng;
        let mut rng = rand::rng();
        writer.set_merge_policy(Box::new(NoMergePolicy));
        for _ in 0..3_000 {
            let term_freq = rng.random_range(1..10000);
            let words: Vec<&str> = std::iter::repeat_n("bbbb", term_freq).collect();
            let text = words.join(" ");
            writer.add_document(doc!(text_field=>text))?;
        }
        writer.commit()?;
        let term_query = TermQuery::new(
            Term::from_field_text(text_field, "bbbb"),
            IndexRecordOption::WithFreqs,
        );
        let segment_ids: Vec<SegmentId>;
        let reader = index.reader()?;
        {
            let searcher = reader.searcher();
            segment_ids = searcher
                .segment_readers()
                .iter()
                .map(|segment| segment.segment_id())
                .collect();
            test_block_wand_aux(&term_query, &searcher)?;
        }
        writer.merge(&segment_ids[..]).wait().unwrap();
        {
            reader.reload()?;
            let searcher = reader.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            test_block_wand_aux(&term_query, &searcher)?;
        }
        Ok(())
    }
    #[test]
    fn saturated_skip_frequency_remains_a_conservative_bound() {
        let frequency = u32::MAX - 255;
        let mut norms = vec![1; 615];
        norms[0] = frequency;
        let average =
            norms.iter().map(|&n| u64::from(n)).sum::<u64>() as Score / norms.len() as Score;
        let postings = (0..128)
            .map(|doc| (doc, if doc == 0 { frequency } else { 1 }))
            .collect::<Vec<_>>();
        for boost in [0.21, 0.3, 1.0, 2.0, 10.0, 100.0] {
            let weight = Bm25Weight::for_one_term(128, 615, average).boost_by(boost);
            let mut scorer = TermScorer::create_for_test(&postings, &norms, weight);
            assert!(scorer.segment_average_fieldnorm.is_some());
            let actual = scorer.score();
            let global = scorer.pruning_score_upper_bound().unwrap();
            let bound = test_block_bound(&mut scorer, global);
            assert!(
                bound >= actual,
                "boost={boost} avg={average} tf={frequency} bound={bound} bits={} actual={actual} \
                 bits={}",
                bound.to_bits(),
                actual.to_bits()
            );
        }
    }
    #[test]
    fn serialized_argmax_reconstructs_every_full_block_and_bounds_drifting_scores() {
        let norms = (0..512)
            .map(|id| 1 + (id * 193) % 2000)
            .collect::<Vec<u32>>();
        let postings = norms
            .iter()
            .enumerate()
            .map(|(id, n)| (id as u32, 1 + (id as u32 * 71) % (*n).min(200)))
            .collect::<Vec<_>>();
        let average =
            norms.iter().map(|&n| u64::from(n)).sum::<u64>() as Score / norms.len() as Score;
        let serializer = Bm25Weight::for_one_term(512, 1000, average);
        for query_average in [
            average * 0.01,
            average.next_down(),
            average,
            average.next_up(),
            average * 100.0,
        ] {
            let query = Bm25Weight::for_one_term(512, 1000, query_average).boost_by(0.21);
            let mut scorer = TermScorer::create_for_test(&postings, &norms, query.clone());
            let mut bounds = scorer.pruning_bounds().unwrap();
            let mut shared = std::collections::HashMap::new();
            for start in (0..512).step_by(128) {
                scorer.seek_block(start as u32);
                let crate::postings::BlockInfo::BitPacked {
                    block_wand_fieldnorm_id: id,
                    block_wand_term_freq: tf,
                    ..
                } = scorer.postings.block_cursor.skip_reader().block_info()
                else {
                    panic!("expected full serialized block")
                };
                let reconstructed = serializer.tf_factor(id, tf);
                let expected = postings[start..start + 128]
                    .iter()
                    .map(|&(doc, tf)| {
                        serializer.tf_factor(
                            crate::fieldnorm::FieldNormReader::fieldnorm_to_id(norms[doc as usize]),
                            tf,
                        )
                    })
                    .fold(0.0, Score::max);
                assert_eq!(reconstructed.to_bits(), expected.to_bits());
                let bound = scorer.pruning_block_max_score(&mut bounds, &mut shared);
                for &(doc, tf) in &postings[start..start + 128] {
                    let score = query.score(
                        crate::fieldnorm::FieldNormReader::fieldnorm_to_id(norms[doc as usize]),
                        tf,
                    );
                    assert!(
                        bound >= score,
                        "avg={query_average} doc={doc} bound={bound} actual={score}"
                    );
                }
            }
        }
    }

    #[test]
    fn correction_preparation_is_lazy_and_shared_across_term_weights() {
        let postings = (0..256).map(|d| (d, 1)).collect::<Vec<_>>();
        let norms = vec![100; 256];
        let mut shared = std::collections::HashMap::new();
        for boost in [0.21, 1.0, 2.0] {
            let mut scorer = TermScorer::create_for_test(
                &postings,
                &norms,
                Bm25Weight::for_one_term(256, 1000, 200.0).boost_by(boost),
            );
            let mut bounds = scorer.pruning_bounds().unwrap();
            assert!(bounds.correction.is_none());
            assert!(bounds.block.is_none());
            let first = scorer.pruning_block_max_score(&mut bounds, &mut shared);
            assert!(bounds.correction.is_some());
            assert_eq!(shared.len(), 1);
            assert_eq!(
                scorer
                    .pruning_block_max_score(&mut bounds, &mut shared)
                    .to_bits(),
                first.to_bits()
            );
            scorer.seek_block(128);
            let second = scorer.pruning_block_max_score(&mut bounds, &mut shared);
            assert_eq!(second.to_bits(), first.to_bits());
            assert_eq!(shared.len(), 1);
        }
    }

    #[test]
    fn corrected_bound_covers_an_argmax_change() {
        let norms = (0..128)
            .map(|d| if d % 2 == 0 { 2 } else { 1000 })
            .collect::<Vec<_>>();
        let postings = (0..128)
            .map(|d| (d, if d % 2 == 0 { 1 } else { 2 }))
            .collect::<Vec<_>>();
        let serializer = Bm25Weight::for_one_term(128, 1000, 501.0);
        let query = Bm25Weight::for_one_term(128, 1000, 10000.0);
        let low = crate::fieldnorm::FieldNormReader::fieldnorm_to_id(2);
        let high = crate::fieldnorm::FieldNormReader::fieldnorm_to_id(1000);
        assert!(serializer.tf_factor(low, 1) > serializer.tf_factor(high, 2));
        assert!(query.tf_factor(low, 1) < query.tf_factor(high, 2));
        let mut scorer = TermScorer::create_for_test(&postings, &norms, query.clone());
        let global = scorer.pruning_score_upper_bound().unwrap();
        assert!(scorer.block_max_score() < query.score(high, 2));
        let bound = test_block_bound(&mut scorer, global);
        assert!(bound >= query.score(high, 2));
        assert!(bound < global);
    }
}
