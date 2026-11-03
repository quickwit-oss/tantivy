use std::sync::Arc;

use crate::fieldnorm::FieldNormReader;
use crate::query::Explanation;
use crate::schema::Field;
use crate::{Score, Searcher, Term};

const K1: Score = 1.2;
const B: Score = 0.75;

/// An interface to compute the statistics needed in BM25 scoring.
///
/// The standard implementation is a [Searcher] but you can also
/// create your own to adjust the statistics.
pub trait Bm25StatisticsProvider {
    /// The total number of tokens in a given field across all documents in
    /// the index.
    fn total_num_tokens(&self, field: Field) -> crate::Result<u64>;

    /// The total number of documents in the index.
    fn total_num_docs(&self) -> crate::Result<u64>;

    /// The number of documents containing the given term.
    fn doc_freq(&self, term: &Term) -> crate::Result<u64>;
}

impl Bm25StatisticsProvider for Searcher {
    fn total_num_tokens(&self, field: Field) -> crate::Result<u64> {
        let mut total_num_tokens = 0u64;

        for segment_reader in self.segment_readers() {
            let inverted_index = segment_reader.inverted_index(field)?;
            total_num_tokens += inverted_index.total_num_tokens();
        }
        Ok(total_num_tokens)
    }

    fn total_num_docs(&self) -> crate::Result<u64> {
        let mut total_num_docs = 0u64;

        for segment_reader in self.segment_readers() {
            total_num_docs += u64::from(segment_reader.max_doc());
        }
        Ok(total_num_docs)
    }

    fn doc_freq(&self, term: &Term) -> crate::Result<u64> {
        self.doc_freq(term)
    }
}

pub(crate) fn idf(doc_freq: u64, doc_count: u64) -> Score {
    assert!(doc_count >= doc_freq, "{doc_count} >= {doc_freq}");
    let x = ((doc_count - doc_freq) as Score + 0.5) / (doc_freq as Score + 0.5);
    (1.0 + x).ln()
}

fn cached_tf_component(fieldnorm: u32, average_fieldnorm: Score) -> Score {
    K1 * (1.0 - B + B * fieldnorm as Score / average_fieldnorm)
}

fn compute_tf_cache(average_fieldnorm: Score) -> Arc<[Score; 256]> {
    let mut cache: [Score; 256] = [0.0; 256];
    for (fieldnorm_id, cache_mut) in cache.iter_mut().enumerate() {
        let fieldnorm = FieldNormReader::id_to_fieldnorm(fieldnorm_id as u8);
        *cache_mut = cached_tf_component(fieldnorm, average_fieldnorm);
    }
    Arc::new(cache)
}

/// A struct used for computing BM25 scores.
#[derive(Clone)]
pub struct Bm25Weight {
    idf_explain: Option<Explanation>,
    weight: Score,
    cache: Arc<[Score; 256]>,
    average_fieldnorm: Score,
}

impl Bm25Weight {
    /// Increase the weight by a multiplicative factor.
    pub fn boost_by(&self, boost: Score) -> Bm25Weight {
        if boost == 1.0f32 {
            return self.clone();
        }
        Bm25Weight {
            idf_explain: self.idf_explain.clone(),
            weight: self.weight * boost,
            cache: self.cache.clone(),
            average_fieldnorm: self.average_fieldnorm,
        }
    }

    /// Construct a [Bm25Weight] for a phrase of terms.
    pub fn for_terms(
        statistics: &dyn Bm25StatisticsProvider,
        terms: &[Term],
    ) -> crate::Result<Bm25Weight> {
        assert!(!terms.is_empty(), "Bm25 requires at least one term");
        let field = terms[0].field();
        for term in &terms[1..] {
            assert_eq!(
                term.field(),
                field,
                "All terms must belong to the same field."
            );
        }

        let total_num_tokens = statistics.total_num_tokens(field)?;
        let total_num_docs = statistics.total_num_docs()?;
        let average_fieldnorm = total_num_tokens as Score / total_num_docs as Score;

        if terms.len() == 1 {
            let term_doc_freq = statistics.doc_freq(&terms[0])?;
            Ok(Bm25Weight::for_one_term(
                term_doc_freq,
                total_num_docs,
                average_fieldnorm,
            ))
        } else {
            let mut idf_sum: Score = 0.0;
            for term in terms {
                let term_doc_freq = statistics.doc_freq(term)?;
                idf_sum += idf(term_doc_freq, total_num_docs);
            }
            let idf_explain = Explanation::new("idf", idf_sum);
            Ok(Bm25Weight::new(idf_explain, average_fieldnorm))
        }
    }

    /// Construct a [Bm25Weight] for a single term.
    pub fn for_one_term(
        term_doc_freq: u64,
        total_num_docs: u64,
        avg_fieldnorm: Score,
    ) -> Bm25Weight {
        let idf = idf(term_doc_freq, total_num_docs);
        let mut idf_explain =
            Explanation::new("idf, computed as log(1 + (N - n + 0.5) / (n + 0.5))", idf);
        idf_explain.add_const(
            "n, number of docs containing this term",
            term_doc_freq as Score,
        );
        idf_explain.add_const("N, total number of docs", total_num_docs as Score);
        Bm25Weight::new(idf_explain, avg_fieldnorm)
    }
    /// Construct a [Bm25Weight] for a single term.
    /// This method does not carry the [Explanation] for the idf.
    pub fn for_one_term_without_explain(
        term_doc_freq: u64,
        total_num_docs: u64,
        avg_fieldnorm: Score,
    ) -> Bm25Weight {
        let idf = idf(term_doc_freq, total_num_docs);
        Bm25Weight::new_without_explain(idf, avg_fieldnorm)
    }

    pub(crate) fn new(idf_explain: Explanation, average_fieldnorm: Score) -> Bm25Weight {
        let weight = idf_explain.value() * (1.0 + K1);
        Bm25Weight {
            idf_explain: Some(idf_explain),
            weight,
            cache: compute_tf_cache(average_fieldnorm),
            average_fieldnorm,
        }
    }
    pub(crate) fn new_without_explain(idf: f32, average_fieldnorm: Score) -> Bm25Weight {
        let weight = idf * (1.0 + K1);
        Bm25Weight {
            idf_explain: None,
            weight,
            cache: compute_tf_cache(average_fieldnorm),
            average_fieldnorm,
        }
    }

    /// Compute the BM25 score of a single document.
    #[inline]
    pub fn score(&self, fieldnorm_id: u8, term_freq: u32) -> Score {
        self.weight * self.tf_factor(fieldnorm_id, term_freq)
    }

    /// Compute the maximum possible BM25 score given this weight.
    pub fn max_score(&self) -> Score {
        self.score(255u8, 2_013_265_944)
    }

    pub(crate) fn is_non_negative(&self) -> bool {
        self.weight >= 0.0
    }

    pub(crate) fn max_score_for(&self, fieldnorm_reader: &FieldNormReader) -> Score {
        if !self.is_non_negative() {
            return 0.0;
        }
        if fieldnorm_reader.is_constant() {
            // A synthetic field length does not bound term frequency; tf_factor is at most 1.
            self.weight
        } else {
            self.max_score()
        }
    }

    /// Bounds scores at this norm for frequencies in `1..=max_term_freq`.
    pub(crate) fn max_score_with_term_freq(&self, fieldnorm_id: u8, max_term_freq: u32) -> Score {
        if !self.is_non_negative() {
            return 0.0;
        }
        if max_term_freq == 1 {
            return self.score(fieldnorm_id, 1);
        }
        let term_freq = max_term_freq as Score;
        let norm = self.cache[fieldnorm_id as usize];
        let denominator = term_freq + norm;
        if !denominator.is_finite() {
            return self.weight;
        }
        // One ULP here bounds addition rounding at every smaller frequency. Subtract it
        // from the norm, rounding down, so tf / (tf + lower_norm) bounds the actual ratio.
        // Unlike score(max_term_freq), this also covers non-monotonic f32 rounding.
        let rounding_error = denominator.next_up() - denominator;
        let lower_norm = (norm - rounding_error).next_down().max(0.0);
        let lower_denominator = (term_freq + lower_norm).next_down().max(term_freq);
        self.weight * (term_freq / lower_denominator)
    }

    #[inline]
    pub(crate) fn tf_factor(&self, fieldnorm_id: u8, term_freq: u32) -> Score {
        let term_freq = term_freq as Score;
        let norm = self.cache[fieldnorm_id as usize];
        term_freq / (term_freq + norm)
    }

    /// Produce an [Explanation] of a BM25 score.
    pub fn explain(&self, fieldnorm_id: u8, term_freq: u32) -> Explanation {
        // The explain format is directly copied from Lucene's.
        // (So, Kudos to Lucene)
        let score = self.score(fieldnorm_id, term_freq);

        let norm = self.cache[fieldnorm_id as usize];
        let term_freq = term_freq as Score;
        let right_factor = term_freq / (term_freq + norm);

        let mut tf_explanation = Explanation::new(
            "freq / (freq + k1 * (1 - b + b * dl / avgdl))",
            right_factor,
        );

        tf_explanation.add_const("freq, occurrences of term within document", term_freq);
        tf_explanation.add_const("k1, term saturation parameter", K1);
        tf_explanation.add_const("b, length normalization parameter", B);
        tf_explanation.add_const(
            "dl, length of field",
            FieldNormReader::id_to_fieldnorm(fieldnorm_id) as Score,
        );
        tf_explanation.add_const("avgdl, average length of field", self.average_fieldnorm);

        let mut explanation = Explanation::new("TermQuery, product of...", score);
        explanation.add_detail(Explanation::new("(K1+1)", K1 + 1.0));
        if let Some(idf_explain) = &self.idf_explain {
            explanation.add_detail(idf_explain.clone());
        }
        explanation.add_detail(tf_explanation);
        explanation
    }
}

#[cfg(test)]
mod tests {

    use super::{idf, Bm25Weight};
    use crate::fieldnorm::FieldNormReader;
    use crate::{assert_nearly_equals, Score};

    #[test]
    fn test_max_score_with_term_freq() {
        for average_fieldnorm in [0.00001, 0.5, 1.0, 10.0, 1000.0, f32::MAX] {
            for boost in [-2.0, 0.0, 0.5, 1.0, 3.0] {
                let weight =
                    Bm25Weight::for_one_term(128, 33_554_432, average_fieldnorm).boost_by(boost);
                for fieldnorm_id in [0, 1, 40, 255] {
                    for bits in 0..=32 {
                        let max_tf = 1u32.checked_shl(bits).unwrap_or(u32::MAX);
                        let bound = weight.max_score_with_term_freq(fieldnorm_id, max_tf);
                        for tf in
                            (1..=max_tf.min(256)).chain(max_tf.saturating_sub(256).max(1)..=max_tf)
                        {
                            assert!(
                                bound >= weight.score(fieldnorm_id, tf),
                                "avg={average_fieldnorm}, boost={boost}, norm={fieldnorm_id}, \
                                 tf={tf}, max_tf={max_tf}"
                            );
                        }
                    }
                }
            }
        }
        let weight = Bm25Weight::for_one_term(128, 33_554_432, 1.0);
        assert!(weight.score(1, (1 << 24) - 1) > weight.score(1, 1 << 24));
        assert!(weight.max_score_with_term_freq(1, 1 << 24) >= weight.score(1, (1 << 24) - 1));
        assert_eq!(weight.max_score_with_term_freq(1, 1), weight.score(1, 1));
        assert!(weight.max_score_with_term_freq(1, 8) < weight.weight);
    }

    #[test]
    fn test_max_score_for_fieldnorm_reader() {
        for average_fieldnorm in [0.5, 1.0, 10.0, 1000.0] {
            for boost in [-2.0, 0.0, 0.5, 1.0, 3.0] {
                let weight = Bm25Weight::for_one_term(10, 100, average_fieldnorm).boost_by(boost);
                let stored = FieldNormReader::for_test(&[1, 10, 100]);
                if boost >= 0.0 {
                    assert_eq!(weight.max_score_for(&stored), weight.max_score());
                }
                for fieldnorm in [0, 1, 10, 100, u32::MAX] {
                    let reader = FieldNormReader::constant(1, fieldnorm);
                    let bound = weight.max_score_for(&reader);
                    for term_freq in [
                        1,
                        3,
                        20,
                        255,
                        100_000,
                        u32::MAX - 512,
                        u32::MAX - 127,
                        u32::MAX,
                    ] {
                        assert!(bound >= weight.score(reader.fieldnorm_id(0), term_freq));
                    }
                }
            }
        }
    }

    #[test]
    fn test_idf() {
        let score: Score = 2.0;
        assert_nearly_equals!(idf(1, 2), score.ln());
    }
}
