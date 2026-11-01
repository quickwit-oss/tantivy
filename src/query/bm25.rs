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

#[inline]
fn tf_factor(term_freq: u32, norm: Score) -> Score {
    let term_freq = term_freq as Score;
    term_freq / (term_freq + norm)
}

fn compute_tf_cache(average_fieldnorm: Score) -> (Arc<[Score; 256]>, bool) {
    let mut cache: [Score; 256] = [0.0; 256];
    for (fieldnorm_id, cache_mut) in cache.iter_mut().enumerate() {
        let fieldnorm = FieldNormReader::id_to_fieldnorm(fieldnorm_id as u8);
        *cache_mut = cached_tf_component(fieldnorm, average_fieldnorm);
    }
    let valid = cache.iter().all(|norm| norm.is_finite() && *norm >= 0.0);
    (Arc::new(cache), valid)
}

pub(crate) fn segment_average_fieldnorm(total_tokens: u64, num_docs: u32) -> Score {
    total_tokens as Score / num_docs as Score
}

#[derive(Clone, Copy)]
pub(crate) enum Bm25BlockMax {
    Exact,
    Corrected { average: Score, ratio: f64 },
}

/// A struct used for computing BM25 scores.
#[derive(Clone)]
pub struct Bm25Weight {
    idf_explain: Option<Explanation>,
    weight: Score,
    cache: Arc<[Score; 256]>,
    cache_valid: bool,
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
            cache_valid: self.cache_valid,
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
        let (cache, cache_valid) = compute_tf_cache(average_fieldnorm);
        Bm25Weight {
            idf_explain: Some(idf_explain),
            weight,
            cache,
            cache_valid,
            average_fieldnorm,
        }
    }
    pub(crate) fn new_without_explain(idf: f32, average_fieldnorm: Score) -> Bm25Weight {
        let weight = idf * (1.0 + K1);
        let (cache, cache_valid) = compute_tf_cache(average_fieldnorm);
        Bm25Weight {
            idf_explain: None,
            weight,
            cache,
            cache_valid,
            average_fieldnorm,
        }
    }

    /// Compute the BM25 score of a single document.
    #[inline]
    pub fn score(&self, fieldnorm_id: u8, term_freq: u32) -> Score {
        self.weight * self.tf_factor(fieldnorm_id, term_freq)
    }

    pub(crate) fn pruning_score_upper_bound(&self) -> Option<Score> {
        // Positive frequencies and nonnegative norms keep the rounded tf factor at most one.
        (self.weight.is_finite() && self.weight >= 0.0 && self.cache_valid).then_some(self.weight)
    }

    pub(crate) fn has_average_fieldnorm(&self, average: Score) -> bool {
        self.average_fieldnorm.to_bits() == average.to_bits()
    }

    pub(crate) fn average_fieldnorm(&self) -> Score {
        self.average_fieldnorm
    }

    pub(crate) fn prepare_block_max(&self, average: Score) -> Option<Bm25BlockMax> {
        self.pruning_score_upper_bound()?;
        if !average.is_finite() || average <= 0.0 {
            return None;
        }
        if self.has_average_fieldnorm(average) {
            return Some(Bm25BlockMax::Exact);
        }
        let mut ratio = 1.0f64;
        for (id, &query_norm) in self.cache.iter().enumerate() {
            let norm = cached_tf_component(FieldNormReader::id_to_fieldnorm(id as u8), average);
            if !norm.is_finite() || norm < 0.0 || query_norm <= 0.0 {
                return None;
            }
            ratio = ratio.max((f64::from(norm) / f64::from(query_norm)).next_up());
        }
        Some(Bm25BlockMax::Corrected { average, ratio })
    }

    /// A serializer maximum M bounds x/(x+S) by m=nextUp(M)(1+u). With R>=S/Q,
    /// x/(x+Q) <= 1/(1+(1-m)/(mR)); dividing by 1-u covers denominator rounding.
    pub(crate) fn pruning_block_score(&self, mode: Bm25BlockMax, id: u8, tf: u32) -> Option<Score> {
        if tf == 0 || tf >= 255 {
            return None;
        }
        match mode {
            Bm25BlockMax::Exact => Some(self.score(id, tf)),
            Bm25BlockMax::Corrected { average, ratio } => {
                let norm = cached_tf_component(FieldNormReader::id_to_fieldnorm(id), average);
                let maximum = tf_factor(tf, norm);
                let u = 1.0 / 16_777_216.0;
                let m = (f64::from(maximum.next_up()) * (1.0 + u)).next_up();
                if m >= 1.0 {
                    return Some(self.weight);
                }
                let numerator = (1.0 - m).next_down().max(0.0);
                let denominator = (m * ratio).next_up();
                let t = (numerator / denominator).next_down().max(0.0);
                let g = (1.0 / (1.0 + t).next_down()).next_up();
                let upper = (g / (1.0 - u)).next_up().min(1.0) as Score;
                Some(self.weight * upper)
            }
        }
    }

    /// Compute the maximum possible BM25 score given this weight.
    pub fn max_score(&self) -> Score {
        self.score(255u8, 2_013_265_944)
    }

    #[inline]
    pub(crate) fn tf_factor(&self, fieldnorm_id: u8, term_freq: u32) -> Score {
        let norm = self.cache[fieldnorm_id as usize];
        tf_factor(term_freq, norm)
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

    use super::idf;
    use crate::{assert_nearly_equals, Score};

    #[test]
    fn corrected_block_bounds_cover_serializer_argmax_grid() {
        for segment_average in [1e-28, 0.01, 1.0, 10.0, 100.0, 1_000_000.0] {
            let serializer = super::Bm25Weight::new_without_explain(1.0, segment_average);
            let mut docs = (0..=255u8)
                .map(|id| (id, [1, 2, 3, 17, 127, 254][usize::from(id) % 6]))
                .collect::<Vec<_>>();
            let &(id, tf) = docs
                .iter()
                .max_by(|&&(left, ltf), &&(right, rtf)| {
                    serializer
                        .tf_factor(left, ltf)
                        .partial_cmp(&serializer.tf_factor(right, rtf))
                        .unwrap()
                })
                .unwrap();
            let maximum = serializer.tf_factor(id, tf);
            for norm in 0..=255u8 {
                for frequency in [255, 65535, 1 << 24, (1 << 24) + 2, u32::MAX] {
                    if serializer.tf_factor(norm, frequency) <= maximum {
                        docs.push((norm, frequency));
                    }
                }
            }
            for query_average in [
                segment_average.next_down(),
                segment_average.next_up(),
                1e-28,
                0.01,
                1.0,
                10.0,
                100.0,
                1_000_000.0,
            ] {
                for boost in [0.0, 0.3, 100.0] {
                    let query =
                        super::Bm25Weight::new_without_explain(1.3, query_average).boost_by(boost);
                    let mode = query.prepare_block_max(segment_average).unwrap();
                    let upper = query.pruning_block_score(mode, id, tf).unwrap();
                    for &(norm, freq) in &docs {
                        assert!(
                            query.score(norm, freq) <= upper,
                            "segment={segment_average} query={query_average} boost={boost} \
                             norm={norm} freq={freq}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn corrected_block_bounds_cover_subnormal_ratios_and_reject_quantized_pairs() {
        let query = super::Bm25Weight::new_without_explain(1.0, 1e-28);
        let mode = query.prepare_block_max(1e-29).unwrap();
        let serializer = super::Bm25Weight::new_without_explain(1.0, 1e-29);
        assert!(serializer.tf_factor(255, 1).is_subnormal());
        assert!(query.pruning_block_score(mode, 255, 1).unwrap() >= query.score(255, 1));
        for tf in [0, 255, u32::MAX] {
            assert!(query.pruning_block_score(mode, 255, tf).is_none());
        }
        for average in [0.0, -1.0, f32::INFINITY, f32::NAN, f32::MIN_POSITIVE] {
            assert!(query.prepare_block_max(average).is_none());
        }
        assert!(query.boost_by(-1.0).prepare_block_max(1e-28).is_none());
    }

    #[test]
    fn pruning_bound_covers_finite_nonnegative_scores() {
        for average in [0.01, 1.0, 100.0, 1_000_000.0] {
            for boost in [0.0, 0.3, 1.0, 100.0] {
                let weight = super::Bm25Weight::new_without_explain(1.3, average).boost_by(boost);
                let upper = weight.pruning_score_upper_bound().unwrap();
                for norm in 0..=255 {
                    for frequency in [1, 2, 254, 255, 65535, u32::MAX] {
                        let score = weight.score(norm, frequency);
                        assert!(score.is_finite() && score <= upper);
                    }
                }
            }
        }
        for boost in [-1.0, f32::INFINITY, f32::NAN] {
            assert!(super::Bm25Weight::new_without_explain(1.0, 100.0)
                .boost_by(boost)
                .pruning_score_upper_bound()
                .is_none());
        }
        assert!(super::Bm25Weight::new_without_explain(1.0, 0.0)
            .pruning_score_upper_bound()
            .is_none());
    }

    #[test]
    fn test_idf() {
        let score: Score = 2.0;
        assert_nearly_equals!(idf(1, 2), score.ln());
    }
}
