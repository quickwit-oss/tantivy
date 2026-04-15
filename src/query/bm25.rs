use std::sync::Arc;

use crate::fieldnorm::FieldNormReader;
use crate::index::Bm25Params;
use crate::query::Explanation;
use crate::schema::Field;
use crate::{Score, Searcher, Term};

/// Provides the corpus-level statistics needed by BM25 scoring.
///
/// The standard implementation is [`Searcher`], but you can implement this
/// trait on your own type to supply custom statistics (e.g. cluster-wide
/// counts in a distributed setting).
pub trait Bm25StatisticsProvider {
    /// Returns the total number of tokens indexed for `field` across all
    /// segments.
    fn total_num_tokens(&self, field: Field) -> crate::Result<u64>;

    /// Returns the total number of documents in the index.
    fn total_num_docs(&self) -> crate::Result<u64>;

    /// Returns the number of documents containing `term`.
    fn doc_freq(&self, term: &Term) -> crate::Result<u64>;

    /// Returns the BM25 parameters (`k1`, `b`) for `field`.
    ///
    /// Defaults to [`Bm25Params::DEFAULT`] (`k1 = 1.2`, `b = 0.75`).
    fn bm25_params(&self, _field: Field) -> Bm25Params {
        Bm25Params::default()
    }
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

    fn bm25_params(&self, field: Field) -> Bm25Params {
        self.schema()
            .get_field_entry(field)
            .field_type()
            .bm25_params()
            .unwrap_or_default()
    }
}

pub(crate) fn idf(doc_freq: u64, doc_count: u64) -> Score {
    assert!(doc_count >= doc_freq, "{doc_count} >= {doc_freq}");
    let x = ((doc_count - doc_freq) as Score + 0.5) / (doc_freq as Score + 0.5);
    (1.0 + x).ln()
}

fn cached_tf_component(fieldnorm: u32, average_fieldnorm: Score, k1: Score, b: Score) -> Score {
    k1 * (1.0 - b + b * fieldnorm as Score / average_fieldnorm)
}

fn compute_tf_cache(average_fieldnorm: Score, k1: Score, b: Score) -> Arc<[Score; 256]> {
    let mut cache: [Score; 256] = [0.0; 256];
    for (fieldnorm_id, cache_mut) in cache.iter_mut().enumerate() {
        let fieldnorm = FieldNormReader::id_to_fieldnorm(fieldnorm_id as u8);
        *cache_mut = cached_tf_component(fieldnorm, average_fieldnorm, k1, b);
    }
    Arc::new(cache)
}

#[derive(Clone)]
pub struct Bm25Weight {
    idf_explain: Option<Explanation>,
    weight: Score,
    cache: Arc<[Score; 256]>,
    average_fieldnorm: Score,
    params: Bm25Params,
}

impl Bm25Weight {
    pub fn boost_by(&self, boost: Score) -> Bm25Weight {
        if boost == 1.0f32 {
            return self.clone();
        }
        Bm25Weight {
            idf_explain: self.idf_explain.clone(),
            weight: self.weight * boost,
            cache: self.cache.clone(),
            average_fieldnorm: self.average_fieldnorm,
            params: self.params,
        }
    }

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
        let params = statistics.bm25_params(field);

        if terms.len() == 1 {
            let term_doc_freq = statistics.doc_freq(&terms[0])?;
            Ok(Bm25Weight::for_one_term(
                term_doc_freq,
                total_num_docs,
                average_fieldnorm,
                params,
            ))
        } else {
            let mut idf_sum: Score = 0.0;
            for term in terms {
                let term_doc_freq = statistics.doc_freq(term)?;
                idf_sum += idf(term_doc_freq, total_num_docs);
            }
            let idf_explain = Explanation::new("idf", idf_sum);
            Ok(Bm25Weight::new(idf_explain, average_fieldnorm, params))
        }
    }

    pub fn for_one_term(
        term_doc_freq: u64,
        total_num_docs: u64,
        avg_fieldnorm: Score,
        params: Bm25Params,
    ) -> Bm25Weight {
        let idf = idf(term_doc_freq, total_num_docs);
        let mut idf_explain =
            Explanation::new("idf, computed as log(1 + (N - n + 0.5) / (n + 0.5))", idf);
        idf_explain.add_const(
            "n, number of docs containing this term",
            term_doc_freq as Score,
        );
        idf_explain.add_const("N, total number of docs", total_num_docs as Score);
        Bm25Weight::new(idf_explain, avg_fieldnorm, params)
    }

    pub fn for_one_term_without_explain(
        term_doc_freq: u64,
        total_num_docs: u64,
        avg_fieldnorm: Score,
        params: Bm25Params,
    ) -> Bm25Weight {
        let idf = idf(term_doc_freq, total_num_docs);
        Bm25Weight::new_without_explain(idf, avg_fieldnorm, params)
    }

    pub(crate) fn new(
        idf_explain: Explanation,
        average_fieldnorm: Score,
        params: Bm25Params,
    ) -> Bm25Weight {
        let weight = idf_explain.value() * (1.0 + params.k1());
        Bm25Weight {
            idf_explain: Some(idf_explain),
            weight,
            cache: compute_tf_cache(average_fieldnorm, params.k1(), params.b()),
            average_fieldnorm,
            params,
        }
    }

    pub(crate) fn new_without_explain(
        idf: f32,
        average_fieldnorm: Score,
        params: Bm25Params,
    ) -> Bm25Weight {
        let weight = idf * (1.0 + params.k1());
        Bm25Weight {
            idf_explain: None,
            weight,
            cache: compute_tf_cache(average_fieldnorm, params.k1(), params.b()),
            average_fieldnorm,
            params,
        }
    }

    #[inline]
    pub fn score(&self, fieldnorm_id: u8, term_freq: u32) -> Score {
        self.weight * self.tf_factor(fieldnorm_id, term_freq)
    }

    pub fn max_score(&self) -> Score {
        self.score(255u8, 2_013_265_944)
    }

    #[inline]
    pub(crate) fn tf_factor(&self, fieldnorm_id: u8, term_freq: u32) -> Score {
        let term_freq = term_freq as Score;
        let norm = self.cache[fieldnorm_id as usize];
        term_freq / (term_freq + norm)
    }

    pub fn explain(&self, fieldnorm_id: u8, term_freq: u32) -> Explanation {
        let score = self.score(fieldnorm_id, term_freq);

        let norm = self.cache[fieldnorm_id as usize];
        let term_freq = term_freq as Score;
        let right_factor = term_freq / (term_freq + norm);

        let mut tf_explanation = Explanation::new(
            "freq / (freq + k1 * (1 - b + b * dl / avgdl))",
            right_factor,
        );

        tf_explanation.add_const("freq, occurrences of term within document", term_freq);
        tf_explanation.add_const("k1, term saturation parameter", self.params.k1());
        tf_explanation.add_const("b, length normalization parameter", self.params.b());
        tf_explanation.add_const(
            "dl, length of field",
            FieldNormReader::id_to_fieldnorm(fieldnorm_id) as Score,
        );
        tf_explanation.add_const("avgdl, average length of field", self.average_fieldnorm);

        let mut explanation = Explanation::new("TermQuery, product of...", score);
        explanation.add_detail(Explanation::new("(K1+1)", self.params.k1() + 1.0));
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
    fn test_idf() {
        let score: Score = 2.0;
        assert_nearly_equals!(idf(1, 2), score.ln());
    }

    #[test]
    fn test_custom_bm25_params_produce_different_scores() {
        use super::Bm25Weight;
        use crate::index::Bm25Params;

        let default_params = Bm25Params::default();
        let custom_params = Bm25Params::new(2.0, 0.3);

        let w_default = Bm25Weight::for_one_term(10, 100, 50.0, default_params);
        let w_custom = Bm25Weight::for_one_term(10, 100, 50.0, custom_params);

        let fieldnorm_id = 10u8;
        let term_freq = 5u32;

        let score_default = w_default.score(fieldnorm_id, term_freq);
        let score_custom = w_custom.score(fieldnorm_id, term_freq);

        assert!(
            (score_default - score_custom).abs() > 1e-6,
            "Custom k1/b should produce different scores: default={score_default}, \
             custom={score_custom}"
        );
    }

    #[test]
    #[should_panic(expected = "k1 must be non-negative")]
    fn test_bm25_params_rejects_negative_k1() {
        use crate::index::Bm25Params;
        Bm25Params::new(-1.0, 0.75);
    }

    #[test]
    #[should_panic(expected = "b must be in [0, 1]")]
    fn test_bm25_params_rejects_b_out_of_range() {
        use crate::index::Bm25Params;
        Bm25Params::new(1.2, 1.5);
    }
}
