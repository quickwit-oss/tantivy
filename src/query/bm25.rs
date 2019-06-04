use fieldnorm::FieldNormReader;
use query::Explanation;
use Score;
use Searcher;
use Term;

const K1: f32 = 1.2;
const B: f32 = 0.75;

fn idf(doc_freq: u64, doc_count: u64) -> f32 {
    let x = ((doc_count - doc_freq) as f32 + 0.5) / (doc_freq as f32 + 0.5);
    (1f32 + x).ln()
}

fn cached_tf_component(fieldnorm: u32, average_fieldnorm: f32) -> f32 {
    K1 * (1f32 - B + B * fieldnorm as f32 / average_fieldnorm)
}

fn compute_tf_cache(average_fieldnorm: f32) -> [f32; 256] {
    let mut cache = [0f32; 256];
    for (fieldnorm_id, cache_mut) in cache.iter_mut().enumerate() {
        let fieldnorm = FieldNormReader::id_to_fieldnorm(fieldnorm_id as u8);
        *cache_mut = cached_tf_component(fieldnorm, average_fieldnorm);
    }
    cache
}

#[derive(Clone)]
pub struct BM25Weight {
    idf_explain: Explanation,
    weight: f32,
    cache: [f32; 256],
    average_fieldnorm: f32,
}

impl BM25Weight {
    pub fn null() -> BM25Weight {
        BM25Weight {
            idf_explain: Explanation::const_value(0f32),
            weight: 0f32,
            cache: [1f32; 256],
            average_fieldnorm: 1f32,
        }
    }
    pub fn for_terms(searcher: &Searcher, terms: &[Term]) -> BM25Weight {
        assert!(!terms.is_empty(), "BM25 requires at least one term");
        let field = terms[0].field();
        for term in &terms[1..] {
            assert_eq!(
                term.field(),
                field,
                "All terms must belong to the same field."
            );
        }

        let mut total_num_tokens = 0u64;
        let mut total_num_docs = 0u64;
        for segment_reader in searcher.segment_readers() {
            let inverted_index = segment_reader.inverted_index(field);
            total_num_tokens += inverted_index.total_num_tokens();
            total_num_docs += u64::from(segment_reader.max_doc());
        }
        let average_fieldnorm = total_num_tokens as f32 / total_num_docs as f32;

        let mut idf_explain: Explanation;
        if terms.len() == 1 {
            let term_doc_freq = searcher.doc_freq(&terms[0]);
            let idf = idf(term_doc_freq, total_num_docs);
            idf_explain =
                Explanation::new("idf, computed as log(1 + (N - n + 0.5) / (n + 0.5))", idf);
            idf_explain.add_const(
                "n, number of docs containing this term",
                term_doc_freq as f32,
            );
            idf_explain.add_const("N, total number of docs", total_num_docs as f32);
        } else {
            let idf = terms
                .iter()
                .map(|term| {
                    let term_doc_freq = searcher.doc_freq(term);
                    idf(term_doc_freq, total_num_docs)
                })
                .sum::<f32>();
            idf_explain = Explanation::new("idf", idf);
        }
        BM25Weight::new(idf_explain, average_fieldnorm)
    }

    fn new(idf_explain: Explanation, average_fieldnorm: f32) -> BM25Weight {
        let weight = idf_explain.val() * (1f32 + K1);
        BM25Weight {
            idf_explain,
            weight,
            cache: compute_tf_cache(average_fieldnorm),
            average_fieldnorm,
        }
    }

    #[inline(always)]
    pub fn score(&self, fieldnorm_id: u8, term_freq: u32) -> Score {
        let norm = self.cache[fieldnorm_id as usize];
        let term_freq = term_freq as f32;
        self.weight * term_freq / (term_freq + norm)
    }

    pub fn explain(&self, fieldnorm_id: u8, term_freq: u32) -> Explanation {
        let score = self.score(fieldnorm_id, term_freq);

        let norm = self.cache[fieldnorm_id as usize];
        let term_freq = term_freq as f32;
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
            FieldNormReader::id_to_fieldnorm(fieldnorm_id) as f32,
        );
        tf_explanation.add_const("avgdl, average length of field", self.average_fieldnorm);

        let mut explanation = Explanation::new("TermQuery, product of...", score);
        explanation.add_detail(Explanation::new("(K1+1)", K1 + 1f32));
        explanation.add_detail(self.idf_explain.clone());
        explanation.add_detail(tf_explanation);
        explanation
    }
}

#[cfg(test)]
mod tests {

    use super::idf;
    use tests::assert_nearly_equals;

    #[test]
    fn test_idf() {
        assert_nearly_equals(idf(1, 2), 0.6931472);
    }

}
