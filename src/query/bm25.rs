use fieldnorm::FieldNormReader;
use Score;
use Searcher;
use Term;

pub const DEFAULT_K1: f32 = 1.2;
pub const DEFAULT_B: f32 = 0.75;

fn idf(doc_freq: u64, doc_count: u64) -> f32 {
    let x = ((doc_count - doc_freq) as f32 + 0.5) / (doc_freq as f32 + 0.5);
    (1f32 + x).ln()
}

fn cached_tf_component(fieldnorm: u32, average_fieldnorm: f32, k1: f32, b: f32)
    -> f32
{
    k1 * (1f32 - b + b * fieldnorm as f32 / average_fieldnorm)
}

fn compute_tf_cache(average_fieldnorm: f32, k1: f32, b: f32) -> [f32; 256] {
    let mut cache = [0f32; 256];
    for fieldnorm_id in 0..256 {
        let fieldnorm = FieldNormReader::id_to_fieldnorm(fieldnorm_id as u8);
        cache[fieldnorm_id] = cached_tf_component(
            fieldnorm, average_fieldnorm, k1, b);
    }
    cache
}

#[derive(Clone)]
pub struct BM25Weight {
    weight: f32,
    cache: [f32; 256],
}

impl BM25Weight {
    pub fn null() -> BM25Weight {
        BM25Weight {
            weight: 0f32,
            cache: [1f32; 256],
        }
    }

    pub fn for_terms(searcher: &Searcher, terms: &[Term],
        k1: f32, b: f32)
        -> BM25Weight
    {
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
            total_num_docs += segment_reader.max_doc() as u64;
        }
        let average_fieldnorm = total_num_tokens as f32 / total_num_docs as f32;

        let idf = terms
            .iter()
            .map(|term| {
                let term_doc_freq = searcher.doc_freq(term);
                idf(term_doc_freq, total_num_docs)
            })
            .sum::<f32>();
        BM25Weight::new(idf, average_fieldnorm, k1, b)
    }

    fn new(idf: f32, average_fieldnorm: f32, k1: f32, b: f32) -> BM25Weight {
        BM25Weight {
            weight: idf * (1f32 + k1),
            cache: compute_tf_cache(average_fieldnorm, k1, b),
        }
    }

    #[inline(always)]
    pub fn score(&self, fieldnorm_id: u8, term_freq: u32) -> Score {
        let norm = self.cache[fieldnorm_id as usize];
        let term_freq = term_freq as f32;
        self.weight * term_freq / (term_freq + norm)
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
