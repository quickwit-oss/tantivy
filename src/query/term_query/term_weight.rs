use Term;
use query::Weight;
use core::SegmentReader;
use query::Scorer;
use docset::DocSet;
use postings::SegmentPostings;
use schema::IndexRecordOption;
use super::term_scorer::TermScorer;
use fastfield::DeleteBitSet;
use postings::NoDelete;
use Result;
use fieldnorm::FieldNormReader;
use std::f32;

pub struct TermWeight {
    term: Term,
    index_record_option: IndexRecordOption,
    weight: f32,
    cache: [f32; 256],
}

impl Weight for TermWeight {

    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>> {
        let field = self.term.field();
        let inverted_index = reader.inverted_index(field);
        let fieldnorm_reader_opt = reader.get_fieldnorms_reader(field);
        let scorer: Box<Scorer>;
        if reader.has_deletes() {
            let postings_opt: Option<SegmentPostings<DeleteBitSet>> =
                inverted_index.read_postings(&self.term, self.index_record_option);
            scorer =
                if let Some(segment_postings) = postings_opt {
                    box TermScorer {
                        fieldnorm_reader_opt,
                        postings: segment_postings,
                        weight: self.weight,
                        cache: self.cache
                    }
                } else {
                    box TermScorer {
                        fieldnorm_reader_opt: None,
                        postings: SegmentPostings::<NoDelete>::empty(),
                        weight: self.weight,
                        cache: self.cache
                    }
                };
        } else {
            let postings_opt: Option<SegmentPostings<NoDelete>> =
                inverted_index.read_postings_no_deletes(&self.term, self.index_record_option);
            scorer =
                if let Some(segment_postings) = postings_opt {
                    box TermScorer {
                        fieldnorm_reader_opt,
                        postings: segment_postings,
                        weight: self.weight,
                        cache: self.cache
                    }
                } else {
                    box TermScorer {
                        fieldnorm_reader_opt: None,
                        postings: SegmentPostings::<NoDelete>::empty(),
                        weight: self.weight,
                        cache: self.cache
                    }
                };
        }
        Ok(scorer)
    }

    fn count(&self, reader: &SegmentReader) -> Result<u32> {
        if reader.num_deleted_docs() == 0 {
            let field = self.term.field();
            Ok(reader
                .inverted_index(field)
                .get_term_info(&self.term)
                .map(|term_info| term_info.doc_freq)
                .unwrap_or(0))
        } else {
            Ok(self.scorer(reader)?.count())
        }
    }
}

const K1: f32 = 1.2;
const B: f32 = 0.75;

fn cached_tf_component(fieldnorm: u32, average_fieldnorm: f32) -> f32 {
    K1 * (1f32 - B + B * fieldnorm as f32 / average_fieldnorm)
}

fn compute_tf_cache(average_fieldnorm: f32) -> [f32; 256] {
    let mut cache = [0f32; 256];
    for fieldnorm_id in 0..256 {
        let fieldnorm = FieldNormReader::id_to_fieldnorm(fieldnorm_id as u8);
        cache[fieldnorm_id] = cached_tf_component(fieldnorm, average_fieldnorm);
    }
    cache
}


fn idf(doc_freq: u64, doc_count: u64) -> f32 {
    let x = ((doc_count - doc_freq) as f32 + 0.5) / (doc_freq as f32 + 0.5);
    (1f32 + x).ln()
}


impl TermWeight {

    pub fn new(doc_freq: u64,
               doc_count: u64,
               average_fieldnorm: f32,
               term: Term,
               index_record_option: IndexRecordOption) -> TermWeight {
        let idf = idf(doc_freq, doc_count);
        TermWeight {
            term,
            index_record_option,
            weight: idf * (1f32 + K1),
            cache: compute_tf_cache(average_fieldnorm),
        }
    }
}

#[cfg(test)]
mod tests {

    use tests::assert_nearly_equals;
    use super::idf;

    #[test]
    fn test_idf() {
        assert_nearly_equals(idf(1, 2),  0.6931472);
    }

}
