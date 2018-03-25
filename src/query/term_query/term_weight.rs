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
use query::bm25::BM25Weight;

pub struct TermWeight {
    term: Term,
    index_record_option: IndexRecordOption,
    similarity_weight: BM25Weight,
}

impl Weight for TermWeight {

    fn scorer(&self, reader: &SegmentReader) -> Result<Box<Scorer>> {
        let field = self.term.field();
        let inverted_index = reader.inverted_index(field);
        let fieldnorm_reader = reader.get_fieldnorms_reader(field).expect("Failed to find fieldnorm reader for field.");
        let scorer: Box<Scorer>;
        if reader.has_deletes() {
            let postings_opt: Option<SegmentPostings<DeleteBitSet>> =
                inverted_index.read_postings(&self.term, self.index_record_option);
            scorer =
                if let Some(segment_postings) = postings_opt {
                    box TermScorer {
                        fieldnorm_reader,
                        postings: segment_postings,
                        similarity_weight: self.similarity_weight.clone()
                    }
                } else {
                    box TermScorer {
                        fieldnorm_reader,
                        postings: SegmentPostings::<NoDelete>::empty(),
                        similarity_weight: self.similarity_weight.clone()
                    }
                };
        } else {
            let postings_opt: Option<SegmentPostings<NoDelete>> =
                inverted_index.read_postings_no_deletes(&self.term, self.index_record_option);
            scorer =
                if let Some(segment_postings) = postings_opt {
                    box TermScorer {
                        fieldnorm_reader,
                        postings: segment_postings,
                        similarity_weight: self.similarity_weight.clone()
                    }
                } else {
                    box TermScorer {
                        fieldnorm_reader,
                        postings: SegmentPostings::<NoDelete>::empty(),
                        similarity_weight: self.similarity_weight.clone()
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


impl TermWeight {

    pub fn new(term: Term,
               index_record_option: IndexRecordOption,
               similarity_weight: BM25Weight) -> TermWeight {
        TermWeight {
            term,
            index_record_option,
            similarity_weight,
        }
    }
}

