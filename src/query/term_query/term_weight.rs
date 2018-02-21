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

pub struct TermWeight {
    pub(crate) num_docs: u32,
    pub(crate) doc_freq: u32,
    pub(crate) term: Term,
    pub(crate) index_record_option: IndexRecordOption,
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
                        idf: self.idf(),
                        fieldnorm_reader_opt,
                        postings: segment_postings,
                    }
                } else {
                    box TermScorer {
                        idf: 1f32,
                        fieldnorm_reader_opt: None,
                        postings: SegmentPostings::<NoDelete>::empty(),
                    }
                };
        } else {
            let postings_opt: Option<SegmentPostings<NoDelete>> =
                inverted_index.read_postings_no_deletes(&self.term, self.index_record_option);
            scorer =
                if let Some(segment_postings) = postings_opt {
                    box TermScorer {
                        idf: self.idf(),
                        fieldnorm_reader_opt,
                        postings: segment_postings,
                    }
                } else {
                    box TermScorer {
                        idf: 1f32,
                        fieldnorm_reader_opt: None,
                        postings: SegmentPostings::<NoDelete>::empty(),
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
    fn idf(&self) -> f32 {
        1.0 + (self.num_docs as f32 / (self.doc_freq as f32 + 1.0)).ln()
    }
}
