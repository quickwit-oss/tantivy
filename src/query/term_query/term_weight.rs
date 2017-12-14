use Term;
use query::Weight;
use core::SegmentReader;
use query::Scorer;
use postings::SegmentPostings;
use schema::IndexRecordOption;
use super::term_scorer::TermScorer;
use Result;

pub struct TermWeight {
    pub(crate) num_docs: u32,
    pub(crate) doc_freq: u32,
    pub(crate) term: Term,
    pub(crate) index_record_option: IndexRecordOption,
}


impl Weight for TermWeight {
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        let specialized_scorer = self.specialized_scorer(reader)?;
        Ok(box specialized_scorer)
    }
}

impl TermWeight {
    fn idf(&self) -> f32 {
        1.0 + (self.num_docs as f32 / (self.doc_freq as f32 + 1.0)).ln()
    }

    /// If the field is not found, returns an empty `DocSet`.
    pub fn specialized_scorer(
        &self,
        reader: &SegmentReader,
    ) -> Result<TermScorer<SegmentPostings>> {
        let field = self.term.field();
        let inverted_index = reader.inverted_index(field);
        let fieldnorm_reader_opt = reader.get_fieldnorms_reader(field);
        let postings_opt: Option<SegmentPostings> =
            inverted_index.read_postings(&self.term, self.index_record_option);
        if let Some(segment_postings) = postings_opt {
            Ok(TermScorer {
                idf: self.idf(),
                fieldnorm_reader_opt,
                postings: segment_postings,
            })
        } else {
            Ok(TermScorer {
                idf: 1f32,
                fieldnorm_reader_opt: None,
                postings: SegmentPostings::empty(),
            })
        }
    }
}
