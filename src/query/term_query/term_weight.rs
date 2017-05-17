use Term;
use query::Weight;
use core::SegmentReader;
use query::Scorer;
use postings::SegmentPostingsOption;
use postings::SegmentPostings;
use super::term_scorer::TermScorer;
use Result;

pub struct TermWeight {
    pub num_docs: u32,
    pub doc_freq: u32,
    pub term: Term,
    pub segment_postings_options: SegmentPostingsOption,
}


impl Weight for TermWeight {
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        let specialized_scorer = try!(self.specialized_scorer(reader));
        Ok(box specialized_scorer)
    }
}

impl TermWeight {
    fn idf(&self) -> f32 {
        1.0 + (self.num_docs as f32 / (self.doc_freq as f32 + 1.0)).ln()
    }

    pub fn specialized_scorer<'a>(&'a self,
                                  reader: &'a SegmentReader)
                                  -> Result<TermScorer<SegmentPostings<'a>>> {
        let field = self.term.field();
        let fieldnorm_reader_opt = reader.get_fieldnorms_reader(field);
        Ok(reader
               .read_postings(&self.term, self.segment_postings_options)
               .map(|segment_postings| {
                        TermScorer {
                            idf: self.idf(),
                            fieldnorm_reader_opt: fieldnorm_reader_opt,
                            postings: segment_postings,
                        }
                    })
               .unwrap_or(TermScorer {
                              idf: 1f32,
                              fieldnorm_reader_opt: None,
                              postings: SegmentPostings::empty(),
                          }))
    }
}
