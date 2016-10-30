use Term;
use query::Weight;
use core::SegmentReader;
use query::Scorer;
use query::EmptyScorer;
use postings::SegmentPostingsOption;
use super::term_scorer::TermScorer;
use Result;

pub struct TermWeight {
    pub term: Term
}


impl Weight for TermWeight {
    
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        let field = self.term.field();
        let fieldnorm_reader = try!(reader.get_fieldnorms_reader(field));
        if let Some(segment_postings) = reader.read_postings(&self.term, SegmentPostingsOption::Freq) {
            let scorer: TermScorer = TermScorer {
                fieldnorm_reader: fieldnorm_reader,
                segment_postings: segment_postings,
            };
            Ok(box scorer)
        }
        else {
            Ok(box EmptyScorer)
        }
    }
    
}