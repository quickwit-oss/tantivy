use query::Weight;
use query::Scorer;
use core::SegmentReader;
use super::PhraseScorer;
use query::MultiTermWeight;
use Result;

pub struct PhraseWeight {
    all_term_weight: MultiTermWeight,
}

impl From<MultiTermWeight> for PhraseWeight {
    fn from(all_term_weight: MultiTermWeight) -> PhraseWeight {
        PhraseWeight {
            all_term_weight: all_term_weight
        }
    }
}

impl Weight for PhraseWeight {
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        let all_term_scorer = try!(self.all_term_weight.specialized_scorer(reader));
        let positions_offsets: Vec<u32> = (0u32..all_term_scorer.num_subscorers() as u32).collect(); 
        Ok(box PhraseScorer {
            all_term_scorer:  all_term_scorer,
            positions_offsets: positions_offsets
        })
    }
}
