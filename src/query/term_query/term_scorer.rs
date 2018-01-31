use Score;
use DocId;
use fastfield::U64FastFieldReader;
use postings::DocSet;
use query::Scorer;
use postings::Postings;
use fastfield::FastFieldReader;

pub struct TermScorer<TPostings>
where
    TPostings: Postings,
{
    pub idf: Score,
    pub fieldnorm_reader_opt: Option<U64FastFieldReader>,
    pub postings: TPostings,
}

impl<TPostings> TermScorer<TPostings>
where
    TPostings: Postings,
{
    pub fn postings(&self) -> &TPostings {
        &self.postings
    }
}

impl<TPostings> DocSet for TermScorer<TPostings>
where
    TPostings: Postings,
{
    fn advance(&mut self) -> bool {
        self.postings.advance()
    }

    fn doc(&self) -> DocId {
        self.postings.doc()
    }

    fn size_hint(&self) -> u32 {
        self.postings.size_hint()
    }
}

impl<TPostings> Scorer for TermScorer<TPostings>
where
    TPostings: Postings,
{
    fn score(&self) -> Score {
        let doc = self.postings.doc();
        let tf = match self.fieldnorm_reader_opt {
            Some(ref fieldnorm_reader) => {
                let field_norm = fieldnorm_reader.get(doc);
                (self.postings.term_freq() as f32 / field_norm as f32)
            }
            None => self.postings.term_freq() as f32,
        };
        self.idf * tf.sqrt()
    }
}
