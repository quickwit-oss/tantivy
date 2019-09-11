use docset::DocSet;
use query::Scorer;
use DocId;
use Score;
use SkipResult;


/// Creates a `DocSet` that iterate through the intersection of two `DocSet`s.
pub struct IntersectionTwoTerms<TDocSet> {
    left: TDocSet,
    right: TDocSet
}

impl<TDocSet: DocSet> IntersectionTwoTerms<TDocSet> {
    pub fn new(left: TDocSet, right: TDocSet) -> IntersectionTwoTerms<TDocSet> {
        IntersectionTwoTerms {
            left,
            right
        }
    }
}

impl<TDocSet: DocSet> DocSet for IntersectionTwoTerms<TDocSet> {

    fn advance(&mut self) -> bool {
        let (left, right) = (&mut self.left, &mut self.right);
        if !left.advance() {
            return false;
        }
        let mut candidate = left.doc();
        loop {
            match right.skip_next(candidate) {
                SkipResult::Reached => {
                    return true;
                }
                SkipResult::End => {
                    return false;
                }
                SkipResult::OverStep => {
                    candidate = right.doc();
                }
            }
            match left.skip_next(candidate) {
                SkipResult::Reached => {
                    return true;
                }
                SkipResult::End => {
                    return false;
                }
                SkipResult::OverStep => {
                    candidate = left.doc();
                }
            }
        }
    }

    fn doc(&self) -> DocId {
        self.left.doc()
    }

    fn size_hint(&self) -> u32 {
        self.left.size_hint().min(self.right.size_hint())
    }
}

impl<TScorer: Scorer> Scorer for IntersectionTwoTerms<TScorer> {
    fn score(&mut self) -> Score {
        self.left.score() + self.right.score()
    }
}
