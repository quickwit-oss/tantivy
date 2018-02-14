use query::Scorer;
use postings::SkipResult;
use DocSet;
use Score;
use DocId;

#[derive(Clone, Copy)]
enum State {
    ExcludeOne(DocId),
    Finished
}

pub struct ExcludeScorer<TDocSet, TDocSetExclude> {
    underlying_docset: TDocSet,
    excluding_docset: TDocSetExclude,
    excluding_state: State,
}


impl<TDocSet, TDocSetExclude> ExcludeScorer<TDocSet, TDocSetExclude>
    where TDocSetExclude: DocSet {

    pub fn new(underlying_docset: TDocSet, mut excluding_docset: TDocSetExclude) -> ExcludeScorer<TDocSet, TDocSetExclude> {
        let state =
            if excluding_docset.advance() {
                State::ExcludeOne(excluding_docset.doc())
            } else {
                State::Finished
            };
        ExcludeScorer {
            underlying_docset,
            excluding_docset,
            excluding_state: state,
        }
    }
}

impl<TDocSet, TDocSetExclude> ExcludeScorer<TDocSet, TDocSetExclude>
    where TDocSet: DocSet, TDocSetExclude: DocSet {

    /// Returns true iff the doc is not removed.
    ///
    /// The method has to be called with non strictly
    /// increasing `doc`.
    fn accept(&mut self) -> bool {
        let doc = self.underlying_docset.doc();
        match self.excluding_state {
            State::ExcludeOne(excluded_doc) => {
                if doc == excluded_doc {
                    return false;
                }
                match self.excluding_docset.skip_next(doc) {
                    SkipResult::OverStep => {
                        self.excluding_state = State::ExcludeOne(self.excluding_docset.doc());
                        true
                    }
                    SkipResult::End => {
                        self.excluding_state = State::Finished;
                        true
                    }
                    SkipResult::Reached => {
                        false
                    }
                }
            }
            State::Finished => {
                true
            }
        }
    }
}

impl<TDocSet, TDocSetExclude> DocSet for ExcludeScorer<TDocSet, TDocSetExclude>
    where TDocSet: DocSet, TDocSetExclude: DocSet {

    fn advance(&mut self) -> bool {
        while self.underlying_docset.advance() {
            if self.accept() {
                return true;
            }
        }
        false
    }

    fn skip_next(&mut self, target: DocId) -> SkipResult {
        let underlying_skip_result = self.underlying_docset.skip_next(target);
        if underlying_skip_result == SkipResult::End {
            return SkipResult::End;
        }
        if self.accept() {
            underlying_skip_result
        } else if self.advance() {
            SkipResult::OverStep
        } else {
            SkipResult::End
        }

    }

    fn doc(&self) -> DocId {
        self.underlying_docset.doc()
    }

    /// `.size_hint()` directly returns the size
    /// of the underlying docset without taking in account
    /// the fact that docs might be deleted.
    fn size_hint(&self) -> u32 {
        self.underlying_docset.size_hint()
    }
}


impl<TDocSet, TDocSetExclude> Scorer for ExcludeScorer<TDocSet, TDocSetExclude>
    where TDocSet: Scorer, TDocSetExclude: Scorer {
    fn score(&self) -> Score {
        self.underlying_docset.score()
    }
}