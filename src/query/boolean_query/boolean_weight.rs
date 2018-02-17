use query::Weight;
use core::SegmentReader;
use postings::{Intersection, Union};
use std::collections::HashMap;
use query::EmptyScorer;
use query::Scorer;
use query::Exclude;
use query::Occur;
use query::RequiredOptionalScorer;
use query::score_combiner::{SumWithCoordsCombiner, DoNothingCombiner, ScoreCombiner};
use Result;

fn scorer_union<'a, TScoreCombiner>(docsets: Vec<Box<Scorer + 'a>>) -> Box<Scorer + 'a>
    where TScoreCombiner: ScoreCombiner + 'static
{
    assert!(!docsets.is_empty());
    if docsets.len() == 1 {
        docsets.into_iter().next().unwrap() //< we checked the size beforehands
    } else {
        // TODO have a UnionScorer instead.
        box Union::<_, TScoreCombiner>::from(docsets)
    }
}

pub struct BooleanWeight {
    weights: Vec<(Occur, Box<Weight>)>,
    scoring_enabled: bool,
}

impl BooleanWeight {
    pub fn new(weights: Vec<(Occur, Box<Weight>)>, scoring_enabled: bool) -> BooleanWeight {
        BooleanWeight {
            weights,
            scoring_enabled,
        }
    }

    fn complex_scorer<'a, TScoreCombiner: ScoreCombiner + 'static>(
        &'a self,
        reader: &'a SegmentReader,
    ) -> Result<Box<Scorer + 'a>> {
        let mut per_occur_scorers: HashMap<Occur, Vec<Box<Scorer + 'a>>> = HashMap::new();
        for &(ref occur, ref subweight) in self.weights.iter() {
            let sub_scorer: Box<Scorer + 'a> = subweight.scorer(reader)?;
            per_occur_scorers
                .entry(*occur)
                .or_insert_with(Vec::new)
                .push(sub_scorer);
        }

        let should_scorer_opt: Option<Box<Scorer + 'a>> =
            per_occur_scorers.remove(&Occur::Should).map(scorer_union::<TScoreCombiner>);

        let exclude_scorer_opt: Option<Box<Scorer + 'a>> =
            per_occur_scorers.remove(&Occur::MustNot).map(scorer_union::<TScoreCombiner>);

        let must_scorer_opt: Option<Box<Scorer + 'a>> =
            per_occur_scorers.remove(&Occur::Must).map(|scorers| {
                if scorers.len() == 1 {
                    scorers.into_iter().next().unwrap()
                } else {
                    let scorer: Box<Scorer> = box Intersection::from(scorers);
                    scorer
                }
            });

        let positive_scorer: Box<Scorer> = match (should_scorer_opt, must_scorer_opt) {
            (Some(should_scorer), Some(must_scorer)) => {
                if self.scoring_enabled {
                    box RequiredOptionalScorer::<_,_,TScoreCombiner>::new(must_scorer, should_scorer)
                } else {
                    must_scorer
                }
            }
            (None, Some(must_scorer)) => must_scorer,
            (Some(should_scorer), None) => should_scorer,
            (None, None) => {
                return Ok(box EmptyScorer);
            }
        };

        if let Some(exclude_scorer) = exclude_scorer_opt {
            Ok(box Exclude::new(positive_scorer, exclude_scorer))
        } else {
            Ok(positive_scorer)
        }
    }
}

impl Weight for BooleanWeight {
    fn scorer<'a>(&'a self, reader: &'a SegmentReader) -> Result<Box<Scorer + 'a>> {
        if self.weights.is_empty() {
            Ok(box EmptyScorer)
        } else if self.weights.len() == 1 {
            let &(occur, ref weight) = &self.weights[0];
            if occur == Occur::MustNot {
                Ok(box EmptyScorer)
            } else {
                weight.scorer(reader)
            }
        } else if self.scoring_enabled {
            self.complex_scorer::<SumWithCoordsCombiner>(reader)
        } else {
            self.complex_scorer::<DoNothingCombiner>(reader)
        }
    }
}
