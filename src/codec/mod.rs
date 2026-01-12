/// Codec specific to postings data.
pub mod postings;

/// Standard tantivy codec. This is the codec you use by default.
pub mod standard;

use std::io;

pub use standard::StandardCodec;

use crate::codec::postings::PostingsCodec;
use crate::fieldnorm::FieldNormReader;
use crate::postings::{Postings, TermInfo};
use crate::query::score_combiner::DoNothingCombiner;
use crate::query::term_query::TermScorer;
use crate::query::{box_scorer, Bm25Weight, BufferedUnionScorer, Scorer, SumCombiner};
use crate::schema::IndexRecordOption;
use crate::{DocId, InvertedIndexReader, Score};

/// Codecs describes how data is layed out on disk.
///
/// For the moment, only postings codec can be custom.
pub trait Codec: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// The specific postings type used by this codec.
    type PostingsCodec: PostingsCodec;

    /// ID of the codec. It should be unique to your codec.
    /// Make it human-readable, descriptive, short and unique.
    const ID: &'static str;

    /// Load codec based on the codec configuration.
    fn from_json_props(json_value: &serde_json::Value) -> crate::Result<Self>;

    /// Get codec configuration.
    fn to_json_props(&self) -> serde_json::Value;

    /// Returns the postings codec.
    fn postings_codec(&self) -> &Self::PostingsCodec;
}

/// Object-safe codec is a Codec that can be used in a trait object.
///
/// The point of it is to offer a way to use a codec without a proliferation of generics.
pub trait ObjectSafeCodec: 'static + Send + Sync {
    /// Loads a type-erased Postings object for the given term.
    ///
    /// If the schema used to build the index did not provide enough
    /// information to match the requested `option`, a Postings is still
    /// returned in a best-effort manner.
    fn load_postings_type_erased(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        inverted_index_reader: &InvertedIndexReader,
    ) -> io::Result<Box<dyn Postings>>;

    /// Loads a type-erased TermScorer object for the given term.
    ///
    /// If the schema used to build the index did not provide enough
    /// information to match the requested `option`, a TermScorer is still
    /// returned in a best-effort manner.
    ///
    /// The point of this contraption is that the return TermScorer is backed,
    /// not by Box<dyn Postings> but by the codec's concrete Postings type.
    fn load_term_scorer_type_erased(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        inverted_index_reader: &InvertedIndexReader,
        fieldnorm_reader: FieldNormReader,
        similarity_weight: Bm25Weight,
    ) -> io::Result<Box<dyn Scorer>>;

    /// Loads a type-erased PhraseScorer object for the given term.
    ///
    /// If the schema used to build the index did not provide enough
    /// information to match the requested `option`, a TermScorer is still
    /// returned in a best-effort manner.
    ///
    /// The point of this contraption is that the return PhraseScorer is backed,
    /// not by Box<dyn Postings> but by the codec's concrete Postings type.
    fn new_phrase_scorer_type_erased(
        &self,
        term_infos: &[(usize, TermInfo)],
        similarity_weight: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
        inverted_index_reader: &InvertedIndexReader,
    ) -> io::Result<Box<dyn Scorer>>;

    /// Performs a for_each_pruning operation on the given scorer.
    ///
    /// The function will go through matching documents and call the callback
    /// function for all docs with a score exceeding the threshold.
    ///
    /// The function itself will return a larger threshold value,
    /// meant to update the threshold value.
    ///
    /// If the codec and the scorer allow it, this function can rely on
    /// optimizations like the block-max wand.
    fn for_each_pruning(
        &self,
        threshold: Score,
        scorer: Box<dyn Scorer>,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    );

    /// Builds a union scorer possibly specialized if
    /// all scorers are `Term<Self::Postings>`.
    fn build_union_scorer_with_sum_combiner(
        &self,
        scorers: Vec<Box<dyn Scorer>>,
        num_docs: DocId,
        score_combiner_type: SumOrDoNothingCombiner,
    ) -> Box<dyn Scorer>;
}

impl<TCodec: Codec> ObjectSafeCodec for TCodec {
    fn load_postings_type_erased(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        inverted_index_reader: &InvertedIndexReader,
    ) -> io::Result<Box<dyn Postings>> {
        let postings = inverted_index_reader
            .read_postings_from_terminfo_specialized(term_info, option, self)?;
        Ok(Box::new(postings))
    }

    fn load_term_scorer_type_erased(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        inverted_index_reader: &InvertedIndexReader,
        fieldnorm_reader: FieldNormReader,
        similarity_weight: Bm25Weight,
    ) -> io::Result<Box<dyn Scorer>> {
        let scorer = inverted_index_reader.new_term_scorer_specialized(
            term_info,
            option,
            fieldnorm_reader,
            similarity_weight,
            self,
        )?;
        Ok(box_scorer(scorer))
    }

    fn new_phrase_scorer_type_erased(
        &self,
        term_infos: &[(usize, TermInfo)],
        similarity_weight: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
        inverted_index_reader: &InvertedIndexReader,
    ) -> io::Result<Box<dyn Scorer>> {
        let scorer = inverted_index_reader.new_phrase_scorer_type_specialized(
            term_infos,
            similarity_weight,
            fieldnorm_reader,
            slop,
            self,
        )?;
        Ok(box_scorer(scorer))
    }

    fn build_union_scorer_with_sum_combiner(
        &self,
        scorers: Vec<Box<dyn Scorer>>,
        num_docs: DocId,
        sum_or_do_nothing_combiner: SumOrDoNothingCombiner,
    ) -> Box<dyn Scorer> {
        if !scorers.iter().all(|scorer| {
            scorer.is::<TermScorer<<<Self as Codec>::PostingsCodec as PostingsCodec>::Postings>>()
        }) {
            return box_scorer(BufferedUnionScorer::build(
                scorers,
                SumCombiner::default,
                num_docs,
            ));
        }
        let specialized_scorers: Vec<
            TermScorer<<<Self as Codec>::PostingsCodec as PostingsCodec>::Postings>,
        > = scorers
            .into_iter()
            .map(|scorer| {
                *scorer.downcast::<TermScorer<_>>().ok().expect(
                    "Downcast failed despite the fact we already checked the type was correct",
                )
            })
            .collect();
        match sum_or_do_nothing_combiner {
            SumOrDoNothingCombiner::Sum => box_scorer(BufferedUnionScorer::build(
                specialized_scorers,
                SumCombiner::default,
                num_docs,
            )),
            SumOrDoNothingCombiner::DoNothing => box_scorer(BufferedUnionScorer::build(
                specialized_scorers,
                DoNothingCombiner::default,
                num_docs,
            )),
        }
    }

    fn for_each_pruning(
        &self,
        threshold: Score,
        scorer: Box<dyn Scorer>,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) {
        let accerelerated_foreach_pruning_res =
            <TCodec as Codec>::PostingsCodec::try_accelerated_for_each_pruning(
                threshold, scorer, callback,
            );
        if let Err(mut scorer) = accerelerated_foreach_pruning_res {
            // No acceleration available. We need to do things manually.
            scorer.for_each_pruning(threshold, callback);
        }
    }
}

/// SumCombiner or DoNothingCombiner
#[derive(Copy, Clone)]
pub enum SumOrDoNothingCombiner {
    /// Sum scores together
    Sum,
    /// Do not track any score.
    DoNothing,
}
