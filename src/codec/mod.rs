/// Codec specific to postings data.
pub mod postings;

/// Standard tantivy codec. This is the codec you use by default.
pub mod standard;

use std::sync::Arc;

pub use standard::StandardCodec;

use crate::codec::postings::PostingsCodec;
use crate::directory::Directory;
use crate::fastfield::AliveBitSet;
use crate::query::score_combiner::DoNothingCombiner;
use crate::query::term_query::TermScorer;
use crate::query::{box_scorer, BufferedUnionScorer, Scorer, SumCombiner};
use crate::schema::Schema;
use crate::{DocId, Score, SegmentMeta, SegmentReader, TantivySegmentReader};

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

    /// Loads postings using the codec's concrete postings type.
    fn load_postings_typed(
        &self,
        reader: &dyn crate::index::InvertedIndexReader,
        term_info: &crate::postings::TermInfo,
        option: crate::schema::IndexRecordOption,
    ) -> std::io::Result<<Self::PostingsCodec as crate::codec::postings::PostingsCodec>::Postings>
    {
        let postings_data = reader.read_raw_postings_data(term_info, option)?;
        self.postings_codec()
            .load_postings(term_info.doc_freq, postings_data)
    }

    /// Opens a segment reader using this codec.
    ///
    /// Override this if your codec uses a custom segment reader implementation.
    fn open_segment_reader(
        &self,
        directory: &dyn Directory,
        segment_meta: &SegmentMeta,
        schema: Schema,
        custom_bitset: Option<AliveBitSet>,
    ) -> crate::Result<Arc<dyn SegmentReader>> {
        let codec: Arc<dyn ObjectSafeCodec> = Arc::new(self.clone());
        let reader = TantivySegmentReader::open_with_custom_alive_set_from_directory(
            directory,
            segment_meta,
            schema,
            codec,
            custom_bitset,
        )?;
        Ok(Arc::new(reader))
    }
}

/// Object-safe codec is a Codec that can be used in a trait object.
///
/// The point of it is to offer a way to use a codec without a proliferation of generics.
pub trait ObjectSafeCodec: 'static + Send + Sync {
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
