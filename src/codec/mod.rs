/// Codec specific to postings data.
pub mod postings;

/// Standard tantivy codec. This is the codec you use by default.
pub mod standard;

use std::io;

pub use standard::StandardCodec;

use crate::codec::postings::PostingsCodec;
use crate::fieldnorm::FieldNormReader;
use crate::postings::{Postings, TermInfo};
use crate::query::{box_scorer, Bm25Weight, Scorer};
use crate::schema::IndexRecordOption;
use crate::{DocId, InvertedIndexReader, Score};

/// Codecs describes how data is layed out on disk.
///
/// For the moment, only postings codec can be custom.
pub trait Codec: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// The specific postings type used by this codec.
    type PostingsCodec: PostingsCodec;

    /// Name of the codec. It should be unique to your codec.
    const NAME: &'static str;

    /// Load codec based on the codec configuration.
    fn from_json_props(json_value: &serde_json::Value) -> crate::Result<Self>;

    /// Get codec configuration.
    fn to_json_props(&self) -> serde_json::Value;

    /// Returns the postings codec.
    fn postings_codec(&self) -> &Self::PostingsCodec;
}

pub trait ObjectSafeCodec: 'static + Send + Sync {
    fn load_postings_type_erased(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        inverted_index_reader: &InvertedIndexReader,
    ) -> io::Result<Box<dyn Postings>>;

    fn load_term_scorer_type_erased(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        inverted_index_reader: &InvertedIndexReader,
        fieldnorm_reader: FieldNormReader,
        similarity_weight: Bm25Weight,
    ) -> io::Result<Box<dyn Scorer>>;

    fn new_phrase_scorer_type_erased(
        &self,
        term_infos: &[(usize, TermInfo)],
        similarity_weight: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
        inverted_index_reader: &InvertedIndexReader,
    ) -> io::Result<Box<dyn Scorer>>;

    fn for_each_pruning(
        &self,
        threshold: Score,
        scorer: Box<dyn Scorer>,
        callback: &mut dyn FnMut(DocId, Score) -> Score,
    );
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

pub trait PostingsWithBlockMax: Postings {
    fn seek_block(
        &mut self,
        target_doc: crate::DocId,
        fieldnorm_reader: &FieldNormReader,
        similarity_weight: &Bm25Weight,
    ) -> Score;

    fn last_doc_in_block(&self) -> crate::DocId;
}
