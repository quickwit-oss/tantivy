use std::io;

/// Block-max WAND algorithm.
pub mod block_wand;
use common::OwnedBytes;

use crate::fieldnorm::FieldNormReader;
use crate::postings::Postings;
use crate::query::{Bm25Weight, Scorer};
use crate::schema::IndexRecordOption;
use crate::{DocId, Score};

/// Postings codec (read path).
pub trait PostingsCodec: Send + Sync + 'static {
    /// Postings type for the postings codec.
    type Postings: Postings + Clone;
    /// Codec-specific postings data payload.
    type PostingsData: Send + Sync + 'static;

    /// Builds codec-specific postings data from raw bytes.
    fn postings_data_from_raw(&self, data: RawPostingsData) -> io::Result<Self::PostingsData>;

    /// Loads postings
    ///
    /// Record option is the option that was passed at indexing time.
    /// Requested option is the option that is requested.
    /// These are expected to be carried by the codec-specific postings data.
    ///
    /// For instance, we may have term_freq in the posting list
    /// but we can skip decompressing as we read the posting list.
    ///
    /// If record option does not support the requested option,
    /// this method does NOT return an error and will in fact restrict
    /// requested_option to what is available.
    fn load_postings(
        &self,
        doc_freq: u32,
        postings_data: Self::PostingsData,
    ) -> io::Result<Self::Postings>;

    /// If your codec supports different ways to accelerate `for_each_pruning` that's
    /// where you should implement it.
    ///
    /// Returning `Err(scorer)` without mutating the scorer nor calling the callback function,
    /// is never "wrong". It just leaves the responsability to the caller to call a fallback
    /// implementation on the scorer.
    ///
    /// If your codec supports BlockMax-Wand, you just need to have your
    /// postings implement `PostingsWithBlockMax` and copy what is done in the StandardPostings
    /// codec to enable it.
    fn try_accelerated_for_each_pruning(
        _threshold: Score,
        scorer: Box<dyn Scorer>,
        _callback: &mut dyn FnMut(DocId, Score) -> Score,
    ) -> Result<(), Box<dyn Scorer>> {
        Err(scorer)
    }
}

/// Raw postings bytes and metadata read from storage.
#[derive(Debug, Clone)]
pub struct RawPostingsData {
    /// Raw postings bytes for the term.
    pub postings_data: OwnedBytes,
    /// Raw positions bytes for the term, if positions are available.
    pub positions_data: Option<OwnedBytes>,
    /// Record option of the indexed field.
    pub record_option: IndexRecordOption,
    /// Effective record option after downgrading to the indexed field capability.
    pub effective_option: IndexRecordOption,
}

/// A light complement interface to Postings to allow block-max wand acceleration.
pub trait PostingsWithBlockMax: Postings {
    /// Moves the postings to the block containign `target_doc` and returns
    /// an upperbound of the score for documents in the block.
    ///
    /// `Warning`: Calling this method may leave the postings in an invalid state.
    /// callers are required to call seek before calling any other of the
    /// `Postings` method (like doc / advance etc.).
    fn seek_block_max(
        &mut self,
        target_doc: crate::DocId,
        fieldnorm_reader: &FieldNormReader,
        similarity_weight: &Bm25Weight,
    ) -> Score;

    /// Returns the last document in the current block (or Terminated if this
    /// is the last block).
    fn last_doc_in_block(&self) -> crate::DocId;
}
