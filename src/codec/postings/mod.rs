use std::io;

/// Block-max WAND algorithm.
pub mod block_wand;
use common::OwnedBytes;

use crate::fieldnorm::FieldNormReader;
use crate::postings::Postings;
use crate::query::{Bm25Weight, Scorer};
use crate::schema::IndexRecordOption;
use crate::{DocId, Score};

/// Postings codec.
pub trait PostingsCodec: Send + Sync + 'static {
    /// Serializer type for the postings codec.
    type PostingsSerializer: PostingsSerializer;
    /// Postings type for the postings codec.
    type Postings: Postings + Clone;
    /// Creates a new postings serializer.
    fn new_serializer(
        &self,
        avg_fieldnorm: Score,
        mode: IndexRecordOption,
        fieldnorm_reader: Option<FieldNormReader>,
    ) -> Self::PostingsSerializer;

    /// Loads postings
    ///
    /// Record option is the option that was passed at indexing time.
    /// Requested option is the option that is requested.
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
        postings_data: OwnedBytes,
        record_option: IndexRecordOption,
        requested_option: IndexRecordOption,
        positions_data: Option<OwnedBytes>,
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

/// A postings serializer is a listener that is in charge of serializing postings
///
/// IO is done only once per postings, once all of the data has been received.
/// A serializer will therefore contain internal buffers.
///
/// A serializer is created once and recycled for all postings.
///
/// Clients should use PostingsSerializer as follows.
/// ```
/// // First postings list
/// serializer.new_term(2, true);
/// serializer.write_doc(2, 1);
/// serializer.write_doc(6, 2);
/// serializer.close_term(3);
/// serializer.clear();
/// // Second postings list
/// serializer.new_term(1, true);
/// serializer.write_doc(3, 1);
/// serializer.close_term(3);
/// ```
pub trait PostingsSerializer {
    /// The term_doc_freq here is the number of documents
    /// in the postings lists.
    ///
    /// It can be used to compute the idf that will be used for the
    /// blockmax parameters.
    ///
    /// If not available (e.g. if we do not collect `term_frequencies`
    /// blockwand is disabled), the term_doc_freq passed will be set 0.
    fn new_term(&mut self, term_doc_freq: u32, record_term_freq: bool);

    /// Records a new document id for the current term.
    /// The serializer may ignore it.
    fn write_doc(&mut self, doc_id: DocId, term_freq: u32);

    /// Closes the current term and writes the postings list associated.
    fn close_term(&mut self, doc_freq: u32, wrt: &mut impl io::Write) -> io::Result<()>;
}

/// A light complement interface to Postings to allow block-max wand acceleration.
pub trait PostingsWithBlockMax: Postings {
    /// Moves the postings to the block containign `target_doc` and returns
    /// an upperbound of the score for documents in the block.
    ///
    /// `Warning`: Calling this method may leave the postings in an invalid state.
    /// callers are required to call seek before calling any other of the
    /// `Postings` method (like doc / advance etc.).
    fn seek_block_max(&mut self, target_doc: crate::DocId, similarity_weight: &Bm25Weight)
        -> Score;

    /// Returns the last document in the current block (or Terminated if this
    /// is the last block).
    fn last_doc_in_block(&self) -> crate::DocId;
}
