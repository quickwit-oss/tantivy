#[cfg(feature = "quickwit")]
use std::future::Future;
use std::io;
#[cfg(feature = "quickwit")]
use std::pin::Pin;

use common::json_path_writer::JSON_END_OF_PATH;
use common::{BinarySerializable, BitSet, ByteCount, OwnedBytes};
#[cfg(feature = "quickwit")]
use futures_util::{FutureExt, StreamExt, TryStreamExt};
#[cfg(feature = "quickwit")]
use itertools::Itertools;
#[cfg(feature = "quickwit")]
use tantivy_fst::automaton::{AlwaysMatch, Automaton};

use crate::codec::postings::RawPostingsData;
use crate::codec::standard::postings::{
    fill_bitset_from_raw_data, load_postings_from_raw_data, SegmentPostings,
};
use crate::directory::FileSlice;
use crate::fieldnorm::FieldNormReader;
use crate::postings::{Postings, TermInfo};
use crate::query::term_query::TermScorer;
use crate::query::{box_scorer, Bm25Weight, PhraseScorer, Scorer};
use crate::schema::{IndexRecordOption, Term, Type};
use crate::termdict::TermDictionary;

#[cfg(feature = "quickwit")]
pub type TermRangeBounds = (std::ops::Bound<Term>, std::ops::Bound<Term>);

/// Type-erased term scorer guaranteed to wrap a Tantivy [`TermScorer`].
pub struct BoxedTermScorer(Box<dyn Scorer>);

impl BoxedTermScorer {
    /// Creates a boxed term scorer from a concrete Tantivy [`TermScorer`].
    pub fn new<TPostings: Postings>(term_scorer: TermScorer<TPostings>) -> BoxedTermScorer {
        BoxedTermScorer(box_scorer(term_scorer))
    }

    /// Converts this boxed term scorer into a generic boxed scorer.
    pub fn into_boxed_scorer(self) -> Box<dyn Scorer> {
        self.0
    }
}

/// Trait defining the contract for inverted index readers.
pub trait InvertedIndexReader: Send + Sync {
    /// Returns the term info associated with the term.
    fn get_term_info(&self, term: &Term) -> io::Result<Option<TermInfo>> {
        self.terms().get(term.serialized_value_bytes())
    }

    /// Return the term dictionary datastructure.
    fn terms(&self) -> &TermDictionary;

    /// Return the fields and types encoded in the dictionary in lexicographic order.
    /// Only valid on JSON fields.
    ///
    /// Notice: This requires a full scan and therefore **very expensive**.
    fn list_encoded_json_fields(&self) -> io::Result<Vec<InvertedIndexFieldSpace>>;

    /// Build a new term scorer.
    fn new_term_scorer(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        fieldnorm_reader: FieldNormReader,
        similarity_weight: Bm25Weight,
    ) -> io::Result<BoxedTermScorer>;

    /// Returns a posting object given a `term_info`.
    /// This method is for an advanced usage only.
    ///
    /// Most users should prefer using [`Self::read_postings()`] instead.
    fn read_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<Box<dyn Postings>>;

    /// Returns the raw postings bytes and metadata for a term.
    fn read_raw_postings_data(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<RawPostingsData>;

    /// Fills a bitset with documents containing the term.
    ///
    /// Implementers can override this to avoid boxing postings.
    fn fill_bitset_for_term(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        doc_bitset: &mut BitSet,
    ) -> io::Result<()> {
        let mut postings = self.read_postings_from_terminfo(term_info, option)?;
        postings.fill_bitset(doc_bitset);
        Ok(())
    }

    /// Builds a phrase scorer for the given term infos.
    fn new_phrase_scorer(
        &self,
        term_infos: &[(usize, TermInfo)],
        similarity_weight: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
    ) -> io::Result<Box<dyn Scorer>>;

    /// Returns the total number of tokens recorded for all documents
    /// (including deleted documents).
    fn total_num_tokens(&self) -> u64;

    /// Returns the segment postings associated with the term, and with the given option,
    /// or `None` if the term has never been encountered and indexed.
    fn read_postings(
        &self,
        term: &Term,
        option: IndexRecordOption,
    ) -> io::Result<Option<Box<dyn Postings>>> {
        self.get_term_info(term)?
            .map(move |term_info| self.read_postings_from_terminfo(&term_info, option))
            .transpose()
    }

    /// Returns the number of documents containing the term.
    fn doc_freq(&self, term: &Term) -> io::Result<u32>;

    /// Returns the number of documents containing the term asynchronously.
    #[cfg(feature = "quickwit")]
    fn doc_freq_async<'a>(
        &'a self,
        term: &'a Term,
    ) -> Pin<Box<dyn Future<Output = io::Result<u32>> + Send + 'a>>;

    /// Warmup fieldnorm readers for this inverted index field.
    #[cfg(feature = "quickwit")]
    fn warm_fieldnorms_readers<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>>;

    /// Warmup the block postings for all terms.
    ///
    /// Default implementation is a no-op.
    #[cfg(feature = "quickwit")]
    fn warm_postings_full<'a>(
        &'a self,
        _with_positions: bool,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    /// Warmup a block postings given a `Term`.
    ///
    /// Returns whether the term was found in the dictionary.
    #[cfg(feature = "quickwit")]
    fn warm_postings<'a>(
        &'a self,
        term: &'a Term,
        with_positions: bool,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + 'a>>;

    /// Warmup block postings for terms in a range.
    ///
    /// Returns whether at least one matching term was found.
    #[cfg(feature = "quickwit")]
    fn warm_postings_range<'a>(
        &'a self,
        terms: TermRangeBounds,
        limit: Option<u64>,
        with_positions: bool,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + 'a>>;

    /// Warmup block postings for terms matching an automaton.
    ///
    /// Returns whether at least one matching term was found.
    #[cfg(feature = "quickwit")]
    fn warm_postings_automaton<'a, A: Automaton + Clone + Send + Sync + 'static>(
        &'a self,
        automaton: A,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + 'a>>
    where
        A::State: Clone + Send,
        Self: Sized;
}

/// Tantivy's default inverted index reader implementation.
///
/// The inverted index reader is in charge of accessing
/// the inverted index associated with a specific field.
///
/// # Note
///
/// It is safe to delete the segment associated with
/// an `InvertedIndexReader` implementation. As long as it is open,
/// the [`FileSlice`] it is relying on should
/// stay available.
///
/// `TantivyInvertedIndexReader` instances are created by calling
/// [`SegmentReader::inverted_index()`](crate::SegmentReader::inverted_index).
pub struct TantivyInvertedIndexReader {
    termdict: TermDictionary,
    postings_file_slice: FileSlice,
    positions_file_slice: FileSlice,
    #[cfg_attr(not(feature = "quickwit"), allow(dead_code))]
    fieldnorms_file_slice: FileSlice,
    record_option: IndexRecordOption,
    total_num_tokens: u64,
}

/// Object that records the amount of space used by a field in an inverted index.
pub struct InvertedIndexFieldSpace {
    /// Field name as encoded in the term dictionary.
    pub field_name: String,
    /// Value type for the encoded field.
    pub field_type: Type,
    /// Total bytes used by postings for this field.
    pub postings_size: ByteCount,
    /// Total bytes used by positions for this field.
    pub positions_size: ByteCount,
    /// Number of terms in the field.
    pub num_terms: u64,
}

/// Returns None if the term is not a valid JSON path.
fn extract_field_name_and_field_type_from_json_path(term: &[u8]) -> Option<(String, Type)> {
    let index = term.iter().position(|&byte| byte == JSON_END_OF_PATH)?;
    let field_type_code = term.get(index + 1).copied()?;
    let field_type = Type::from_code(field_type_code)?;
    // Let's flush the current field.
    let field_name = String::from_utf8_lossy(&term[..index]).to_string();
    Some((field_name, field_type))
}

impl InvertedIndexFieldSpace {
    fn record(&mut self, term_info: &TermInfo) {
        self.postings_size += ByteCount::from(term_info.posting_num_bytes() as u64);
        self.positions_size += ByteCount::from(term_info.positions_num_bytes() as u64);
        self.num_terms += 1;
    }
}

impl TantivyInvertedIndexReader {
    pub(crate) fn read_raw_postings_data_inner(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<RawPostingsData> {
        let effective_option = option.downgrade(self.record_option);
        let postings_data = self
            .postings_file_slice
            .slice(term_info.postings_range.clone())
            .read_bytes()?;
        let positions_data: Option<OwnedBytes> = if effective_option.has_positions() {
            let positions_data = self
                .positions_file_slice
                .slice(term_info.positions_range.clone())
                .read_bytes()?;
            Some(positions_data)
        } else {
            None
        };
        Ok(RawPostingsData {
            postings_data,
            positions_data,
            record_option: self.record_option,
            effective_option,
        })
    }

    /// Opens an inverted index reader from already-loaded term/postings/positions slices.
    ///
    /// The first 8 bytes of `postings_file_slice` are expected to contain
    /// the serialized total token count.
    pub fn new(
        termdict: TermDictionary,
        postings_file_slice: FileSlice,
        positions_file_slice: FileSlice,
        fieldnorms_file_slice: FileSlice,
        record_option: IndexRecordOption,
    ) -> io::Result<TantivyInvertedIndexReader> {
        let (total_num_tokens_slice, postings_body) = postings_file_slice.split(8);
        let total_num_tokens = u64::deserialize(&mut total_num_tokens_slice.read_bytes()?)?;
        Ok(TantivyInvertedIndexReader {
            termdict,
            postings_file_slice: postings_body,
            positions_file_slice,
            fieldnorms_file_slice,
            record_option,
            total_num_tokens,
        })
    }

    /// Creates an empty `TantivyInvertedIndexReader` object, which
    /// contains no terms at all.
    pub fn empty(record_option: IndexRecordOption) -> TantivyInvertedIndexReader {
        TantivyInvertedIndexReader {
            termdict: TermDictionary::empty(),
            postings_file_slice: FileSlice::empty(),
            positions_file_slice: FileSlice::empty(),
            fieldnorms_file_slice: FileSlice::empty(),
            record_option,
            total_num_tokens: 0u64,
        }
    }

    fn load_segment_postings(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<SegmentPostings> {
        let postings_data = self.read_raw_postings_data_inner(term_info, option)?;
        load_postings_from_raw_data(term_info.doc_freq, postings_data)
    }
}

impl InvertedIndexReader for TantivyInvertedIndexReader {
    fn terms(&self) -> &TermDictionary {
        &self.termdict
    }

    fn list_encoded_json_fields(&self) -> io::Result<Vec<InvertedIndexFieldSpace>> {
        let mut stream = self.termdict.stream()?;
        let mut fields: Vec<InvertedIndexFieldSpace> = Vec::new();

        let mut current_field_opt: Option<InvertedIndexFieldSpace> = None;
        // Current field bytes, including the JSON_END_OF_PATH.
        let mut current_field_bytes: Vec<u8> = Vec::new();

        while let Some((term, term_info)) = stream.next() {
            if let Some(current_field) = &mut current_field_opt {
                if term.starts_with(&current_field_bytes) {
                    // We are still in the same field.
                    current_field.record(term_info);
                    continue;
                }
            }

            // This is a new field!
            // Let's flush the current field.
            fields.extend(current_field_opt.take());
            current_field_bytes.clear();

            // And create a new one.
            let Some((field_name, field_type)) =
                extract_field_name_and_field_type_from_json_path(term)
            else {
                error!(
                    "invalid term bytes encountered {term:?}. this only happens if the term \
                     dictionary is corrupted. please report"
                );
                continue;
            };
            let mut field_space = InvertedIndexFieldSpace {
                field_name,
                field_type,
                postings_size: ByteCount::default(),
                positions_size: ByteCount::default(),
                num_terms: 0u64,
            };
            field_space.record(term_info);

            // We include the json type and the json end of path to make sure the prefix check
            // is meaningful.
            current_field_bytes.extend_from_slice(&term[..field_space.field_name.len() + 2]);
            current_field_opt = Some(field_space);
        }

        // We need to flush the last field as well.
        fields.extend(current_field_opt.take());

        Ok(fields)
    }

    fn new_term_scorer(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        fieldnorm_reader: FieldNormReader,
        similarity_weight: Bm25Weight,
    ) -> io::Result<BoxedTermScorer> {
        let postings = self.load_segment_postings(term_info, option)?;
        let term_scorer = TermScorer::new(postings, fieldnorm_reader, similarity_weight);
        Ok(BoxedTermScorer::new(term_scorer))
    }

    fn read_postings_from_terminfo(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<Box<dyn Postings>> {
        let postings = self.load_segment_postings(term_info, option)?;
        Ok(Box::new(postings))
    }

    fn read_raw_postings_data(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
    ) -> io::Result<RawPostingsData> {
        self.read_raw_postings_data_inner(term_info, option)
    }

    fn fill_bitset_for_term(
        &self,
        term_info: &TermInfo,
        option: IndexRecordOption,
        doc_bitset: &mut BitSet,
    ) -> io::Result<()> {
        let postings_data = self.read_raw_postings_data_inner(term_info, option)?;
        fill_bitset_from_raw_data(term_info.doc_freq, postings_data, doc_bitset)
    }

    fn new_phrase_scorer(
        &self,
        term_infos: &[(usize, TermInfo)],
        similarity_weight: Option<Bm25Weight>,
        fieldnorm_reader: FieldNormReader,
        slop: u32,
    ) -> io::Result<Box<dyn Scorer>> {
        let mut offset_and_term_postings: Vec<(usize, SegmentPostings)> =
            Vec::with_capacity(term_infos.len());
        for (offset, term_info) in term_infos {
            let postings =
                self.load_segment_postings(term_info, IndexRecordOption::WithFreqsAndPositions)?;
            offset_and_term_postings.push((*offset, postings));
        }
        let scorer = PhraseScorer::new(
            offset_and_term_postings,
            similarity_weight,
            fieldnorm_reader,
            slop,
        );
        Ok(box_scorer(scorer))
    }

    fn total_num_tokens(&self) -> u64 {
        self.total_num_tokens
    }

    fn read_postings(
        &self,
        term: &Term,
        option: IndexRecordOption,
    ) -> io::Result<Option<Box<dyn Postings>>> {
        self.get_term_info(term)?
            .map(move |term_info| self.read_postings_from_terminfo(&term_info, option))
            .transpose()
    }

    fn doc_freq(&self, term: &Term) -> io::Result<u32> {
        Ok(self
            .get_term_info(term)?
            .map(|term_info| term_info.doc_freq)
            .unwrap_or(0u32))
    }

    #[cfg(feature = "quickwit")]
    fn doc_freq_async<'a>(
        &'a self,
        term: &'a Term,
    ) -> Pin<Box<dyn Future<Output = io::Result<u32>> + Send + 'a>> {
        Box::pin(async move {
            Ok(self
                .get_term_info_async(term)
                .await?
                .map(|term_info| term_info.doc_freq)
                .unwrap_or(0u32))
        })
    }

    #[cfg(feature = "quickwit")]
    fn warm_fieldnorms_readers<'a>(
        &'a self,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.fieldnorms_file_slice.read_bytes_async().await?;
            Ok(())
        })
    }

    #[cfg(feature = "quickwit")]
    fn warm_postings_full<'a>(
        &'a self,
        with_positions: bool,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            self.postings_file_slice.read_bytes_async().await?;
            if with_positions {
                self.positions_file_slice.read_bytes_async().await?;
            }
            Ok(())
        })
    }

    #[cfg(feature = "quickwit")]
    fn warm_postings<'a>(
        &'a self,
        term: &'a Term,
        with_positions: bool,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + 'a>> {
        Box::pin(async move {
            let term_info_opt: Option<TermInfo> = self.get_term_info_async(term).await?;
            if let Some(term_info) = term_info_opt {
                let postings = self
                    .postings_file_slice
                    .read_bytes_slice_async(term_info.postings_range.clone());
                if with_positions {
                    let positions = self
                        .positions_file_slice
                        .read_bytes_slice_async(term_info.positions_range.clone());
                    futures_util::future::try_join(postings, positions).await?;
                } else {
                    postings.await?;
                }
                Ok(true)
            } else {
                Ok(false)
            }
        })
    }

    #[cfg(feature = "quickwit")]
    fn warm_postings_range<'a>(
        &'a self,
        terms: TermRangeBounds,
        limit: Option<u64>,
        with_positions: bool,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + 'a>> {
        Box::pin(async move {
            let mut term_info = self
                .get_term_range_async(terms, AlwaysMatch, limit, 0)
                .await?;

            let Some(first_terminfo) = term_info.next() else {
                // no key matches, nothing more to load
                return Ok(false);
            };

            let last_terminfo = term_info.last().unwrap_or_else(|| first_terminfo.clone());

            let postings_range =
                first_terminfo.postings_range.start..last_terminfo.postings_range.end;
            let positions_range =
                first_terminfo.positions_range.start..last_terminfo.positions_range.end;

            let postings = self
                .postings_file_slice
                .read_bytes_slice_async(postings_range);
            if with_positions {
                let positions = self
                    .positions_file_slice
                    .read_bytes_slice_async(positions_range);
                futures_util::future::try_join(postings, positions).await?;
            } else {
                postings.await?;
            }
            Ok(true)
        })
    }

    #[cfg(feature = "quickwit")]
    fn warm_postings_automaton<'a, A: Automaton + Clone + Send + Sync + 'static>(
        &'a self,
        automaton: A,
    ) -> Pin<Box<dyn Future<Output = io::Result<bool>> + Send + 'a>>
    where
        A::State: Clone + Send,
        Self: Sized,
    {
        Box::pin(async move {
            // merge holes under 4MiB, that's how many bytes we can hope to receive during a TTFB
            // from S3 (~80MiB/s, and 50ms latency)
            const MERGE_HOLES_UNDER_BYTES: usize = (80 * 1024 * 1024 * 50) / 1000;
            // Trigger async prefetch of relevant termdict blocks.
            let _term_info_iter = self
                .get_term_range_async(
                    (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
                    automaton.clone(),
                    None,
                    MERGE_HOLES_UNDER_BYTES,
                )
                .await?;
            drop(_term_info_iter);

            // Build a 2nd stream without merged holes so we only scan matching blocks.
            // This assumes the storage layer caches data fetched by the first pass.
            let mut stream = self.termdict.search(automaton).into_stream()?;
            let posting_ranges_iter =
                std::iter::from_fn(move || stream.next().map(|(_k, v)| v.postings_range.clone()));
            let merged_posting_ranges: Vec<std::ops::Range<usize>> = posting_ranges_iter
                .coalesce(|range1, range2| {
                    if range1.end + MERGE_HOLES_UNDER_BYTES >= range2.start {
                        Ok(range1.start..range2.end)
                    } else {
                        Err((range1, range2))
                    }
                })
                .collect();

            if merged_posting_ranges.is_empty() {
                return Ok(false);
            }

            let slices_downloaded = futures_util::stream::iter(merged_posting_ranges.into_iter())
                .map(|posting_slice| {
                    self.postings_file_slice
                        .read_bytes_slice_async(posting_slice)
                        .map(|result| result.map(|_slice| ()))
                })
                .buffer_unordered(5)
                .try_collect::<Vec<()>>()
                .await?;

            Ok(!slices_downloaded.is_empty())
        })
    }
}

#[cfg(feature = "quickwit")]
impl TantivyInvertedIndexReader {
    pub(crate) async fn get_term_info_async(&self, term: &Term) -> io::Result<Option<TermInfo>> {
        self.termdict.get_async(term.serialized_value_bytes()).await
    }

    async fn get_term_range_async<'a, A: Automaton + 'a>(
        &'a self,
        terms: TermRangeBounds,
        automaton: A,
        limit: Option<u64>,
        merge_holes_under_bytes: usize,
    ) -> io::Result<impl Iterator<Item = TermInfo> + 'a>
    where
        A::State: Clone,
    {
        let range_builder = self.termdict.search(automaton);
        let (start_bound, end_bound) = terms;
        let range_builder = match start_bound {
            std::ops::Bound::Included(bound) => range_builder.ge(bound.serialized_value_bytes()),
            std::ops::Bound::Excluded(bound) => range_builder.gt(bound.serialized_value_bytes()),
            std::ops::Bound::Unbounded => range_builder,
        };
        let range_builder = match end_bound {
            std::ops::Bound::Included(bound) => range_builder.le(bound.serialized_value_bytes()),
            std::ops::Bound::Excluded(bound) => range_builder.lt(bound.serialized_value_bytes()),
            std::ops::Bound::Unbounded => range_builder,
        };
        let range_builder = if let Some(limit) = limit {
            range_builder.limit(limit)
        } else {
            range_builder
        };

        let mut stream = range_builder
            .into_stream_async_merging_holes(merge_holes_under_bytes)
            .await?;

        let iter = std::iter::from_fn(move || stream.next().map(|(_k, v)| v.clone()));

        // limit on stream is only an optimization to load less data, the stream may still return
        // more than limit elements.
        let limit = limit.map(|limit| limit as usize).unwrap_or(usize::MAX);
        let iter = iter.take(limit);

        Ok(iter)
    }
}
