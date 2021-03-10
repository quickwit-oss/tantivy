use super::TermInfo;
use crate::common::{BinarySerializable, VInt};
use crate::common::{CompositeWrite, CountingWriter};
use crate::core::Segment;
use crate::directory::WritePtr;
use crate::fieldnorm::FieldNormReader;
use crate::positions::PositionSerializer;
use crate::postings::compression::{BlockEncoder, VIntEncoder, COMPRESSION_BLOCK_SIZE};
use crate::postings::skip::SkipSerializer;
use crate::query::BM25Weight;
use crate::schema::{Field, FieldEntry, FieldType};
use crate::schema::{IndexRecordOption, Schema};
use crate::termdict::{TermDictionaryBuilder, TermOrdinal};
use crate::{DocId, Score};
use std::cmp::Ordering;
use std::io::{self, Write};

/// `InvertedIndexSerializer` is in charge of serializing
/// postings on disk, in the
/// * `.idx` (inverted index)
/// * `.pos` (positions file)
/// * `.term` (term dictionary)
///
/// `PostingsWriter` are in charge of pushing the data to the
/// serializer.
///
/// The serializer expects to receive the following calls
/// in this order :
/// * `set_field(...)`
/// * `new_term(...)`
/// * `write_doc(...)`
/// * `write_doc(...)`
/// * `write_doc(...)`
/// * ...
/// * `close_term()`
/// * `new_term(...)`
/// * `write_doc(...)`
/// * ...
/// * `close_term()`
/// * `set_field(...)`
/// * ...
/// * `close()`
///
/// Terms have to be pushed in a lexicographically-sorted order.
/// Within a term, document have to be pushed in increasing order.
///
/// A description of the serialization format is
/// [available here](https://fulmicoton.gitbooks.io/tantivy-doc/content/inverted-index.html).
pub struct InvertedIndexSerializer {
    terms_write: CompositeWrite<WritePtr>,
    postings_write: CompositeWrite<WritePtr>,
    positions_write: CompositeWrite<WritePtr>,
    positionsidx_write: CompositeWrite<WritePtr>,
    schema: Schema,
}

impl InvertedIndexSerializer {
    /// Open a new `PostingsSerializer` for the given segment
    pub fn open(segment: &mut Segment) -> crate::Result<InvertedIndexSerializer> {
        use crate::SegmentComponent::{POSITIONS, POSITIONSSKIP, POSTINGS, TERMS};
        let inv_index_serializer = InvertedIndexSerializer {
            terms_write: CompositeWrite::wrap(segment.open_write(TERMS)?),
            postings_write: CompositeWrite::wrap(segment.open_write(POSTINGS)?),
            positions_write: CompositeWrite::wrap(segment.open_write(POSITIONS)?),
            positionsidx_write: CompositeWrite::wrap(segment.open_write(POSITIONSSKIP)?),
            schema: segment.schema(),
        };
        Ok(inv_index_serializer)
    }

    /// Must be called before starting pushing terms of
    /// a given field.
    ///
    /// Loads the indexing options for the given field.
    pub fn new_field(
        &mut self,
        field: Field,
        total_num_tokens: u64,
        fieldnorm_reader: Option<FieldNormReader>,
    ) -> io::Result<FieldSerializer<'_>> {
        let field_entry: &FieldEntry = self.schema.get_field_entry(field);
        let term_dictionary_write = self.terms_write.for_field(field);
        let postings_write = self.postings_write.for_field(field);
        let positions_write = self.positions_write.for_field(field);
        let positionsidx_write = self.positionsidx_write.for_field(field);
        let field_type: FieldType = (*field_entry.field_type()).clone();
        FieldSerializer::create(
            &field_type,
            total_num_tokens,
            term_dictionary_write,
            postings_write,
            positions_write,
            positionsidx_write,
            fieldnorm_reader,
        )
    }

    /// Closes the serializer.
    pub fn close(self) -> io::Result<()> {
        self.terms_write.close()?;
        self.postings_write.close()?;
        self.positions_write.close()?;
        self.positionsidx_write.close()?;
        Ok(())
    }
}

/// The field serializer is in charge of
/// the serialization of a specific field.
pub struct FieldSerializer<'a> {
    term_dictionary_builder: TermDictionaryBuilder<&'a mut CountingWriter<WritePtr>>,
    postings_serializer: PostingsSerializer<&'a mut CountingWriter<WritePtr>>,
    positions_serializer_opt: Option<PositionSerializer<&'a mut CountingWriter<WritePtr>>>,
    current_term_info: TermInfo,
    term_open: bool,
    num_terms: TermOrdinal,
}

impl<'a> FieldSerializer<'a> {
    fn create(
        field_type: &FieldType,
        total_num_tokens: u64,
        term_dictionary_write: &'a mut CountingWriter<WritePtr>,
        postings_write: &'a mut CountingWriter<WritePtr>,
        positions_write: &'a mut CountingWriter<WritePtr>,
        positionsidx_write: &'a mut CountingWriter<WritePtr>,
        fieldnorm_reader: Option<FieldNormReader>,
    ) -> io::Result<FieldSerializer<'a>> {
        total_num_tokens.serialize(postings_write)?;
        let mode = match field_type {
            FieldType::Str(ref text_options) => {
                if let Some(text_indexing_options) = text_options.get_indexing_options() {
                    text_indexing_options.index_option()
                } else {
                    IndexRecordOption::Basic
                }
            }
            _ => IndexRecordOption::Basic,
        };
        let term_dictionary_builder = TermDictionaryBuilder::create(term_dictionary_write)?;
        let average_fieldnorm = fieldnorm_reader
            .as_ref()
            .map(|ff_reader| (total_num_tokens as Score / ff_reader.num_docs() as Score))
            .unwrap_or(0.0);
        let postings_serializer =
            PostingsSerializer::new(postings_write, average_fieldnorm, mode, fieldnorm_reader);
        let positions_serializer_opt = if mode.has_positions() {
            Some(PositionSerializer::new(positions_write, positionsidx_write))
        } else {
            None
        };

        Ok(FieldSerializer {
            term_dictionary_builder,
            postings_serializer,
            positions_serializer_opt,
            current_term_info: TermInfo::default(),
            term_open: false,
            num_terms: TermOrdinal::default(),
        })
    }

    fn current_term_info(&self) -> TermInfo {
        let positions_idx =
            if let Some(positions_serializer) = self.positions_serializer_opt.as_ref() {
                positions_serializer.positions_idx()
            } else {
                0u64
            };
        let addr = self.postings_serializer.addr() as usize;
        TermInfo {
            doc_freq: 0,
            postings_range: addr..addr,
            positions_idx,
        }
    }

    /// Starts the postings for a new term.
    /// * term - the term. It needs to come after the previous term according
    ///   to the lexicographical order.
    /// * term_doc_freq - return the number of document containing the term.
    pub fn new_term(&mut self, term: &[u8], term_doc_freq: u32) -> io::Result<TermOrdinal> {
        assert!(
            !self.term_open,
            "Called new_term, while the previous term was not closed."
        );

        self.term_open = true;
        self.postings_serializer.clear();
        self.current_term_info = self.current_term_info();
        self.term_dictionary_builder.insert_key(term)?;
        let term_ordinal = self.num_terms;
        self.num_terms += 1;
        self.postings_serializer.new_term(term_doc_freq);
        Ok(term_ordinal)
    }

    /// Serialize the information that a document contains the current term,
    /// its term frequency, and the position deltas.
    ///
    /// At this point, the positions are already `delta-encoded`.
    /// For instance, if the positions are `2, 3, 17`,
    /// `position_deltas` is `2, 1, 14`
    ///
    /// Term frequencies and positions may be ignored by the serializer depending
    /// on the configuration of the field in the `Schema`.
    pub fn write_doc(
        &mut self,
        doc_id: DocId,
        term_freq: u32,
        position_deltas: &[u32],
    ) -> io::Result<()> {
        self.current_term_info.doc_freq += 1;
        self.postings_serializer.write_doc(doc_id, term_freq);
        if let Some(ref mut positions_serializer) = self.positions_serializer_opt.as_mut() {
            positions_serializer.write_all(position_deltas)?;
        }
        Ok(())
    }

    /// Finish the serialization for this term postings.
    ///
    /// If the current block is incomplete, it need to be encoded
    /// using `VInt` encoding.
    pub fn close_term(&mut self) -> io::Result<()> {
        if self.term_open {
            self.postings_serializer
                .close_term(self.current_term_info.doc_freq)?;
            self.current_term_info.postings_range.end = self.postings_serializer.addr() as usize;
            self.term_dictionary_builder
                .insert_value(&self.current_term_info)?;
            self.term_open = false;
        }
        Ok(())
    }

    /// Closes the current current field.
    pub fn close(mut self) -> io::Result<()> {
        self.close_term()?;
        if let Some(positions_serializer) = self.positions_serializer_opt {
            positions_serializer.close()?;
        }
        self.postings_serializer.close()?;
        self.term_dictionary_builder.finish()?;
        Ok(())
    }
}

struct Block {
    doc_ids: [DocId; COMPRESSION_BLOCK_SIZE],
    term_freqs: [u32; COMPRESSION_BLOCK_SIZE],
    len: usize,
}

impl Block {
    fn new() -> Self {
        Block {
            doc_ids: [0u32; COMPRESSION_BLOCK_SIZE],
            term_freqs: [0u32; COMPRESSION_BLOCK_SIZE],
            len: 0,
        }
    }

    fn doc_ids(&self) -> &[DocId] {
        &self.doc_ids[..self.len]
    }

    fn term_freqs(&self) -> &[u32] {
        &self.term_freqs[..self.len]
    }

    fn clear(&mut self) {
        self.len = 0;
    }

    fn append_doc(&mut self, doc: DocId, term_freq: u32) {
        let len = self.len;
        self.doc_ids[len] = doc;
        self.term_freqs[len] = term_freq;
        self.len = len + 1;
    }

    fn is_full(&self) -> bool {
        self.len == COMPRESSION_BLOCK_SIZE
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn last_doc(&self) -> DocId {
        assert_eq!(self.len, COMPRESSION_BLOCK_SIZE);
        self.doc_ids[COMPRESSION_BLOCK_SIZE - 1]
    }
}

pub struct PostingsSerializer<W: Write> {
    output_write: CountingWriter<W>,
    last_doc_id_encoded: u32,

    block_encoder: BlockEncoder,
    block: Box<Block>,

    postings_write: Vec<u8>,
    skip_write: SkipSerializer,

    mode: IndexRecordOption,
    fieldnorm_reader: Option<FieldNormReader>,

    bm25_weight: Option<BM25Weight>,

    num_docs: u32, // Number of docs in the segment
    avg_fieldnorm: Score, // Average number of term in the field for that segment.
                   // this value is used to compute the block wand information.
}

impl<W: Write> PostingsSerializer<W> {
    pub fn new(
        write: W,
        avg_fieldnorm: Score,
        mode: IndexRecordOption,
        fieldnorm_reader: Option<FieldNormReader>,
    ) -> PostingsSerializer<W> {
        let num_docs = fieldnorm_reader
            .as_ref()
            .map(|fieldnorm_reader| fieldnorm_reader.num_docs())
            .unwrap_or(0u32);
        PostingsSerializer {
            output_write: CountingWriter::wrap(write),

            block_encoder: BlockEncoder::new(),
            block: Box::new(Block::new()),

            postings_write: Vec::new(),
            skip_write: SkipSerializer::new(),

            last_doc_id_encoded: 0u32,
            mode,

            fieldnorm_reader,
            bm25_weight: None,

            num_docs,
            avg_fieldnorm,
        }
    }

    pub fn new_term(&mut self, term_doc_freq: u32) {
        if self.mode.has_freq() && self.num_docs > 0 {
            let bm25_weight = BM25Weight::for_one_term(
                term_doc_freq as u64,
                self.num_docs as u64,
                self.avg_fieldnorm,
            );
            self.bm25_weight = Some(bm25_weight);
        }
    }

    fn write_block(&mut self) {
        {
            // encode the doc ids
            let (num_bits, block_encoded): (u8, &[u8]) = self
                .block_encoder
                .compress_block_sorted(&self.block.doc_ids(), self.last_doc_id_encoded);
            self.last_doc_id_encoded = self.block.last_doc();
            self.skip_write
                .write_doc(self.last_doc_id_encoded, num_bits);
            // last el block 0, offset block 1,
            self.postings_write.extend(block_encoded);
        }
        if self.mode.has_freq() {
            let (num_bits, block_encoded): (u8, &[u8]) = self
                .block_encoder
                .compress_block_unsorted(&self.block.term_freqs());
            self.postings_write.extend(block_encoded);
            self.skip_write.write_term_freq(num_bits);
            if self.mode.has_positions() {
                // We serialize the sum of term freqs within the skip information
                // in order to navigate through positions.
                let sum_freq = self.block.term_freqs().iter().cloned().sum();
                self.skip_write.write_total_term_freq(sum_freq);
            }
            let mut blockwand_params = (0u8, 0u32);
            if let Some(bm25_weight) = self.bm25_weight.as_ref() {
                if let Some(fieldnorm_reader) = self.fieldnorm_reader.as_ref() {
                    let docs = self.block.doc_ids().iter().cloned();
                    let term_freqs = self.block.term_freqs().iter().cloned();
                    let fieldnorms = docs.map(|doc| fieldnorm_reader.fieldnorm_id(doc));
                    blockwand_params = fieldnorms
                        .zip(term_freqs)
                        .max_by(
                            |(left_fieldnorm_id, left_term_freq),
                             (right_fieldnorm_id, right_term_freq)| {
                                let left_score =
                                    bm25_weight.tf_factor(*left_fieldnorm_id, *left_term_freq);
                                let right_score =
                                    bm25_weight.tf_factor(*right_fieldnorm_id, *right_term_freq);
                                left_score
                                    .partial_cmp(&right_score)
                                    .unwrap_or(Ordering::Equal)
                            },
                        )
                        .unwrap();
                }
            }
            let (fieldnorm_id, term_freq) = blockwand_params;
            self.skip_write.write_blockwand_max(fieldnorm_id, term_freq);
        }
        self.block.clear();
    }

    pub fn write_doc(&mut self, doc_id: DocId, term_freq: u32) {
        self.block.append_doc(doc_id, term_freq);
        if self.block.is_full() {
            self.write_block();
        }
    }

    fn close(mut self) -> io::Result<()> {
        self.postings_write.flush()
    }

    pub fn close_term(&mut self, doc_freq: u32) -> io::Result<()> {
        if !self.block.is_empty() {
            // we have doc ids waiting to be written
            // this happens when the number of doc ids is
            // not a perfect multiple of our block size.
            //
            // In that case, the remaining part is encoded
            // using variable int encoding.
            {
                let block_encoded = self
                    .block_encoder
                    .compress_vint_sorted(&self.block.doc_ids(), self.last_doc_id_encoded);
                self.postings_write.write_all(block_encoded)?;
            }
            // ... Idem for term frequencies
            if self.mode.has_freq() {
                let block_encoded = self
                    .block_encoder
                    .compress_vint_unsorted(self.block.term_freqs());
                self.postings_write.write_all(block_encoded)?;
            }
            self.block.clear();
        }
        if doc_freq >= COMPRESSION_BLOCK_SIZE as u32 {
            let skip_data = self.skip_write.data();
            VInt(skip_data.len() as u64).serialize(&mut self.output_write)?;
            self.output_write.write_all(skip_data)?;
            self.output_write.write_all(&self.postings_write[..])?;
        } else {
            self.output_write.write_all(&self.postings_write[..])?;
        }
        self.skip_write.clear();
        self.postings_write.clear();
        self.bm25_weight = None;
        Ok(())
    }

    fn addr(&self) -> u64 {
        self.output_write.written_bytes() as u64
    }

    fn clear(&mut self) {
        self.block.clear();
        self.last_doc_id_encoded = 0;
    }
}
