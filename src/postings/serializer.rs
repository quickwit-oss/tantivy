use super::TermInfo;
use common::BinarySerializable;
use common::{CompositeWrite, CountingWriter};
use compression::VIntEncoder;
use compression::{BlockEncoder, COMPRESSION_BLOCK_SIZE};
use core::Segment;
use directory::WritePtr;
use schema::Schema;
use schema::{Field, FieldEntry, FieldType};
use std::io::{self, Write};
use termdict::{TermDictionaryBuilder, TermOrdinal};
use DocId;
use Result;

/// `PostingsSerializer` is in charge of serializing
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
    schema: Schema,
}

impl InvertedIndexSerializer {
    /// Open a new `PostingsSerializer` for the given segment
    fn new(
        terms_write: CompositeWrite<WritePtr>,
        postings_write: CompositeWrite<WritePtr>,
        positions_write: CompositeWrite<WritePtr>,
        schema: Schema,
    ) -> Result<InvertedIndexSerializer> {
        Ok(InvertedIndexSerializer {
            terms_write,
            postings_write,
            positions_write,
            schema,
        })
    }

    /// Open a new `PostingsSerializer` for the given segment
    pub fn open(segment: &mut Segment) -> Result<InvertedIndexSerializer> {
        use SegmentComponent::{POSITIONS, POSTINGS, TERMS};
        InvertedIndexSerializer::new(
            CompositeWrite::wrap(segment.open_write(TERMS)?),
            CompositeWrite::wrap(segment.open_write(POSTINGS)?),
            CompositeWrite::wrap(segment.open_write(POSITIONS)?),
            segment.schema(),
        )
    }

    /// Must be called before starting pushing terms of
    /// a given field.
    ///
    /// Loads the indexing options for the given field.
    pub fn new_field(
        &mut self,
        field: Field,
        total_num_tokens: u64,
    ) -> io::Result<FieldSerializer> {
        let field_entry: &FieldEntry = self.schema.get_field_entry(field);
        let term_dictionary_write = self.terms_write.for_field(field);
        let postings_write = self.postings_write.for_field(field);
        total_num_tokens.serialize(postings_write)?;
        let positions_write = self.positions_write.for_field(field);
        FieldSerializer::new(
            field_entry.field_type().clone(),
            term_dictionary_write,
            postings_write,
            positions_write,
        )
    }

    /// Closes the serializer.
    pub fn close(self) -> io::Result<()> {
        self.terms_write.close()?;
        self.postings_write.close()?;
        self.positions_write.close()?;
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
    fn new(
        field_type: FieldType,
        term_dictionary_write: &'a mut CountingWriter<WritePtr>,
        postings_write: &'a mut CountingWriter<WritePtr>,
        positions_write: &'a mut CountingWriter<WritePtr>,
    ) -> io::Result<FieldSerializer<'a>> {
        let (term_freq_enabled, position_enabled): (bool, bool) = match field_type {
            FieldType::Str(ref text_options) => {
                if let Some(text_indexing_options) = text_options.get_indexing_options() {
                    let index_option = text_indexing_options.index_option();
                    (
                        index_option.is_termfreq_enabled(),
                        index_option.is_position_enabled(),
                    )
                } else {
                    (false, false)
                }
            }
            _ => (false, false),
        };
        let term_dictionary_builder =
            TermDictionaryBuilder::new(term_dictionary_write, field_type)?;
        let postings_serializer = PostingsSerializer::new(postings_write, term_freq_enabled);
        let positions_serializer_opt = if position_enabled {
            Some(PositionSerializer::new(positions_write))
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
        let (filepos, offset) = self.positions_serializer_opt
            .as_ref()
            .map(|positions_serializer| positions_serializer.addr())
            .unwrap_or((0u64, 0u8));
        TermInfo {
            doc_freq: 0,
            postings_offset: self.postings_serializer.addr(),
            positions_offset: filepos,
            positions_inner_offset: offset,
        }
    }

    /// Starts the postings for a new term.
    /// * term - the term. It needs to come after the previous term according
    ///   to the lexicographical order.
    /// * doc_freq - return the number of document containing the term.
    pub fn new_term(&mut self, term: &[u8]) -> io::Result<TermOrdinal> {
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
        self.postings_serializer.write_doc(doc_id, term_freq)?;
        if let Some(ref mut positions_serializer) = self.positions_serializer_opt.as_mut() {
            positions_serializer.write(position_deltas)?;
        }
        Ok(())
    }

    /// Finish the serialization for this term postings.
    ///
    /// If the current block is incomplete, it need to be encoded
    /// using `VInt` encoding.
    pub fn close_term(&mut self) -> io::Result<()> {
        if self.term_open {
            self.term_dictionary_builder
                .insert_value(&self.current_term_info)?;
            self.postings_serializer.close_term()?;
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

pub struct PostingsSerializer<W: Write> {
    postings_write: CountingWriter<W>,
    last_doc_id_encoded: u32,

    block_encoder: BlockEncoder,
    doc_ids: Vec<DocId>,
    term_freqs: Vec<u32>,

    termfreq_enabled: bool,
}

impl<W: Write> PostingsSerializer<W> {
    pub fn new(write: W, termfreq_enabled: bool) -> PostingsSerializer<W> {
        PostingsSerializer {
            postings_write: CountingWriter::wrap(write),

            block_encoder: BlockEncoder::new(),
            doc_ids: vec![],
            term_freqs: vec![],

            last_doc_id_encoded: 0u32,
            termfreq_enabled,
        }
    }

    pub fn write_doc(&mut self, doc_id: DocId, term_freq: u32) -> io::Result<()> {
        self.doc_ids.push(doc_id);
        if self.termfreq_enabled {
            self.term_freqs.push(term_freq as u32);
        }
        if self.doc_ids.len() == COMPRESSION_BLOCK_SIZE {
            {
                // encode the doc ids
                let block_encoded: &[u8] = self.block_encoder
                    .compress_block_sorted(&self.doc_ids, self.last_doc_id_encoded);
                self.last_doc_id_encoded = self.doc_ids[self.doc_ids.len() - 1];
                self.postings_write.write_all(block_encoded)?;
            }
            if self.termfreq_enabled {
                // encode the term_freqs
                let block_encoded: &[u8] =
                    self.block_encoder.compress_block_unsorted(&self.term_freqs);
                self.postings_write.write_all(block_encoded)?;
                self.term_freqs.clear();
            }
            self.doc_ids.clear();
        }
        Ok(())
    }

    pub fn close_term(&mut self) -> io::Result<()> {
        if !self.doc_ids.is_empty() {
            // we have doc ids waiting to be written
            // this happens when the number of doc ids is
            // not a perfect multiple of our block size.
            //
            // In that case, the remaining part is encoded
            // using variable int encoding.
            {
                let block_encoded = self.block_encoder
                    .compress_vint_sorted(&self.doc_ids, self.last_doc_id_encoded);
                self.postings_write.write_all(block_encoded)?;
                self.doc_ids.clear();
            }
            // ... Idem for term frequencies
            if self.termfreq_enabled {
                let block_encoded = self.block_encoder
                    .compress_vint_unsorted(&self.term_freqs[..]);
                self.postings_write.write_all(block_encoded)?;
                self.term_freqs.clear();
            }
        }
        Ok(())
    }

    fn close(mut self) -> io::Result<()> {
        self.postings_write.flush()
    }

    fn addr(&self) -> u64 {
        self.postings_write.written_bytes() as u64
    }

    fn clear(&mut self) {
        self.doc_ids.clear();
        self.term_freqs.clear();
        self.last_doc_id_encoded = 0;
    }
}

struct PositionSerializer<W: Write> {
    buffer: Vec<u32>,
    write: CountingWriter<W>, // See if we can offset the original counting writer.
    block_encoder: BlockEncoder,
}

impl<W: Write> PositionSerializer<W> {
    fn new(write: W) -> PositionSerializer<W> {
        PositionSerializer {
            buffer: Vec::with_capacity(COMPRESSION_BLOCK_SIZE),
            write: CountingWriter::wrap(write),
            block_encoder: BlockEncoder::new(),
        }
    }

    fn addr(&self) -> (u64, u8) {
        (self.write.written_bytes() as u64, self.buffer.len() as u8)
    }

    fn write_block(&mut self) -> io::Result<()> {
        assert_eq!(self.buffer.len(), COMPRESSION_BLOCK_SIZE);
        let block_compressed: &[u8] = self.block_encoder.compress_block_unsorted(&self.buffer);
        self.write.write_all(block_compressed)?;
        self.buffer.clear();
        Ok(())
    }

    fn write(&mut self, mut vals: &[u32]) -> io::Result<()> {
        let mut buffer_len = self.buffer.len();
        while vals.len() + buffer_len >= COMPRESSION_BLOCK_SIZE {
            let len_to_completion = COMPRESSION_BLOCK_SIZE - buffer_len;
            self.buffer.extend_from_slice(&vals[..len_to_completion]);
            self.write_block()?;
            vals = &vals[len_to_completion..];
            buffer_len = self.buffer.len();
        }
        self.buffer.extend_from_slice(vals);
        Ok(())
    }

    fn close(mut self) -> io::Result<()> {
        self.buffer.resize(COMPRESSION_BLOCK_SIZE, 0u32);
        self.write_block()?;
        self.write.flush()
    }
}
