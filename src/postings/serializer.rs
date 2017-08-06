use Result;
use termdict::TermDictionaryBuilderImpl;
use super::TermInfo;
use schema::Field;
use schema::FieldEntry;
use schema::FieldType;
use schema::Schema;
use schema::TextIndexingOptions;
use directory::WritePtr;
use compression::{NUM_DOCS_PER_BLOCK, BlockEncoder};
use DocId;
use core::Segment;
use std::io::{self, Write};
use compression::VIntEncoder;
use common::CountingWriter;
use termdict::TermDictionaryBuilder;


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
pub struct PostingsSerializer {
    terms_fst_builder: TermDictionaryBuilderImpl<WritePtr, TermInfo>,
    postings_write: CountingWriter<WritePtr>,
    last_doc_id_encoded: u32,
    positions_writer: PositionWriter,
    block_encoder: BlockEncoder,
    doc_ids: Vec<DocId>,
    term_freqs: Vec<u32>,
    schema: Schema,
    text_indexing_options: TextIndexingOptions,
    term_open: bool,
    current_term_info: TermInfo,
}

struct PositionWriter {
    buffer: Vec<u32>,
    write: CountingWriter<WritePtr>,
    block_encoder: BlockEncoder,
}

impl PositionWriter {
    fn new(write: WritePtr) -> PositionWriter {
        PositionWriter {
            buffer: Vec::with_capacity(NUM_DOCS_PER_BLOCK),
            write: CountingWriter::wrap(write),
            block_encoder: BlockEncoder::new(),
        }
    }

    fn addr(&self) -> (u32, u8) {
        (self.write.written_bytes() as u32, self.buffer.len() as u8)
    }

    fn write_block(&mut self) -> io::Result<()> {
        assert_eq!(self.buffer.len(), NUM_DOCS_PER_BLOCK);
        let block_compressed: &[u8] = self.block_encoder.compress_block_unsorted(&self.buffer);
        self.write.write_all(block_compressed)?;
        self.buffer.clear();
        Ok(())
    }

    fn write(&mut self, mut vals: &[u32]) -> io::Result<()> {
        let mut buffer_len = self.buffer.len();
        while vals.len() + buffer_len >= NUM_DOCS_PER_BLOCK {
            let len_to_completion = NUM_DOCS_PER_BLOCK - buffer_len;
            self.buffer.extend_from_slice(&vals[..len_to_completion]);
            self.write_block()?;
            vals = &vals[len_to_completion..];
            buffer_len = self.buffer.len();
        }
        self.buffer.extend_from_slice(&vals);
        Ok(())
    }

    fn close(mut self) -> io::Result<()> {
        self.buffer.resize(NUM_DOCS_PER_BLOCK, 0u32);
        self.write_block()?;
        self.write.flush()
    }
}

impl PostingsSerializer {
    /// Open a new `PostingsSerializer` for the given segment
    pub fn new(terms_write: WritePtr,
               postings_write: WritePtr,
               positions_write: WritePtr,
               schema: Schema)
               -> Result<PostingsSerializer> {
        let terms_fst_builder = TermDictionaryBuilderImpl::new(terms_write)?;
        Ok(PostingsSerializer {
               terms_fst_builder: terms_fst_builder,
               postings_write: CountingWriter::wrap(postings_write),
               positions_writer: PositionWriter::new(positions_write),
               last_doc_id_encoded: 0u32,
               block_encoder: BlockEncoder::new(),
               doc_ids: Vec::new(),
               term_freqs: Vec::new(),
               schema: schema,
               text_indexing_options: TextIndexingOptions::Unindexed,
               term_open: false,
               current_term_info: TermInfo::default(),
           })
    }


    /// Open a new `PostingsSerializer` for the given segment
    pub fn open(segment: &mut Segment) -> Result<PostingsSerializer> {
        use SegmentComponent::{TERMS, POSTINGS, POSITIONS};
        PostingsSerializer::new(segment.open_write(TERMS)?,
                                segment.open_write(POSTINGS)?,
                                segment.open_write(POSITIONS)?,
                                segment.schema())
    }

    /// Must be called before starting pushing terms of
    /// a given field.
    ///
    /// Loads the indexing options for the given field.
    pub fn new_field(&mut self, field: Field) {
        let field_entry: &FieldEntry = self.schema.get_field_entry(field);
        self.text_indexing_options = match *field_entry.field_type() {
            FieldType::Str(ref text_options) => text_options.get_indexing_options(),
            FieldType::U64(ref int_options) |
            FieldType::I64(ref int_options) => {
                if int_options.is_indexed() {
                    TextIndexingOptions::Unindexed
                } else {
                    TextIndexingOptions::Untokenized
                }
            }
        };
    }

    /// Starts the postings for a new term.
    /// * term - the term. It needs to come after the previous term according
    ///   to the lexicographical order.
    /// * doc_freq - return the number of document containing the term.
    pub fn new_term(&mut self, term: &[u8]) -> io::Result<()> {
        if self.term_open {
            panic!("Called new_term, while the previous term was not closed.");
        }
        self.term_open = true;
        self.doc_ids.clear();
        self.last_doc_id_encoded = 0;
        self.term_freqs.clear();
        let (filepos, offset) = self.positions_writer.addr();
        self.current_term_info = TermInfo {
            doc_freq: 0,
            postings_offset: self.postings_write.written_bytes() as u32,
            positions_offset: filepos,
            positions_inner_offset: offset,
        };
        self.terms_fst_builder.insert_key(term)
    }

    /// Finish the serialization for this term postings.
    ///
    /// If the current block is incomplete, it need to be encoded
    /// using `VInt` encoding.
    pub fn close_term(&mut self) -> io::Result<()> {
        if self.term_open {

            self.terms_fst_builder
                .insert_value(&self.current_term_info)?;

            if !self.doc_ids.is_empty() {
                // we have doc ids waiting to be written
                // this happens when the number of doc ids is
                // not a perfect multiple of our block size.
                //
                // In that case, the remaining part is encoded
                // using variable int encoding.
                {
                    let block_encoded =
                        self.block_encoder
                            .compress_vint_sorted(&self.doc_ids, self.last_doc_id_encoded);
                    self.postings_write.write_all(block_encoded)?;
                    self.doc_ids.clear();
                }
                // ... Idem for term frequencies
                if self.text_indexing_options.is_termfreq_enabled() {
                    let block_encoded = self.block_encoder
                        .compress_vint_unsorted(&self.term_freqs[..]);
                    self.postings_write.write_all(block_encoded)?;
                    self.term_freqs.clear();
                }
            }
            self.term_open = false;
        }
        Ok(())
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
    pub fn write_doc(&mut self,
                     doc_id: DocId,
                     term_freq: u32,
                     position_deltas: &[u32])
                     -> io::Result<()> {
        self.current_term_info.doc_freq += 1;
        self.doc_ids.push(doc_id);
        if self.text_indexing_options.is_termfreq_enabled() {
            self.term_freqs.push(term_freq as u32);
        }
        if self.text_indexing_options.is_position_enabled() {
            self.positions_writer.write(position_deltas)?;
        }
        if self.doc_ids.len() == NUM_DOCS_PER_BLOCK {
            {
                // encode the doc ids
                let block_encoded: &[u8] =
                    self.block_encoder
                        .compress_block_sorted(&self.doc_ids, self.last_doc_id_encoded);
                self.last_doc_id_encoded = self.doc_ids[self.doc_ids.len() - 1];
                self.postings_write.write_all(block_encoded)?;
            }
            if self.text_indexing_options.is_termfreq_enabled() {
                // encode the term_freqs
                let block_encoded: &[u8] = self.block_encoder
                    .compress_block_unsorted(&self.term_freqs);
                self.postings_write.write_all(block_encoded)?;
                self.term_freqs.clear();
            }
            self.doc_ids.clear();
        }
        Ok(())
    }

    /// Closes the serializer.
    pub fn close(mut self) -> io::Result<()> {
        self.close_term()?;
        self.terms_fst_builder.finish()?;
        self.postings_write.flush()?;
        self.positions_writer.close()?;
        Ok(())
    }
}
