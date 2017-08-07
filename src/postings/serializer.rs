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
pub struct InvertedIndexSerializer {
    terms_fst_builder: TermDictionaryBuilderImpl<WritePtr, TermInfo>,
    postings_serializer: PostingsSerializer,
    positions_serializer: PositionSerializer,
    schema: Schema,

    term_open: bool,
    text_indexing_options: TextIndexingOptions,

    current_term_info: TermInfo,

}

struct PostingsSerializer {
    postings_write: CountingWriter<WritePtr>,
    last_doc_id_encoded: u32,

    block_encoder: BlockEncoder,
    doc_ids: Vec<DocId>,
    term_freqs: Vec<u32>,

    termfreq_enabled: bool,
}

impl PostingsSerializer {
    fn new(write: WritePtr) -> PostingsSerializer {
        PostingsSerializer {
            postings_write: CountingWriter::wrap(write),

            block_encoder: BlockEncoder::new(),
            doc_ids: vec!(),
            term_freqs: vec!(),

            last_doc_id_encoded: 0u32,
            termfreq_enabled: false,
        }
    }

    fn write_doc(&mut self, doc_id: DocId, term_freq: u32) -> io::Result<()> {
        self.doc_ids.push(doc_id);
        if self.termfreq_enabled {
            self.term_freqs.push(term_freq as u32);
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
            if self.termfreq_enabled {
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

    fn set_termfreq_enabled(&mut self, termfreq_enabled: bool) {
        self.termfreq_enabled = termfreq_enabled;
    }

    fn close_term(&mut self) -> io::Result<()> {
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


    fn addr(&self) -> u32 {
        self.postings_write.written_bytes() as u32
    }

    fn clear(&mut self) {
        self.doc_ids.clear();
        self.term_freqs.clear();
        self.last_doc_id_encoded = 0;
    }
}

struct PositionSerializer {
    buffer: Vec<u32>,
    write: CountingWriter<WritePtr>,
    block_encoder: BlockEncoder,
}

impl PositionSerializer {
    fn new(write: WritePtr) -> PositionSerializer {
        PositionSerializer {
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

impl InvertedIndexSerializer {
    /// Open a new `PostingsSerializer` for the given segment
    pub fn new(terms_write: WritePtr,
               postings_write: WritePtr,
               positions_write: WritePtr,
               schema: Schema)
               -> Result<InvertedIndexSerializer> {
        let terms_fst_builder = TermDictionaryBuilderImpl::new(terms_write)?;
        Ok(InvertedIndexSerializer {
            terms_fst_builder: terms_fst_builder,
            positions_serializer: PositionSerializer::new(positions_write),
            postings_serializer: PostingsSerializer::new(postings_write),
            schema: schema,
            term_open: false,
            current_term_info: TermInfo::default(),
            text_indexing_options: TextIndexingOptions::Untokenized,
        })
    }


    /// Open a new `PostingsSerializer` for the given segment
    pub fn open(segment: &mut Segment) -> Result<InvertedIndexSerializer> {
        use SegmentComponent::{TERMS, POSTINGS, POSITIONS};
        InvertedIndexSerializer::new(segment.open_write(TERMS)?,
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
        self.postings_serializer.set_termfreq_enabled(self.text_indexing_options.is_termfreq_enabled());
    }

    fn current_term_info(&self) -> TermInfo {
        let (filepos, offset) = self.positions_serializer.addr();
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
    pub fn new_term(&mut self, term: &[u8]) -> io::Result<()> {
        if self.term_open {
            panic!("Called new_term, while the previous term was not closed.");
        }
        self.term_open = true;
        self.postings_serializer.clear();
        self.current_term_info = self.current_term_info();
        self.terms_fst_builder.insert_key(term)
    }

    /// Finish the serialization for this term postings.
    ///
    /// If the current block is incomplete, it need to be encoded
    /// using `VInt` encoding.
    pub fn close_term(&mut self) -> io::Result<()> {
        if self.term_open {
            self.terms_fst_builder.insert_value(&self.current_term_info)?;
            self.postings_serializer.close_term()?;
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
        self.postings_serializer.write_doc(doc_id, term_freq)?;
        if self.text_indexing_options.is_position_enabled() {
            self.positions_serializer.write(position_deltas)?;
        }

        Ok(())
    }

    /// Closes the serializer.
    pub fn close(mut self) -> io::Result<()> {
        self.close_term()?;
        self.terms_fst_builder.finish()?;
        self.postings_serializer.close()?;
        self.positions_serializer.close()?;
        Ok(())
    }
}
