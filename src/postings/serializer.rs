use std::io::{self, Write};

use common::{BinarySerializable, CountingWriter};

use super::TermInfo;
use crate::codec::postings::{PostingsCodec, PostingsSerializer};
use crate::codec::Codec;
use crate::directory::{CompositeWrite, WritePtr};
use crate::fieldnorm::FieldNormReader;
use crate::index::Segment;
use crate::positions::PositionSerializer;
use crate::schema::{Field, FieldEntry, FieldType, IndexRecordOption, Schema};
use crate::termdict::TermDictionaryBuilder;
use crate::{DocId, Score};

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
/// Within a term, documents have to be pushed in increasing order.
///
/// A description of the serialization format is
/// [available here](https://fulmicoton.gitbooks.io/tantivy-doc/content/inverted-index.html).
pub struct InvertedIndexSerializer<C: Codec> {
    terms_write: CompositeWrite<WritePtr>,
    postings_write: CompositeWrite<WritePtr>,
    positions_write: CompositeWrite<WritePtr>,
    schema: Schema,
    codec: C,
}

impl<C: Codec> InvertedIndexSerializer<C> {
    /// Open a new `InvertedIndexSerializer` for the given segment
    pub fn open(segment: &mut Segment<C>) -> crate::Result<InvertedIndexSerializer<C>> {
        use crate::index::SegmentComponent::{Positions, Postings, Terms};
        let codec = segment.index().codec().clone();
        let inv_index_serializer = InvertedIndexSerializer {
            terms_write: CompositeWrite::wrap(segment.open_write(Terms)?),
            postings_write: CompositeWrite::wrap(segment.open_write(Postings)?),
            positions_write: CompositeWrite::wrap(segment.open_write(Positions)?),
            schema: segment.schema(),
            codec,
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
    ) -> io::Result<FieldSerializer<'_, C>> {
        let field_entry: &FieldEntry = self.schema.get_field_entry(field);
        let term_dictionary_write = self.terms_write.for_field(field);
        let postings_write = self.postings_write.for_field(field);
        let positions_write = self.positions_write.for_field(field);
        let field_type: FieldType = (*field_entry.field_type()).clone();
        FieldSerializer::create(
            &field_type,
            total_num_tokens,
            term_dictionary_write,
            postings_write,
            positions_write,
            fieldnorm_reader,
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
pub struct FieldSerializer<'a, C: Codec> {
    term_dictionary_builder: TermDictionaryBuilder<&'a mut CountingWriter<WritePtr>>,
    postings_serializer: <C::PostingsCodec as PostingsCodec>::PostingsSerializer,
    positions_serializer_opt: Option<PositionSerializer<&'a mut CountingWriter<WritePtr>>>,
    current_term_info: TermInfo,
    term_open: bool,
    postings_write: &'a mut CountingWriter<WritePtr>,
    postings_start_offset: u64,
}

impl<'a, C: Codec> FieldSerializer<'a, C> {
    fn create(
        field_type: &FieldType,
        total_num_tokens: u64,
        term_dictionary_write: &'a mut CountingWriter<WritePtr>,
        postings_write: &'a mut CountingWriter<WritePtr>,
        positions_write: &'a mut CountingWriter<WritePtr>,
        fieldnorm_reader: Option<FieldNormReader>,
    ) -> io::Result<FieldSerializer<'a, C>> {
        total_num_tokens.serialize(postings_write)?;
        let index_record_option = field_type
            .index_record_option()
            .unwrap_or(IndexRecordOption::Basic);
        let term_dictionary_builder = TermDictionaryBuilder::create(term_dictionary_write)?;
        let average_fieldnorm = fieldnorm_reader
            .as_ref()
            .map(|ff_reader| total_num_tokens as Score / ff_reader.num_docs() as Score)
            .unwrap_or(0.0);
        let postings_serializer =
            <<C::PostingsCodec as PostingsCodec>::PostingsSerializer as PostingsSerializer>::new(
                average_fieldnorm,
                index_record_option,
                fieldnorm_reader,
            );
        let positions_serializer_opt = if index_record_option.has_positions() {
            Some(PositionSerializer::new(positions_write))
        } else {
            None
        };

        let postings_start_offset = postings_write.written_bytes();
        Ok(FieldSerializer {
            term_dictionary_builder,
            postings_serializer,
            positions_serializer_opt,
            current_term_info: TermInfo::default(),
            term_open: false,
            postings_write,
            postings_start_offset,
        })
    }

    fn postings_offset(&self) -> usize {
        (self.postings_write.written_bytes() - self.postings_start_offset) as usize
    }

    fn current_term_info(&self) -> TermInfo {
        let positions_start =
            if let Some(positions_serializer) = self.positions_serializer_opt.as_ref() {
                positions_serializer.written_bytes()
            } else {
                0u64
            } as usize;
        let addr = self.postings_offset();
        TermInfo {
            doc_freq: 0,
            postings_range: addr..addr,
            positions_range: positions_start..positions_start,
        }
    }

    /// Starts the postings for a new term.
    /// * term - the term. It needs to come after the previous term according to the lexicographical
    ///   order.
    /// * term_doc_freq - return the number of document containing the term.
    pub fn new_term(
        &mut self,
        term: &[u8],
        term_doc_freq: u32,
        record_term_freq: bool,
    ) -> io::Result<()> {
        assert!(
            !self.term_open,
            "Called new_term, while the previous term was not closed."
        );
        self.term_open = true;
        self.postings_serializer.clear();
        self.current_term_info = self.current_term_info();
        self.term_dictionary_builder.insert_key(term)?;
        self.postings_serializer
            .new_term(term_doc_freq, record_term_freq);
        Ok(())
    }

    /// Serialize the information that a document contains for the current term:
    /// its term frequency, and the position deltas.
    ///
    /// At this point, the positions are already `delta-encoded`.
    /// For instance, if the positions are `2, 3, 17`,
    /// `position_deltas` is `2, 1, 14`
    ///
    /// Term frequencies and positions may be ignored by the serializer depending
    /// on the configuration of the field in the `Schema`.
    pub fn write_doc(&mut self, doc_id: DocId, term_freq: u32, position_deltas: &[u32]) {
        self.current_term_info.doc_freq += 1;
        self.postings_serializer.write_doc(doc_id, term_freq);
        if let Some(ref mut positions_serializer) = self.positions_serializer_opt.as_mut() {
            assert_eq!(term_freq as usize, position_deltas.len());
            positions_serializer.write_positions_delta(position_deltas);
        }
    }

    /// Finish the serialization for this term postings.
    ///
    /// If the current block is incomplete, it needs to be encoded
    /// using `VInt` encoding.
    pub fn close_term(&mut self) -> io::Result<()> {
        crate::fail_point!("FieldSerializer::close_term", |msg: Option<String>| {
            Err(io::Error::new(io::ErrorKind::Other, format!("{msg:?}")))
        });

        if !self.term_open {
            return Ok(());
        };

        self.postings_serializer
            .close_term(self.current_term_info.doc_freq, self.postings_write)?;
        self.current_term_info.postings_range.end = self.postings_offset();
        if let Some(positions_serializer) = self.positions_serializer_opt.as_mut() {
            positions_serializer.close_term()?;
            self.current_term_info.positions_range.end =
                positions_serializer.written_bytes() as usize;
        }
        self.term_dictionary_builder
            .insert_value(&self.current_term_info)?;
        self.term_open = false;
        Ok(())
    }

    /// Closes the current field.
    pub fn close(mut self) -> io::Result<()> {
        self.close_term()?;
        if let Some(positions_serializer) = self.positions_serializer_opt {
            positions_serializer.close()?;
        }
        self.postings_write.flush()?;
        self.term_dictionary_builder.finish()?;
        Ok(())
    }
}
