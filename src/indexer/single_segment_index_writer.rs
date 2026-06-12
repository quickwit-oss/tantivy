use std::any::Any;
use std::marker::PhantomData;

use crate::codec::StandardCodec;
use crate::index::CodecConfiguration;
use crate::indexer::operation::AddOperation;
use crate::indexer::segment_updater::save_metas;
use crate::indexer::SegmentWriter;
use crate::schema::document::Document;
use crate::schema::{Field, Schema};
use crate::{Directory, Index, IndexMeta, Opstamp, Segment, TantivyDocument};

#[doc(hidden)]
pub struct SingleSegmentIndexWriter<
    Codec: crate::codec::Codec = StandardCodec,
    D: Document = TantivyDocument,
> {
    pub segment_writer: SegmentWriter<Codec>,
    segment: Segment<Codec>,
    opstamp: Opstamp,
    _doc: PhantomData<D>,
}

impl<Codec: crate::codec::Codec, D: Document> SingleSegmentIndexWriter<Codec, D> {
    pub fn new(index: Index<Codec>, mem_budget: usize) -> crate::Result<Self> {
        let segment = index.new_segment();
        let segment_writer = SegmentWriter::for_segment(mem_budget, segment.clone())?;
        Ok(Self {
            segment_writer,
            segment,
            opstamp: 0,
            _doc: PhantomData,
        })
    }

    pub fn mem_usage(&self) -> usize {
        self.segment_writer.mem_usage()
    }

    pub fn add_document(&mut self, document: D) -> crate::Result<()> {
        let opstamp = self.opstamp;
        self.opstamp += 1;
        self.segment_writer
            .add_document(AddOperation { opstamp, document })
    }

    pub fn schema(&self) -> Schema {
        self.segment.schema()
    }

    /// Attaches or updates a codec-specific payload on a term of a regular
    /// (non-JSON) field.
    ///
    /// `value_bytes` is the serialized term value, i.e. exactly what would be
    /// appended after the field id (the raw text bytes for a str field, or the
    /// big-endian bytes for a numeric field).
    ///
    /// The term does not need to belong to any document: if it does not exist
    /// yet, it is created with an empty recorder so it still gets serialized.
    /// `updater` receives the previously registered payload (`None` if absent)
    /// and returns the payload to store. The payload is handed to the codec at
    /// the beginning of the term during serialization.
    pub fn update_term_payload(
        &mut self,
        field: Field,
        value_bytes: &[u8],
        updater: impl FnOnce(Option<Box<dyn Any + Send>>) -> Box<dyn Any + Send>,
    ) {
        self.segment_writer
            .update_term_payload(field, value_bytes, updater);
    }

    /// Same as [`Self::update_term_payload`] for a JSON field.
    ///
    /// `value_bytes` must be the type-tagged value (`[type code][value]`), the
    /// representation that follows the path within a JSON term.
    pub fn update_json_term_payload(
        &mut self,
        field: Field,
        json_path: &str,
        value_bytes: &[u8],
        updater: impl FnOnce(Option<Box<dyn Any + Send>>) -> Box<dyn Any + Send>,
    ) {
        self.segment_writer
            .update_json_term_payload(field, json_path, value_bytes, updater);
    }

    pub fn finalize(self) -> crate::Result<Index<Codec>> {
        let max_doc = self.segment_writer.max_doc();
        self.segment_writer.finalize()?;
        let segment: Segment<Codec> = self.segment.with_max_doc(max_doc);
        let index = segment.index();
        let index_meta = IndexMeta {
            index_settings: index.settings().clone(),
            segments: vec![segment.meta().clone()],
            schema: index.schema(),
            opstamp: 0,
            payload: None,
            codec: CodecConfiguration::from(index.codec()),
        };
        save_metas(&index_meta, index.directory())?;
        index.directory().sync_directory()?;
        Ok(segment.index().clone())
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::cell::RefCell;
    use std::io;

    use super::SingleSegmentIndexWriter;
    use crate::codec::positions::PositionsReader;
    use crate::codec::postings::{PostingsCodec, PostingsSerializer};
    use crate::codec::standard::positions::StandardPositionsCodec;
    use crate::codec::standard::postings::{
        SegmentPostings, StandardPostingsCodec, StandardPostingsSerializer,
    };
    use crate::codec::Codec;
    use crate::fieldnorm::FieldNormReader;
    use crate::schema::{Field, IndexRecordOption, Schema, Type, STRING};
    use crate::{DocId, Score, Term};

    // The codec is round-tripped through `from_json_props` when the index is
    // opened, so it cannot carry the capture sink itself. We use a thread-local
    // sink instead: the `SingleSegmentIndexWriter` is single-threaded, so
    // serialization runs on the test thread, and each test owns its own
    // thread-local (clear it at the start of the test).
    thread_local! {
        static CAPTURED_PAYLOADS: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
    }

    fn reset_captured() {
        CAPTURED_PAYLOADS.with(|captured| captured.borrow_mut().clear());
    }

    fn captured_payloads() -> Vec<u64> {
        CAPTURED_PAYLOADS.with(|captured| captured.borrow().clone())
    }

    /// A postings serializer that delegates to the standard one, but records
    /// the `u64` payload value of every term that carries a codec payload.
    struct CapturingPostingsSerializer {
        inner: StandardPostingsSerializer,
    }

    impl PostingsSerializer for CapturingPostingsSerializer {
        fn new_term(&mut self, term_doc_freq: u32, record_term_freq: bool) {
            self.inner.new_term(term_doc_freq, record_term_freq);
        }

        fn set_term_payload(&mut self, payload: &dyn Any) {
            let value = *payload
                .downcast_ref::<u64>()
                .expect("payload should be a u64");
            CAPTURED_PAYLOADS.with(|captured| captured.borrow_mut().push(value));
        }

        fn write_doc(&mut self, doc_id: DocId, term_freq: u32) {
            self.inner.write_doc(doc_id, term_freq);
        }

        fn close_term(&mut self, doc_freq: u32, wrt: &mut impl io::Write) -> io::Result<()> {
            self.inner.close_term(doc_freq, wrt)
        }
    }

    #[derive(Clone, Debug)]
    struct CapturingPostingsCodec;

    impl PostingsCodec for CapturingPostingsCodec {
        type PostingsSerializer = CapturingPostingsSerializer;
        type Postings = SegmentPostings;

        fn new_serializer(
            &self,
            avg_fieldnorm: Score,
            mode: IndexRecordOption,
            fieldnorm_reader: Option<FieldNormReader>,
        ) -> Self::PostingsSerializer {
            CapturingPostingsSerializer {
                inner: StandardPostingsCodec.new_serializer(avg_fieldnorm, mode, fieldnorm_reader),
            }
        }

        fn load_postings(
            &self,
            field: Field,
            doc_freq: u32,
            postings_data: common::OwnedBytes,
            record_option: IndexRecordOption,
            requested_option: IndexRecordOption,
            position_reader: Option<Box<dyn PositionsReader>>,
        ) -> io::Result<Self::Postings> {
            StandardPostingsCodec.load_postings(
                field,
                doc_freq,
                postings_data,
                record_option,
                requested_option,
                position_reader,
            )
        }
    }

    #[derive(Clone, Debug, Default)]
    struct CapturingCodec;

    impl Codec for CapturingCodec {
        type PostingsCodec = CapturingPostingsCodec;
        type PositionsCodec = StandardPositionsCodec;

        const ID: &'static str = "test-capturing-codec";

        fn from_json_props(_json_value: &serde_json::Value) -> crate::Result<Self> {
            Ok(CapturingCodec)
        }

        fn to_json_props(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        fn postings_codec(&self) -> &Self::PostingsCodec {
            &CapturingPostingsCodec
        }

        fn positions_codec(&self) -> &Self::PositionsCodec {
            &StandardPositionsCodec
        }
    }

    fn build_writer(schema: Schema) -> SingleSegmentIndexWriter<CapturingCodec> {
        let index = crate::IndexBuilder::default()
            .codec(CapturingCodec)
            .schema(schema)
            .create_in_ram()
            .unwrap();
        SingleSegmentIndexWriter::new(index, 15_000_000).unwrap()
    }

    #[test]
    fn test_update_term_payload_regular_field() {
        reset_captured();
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", STRING);
        let schema = schema_builder.build();
        let mut writer = build_writer(schema);

        writer.add_document(crate::doc!(text => "alpha")).unwrap();
        writer.add_document(crate::doc!(text => "beta")).unwrap();
        writer.add_document(crate::doc!(text => "gamma")).unwrap();

        // Existing term that belongs to a document.
        writer.update_term_payload(text, b"beta", |previous_payload| {
            assert!(previous_payload.is_none());
            Box::new(100u64)
        });
        // Updating the same term: the previous payload is handed back.
        writer.update_term_payload(text, b"beta", |previous_payload| {
            let previous = previous_payload.expect("expected the previous payload");
            assert_eq!(*previous.downcast::<u64>().unwrap(), 100u64);
            Box::new(101u64)
        });
        // Brand-new term that belongs to no document: an empty recorder is
        // created so it still lands in the term dictionary.
        writer.update_term_payload(text, b"zeta", |previous_payload| {
            assert!(previous_payload.is_none());
            Box::new(200u64)
        });

        let index = writer.finalize().unwrap();

        // Terms are serialized in sorted order: alpha, beta, gamma, zeta.
        // Only beta and zeta carry a payload.
        assert_eq!(captured_payloads(), vec![101u64, 200u64]);

        let searcher = index.reader().unwrap().searcher();
        let segment_reader = searcher.segment_reader(0);
        let inverted_index = segment_reader.inverted_index(text).unwrap();

        let beta_info = inverted_index
            .get_term_info(&Term::from_field_text(text, "beta"))
            .unwrap()
            .expect("beta should be in the dictionary");
        assert_eq!(beta_info.doc_freq, 1);

        let zeta_info = inverted_index
            .get_term_info(&Term::from_field_text(text, "zeta"))
            .unwrap()
            .expect("zeta (no document) should still be in the dictionary");
        assert_eq!(zeta_info.doc_freq, 0);
    }

    #[test]
    fn test_update_json_term_payload() {
        reset_captured();
        let mut schema_builder = Schema::builder();
        let json_field = schema_builder.add_json_field("json", STRING);
        let schema = schema_builder.build();
        let mut writer = build_writer(schema);

        writer
            .add_document(crate::doc!(json_field => serde_json::json!({"name": "hello"})))
            .unwrap();

        let str_value = |value: &str| {
            let mut bytes = vec![Type::Str.to_code()];
            bytes.extend_from_slice(value.as_bytes());
            bytes
        };

        // Existing str JSON term (path "name", value "hello").
        writer.update_json_term_payload(json_field, "name", &str_value("hello"), |previous| {
            assert!(previous.is_none());
            Box::new(1u64)
        });
        // Brand-new str JSON term with no document.
        writer.update_json_term_payload(json_field, "name", &str_value("world"), |previous| {
            assert!(previous.is_none());
            Box::new(2u64)
        });
        // Brand-new non-str (numeric) JSON term with no document: exercises the
        // DocIdRecorder branch of `ensure_term`.
        let numeric_value = {
            let mut bytes = vec![Type::I64.to_code()];
            bytes.extend_from_slice(&[0u8; 8]);
            bytes
        };
        writer.update_json_term_payload(json_field, "count", &numeric_value, |previous| {
            assert!(previous.is_none());
            Box::new(3u64)
        });

        // Should not panic and should serialize cleanly.
        let _index = writer.finalize().unwrap();

        let mut got = captured_payloads();
        got.sort_unstable();
        assert_eq!(got, vec![1u64, 2u64, 3u64]);
    }
}
