//! The fieldnorm represents the length associated to
//! a given Field of a given document.
//!
//! This metric is important to compute the score of a
//! document : a document having a query word in one its short fields
//! (e.g. title)  is likely to be more relevant than in one of its longer field
//! (e.g. body).
//!
//! It encodes `fieldnorm` on one byte with some precision loss,
//! using the exact same scheme as Lucene. Each value is place on a log-scale
//! that takes values from `0` to `255`.
//!
//! A value on this scale is identified by a `fieldnorm_id`.
//! Apart from compression, this scale also makes it possible to
//! precompute computationally expensive functions of the fieldnorm
//! in a very short array.
//!
//! This trick is used by the Bm25 similarity.
mod code;
mod reader;
mod serializer;
mod writer;

pub use self::reader::{FieldNormReader, FieldNormReaders};
pub use self::serializer::FieldNormsSerializer;
pub use self::writer::FieldNormsWriter;

use self::code::{fieldnorm_to_id, id_to_fieldnorm};

#[cfg(test)]
mod tests {
    use crate::directory::CompositeFile;
    use crate::directory::{Directory, RamDirectory, WritePtr};
    use crate::fieldnorm::FieldNormReader;
    use crate::fieldnorm::FieldNormsSerializer;
    use crate::fieldnorm::FieldNormsWriter;
    use crate::query::Query;
    use crate::query::TermQuery;
    use crate::schema::IndexRecordOption;
    use crate::schema::TextFieldIndexing;
    use crate::schema::TextOptions;
    use crate::schema::TEXT;
    use crate::Index;
    use crate::Term;
    use crate::TERMINATED;
    use once_cell::sync::Lazy;
    use std::path::Path;

    use crate::schema::{Field, Schema, STORED};

    pub static SCHEMA: Lazy<Schema> = Lazy::new(|| {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("field", STORED);
        schema_builder.add_text_field("txt_field", TEXT);
        schema_builder.add_text_field(
            "str_field",
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_index_option(IndexRecordOption::Basic)
                    .set_fieldnorms(false),
            ),
        );
        schema_builder.build()
    });

    pub static FIELD: Lazy<Field> = Lazy::new(|| SCHEMA.get_field("field").unwrap());
    pub static TXT_FIELD: Lazy<Field> = Lazy::new(|| SCHEMA.get_field("txt_field").unwrap());
    pub static STR_FIELD: Lazy<Field> = Lazy::new(|| SCHEMA.get_field("str_field").unwrap());

    #[test]
    #[should_panic(expected = "Cannot register a given fieldnorm twice")]
    pub fn test_should_panic_when_recording_fieldnorm_twice_for_same_doc() {
        let mut fieldnorm_writers = FieldNormsWriter::for_schema(&SCHEMA);
        fieldnorm_writers.record(0u32, *TXT_FIELD, 5);
        fieldnorm_writers.record(0u32, *TXT_FIELD, 3);
    }

    #[test]
    pub fn test_fieldnorm() -> crate::Result<()> {
        let path = Path::new("test");
        let directory: RamDirectory = RamDirectory::create();
        {
            let write: WritePtr = directory.open_write(Path::new("test"))?;
            let serializer = FieldNormsSerializer::from_write(write)?;
            let mut fieldnorm_writers = FieldNormsWriter::for_schema(&SCHEMA);
            fieldnorm_writers.record(2u32, *TXT_FIELD, 5);
            fieldnorm_writers.record(3u32, *TXT_FIELD, 3);
            fieldnorm_writers.serialize(serializer, None)?;
        }
        let file = directory.open_read(&path)?;
        {
            let fields_composite = CompositeFile::open(&file)?;
            assert!(fields_composite.open_read(*FIELD).is_none());
            assert!(fields_composite.open_read(*STR_FIELD).is_none());
            let data = fields_composite.open_read(*TXT_FIELD).unwrap();
            let fieldnorm_reader = FieldNormReader::open(data)?;
            assert_eq!(fieldnorm_reader.fieldnorm(0u32), 0u32);
            assert_eq!(fieldnorm_reader.fieldnorm(1u32), 0u32);
            assert_eq!(fieldnorm_reader.fieldnorm(2u32), 5u32);
            assert_eq!(fieldnorm_reader.fieldnorm(3u32), 3u32);
        }
        Ok(())
    }

    #[test]
    fn test_fieldnorm_disabled() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_options = TextOptions::default()
            .set_indexing_options(TextFieldIndexing::default().set_fieldnorms(false));
        let text = schema_builder.add_text_field("text", text_options);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(text=>"hello"))?;
        writer.add_document(doc!(text=>"hello hello hello"))?;
        writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query = TermQuery::new(
            Term::from_field_text(text, "hello"),
            IndexRecordOption::WithFreqs,
        );
        let weight = query.weight(&*searcher, true)?;
        let mut scorer = weight.scorer(searcher.segment_reader(0), 1.0f32)?;
        assert_eq!(scorer.doc(), 0);
        assert!((scorer.score() - 0.22920431).abs() < 0.001f32);
        assert_eq!(scorer.advance(), 1);
        assert_eq!(scorer.doc(), 1);
        assert!((scorer.score() - 0.22920431).abs() < 0.001f32);
        assert_eq!(scorer.advance(), TERMINATED);
        Ok(())
    }

    #[test]
    fn test_fieldnorm_enabled() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let text_options = TextOptions::default()
            .set_indexing_options(TextFieldIndexing::default().set_fieldnorms(true));
        let text = schema_builder.add_text_field("text", text_options);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_for_tests()?;
        writer.add_document(doc!(text=>"hello"))?;
        writer.add_document(doc!(text=>"hello hello hello"))?;
        writer.commit()?;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let query = TermQuery::new(
            Term::from_field_text(text, "hello"),
            IndexRecordOption::WithFreqs,
        );
        let weight = query.weight(&*searcher, true)?;
        let mut scorer = weight.scorer(searcher.segment_reader(0), 1.0f32)?;
        assert_eq!(scorer.doc(), 0);
        assert!((scorer.score() - 0.22920431).abs() < 0.001f32);
        assert_eq!(scorer.advance(), 1);
        assert_eq!(scorer.doc(), 1);
        assert!((scorer.score() - 0.15136132).abs() < 0.001f32);
        assert_eq!(scorer.advance(), TERMINATED);
        Ok(())
    }
}
