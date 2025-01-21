// src/postings/per_field_postings_writer.rs
use crate::postings::json_postings_writer::JsonPostingsWriter;
use crate::postings::postings_writer::SpecializedPostingsWriter;
use crate::postings::recorder::{DocIdRecorder, TermFrequencyRecorder, TfAndPositionRecorder};
use crate::postings::PostingsWriter;
use crate::schema::{Field, FieldEntry, FieldType, IndexRecordOption, Schema};

pub(crate) struct PerFieldPostingsWriter {
    per_field_postings_writers: Vec<Box<dyn PostingsWriter>>,
}

impl PerFieldPostingsWriter {
    pub fn for_schema(schema: &Schema) -> Self {
        let per_field_postings_writers = schema
            .fields()
            .map(|(_, field_entry)| posting_writer_from_field_entry(field_entry))
            .collect();
        PerFieldPostingsWriter {
            per_field_postings_writers,
        }
    }

    pub(crate) fn get_for_field(&self, field: Field) -> &dyn PostingsWriter {
        self.per_field_postings_writers[field.field_id() as usize].as_ref()
    }

    pub(crate) fn get_for_field_mut(&mut self, field: Field) -> &mut dyn PostingsWriter {
        self.per_field_postings_writers[field.field_id() as usize].as_mut()
    }
}

fn posting_writer_from_field_entry(field_entry: &FieldEntry) -> Box<dyn PostingsWriter> {
    match field_entry.field_type() {
        FieldType::Str(ref text_options) => {
            println!(
                "FieldType::Str - Processing text field with options: {:?}",
                text_options
            );
            text_options
                .get_indexing_options()
                .map(|indexing_options| match indexing_options.index_option() {
                    IndexRecordOption::Basic => {
                        println!("Creating SpecializedPostingsWriter<DocIdRecorder> for text field - basic indexing");
                        SpecializedPostingsWriter::<DocIdRecorder>::default().into()
                    }
                    IndexRecordOption::WithFreqs => {
                        println!("Creating SpecializedPostingsWriter<TermFrequencyRecorder> for text field - with frequencies");
                        SpecializedPostingsWriter::<TermFrequencyRecorder>::default().into()
                    }
                    IndexRecordOption::WithFreqsAndPositions => {
                        println!("Creating SpecializedPostingsWriter<TfAndPositionRecorder> for text field - with frequencies and positions");
                        SpecializedPostingsWriter::<TfAndPositionRecorder>::default().into()
                    }
                })
                .unwrap_or_else(|| {
                    println!("No indexing options specified for text field, defaulting to DocIdRecorder");
                    SpecializedPostingsWriter::<DocIdRecorder>::default().into()
                })
        }
        FieldType::U64(opts) => {
            println!(
                "FieldType::U64 - Creating default DocIdRecorder for u64 field with options: {:?}",
                opts
            );
            Box::<SpecializedPostingsWriter<DocIdRecorder>>::default()
        }
        FieldType::I64(opts) => {
            println!(
                "FieldType::I64 - Creating default DocIdRecorder for i64 field with options: {:?}",
                opts
            );
            Box::<SpecializedPostingsWriter<DocIdRecorder>>::default()
        }
        FieldType::F64(opts) => {
            println!(
                "FieldType::F64 - Creating default DocIdRecorder for f64 field with options: {:?}",
                opts
            );
            Box::<SpecializedPostingsWriter<DocIdRecorder>>::default()
        }
        FieldType::Bool(opts) => {
            println!("FieldType::Bool - Creating default DocIdRecorder for boolean field with options: {:?}", opts);
            Box::<SpecializedPostingsWriter<DocIdRecorder>>::default()
        }
        FieldType::Date(opts) => {
            println!("FieldType::Date - Creating default DocIdRecorder for date field with options: {:?}", opts);
            Box::<SpecializedPostingsWriter<DocIdRecorder>>::default()
        }
        FieldType::Bytes(opts) => {
            println!("FieldType::Bytes - Creating default DocIdRecorder for bytes field with options: {:?}", opts);
            Box::<SpecializedPostingsWriter<DocIdRecorder>>::default()
        }
        FieldType::IpAddr(opts) => {
            println!("FieldType::IpAddr - Creating default DocIdRecorder for IP address field with options: {:?}", opts);
            Box::<SpecializedPostingsWriter<DocIdRecorder>>::default()
        }
        FieldType::Facet(opts) => {
            println!("FieldType::Facet - Creating default DocIdRecorder for facet field with options: {:?}", opts);
            Box::<SpecializedPostingsWriter<DocIdRecorder>>::default()
        }
        FieldType::Nested(opts) => {
            println!("FieldType::Nested - Creating default DocIdRecorder for nested field with options: {:?}", opts);
            println!("Note: Nested fields currently only support basic doc ID recording");
            Box::<SpecializedPostingsWriter<DocIdRecorder>>::default()
        }
        FieldType::JsonObject(ref json_object_options) => {
            println!(
                "FieldType::JsonObject - Processing JSON field with options: {:?}",
                json_object_options
            );
            if let Some(text_indexing_option) = json_object_options.get_text_indexing_options() {
                match text_indexing_option.index_option() {
                    IndexRecordOption::Basic => {
                        println!("Creating JsonPostingsWriter<DocIdRecorder> for JSON field - basic indexing");
                        JsonPostingsWriter::<DocIdRecorder>::default().into()
                    }
                    IndexRecordOption::WithFreqs => {
                        println!("Creating JsonPostingsWriter<TermFrequencyRecorder> for JSON field - with frequencies");
                        JsonPostingsWriter::<TermFrequencyRecorder>::default().into()
                    }
                    IndexRecordOption::WithFreqsAndPositions => {
                        println!("Creating JsonPostingsWriter<TfAndPositionRecorder> for JSON field - with frequencies and positions");
                        JsonPostingsWriter::<TfAndPositionRecorder>::default().into()
                    }
                }
            } else {
                println!("No text indexing options specified for JSON field, defaulting to DocIdRecorder");
                JsonPostingsWriter::<DocIdRecorder>::default().into()
            }
        }
    }
}
