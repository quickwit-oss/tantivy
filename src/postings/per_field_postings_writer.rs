use crate::postings::postings_writer::SpecializedPostingsWriter;
use crate::postings::recorder::{NothingRecorder, TermFrequencyRecorder, TfAndPositionRecorder};
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
    match *field_entry.field_type() {
        FieldType::Str(ref text_options) => text_options
            .get_indexing_options()
            .map(|indexing_options| match indexing_options.index_option() {
                IndexRecordOption::Basic => {
                    SpecializedPostingsWriter::<NothingRecorder>::new_boxed()
                }
                IndexRecordOption::WithFreqs => {
                    SpecializedPostingsWriter::<TermFrequencyRecorder>::new_boxed()
                }
                IndexRecordOption::WithFreqsAndPositions => {
                    SpecializedPostingsWriter::<TfAndPositionRecorder>::new_boxed()
                }
            })
            .unwrap_or_else(SpecializedPostingsWriter::<NothingRecorder>::new_boxed),
        FieldType::U64(_)
        | FieldType::I64(_)
        | FieldType::F64(_)
        | FieldType::Date(_)
        | FieldType::Bytes(_)
        | FieldType::Facet(_) => SpecializedPostingsWriter::<NothingRecorder>::new_boxed(),
    }
}
