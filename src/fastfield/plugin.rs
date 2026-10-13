//! FastFields as a [`SegmentPlugin`] implementation.
//!
//! This wraps the existing `FastFieldsWriter` and `FastFieldReaders` types behind
//! the plugin interface so that fast fields participate in the unified plugin lifecycle.

use std::any::Any;
use std::collections::BTreeMap;

use columnar::{
    ColumnType, ColumnarReader, MergeRowOrder, RowAddr, ShuffleMergeOrder, StackMergeOrder,
    StrColumnCodec,
};
use common::TerminatingWrite;
use measure_time::debug_time;

use crate::directory::{Directory, WritePtr};
use crate::fastfield::{FastFieldReaders, FastFieldsWriter};
use crate::index::{SegmentComponent, SegmentReader};
use crate::indexer::doc_id_mapping::{DocIdMapping, MappingType, SegmentDocIdMapping};
use crate::plugin::{PluginMergeContext, PluginWriter, PluginWriterContext, SegmentPlugin};
use crate::schema::document::Document;
use crate::schema::{value_type_to_column_type, FieldType, Schema};
use crate::space_usage::{ComponentSpaceUsage, FAST_FIELDS};
use crate::Segment;

/// Built-in segment plugin that stores and merges fast-field columns.
pub struct FastFieldsPlugin;

impl SegmentPlugin for FastFieldsPlugin {
    fn extensions(&self) -> &[&str] {
        &["fast"]
    }

    fn create_writer(&self, _ctx: &PluginWriterContext) -> crate::Result<Box<dyn PluginWriter>> {
        unimplemented!("FastFieldsPlugin is a built-in; use FastFieldsPluginWriter::new")
    }

    fn merge(&self, ctx: PluginMergeContext) -> crate::Result<()> {
        debug_time!("write-fast-fields");
        let path = ctx
            .target_segment
            .relative_path(SegmentComponent::FastFields);
        let mut fast_field_wrt: WritePtr =
            ctx.target_segment.index().directory().open_write(&path)?;

        let required_columns = extract_fast_field_required_columns(ctx.schema);
        let columnars: Vec<&ColumnarReader> = ctx
            .readers
            .iter()
            .map(|reader| reader.fast_fields().columnar())
            .collect();

        // Clone the doc_id_mapping since convert_to_merge_order consumes it by value
        let doc_id_mapping = ctx.doc_id_mapping.clone();
        let merge_row_order = convert_to_merge_order(&columnars[..], doc_id_mapping);

        columnar::merge_columnar_with_codecs(
            &columnars[..],
            &required_columns,
            merge_row_order,
            &mut fast_field_wrt,
        )?;

        fast_field_wrt.terminate()?;
        Ok(())
    }

    fn space_usage(
        &self,
        segment_reader: &SegmentReader,
    ) -> crate::Result<BTreeMap<String, ComponentSpaceUsage>> {
        let file = segment_reader.open_read(SegmentComponent::FastFields)?;
        let readers = FastFieldReaders::open(file, segment_reader.schema().clone())
            .map_err(|e| crate::TantivyError::InternalError(e.to_string()))?;
        let usage = readers.space_usage()?;
        Ok(BTreeMap::from([(
            FAST_FIELDS.to_string(),
            ComponentSpaceUsage::PerField(usage),
        )]))
    }
}

/// Accumulates and serializes fast-field values for a segment.
pub struct FastFieldsPluginWriter {
    /// Underlying fast-field writer.
    pub writer: FastFieldsWriter,
    fast_field_write: WritePtr,
}

impl FastFieldsPluginWriter {
    pub(crate) fn new(ctx: &PluginWriterContext) -> crate::Result<Self> {
        let index = ctx.segment.index();
        let tokenizer_manager = index.fast_field_tokenizer().clone();
        let writer = FastFieldsWriter::from_schema_and_tokenizer_manager(
            &ctx.segment.schema(),
            tokenizer_manager,
        )?;

        let path = ctx.segment.relative_path(SegmentComponent::FastFields);
        let fast_field_write = index.directory().open_write(&path)?;

        Ok(FastFieldsPluginWriter {
            writer,
            fast_field_write,
        })
    }

    /// Generic, zero-copy document ingestion. Called directly by the `SegmentWriter` for
    /// this built-in, so a custom `Document` is indexed in place without materializing a
    /// `TantivyDocument`.
    pub(crate) fn index_document<D: Document>(&mut self, doc: &D) -> crate::Result<()> {
        self.writer.add_document(doc)
    }

    /// Returns mutable access to the underlying fast-field writer.
    pub fn writer_mut(&mut self) -> &mut FastFieldsWriter {
        &mut self.writer
    }

    /// Returns the underlying fast-field writer.
    pub fn writer(&self) -> &FastFieldsWriter {
        &self.writer
    }
}

impl PluginWriter for FastFieldsPluginWriter {
    fn serialize(
        mut self: Box<Self>,
        _segment: &Segment,
        doc_id_map: Option<&DocIdMapping>,
    ) -> crate::Result<()> {
        self.writer
            .serialize(&mut self.fast_field_write, doc_id_map)
            .map_err(|e| crate::TantivyError::InternalError(e.to_string()))?;
        self.fast_field_write.terminate()?;
        Ok(())
    }

    fn mem_usage(&self) -> usize {
        self.writer.mem_usage()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// --- Helper functions moved from merger.rs ---

fn convert_to_merge_order(
    columnars: &[&ColumnarReader],
    doc_id_mapping: SegmentDocIdMapping,
) -> MergeRowOrder {
    match doc_id_mapping.mapping_type() {
        MappingType::Stacked => MergeRowOrder::Stack(StackMergeOrder::stack(columnars)),
        MappingType::StackedWithDeletes | MappingType::Shuffled => {
            let new_row_id_to_old_row_id: Vec<RowAddr> = doc_id_mapping
                .new_doc_id_to_old_doc_addr
                .into_iter()
                .map(|doc_addr| RowAddr {
                    segment_ord: doc_addr.segment_ord,
                    row_id: doc_addr.doc_id,
                })
                .collect();
            MergeRowOrder::Shuffled(ShuffleMergeOrder {
                new_row_id_to_old_row_id,
                alive_bitsets: doc_id_mapping.alive_bitsets,
            })
        }
    }
}

/// Returns the columns of the fast fields of the schema, with the codec of their string values.
fn extract_fast_field_required_columns(
    schema: &Schema,
) -> Vec<(String, ColumnType, StrColumnCodec)> {
    schema
        .fields()
        .map(|(_, field_entry)| field_entry)
        .filter(|field_entry| field_entry.is_fast())
        .filter_map(|field_entry| {
            let column_name = field_entry.name().to_string();
            let column_type = value_type_to_column_type(field_entry.field_type().value_type())?;
            let codec = match field_entry.field_type() {
                FieldType::Str(text_options) => text_options
                    .get_fast_field_codec()
                    .unwrap_or(StrColumnCodec::Dictionary),
                _ => StrColumnCodec::Dictionary,
            };
            Some((column_name, column_type, codec))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use columnar::{ColumnType, StrColumnCodec};

    use crate::index::SegmentId;
    use crate::schema::{Schema, TextOptions};
    use crate::{Index, IndexWriter};

    #[test]
    fn test_merge_plain_text_fast_field() {
        let mut schema_builder = Schema::builder();
        let plain_text_options = TextOptions::default().set_fast_codec(StrColumnCodec::Plain);
        let text = schema_builder.add_text_field("text", plain_text_options.clone());
        // Never has a value: the merged column only exists because it is required.
        let _empty = schema_builder.add_text_field("empty", plain_text_options);
        let index = Index::create_in_ram(schema_builder.build());
        let mut index_writer: IndexWriter = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(text => "a")).unwrap();
        index_writer.add_document(doc!()).unwrap();
        index_writer.commit().unwrap();
        index_writer
            .add_document(doc!(text => "b", text => "c"))
            .unwrap();
        index_writer.commit().unwrap();
        let segment_ids: Vec<SegmentId> = index.searchable_segment_ids().unwrap();
        assert_eq!(segment_ids.len(), 2);
        index_writer.merge(&segment_ids).wait().unwrap();
        index_writer.wait_merging_threads().unwrap();

        let searcher = index.reader().unwrap().searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let fast_fields = searcher.segment_reader(0u32).fast_fields();
        let text_column = fast_fields.plain_str("text").unwrap().unwrap();
        let mut reader = text_column.reader();
        let mut values_per_doc: Vec<Vec<String>> = Vec::new();
        for doc in 0..3u32 {
            let mut doc_values: Vec<String> = Vec::new();
            for value_idx in text_column.column_index().value_row_ids(doc) {
                let mut value = String::new();
                reader.append_str_value(value_idx, &mut value).unwrap();
                doc_values.push(value);
            }
            values_per_doc.push(doc_values);
        }
        assert_eq!(
            values_per_doc,
            vec![
                vec!["a".to_string()],
                vec![],
                vec!["b".to_string(), "c".to_string()]
            ]
        );
        let empty_handle = fast_fields
            .dynamic_column_handle("empty", ColumnType::Str)
            .unwrap()
            .unwrap();
        assert_eq!(empty_handle.str_codec(), Some(StrColumnCodec::Plain));
    }
}
