//! FastFields as a [`SegmentPlugin`] implementation.
//!
//! This wraps the existing `FastFieldsWriter` and `FastFieldReaders` types behind
//! the plugin interface so that fast fields participate in the unified plugin lifecycle.

use std::any::Any;
use std::collections::BTreeMap;

use columnar::{
    ColumnType, ColumnarReader, MergeRowOrder, RowAddr, ShuffleMergeOrder, StackMergeOrder,
};
use common::TerminatingWrite;
use measure_time::debug_time;

use crate::directory::{Directory, WritePtr};
use crate::fastfield::{FastFieldReaders, FastFieldsWriter};
use crate::index::{SegmentComponent, SegmentReader};
use crate::indexer::doc_id_mapping::{DocIdMapping, MappingType, SegmentDocIdMapping};
use crate::plugin::{PluginMergeContext, PluginWriter, PluginWriterContext, SegmentPlugin};
use crate::schema::document::Document;
use crate::schema::{value_type_to_column_type, Schema};
use crate::space_usage::{ComponentSpaceUsage, FAST_FIELDS};
use crate::Segment;

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

        columnar::merge_columnar(
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

pub struct FastFieldsPluginWriter {
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

    pub fn writer_mut(&mut self) -> &mut FastFieldsWriter {
        &mut self.writer
    }

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

fn extract_fast_field_required_columns(schema: &Schema) -> Vec<(String, ColumnType)> {
    schema
        .fields()
        .map(|(_, field_entry)| field_entry)
        .filter(|field_entry| field_entry.is_fast())
        .filter_map(|field_entry| {
            let column_name = field_entry.name().to_string();
            let column_type = value_type_to_column_type(field_entry.field_type().value_type())?;
            Some((column_name, column_type))
        })
        .collect()
}
