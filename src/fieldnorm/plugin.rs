//! FieldNorms as a [`SegmentPlugin`] implementation.
//!
//! This wraps the existing `FieldNormsWriter`, `FieldNormsSerializer`, and
//! `FieldNormReaders` types behind the plugin interface so that fieldnorms
//! participate in the unified plugin lifecycle.

use std::any::Any;
use std::collections::BTreeMap;

use crate::directory::Directory;
use crate::fieldnorm::{FieldNormReader, FieldNormReaders, FieldNormsSerializer, FieldNormsWriter};
use crate::index::{SegmentComponent, SegmentReader};
use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::plugin::{PluginMergeContext, PluginWriter, PluginWriterContext, SegmentPlugin};
use crate::space_usage::{ComponentSpaceUsage, FIELDNORMS};
use crate::{DocId, Segment};

pub struct FieldNormsPlugin;

impl SegmentPlugin for FieldNormsPlugin {
    fn extensions(&self) -> &[&str] {
        &["fieldnorm"]
    }

    fn create_writer(&self, ctx: &PluginWriterContext) -> crate::Result<Box<dyn PluginWriter>> {
        let writer = FieldNormsWriter::for_schema(&ctx.segment.schema());
        let path = ctx.segment.relative_path(SegmentComponent::FieldNorms);
        let write = ctx.segment.index().directory().open_write(&path)?;
        let serializer = Some(FieldNormsSerializer::from_write(write)?);
        Ok(Box::new(FieldNormsPluginWriter { writer, serializer }))
    }

    fn merge(&self, ctx: PluginMergeContext) -> crate::Result<()> {
        let path = ctx
            .target_segment
            .relative_path(SegmentComponent::FieldNorms);
        let write = ctx.target_segment.index().directory().open_write(&path)?;
        let mut serializer = FieldNormsSerializer::from_write(write)?;

        let schema = ctx.schema;
        let fields = FieldNormsWriter::fields_with_fieldnorm(schema);
        let max_doc: usize = ctx.readers.iter().map(|r| r.num_docs() as usize).sum();
        let mut fieldnorms_data = Vec::with_capacity(max_doc);

        for field in fields {
            if ctx.cancel.wants_cancel() {
                return Err(crate::TantivyError::Cancelled);
            }
            fieldnorms_data.clear();
            let fieldnorms_readers: Vec<FieldNormReader> = ctx
                .readers
                .iter()
                .map(|reader| reader.get_fieldnorms_reader(field))
                .collect::<Result<_, _>>()?;
            for old_doc_addr in ctx.doc_id_mapping.iter_old_doc_addrs() {
                let reader = &fieldnorms_readers[old_doc_addr.segment_ord as usize];
                let fieldnorm_id = reader.fieldnorm_id(old_doc_addr.doc_id);
                fieldnorms_data.push(fieldnorm_id);
            }
            serializer.serialize_field(field, &fieldnorms_data)?;
        }
        serializer.close()?;
        Ok(())
    }

    fn space_usage(
        &self,
        segment_reader: &SegmentReader,
    ) -> crate::Result<BTreeMap<String, ComponentSpaceUsage>> {
        let file = segment_reader.open_read(SegmentComponent::FieldNorms)?;
        let readers = FieldNormReaders::open(file)?;
        let usage = readers.space_usage(segment_reader.schema());
        Ok(BTreeMap::from([(
            FIELDNORMS.to_string(),
            ComponentSpaceUsage::PerField(usage),
        )]))
    }
}

pub struct FieldNormsPluginWriter {
    pub writer: FieldNormsWriter,
    serializer: Option<FieldNormsSerializer>,
}

impl FieldNormsPluginWriter {
    pub fn record(&mut self, doc: DocId, field: crate::schema::Field, fieldnorm: u32) {
        self.writer.record(doc, field, fieldnorm);
    }

    pub fn fill_up_to_max_doc(&mut self, max_doc: DocId) {
        self.writer.fill_up_to_max_doc(max_doc);
    }
}

impl PluginWriter for FieldNormsPluginWriter {
    fn serialize(
        &mut self,
        _segment: &Segment,
        doc_id_map: Option<&DocIdMapping>,
    ) -> crate::Result<()> {
        if let Some(serializer) = self.serializer.take() {
            self.writer
                .serialize(serializer, doc_id_map)
                .map_err(|e| crate::TantivyError::InternalError(e.to_string()))?;
        }
        Ok(())
    }

    fn close(self: Box<Self>) -> crate::Result<()> {
        // If serializer wasn't consumed by serialize(), close it now.
        if let Some(serializer) = self.serializer {
            serializer
                .close()
                .map_err(|e| crate::TantivyError::InternalError(e.to_string()))?;
        }
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
