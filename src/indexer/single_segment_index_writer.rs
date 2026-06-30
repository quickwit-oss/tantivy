use std::marker::PhantomData;

use crate::indexer::operation::AddOperation;
use crate::indexer::segment_updater::save_metas;
use crate::indexer::{DocIdMapping, SegmentWriter};
use crate::schema::document::Document;
use crate::{Directory, Index, IndexMeta, IndexSettings, Opstamp, Segment, TantivyDocument};

#[doc(hidden)]
pub struct SingleSegmentIndexWriter<D: Document = TantivyDocument> {
    segment_writer: SegmentWriter,
    segment: Segment,
    opstamp: Opstamp,
    _phantom: PhantomData<D>,
}

impl<D: Document> SingleSegmentIndexWriter<D> {
    pub fn new(index: Index, mem_budget: usize) -> crate::Result<Self> {
        let segment = index.new_segment();
        let segment_writer = SegmentWriter::for_segment(mem_budget, segment.clone())?;
        Ok(Self {
            segment_writer,
            segment,
            opstamp: 0,
            _phantom: PhantomData,
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

    pub fn finalize(self) -> crate::Result<Index> {
        let Self {
            segment,
            segment_writer,
            ..
        } = self;
        let max_doc = segment_writer.max_doc();
        segment_writer.finalize()?;
        let remapping_required = segment.index().settings().sort_by_field.is_some();
        let index_settings = segment.index().settings().clone();
        Self::finalize_inner(segment, max_doc, remapping_required, index_settings)
    }

    pub fn finalize_with_doc_id_mapping(self, mapping: &DocIdMapping) -> crate::Result<Index> {
        let Self {
            segment,
            segment_writer,
            ..
        } = self;
        let max_doc = segment_writer.max_doc();
        segment_writer.finalize_with_doc_id_mapping(mapping)?;
        let mut index_settings = segment.index().settings().clone();
        index_settings.manual_doc_id_mapping = false;
        let mut index = Self::finalize_inner(segment, max_doc, true, index_settings)?;
        index.settings_mut().manual_doc_id_mapping = false;
        Ok(index)
    }

    fn finalize_inner(
        segment: Segment,
        max_doc: u32,
        remapping_required: bool,
        index_settings: IndexSettings,
    ) -> crate::Result<Index> {
        let segment: Segment = segment.with_max_doc(max_doc);
        if remapping_required {
            segment.meta().untrack_temp_docstore();
        }
        let index = segment.index();
        let index_meta = IndexMeta {
            index_settings,
            segments: vec![segment.meta().clone()],
            schema: index.schema(),
            opstamp: 0,
            payload: None,
        };
        save_metas(&index_meta, index.directory())?;
        index.directory().sync_directory()?;
        Ok(index.clone())
    }
}
