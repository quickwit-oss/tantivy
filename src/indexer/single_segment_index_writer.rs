use std::marker::PhantomData;

use crate::indexer::operation::AddOperation;
use crate::indexer::segment_updater::save_metas;
use crate::indexer::SegmentWriter;
use crate::schema::document::Document;
use crate::{Directory, DocId, Index, IndexMeta, Opstamp, Segment, TantivyDocument};

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
        let segment_writer =
            SegmentWriter::for_segment_with_provided_doc_id_mapping(mem_budget, segment.clone())?;
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
        let max_doc = self.segment_writer.max_doc();
        self.segment_writer.finalize()?;
        finalize_segment(self.segment, max_doc)
    }

    /// Finalizes this single-segment index using a caller-provided document order.
    ///
    /// `new_doc_id_to_old_doc_id[new_id]` is the old insertion doc id of the document that should
    /// be serialized at `new_id`.
    pub fn finalize_with_doc_id_mapping(
        self,
        new_doc_id_to_old_doc_id: Vec<DocId>,
    ) -> crate::Result<Index> {
        let max_doc = self.segment_writer.max_doc();
        self.segment_writer
            .finalize_with_doc_id_mapping(new_doc_id_to_old_doc_id)?;
        finalize_segment(self.segment, max_doc)
    }
}

fn finalize_segment(segment: Segment, max_doc: DocId) -> crate::Result<Index> {
    let segment: Segment = segment.with_max_doc(max_doc);
    let index = segment.index();
    let index_meta = IndexMeta {
        index_settings: index.settings().clone(),
        segments: vec![segment.meta().clone()],
        schema: index.schema(),
        opstamp: 0,
        payload: None,
    };
    save_metas(&index_meta, index.directory())?;
    index.directory().sync_directory()?;
    Ok(segment.index().clone())
}

#[cfg(test)]
mod tests {
    use crate::collector::TopDocs;
    use crate::directory::RamDirectory;
    use crate::query::QueryParser;
    use crate::schema::{
        IndexRecordOption, NumericOptions, Schema, TextFieldIndexing, TextOptions, Value, STORED,
    };
    use crate::{Index, ReloadPolicy, TantivyDocument};

    #[test]
    fn test_finalize_with_doc_id_mapping() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_u64_field("id", NumericOptions::default().set_fast());
        let text_field = schema_builder.add_text_field(
            "text",
            TextOptions::default().set_stored().set_indexing_options(
                TextFieldIndexing::default()
                    .set_index_option(IndexRecordOption::WithFreqs)
                    .set_fieldnorms(true),
            ),
        );
        let stored_field = schema_builder.add_text_field("stored", STORED);
        let schema = schema_builder.build();
        let mut writer = Index::builder()
            .schema(schema)
            .single_segment_index_writer(RamDirectory::create(), 15_000_000)?;
        writer.add_document(doc!(
            id_field => 10u64,
            text_field => "alpha beta",
            stored_field => "old-0",
        ))?;
        writer.add_document(doc!(
            id_field => 20u64,
            text_field => "alpha",
            stored_field => "old-1",
        ))?;
        writer.add_document(doc!(
            id_field => 30u64,
            text_field => "beta",
            stored_field => "old-2",
        ))?;

        let index = writer.finalize_with_doc_id_mapping(vec![2, 0, 1])?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let searcher = reader.searcher();
        let segment_reader = searcher.segment_reader(0);

        let fast_field = segment_reader
            .fast_fields()
            .u64("id")?
            .first_or_default_col(0);
        assert_eq!(fast_field.get_val(0), 30u64);
        assert_eq!(fast_field.get_val(1), 10u64);
        assert_eq!(fast_field.get_val(2), 20u64);

        let fieldnorm_reader = segment_reader.get_fieldnorms_reader(text_field)?;
        assert_eq!(fieldnorm_reader.fieldnorm(0), 1);
        assert_eq!(fieldnorm_reader.fieldnorm(1), 2);
        assert_eq!(fieldnorm_reader.fieldnorm(2), 1);

        let mut stored_values = Vec::new();
        for doc_id in 0..segment_reader.max_doc() {
            let doc: TantivyDocument = segment_reader.get_store_reader(1024)?.get(doc_id)?;
            let stored_value = doc
                .get_first(stored_field)
                .and_then(|value| value.as_str())
                .unwrap();
            stored_values.push(stored_value.to_string());
        }
        assert_eq!(stored_values, ["old-2", "old-0", "old-1"]);

        let query = QueryParser::for_index(&index, vec![text_field]).parse_query("beta")?;
        let top_docs: Vec<(_, _)> =
            searcher.search(&query, &TopDocs::with_limit(10).order_by_score())?;
        let doc_ids = top_docs
            .into_iter()
            .map(|(_, doc_address)| doc_address.doc_id)
            .collect::<Vec<_>>();
        assert_eq!(doc_ids, [0, 1]);

        Ok(())
    }
}
