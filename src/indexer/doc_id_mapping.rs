//! This module is used when sorting the index by a property, e.g.
//! to get mappings from old doc_id to new doc_id and vice versa, after sorting

use common::ReadOnlyBitSet;

use super::SegmentWriter;
use crate::schema::{Field, Schema};
use crate::{DocAddress, DocId, IndexSortByField, TantivyError};

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum MappingType {
    Stacked,
    StackedWithDeletes,
    Shuffled,
}

/// Struct to provide mapping from new doc_id to old doc_id and segment.
#[derive(Clone)]
pub(crate) struct SegmentDocIdMapping {
    pub(crate) new_doc_id_to_old_doc_addr: Vec<DocAddress>,
    pub(crate) alive_bitsets: Vec<Option<ReadOnlyBitSet>>,
    mapping_type: MappingType,
}

impl SegmentDocIdMapping {
    pub(crate) fn new(
        new_doc_id_to_old_doc_addr: Vec<DocAddress>,
        mapping_type: MappingType,
        alive_bitsets: Vec<Option<ReadOnlyBitSet>>,
    ) -> Self {
        Self {
            new_doc_id_to_old_doc_addr,
            mapping_type,
            alive_bitsets,
        }
    }

    pub fn mapping_type(&self) -> MappingType {
        self.mapping_type
    }

    /// Returns an iterator over the old document addresses, ordered by the new document ids.
    ///
    /// In the returned `DocAddress`, the `segment_ord` is the ordinal of targeted segment
    /// in the list of merged segments.
    pub(crate) fn iter_old_doc_addrs(&self) -> impl Iterator<Item = DocAddress> + '_ {
        self.new_doc_id_to_old_doc_addr.iter().copied()
    }

    /// This flags means the segments are simply stacked in the order of their ordinal.
    /// e.g. [(0, 1), .. (n, 1), (0, 2)..., (m, 2)]
    ///
    /// The different segment may present some deletes, in which case it is expressed by skipping a
    /// `DocId`. [(0, 1), (0, 3)] <--- here doc_id=0 and doc_id=1 have been deleted
    ///
    /// Being trivial is equivalent to having the `new_doc_id_to_old_doc_addr` array sorted.
    ///
    /// This allows for some optimization.
    pub(crate) fn is_trivial(&self) -> bool {
        match self.mapping_type {
            MappingType::Stacked | MappingType::StackedWithDeletes => true,
            MappingType::Shuffled => false,
        }
    }
}

/// Struct to provide mapping from old doc_id to new doc_id and vice versa within a segment.
pub struct DocIdMapping {
    new_doc_id_to_old: Vec<DocId>,
    old_doc_id_to_new: Vec<DocId>,
}

impl DocIdMapping {
    pub fn from_new_id_to_old_id(new_doc_id_to_old: Vec<DocId>) -> Self {
        let max_doc = new_doc_id_to_old.len();
        let old_max_doc = new_doc_id_to_old
            .iter()
            .cloned()
            .max()
            .map(|n| n + 1)
            .unwrap_or(0);
        let mut old_doc_id_to_new = vec![0; old_max_doc as usize];
        for i in 0..max_doc {
            old_doc_id_to_new[new_doc_id_to_old[i] as usize] = i as DocId;
        }
        DocIdMapping {
            new_doc_id_to_old,
            old_doc_id_to_new,
        }
    }

    /// returns the new doc_id for the old doc_id
    pub fn get_new_doc_id(&self, doc_id: DocId) -> DocId {
        self.old_doc_id_to_new[doc_id as usize]
    }
    /// returns the old doc_id for the new doc_id
    pub fn get_old_doc_id(&self, doc_id: DocId) -> DocId {
        self.new_doc_id_to_old[doc_id as usize]
    }
    /// iterate over old doc_ids in order of the new doc_ids
    pub fn iter_old_doc_ids(&self) -> impl Iterator<Item = DocId> + Clone + '_ {
        self.new_doc_id_to_old.iter().cloned()
    }

    pub fn old_to_new_ids(&self) -> &[DocId] {
        &self.old_doc_id_to_new[..]
    }

    /// Remaps a given array to the new doc ids.
    pub fn remap<T: Copy>(&self, els: &[T]) -> Vec<T> {
        self.new_doc_id_to_old
            .iter()
            .map(|old_doc| els[*old_doc as usize])
            .collect()
    }
    pub fn num_new_doc_ids(&self) -> usize {
        self.new_doc_id_to_old.len()
    }
    pub fn num_old_doc_ids(&self) -> usize {
        self.old_doc_id_to_new.len()
    }
}

pub(crate) fn expect_field_id_for_sort_field(
    schema: &Schema,
    sort_by_field: &IndexSortByField,
) -> crate::Result<Field> {
    schema.get_field(&sort_by_field.field).map_err(|_| {
        TantivyError::InvalidArgument(format!(
            "field to sort index by not found: {:?}",
            sort_by_field.field
        ))
    })
}

// Generates a document mapping in the form of [index new doc_id] -> old doc_id
// TODO detect if field is already sorted and discard mapping
pub(crate) fn get_doc_id_mapping_from_field(
    sort_by_field: IndexSortByField,
    segment_writer: &SegmentWriter,
) -> crate::Result<DocIdMapping> {
    let schema = segment_writer.segment_serializer.segment().schema();
    expect_field_id_for_sort_field(&schema, &sort_by_field)?; // for now expect
    let new_doc_id_to_old = segment_writer.fast_field_writers.sort_order(
        sort_by_field.field.as_str(),
        segment_writer.max_doc(),
        sort_by_field.order.is_desc(),
    );
    // create new doc_id to old doc_id index (used in fast_field_writers)
    Ok(DocIdMapping::from_new_id_to_old_id(new_doc_id_to_old))
}

#[cfg(test)]
mod tests_indexsorting {
    use common::DateTime;

    use crate::collector::TopDocs;
    use crate::indexer::doc_id_mapping::DocIdMapping;
    use crate::indexer::NoMergePolicy;
    use crate::query::QueryParser;
    use crate::schema::{Schema, *};
    use crate::{DocAddress, Index, IndexSettings, IndexSortByField, Order};

    fn create_test_index(
        index_settings: Option<IndexSettings>,
        text_field_options: TextOptions,
    ) -> crate::Result<Index> {
        let mut schema_builder = Schema::builder();

        let my_text_field = schema_builder.add_text_field("text_field", text_field_options);
        let my_string_field = schema_builder.add_text_field("string_field", STRING | STORED);
        let my_number =
            schema_builder.add_u64_field("my_number", NumericOptions::default().set_fast());

        let multi_numbers =
            schema_builder.add_u64_field("multi_numbers", NumericOptions::default().set_fast());

        let schema = schema_builder.build();
        let mut index_builder = Index::builder().schema(schema);
        if let Some(settings) = index_settings {
            index_builder = index_builder.settings(settings);
        }
        let index = index_builder.create_in_ram()?;

        let mut index_writer = index.writer_for_tests()?;
        index_writer.add_document(doc!(my_number=>40_u64))?;
        index_writer.add_document(
            doc!(my_number=>20_u64, multi_numbers => 5_u64, multi_numbers => 6_u64),
        )?;
        index_writer.add_document(doc!(my_number=>100_u64))?;
        index_writer.add_document(
            doc!(my_number=>10_u64, my_string_field=> "blublub", my_text_field => "some text"),
        )?;
        index_writer.add_document(doc!(my_number=>30_u64, multi_numbers => 3_u64 ))?;
        index_writer.commit()?;
        Ok(index)
    }
    fn get_text_options() -> TextOptions {
        TextOptions::default().set_indexing_options(
            TextFieldIndexing::default().set_index_option(IndexRecordOption::Basic),
        )
    }
    #[test]
    fn test_sort_index_test_text_field() -> crate::Result<()> {
        // there are different serializers for different settings in postings/recorder.rs
        // test remapping for all of them
        let options = vec![
            get_text_options(),
            get_text_options().set_indexing_options(
                TextFieldIndexing::default().set_index_option(IndexRecordOption::WithFreqs),
            ),
            get_text_options().set_indexing_options(
                TextFieldIndexing::default()
                    .set_index_option(IndexRecordOption::WithFreqsAndPositions),
            ),
        ];

        for option in options {
            // let options = get_text_options();
            // no index_sort
            let index = create_test_index(None, option.clone())?;
            let my_text_field = index.schema().get_field("text_field").unwrap();
            let searcher = index.reader()?.searcher();

            let query = QueryParser::for_index(&index, vec![my_text_field]).parse_query("text")?;
            let top_docs: Vec<(f32, DocAddress)> =
                searcher.search(&query, &TopDocs::with_limit(3))?;
            assert_eq!(
                top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>(),
                vec![3]
            );

            // sort by field asc
            let index = create_test_index(
                Some(IndexSettings {
                    sort_by_field: Some(IndexSortByField {
                        field: "my_number".to_string(),
                        order: Order::Asc,
                    }),
                    ..Default::default()
                }),
                option.clone(),
            )?;
            let my_text_field = index.schema().get_field("text_field").unwrap();
            let reader = index.reader()?;
            let searcher = reader.searcher();

            let query = QueryParser::for_index(&index, vec![my_text_field]).parse_query("text")?;
            let top_docs: Vec<(f32, DocAddress)> =
                searcher.search(&query, &TopDocs::with_limit(3))?;
            assert_eq!(
                top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>(),
                vec![0]
            );

            // test new field norm mapping
            {
                let my_text_field = index.schema().get_field("text_field").unwrap();
                let fieldnorm_reader = searcher
                    .segment_reader(0)
                    .get_fieldnorms_reader(my_text_field)?;
                assert_eq!(fieldnorm_reader.fieldnorm(0), 2); // some text
                assert_eq!(fieldnorm_reader.fieldnorm(1), 0);
            }
            // sort by field desc
            let index = create_test_index(
                Some(IndexSettings {
                    sort_by_field: Some(IndexSortByField {
                        field: "my_number".to_string(),
                        order: Order::Desc,
                    }),
                    ..Default::default()
                }),
                option.clone(),
            )?;
            let my_string_field = index.schema().get_field("text_field").unwrap();
            let searcher = index.reader()?.searcher();

            let query =
                QueryParser::for_index(&index, vec![my_string_field]).parse_query("text")?;
            let top_docs: Vec<(f32, DocAddress)> =
                searcher.search(&query, &TopDocs::with_limit(3))?;
            assert_eq!(
                top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>(),
                vec![4]
            );
            // test new field norm mapping
            {
                let my_text_field = index.schema().get_field("text_field").unwrap();
                let fieldnorm_reader = searcher
                    .segment_reader(0)
                    .get_fieldnorms_reader(my_text_field)?;
                assert_eq!(fieldnorm_reader.fieldnorm(0), 0);
                assert_eq!(fieldnorm_reader.fieldnorm(1), 0);
                assert_eq!(fieldnorm_reader.fieldnorm(2), 0);
                assert_eq!(fieldnorm_reader.fieldnorm(3), 0);
                assert_eq!(fieldnorm_reader.fieldnorm(4), 2); // some text
            }
        }
        Ok(())
    }
    #[test]
    fn test_sort_index_get_documents() -> crate::Result<()> {
        // default baseline
        let index = create_test_index(None, get_text_options())?;
        let my_string_field = index.schema().get_field("string_field").unwrap();
        let searcher = index.reader()?.searcher();
        {
            assert_eq!(
                searcher
                    .doc(DocAddress::new(0, 0))?
                    .get_first(my_string_field),
                None
            );
            assert_eq!(
                searcher
                    .doc(DocAddress::new(0, 3))?
                    .get_first(my_string_field)
                    .unwrap()
                    .as_text(),
                Some("blublub")
            );
        }
        // sort by field asc
        let index = create_test_index(
            Some(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "my_number".to_string(),
                    order: Order::Asc,
                }),
                ..Default::default()
            }),
            get_text_options(),
        )?;
        let my_string_field = index.schema().get_field("string_field").unwrap();
        let searcher = index.reader()?.searcher();
        {
            assert_eq!(
                searcher
                    .doc(DocAddress::new(0, 0))?
                    .get_first(my_string_field)
                    .unwrap()
                    .as_text(),
                Some("blublub")
            );
            let doc = searcher.doc(DocAddress::new(0, 4))?;
            assert_eq!(doc.get_first(my_string_field), None);
        }
        // sort by field desc
        let index = create_test_index(
            Some(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "my_number".to_string(),
                    order: Order::Desc,
                }),
                ..Default::default()
            }),
            get_text_options(),
        )?;
        let my_string_field = index.schema().get_field("string_field").unwrap();
        let searcher = index.reader()?.searcher();
        {
            let doc = searcher.doc(DocAddress::new(0, 4))?;
            assert_eq!(
                doc.get_first(my_string_field).unwrap().as_text(),
                Some("blublub")
            );
        }
        Ok(())
    }

    #[test]
    fn test_sort_index_test_string_field() -> crate::Result<()> {
        let index = create_test_index(None, get_text_options())?;
        let my_string_field = index.schema().get_field("string_field").unwrap();
        let searcher = index.reader()?.searcher();

        let query = QueryParser::for_index(&index, vec![my_string_field]).parse_query("blublub")?;
        let top_docs: Vec<(f32, DocAddress)> = searcher.search(&query, &TopDocs::with_limit(3))?;
        assert_eq!(
            top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>(),
            vec![3]
        );

        let index = create_test_index(
            Some(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "my_number".to_string(),
                    order: Order::Asc,
                }),
                ..Default::default()
            }),
            get_text_options(),
        )?;
        let my_string_field = index.schema().get_field("string_field").unwrap();
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let query = QueryParser::for_index(&index, vec![my_string_field]).parse_query("blublub")?;
        let top_docs: Vec<(f32, DocAddress)> = searcher.search(&query, &TopDocs::with_limit(3))?;
        assert_eq!(
            top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>(),
            vec![0]
        );

        // test new field norm mapping
        {
            let my_text_field = index.schema().get_field("text_field").unwrap();
            let fieldnorm_reader = searcher
                .segment_reader(0)
                .get_fieldnorms_reader(my_text_field)?;
            assert_eq!(fieldnorm_reader.fieldnorm(0), 2); // some text
            assert_eq!(fieldnorm_reader.fieldnorm(1), 0);
        }
        // sort by field desc
        let index = create_test_index(
            Some(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "my_number".to_string(),
                    order: Order::Desc,
                }),
                ..Default::default()
            }),
            get_text_options(),
        )?;
        let my_string_field = index.schema().get_field("string_field").unwrap();
        let searcher = index.reader()?.searcher();

        let query = QueryParser::for_index(&index, vec![my_string_field]).parse_query("blublub")?;
        let top_docs: Vec<(f32, DocAddress)> = searcher.search(&query, &TopDocs::with_limit(3))?;
        assert_eq!(
            top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>(),
            vec![4]
        );
        // test new field norm mapping
        {
            let my_text_field = index.schema().get_field("text_field").unwrap();
            let fieldnorm_reader = searcher
                .segment_reader(0)
                .get_fieldnorms_reader(my_text_field)?;
            assert_eq!(fieldnorm_reader.fieldnorm(0), 0);
            assert_eq!(fieldnorm_reader.fieldnorm(1), 0);
            assert_eq!(fieldnorm_reader.fieldnorm(2), 0);
            assert_eq!(fieldnorm_reader.fieldnorm(3), 0);
            assert_eq!(fieldnorm_reader.fieldnorm(4), 2); // some text
        }
        Ok(())
    }

    #[test]
    fn test_sort_index_fast_field() -> crate::Result<()> {
        let index = create_test_index(
            Some(IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "my_number".to_string(),
                    order: Order::Asc,
                }),
                ..Default::default()
            }),
            get_text_options(),
        )?;
        assert_eq!(
            index.settings().sort_by_field.as_ref().unwrap().field,
            "my_number".to_string()
        );

        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_reader(0);
        let fast_fields = segment_reader.fast_fields();

        let fast_field = fast_fields
            .u64("my_number")
            .unwrap()
            .first_or_default_col(999);
        assert_eq!(fast_field.get_val(0), 10u64);
        assert_eq!(fast_field.get_val(1), 20u64);
        assert_eq!(fast_field.get_val(2), 30u64);

        let multifield = fast_fields.u64("multi_numbers").unwrap();
        let vals: Vec<u64> = multifield.values_for_doc(0u32).collect();
        assert_eq!(vals, &[] as &[u64]);
        let vals: Vec<_> = multifield.values_for_doc(1u32).collect();
        assert_eq!(vals, &[5, 6]);

        let vals: Vec<_> = multifield.values_for_doc(2u32).collect();
        assert_eq!(vals, &[3]);
        Ok(())
    }

    #[test]
    fn test_with_sort_by_date_field() -> crate::Result<()> {
        let mut schema_builder = Schema::builder();
        let date_field = schema_builder.add_date_field("date", INDEXED | STORED | FAST);
        let schema = schema_builder.build();

        let settings = IndexSettings {
            sort_by_field: Some(IndexSortByField {
                field: "date".to_string(),
                order: Order::Desc,
            }),
            ..Default::default()
        };

        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_ram()?;
        let mut index_writer = index.writer_for_tests()?;
        index_writer.set_merge_policy(Box::new(NoMergePolicy));

        index_writer.add_document(doc!(
            date_field => DateTime::from_timestamp_secs(1000),
        ))?;
        index_writer.add_document(doc!(
            date_field => DateTime::from_timestamp_secs(999),
        ))?;
        index_writer.add_document(doc!(
            date_field => DateTime::from_timestamp_secs(1001),
        ))?;
        index_writer.commit()?;

        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_reader(0);
        let fast_fields = segment_reader.fast_fields();

        let fast_field = fast_fields
            .date("date")
            .unwrap()
            .first_or_default_col(DateTime::from_timestamp_secs(0));
        assert_eq!(fast_field.get_val(0), DateTime::from_timestamp_secs(1001));
        assert_eq!(fast_field.get_val(1), DateTime::from_timestamp_secs(1000));
        assert_eq!(fast_field.get_val(2), DateTime::from_timestamp_secs(999));
        Ok(())
    }

    #[test]
    fn test_doc_mapping() {
        let doc_mapping = DocIdMapping::from_new_id_to_old_id(vec![3, 2, 5]);
        assert_eq!(doc_mapping.get_old_doc_id(0), 3);
        assert_eq!(doc_mapping.get_old_doc_id(1), 2);
        assert_eq!(doc_mapping.get_old_doc_id(2), 5);
        assert_eq!(doc_mapping.get_new_doc_id(0), 0);
        assert_eq!(doc_mapping.get_new_doc_id(1), 0);
        assert_eq!(doc_mapping.get_new_doc_id(2), 1);
        assert_eq!(doc_mapping.get_new_doc_id(3), 0);
        assert_eq!(doc_mapping.get_new_doc_id(4), 0);
        assert_eq!(doc_mapping.get_new_doc_id(5), 2);
    }

    #[test]
    fn test_doc_mapping_remap() {
        let doc_mapping = DocIdMapping::from_new_id_to_old_id(vec![2, 8, 3]);
        assert_eq!(
            &doc_mapping.remap(&[0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000]),
            &[2000, 8000, 3000]
        );
    }
}
