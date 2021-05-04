use super::SegmentWriter;
use crate::{DocId, IndexSettings, Order, TantivyError};
use std::cmp::Reverse;

/// Struct to provide mapping from old docid to new docid and vice versa
pub struct DocidMapping {
    new_docid_to_old: Vec<u32>,
    old_docid_to_new: Vec<u32>,
}

impl DocidMapping {
    /// returns the new docid for the old docid
    pub fn get_new_docid(&self, docid: DocId) -> DocId {
        self.old_docid_to_new[docid as usize]
    }
    /// returns the old docid for the new docid
    pub fn get_old_docid(&self, docid: DocId) -> DocId {
        self.new_docid_to_old[docid as usize]
    }
    /// iterate over old docids in order of the new docids
    pub fn iter_old_docids(&self) -> std::slice::Iter<'_, u32> {
        self.new_docid_to_old.iter()
    }
}

pub(crate) fn sort_index(
    settings: IndexSettings,
    segment_writer: &mut SegmentWriter,
) -> crate::Result<DocidMapping> {
    let docid_mapping = get_docid_mapping(settings, segment_writer)?;
    Ok(docid_mapping)
}

// Generates a document mapping in the form of [index new docid] -> old docid
fn get_docid_mapping(
    settings: IndexSettings,
    segment_writer: &mut SegmentWriter,
) -> crate::Result<DocidMapping> {
    let field_id = segment_writer
        .segment_serializer
        .segment()
        .schema()
        .get_field(&settings.sort_by_field.field)
        .ok_or_else(|| {
            TantivyError::InvalidArgument(format!(
                "field to sort index by not found: {:?}",
                settings.sort_by_field.field
            ))
        })?;
    // for now expect fastfield, but not strictly required
    let fast_field = segment_writer
        .fast_field_writers
        .get_field_writer(field_id)
        .ok_or_else(|| {
            TantivyError::InvalidArgument(format!(
                "sort index by field is required to be a fast field {:?}",
                settings.sort_by_field.field
            ))
        })?;

    // create new docid to old docid index (used in fast_field_writers)
    let data = fast_field.get_data();
    let mut docid_and_data = data
        .into_iter()
        .enumerate()
        .map(|el| (el.0 as DocId, el.1))
        .collect::<Vec<_>>();
    if settings.sort_by_field.order == Order::Desc {
        docid_and_data.sort_unstable_by_key(|k| Reverse(k.1));
    } else {
        docid_and_data.sort_unstable_by_key(|k| k.1);
    }
    let new_docid_to_old = docid_and_data
        .into_iter()
        .map(|el| el.0)
        .collect::<Vec<_>>();

    // create old docid to new docid index (used in posting recorder)
    let mut new_docid_and_old_docid = new_docid_to_old
        .iter()
        .enumerate()
        .map(|el| (el.0 as u32, *el.1))
        .collect::<Vec<_>>();
    new_docid_and_old_docid.sort_unstable_by_key(|k| k.1);
    let old_docid_to_new = new_docid_and_old_docid
        .iter()
        .map(|el| el.0)
        .collect::<Vec<_>>();
    let docid_map = DocidMapping {
        new_docid_to_old,
        old_docid_to_new,
    };
    Ok(docid_map)
}

#[cfg(test)]
mod tests_indexsorting {
    use crate::{collector::TopDocs, query::QueryParser, schema::*};
    use crate::{schema::Schema, DocAddress};
    use crate::{Index, IndexSettings, IndexSortByField, Order};

    fn create_test_index(
        index_settings: Option<IndexSettings>,
        text_field_options: TextOptions,
    ) -> Index {
        let mut schema_builder = Schema::builder();

        let my_text_field = schema_builder.add_text_field("text_field", text_field_options);
        let my_string_field = schema_builder.add_text_field("string_field", STRING | STORED);
        let my_number = schema_builder.add_u64_field(
            "my_number",
            IntOptions::default().set_fast(Cardinality::SingleValue),
        );

        let multi_numbers = schema_builder.add_u64_field(
            "multi_numbers",
            IntOptions::default().set_fast(Cardinality::MultiValues),
        );

        let schema = schema_builder.build();
        let mut index_builder = Index::builder().schema(schema);
        if let Some(settings) = index_settings {
            index_builder = index_builder.settings(settings);
        }
        let index = index_builder.create_in_ram().unwrap();

        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer.add_document(doc!(my_number=>40_u64));
        index_writer
            .add_document(doc!(my_number=>20_u64, multi_numbers => 5_u64, multi_numbers => 6_u64));
        index_writer.add_document(doc!(my_number=>100_u64));
        index_writer.add_document(
            doc!(my_number=>10_u64, my_string_field=> "blublub", my_text_field => "some text"),
        );
        index_writer.add_document(doc!(my_number=>30_u64, multi_numbers => 3_u64 ));
        index_writer.commit().unwrap();
        index
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
            //let options = get_text_options();
            // no index_sort
            let index = create_test_index(None, option.clone());
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
                    sort_by_field: IndexSortByField {
                        field: "my_number".to_string(),
                        order: Order::Asc,
                    },
                }),
                option.clone(),
            );
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
                    sort_by_field: IndexSortByField {
                        field: "my_number".to_string(),
                        order: Order::Desc,
                    },
                }),
                option.clone(),
            );
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
        let index = create_test_index(None, get_text_options());
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
                    .text(),
                Some("blublub")
            );
        }
        // sort by field asc
        let index = create_test_index(
            Some(IndexSettings {
                sort_by_field: IndexSortByField {
                    field: "my_number".to_string(),
                    order: Order::Asc,
                },
            }),
            get_text_options(),
        );
        let my_string_field = index.schema().get_field("string_field").unwrap();
        let searcher = index.reader()?.searcher();
        {
            assert_eq!(
                searcher
                    .doc(DocAddress::new(0, 0))?
                    .get_first(my_string_field)
                    .unwrap()
                    .text(),
                Some("blublub")
            );
            let doc = searcher.doc(DocAddress::new(0, 4))?;
            assert_eq!(doc.get_first(my_string_field), None);
        }
        // sort by field desc
        let index = create_test_index(
            Some(IndexSettings {
                sort_by_field: IndexSortByField {
                    field: "my_number".to_string(),
                    order: Order::Desc,
                },
            }),
            get_text_options(),
        );
        let my_string_field = index.schema().get_field("string_field").unwrap();
        let searcher = index.reader()?.searcher();
        {
            let doc = searcher.doc(DocAddress::new(0, 4))?;
            assert_eq!(
                doc.get_first(my_string_field).unwrap().text(),
                Some("blublub")
            );
        }
        Ok(())
    }

    #[test]
    fn test_sort_index_test_string_field() -> crate::Result<()> {
        let index = create_test_index(None, get_text_options());
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
                sort_by_field: IndexSortByField {
                    field: "my_number".to_string(),
                    order: Order::Asc,
                },
            }),
            get_text_options(),
        );
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
                sort_by_field: IndexSortByField {
                    field: "my_number".to_string(),
                    order: Order::Desc,
                },
            }),
            get_text_options(),
        );
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
                sort_by_field: IndexSortByField {
                    field: "my_number".to_string(),
                    order: Order::Asc,
                },
            }),
            get_text_options(),
        );
        assert_eq!(
            index.settings().as_ref().unwrap().sort_by_field.field,
            "my_number".to_string()
        );

        let searcher = index.reader()?.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_reader(0);
        let fast_fields = segment_reader.fast_fields();
        let my_number = index.schema().get_field("my_number").unwrap();

        let fast_field = fast_fields.u64(my_number).unwrap();
        assert_eq!(fast_field.get(0u32), 10u64);
        assert_eq!(fast_field.get(1u32), 20u64);
        assert_eq!(fast_field.get(2u32), 30u64);

        let multi_numbers = index.schema().get_field("multi_numbers").unwrap();
        let multifield = fast_fields.u64s(multi_numbers).unwrap();
        let mut vals = vec![];
        multifield.get_vals(0u32, &mut vals);
        assert_eq!(vals, &[] as &[u64]);
        let mut vals = vec![];
        multifield.get_vals(1u32, &mut vals);
        assert_eq!(vals, &[5, 6]);

        let mut vals = vec![];
        multifield.get_vals(2u32, &mut vals);
        assert_eq!(vals, &[3]);
        Ok(())
    }
}
