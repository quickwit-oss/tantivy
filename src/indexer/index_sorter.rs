use std::cmp::Reverse;

use super::SegmentWriter;
use crate::{DocId, IndexSettings, Order, TantivyError};

pub(crate) type DocidMapping = Vec<u32>;

pub(crate) fn sort_index(
    settings: IndexSettings,
    segment_writer: &mut SegmentWriter,
) -> crate::Result<DocidMapping> {
    let docid_mapping = get_docid_mapping(settings, segment_writer)?;
    Ok(docid_mapping)
}

// Generates a document mapping in the form of [index docid] -> docid
fn get_docid_mapping(
    settings: IndexSettings,
    segment_writer: &mut SegmentWriter,
) -> crate::Result<Vec<DocId>> {
    let field_id = segment_writer
        .segment_serializer
        .segment()
        .schema()
        .get_field(&settings.sort_by_field.field)
        .unwrap();
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
    Ok(docid_and_data
        .into_iter()
        .map(|el| el.0)
        .collect::<Vec<_>>())
}

mod tests_indexsorting {
    use crate::schema::Schema;
    use crate::schema::*;
    use crate::{Index, IndexSettings, IndexSortByField, Order};

    #[test]
    fn test_sort_index() {
        let mut schema_builder = Schema::builder();
        let my_number = schema_builder.add_u64_field(
            "my_number",
            IntOptions::default().set_fast(Cardinality::SingleValue),
        );
        let multi_numbers = schema_builder.add_u64_field(
            "multi_numbers",
            IntOptions::default().set_fast(Cardinality::MultiValues),
        );
        let schema = schema_builder.build();
        let settings = IndexSettings {
            sort_by_field: IndexSortByField {
                field: "my_number".to_string(),
                order: Order::Asc,
            },
        };
        let index = Index::builder()
            .schema(schema)
            .settings(settings)
            .create_in_ram()
            .unwrap();

        assert_eq!(
            index.settings().as_ref().unwrap().sort_by_field.field,
            "my_number".to_string()
        );
        let mut index_writer = index.writer_for_tests().unwrap();
        index_writer
            .add_document(doc!(my_number=>2_u64, multi_numbers => 5_u64, multi_numbers => 6_u64));
        index_writer.add_document(doc!(my_number=>1_u64));
        index_writer.add_document(doc!(my_number=>3_u64, multi_numbers => 3_u64));
        index_writer.commit().unwrap();

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        assert_eq!(searcher.segment_readers().len(), 1);
        let segment_reader = searcher.segment_reader(0);
        let fast_fields = segment_reader.fast_fields();

        let fast_field = fast_fields.u64(my_number).unwrap();
        assert_eq!(fast_field.get(0u32), 1u64);
        assert_eq!(fast_field.get(1u32), 2u64);
        assert_eq!(fast_field.get(2u32), 3u64);

        let multifield = fast_fields.u64s(multi_numbers).unwrap();
        let mut vals = vec![];
        multifield.get_vals(0u32, &mut vals); // todo add test which includes mapping
        assert_eq!(vals, &[] as &[u64]);
        let mut vals = vec![];
        multifield.get_vals(1u32, &mut vals); // todo add test which includes mapping
        assert_eq!(vals, &[5, 6]);
        let mut vals = vec![];
        multifield.get_vals(2u32, &mut vals); // todo add test which includes mapping
        assert_eq!(vals, &[3]);
    }
}
