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
