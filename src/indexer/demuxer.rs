use common::BitSet;
use itertools::Itertools;

use crate::fastfield::AliveBitSet;
use crate::{merge_filtered_segments, Directory, Index, IndexSettings, Segment, SegmentOrdinal};
/// DemuxMapping can be used to reorganize data from multiple segments.
///
/// DemuxMapping is useful in a multitenant settings, in which each document might actually belong
/// to a different tenant. It allows to reorganize documents as follows:
///
/// e.g. if you have two tenant ids TENANT_A and TENANT_B and two segments with
/// the documents (simplified)
/// Seg 1 [TENANT_A, TENANT_B]
/// Seg 2 [TENANT_A, TENANT_B]
///
/// You may want to group your documents to
/// Seg 1 [TENANT_A, TENANT_A]
/// Seg 2 [TENANT_B, TENANT_B]
///
/// Demuxing is the tool for that.
/// Semantically you can define a mapping from [old segment ordinal, old doc_id] -> [new segment
/// ordinal].
#[derive(Debug, Default)]
pub struct DemuxMapping {
    /// [index old segment ordinal] -> [index doc_id] = new segment ordinal
    mapping: Vec<DocIdToSegmentOrdinal>,
}

/// DocIdToSegmentOrdinal maps from doc_id within a segment to the new segment ordinal for demuxing.
///
/// For every source segment there is a `DocIdToSegmentOrdinal` to distribute its doc_ids.
#[derive(Debug, Default)]
pub struct DocIdToSegmentOrdinal {
    doc_id_index_to_segment_ord: Vec<SegmentOrdinal>,
}

impl DocIdToSegmentOrdinal {
    /// Creates a new DocIdToSegmentOrdinal with size of num_doc_ids.
    /// Initially all doc_ids point to segment ordinal 0 and need to be set
    /// the via `set` method.
    pub fn with_max_doc(max_doc: usize) -> Self {
        DocIdToSegmentOrdinal {
            doc_id_index_to_segment_ord: vec![0; max_doc],
        }
    }

    /// Returns the number of documents in this mapping.
    /// It should be equal to the `max_doc` of the segment it targets.
    pub fn max_doc(&self) -> u32 {
        self.doc_id_index_to_segment_ord.len() as u32
    }

    /// Associates a doc_id with an output `SegmentOrdinal`.
    pub fn set(&mut self, doc_id: u32, segment_ord: SegmentOrdinal) {
        self.doc_id_index_to_segment_ord[doc_id as usize] = segment_ord;
    }

    /// Iterates over the new SegmentOrdinal in the order of the doc_id.
    pub fn iter(&self) -> impl Iterator<Item = SegmentOrdinal> + '_ {
        self.doc_id_index_to_segment_ord.iter().cloned()
    }
}

impl DemuxMapping {
    /// Adds a DocIdToSegmentOrdinal. The order of the pus calls
    /// defines the old segment ordinal. e.g. first push = ordinal 0.
    pub fn add(&mut self, segment_mapping: DocIdToSegmentOrdinal) {
        self.mapping.push(segment_mapping);
    }

    /// Returns the old number of segments.
    pub fn get_old_num_segments(&self) -> usize {
        self.mapping.len()
    }
}

fn docs_for_segment_ord(
    doc_id_to_segment_ord: &DocIdToSegmentOrdinal,
    target_segment_ord: SegmentOrdinal,
) -> AliveBitSet {
    let mut bitset = BitSet::with_max_value(doc_id_to_segment_ord.max_doc());
    for doc_id in doc_id_to_segment_ord
        .iter()
        .enumerate()
        .filter(|(_doc_id, new_segment_ord)| *new_segment_ord == target_segment_ord)
        .map(|(doc_id, _)| doc_id)
    {
        // add document if segment ordinal = target segment ordinal
        bitset.insert(doc_id as u32);
    }
    AliveBitSet::from_bitset(&bitset)
}

fn get_alive_bitsets(
    demux_mapping: &DemuxMapping,
    target_segment_ord: SegmentOrdinal,
) -> Vec<AliveBitSet> {
    demux_mapping
        .mapping
        .iter()
        .map(|doc_id_to_segment_ord| {
            docs_for_segment_ord(doc_id_to_segment_ord, target_segment_ord)
        })
        .collect_vec()
}

/// Demux the segments according to `demux_mapping`. See `DemuxMapping`.
/// The number of output_directories need to match max new segment ordinal from `demux_mapping`.
///
/// The ordinal of `segments` need to match the ordinals provided in `demux_mapping`.
pub fn demux(
    segments: &[Segment],
    demux_mapping: &DemuxMapping,
    target_settings: IndexSettings,
    output_directories: Vec<Box<dyn Directory>>,
) -> crate::Result<Vec<Index>> {
    let mut indices = vec![];
    for (target_segment_ord, output_directory) in output_directories.into_iter().enumerate() {
        let delete_bitsets = get_alive_bitsets(demux_mapping, target_segment_ord as u32)
            .into_iter()
            .map(Some)
            .collect_vec();
        let index = merge_filtered_segments(
            segments,
            target_settings.clone(),
            delete_bitsets,
            output_directory,
        )?;
        indices.push(index);
    }
    Ok(indices)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::TopDocs;
    use crate::directory::RamDirectory;
    use crate::query::QueryParser;
    use crate::schema::{Schema, TEXT};
    use crate::{DocAddress, Term};

    #[test]
    fn test_demux_map_to_deletebitset() {
        let max_value = 2;
        let mut demux_mapping = DemuxMapping::default();
        // segment ordinal 0 mapping
        let mut doc_id_to_segment = DocIdToSegmentOrdinal::with_max_doc(max_value);
        doc_id_to_segment.set(0, 1);
        doc_id_to_segment.set(1, 0);
        demux_mapping.add(doc_id_to_segment);

        // segment ordinal 1 mapping
        let mut doc_id_to_segment = DocIdToSegmentOrdinal::with_max_doc(max_value);
        doc_id_to_segment.set(0, 1);
        doc_id_to_segment.set(1, 1);
        demux_mapping.add(doc_id_to_segment);
        {
            let bit_sets_for_demuxing_to_segment_ord_0 = get_alive_bitsets(&demux_mapping, 0);

            assert_eq!(
                bit_sets_for_demuxing_to_segment_ord_0[0].is_deleted(0),
                true
            );
            assert_eq!(
                bit_sets_for_demuxing_to_segment_ord_0[0].is_deleted(1),
                false
            );
            assert_eq!(
                bit_sets_for_demuxing_to_segment_ord_0[1].is_deleted(0),
                true
            );
            assert_eq!(
                bit_sets_for_demuxing_to_segment_ord_0[1].is_deleted(1),
                true
            );
        }

        {
            let bit_sets_for_demuxing_to_segment_ord_1 = get_alive_bitsets(&demux_mapping, 1);

            assert_eq!(
                bit_sets_for_demuxing_to_segment_ord_1[0].is_deleted(0),
                false
            );
            assert_eq!(
                bit_sets_for_demuxing_to_segment_ord_1[0].is_deleted(1),
                true
            );
            assert_eq!(
                bit_sets_for_demuxing_to_segment_ord_1[1].is_deleted(0),
                false
            );
            assert_eq!(
                bit_sets_for_demuxing_to_segment_ord_1[1].is_deleted(1),
                false
            );
        }
    }

    #[test]
    fn test_demux_segments() -> crate::Result<()> {
        let first_index = {
            let mut schema_builder = Schema::builder();
            let text_field = schema_builder.add_text_field("text", TEXT);
            let index = Index::create_in_ram(schema_builder.build());
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field=>"texto1"))?;
            index_writer.add_document(doc!(text_field=>"texto2"))?;
            index_writer.commit()?;
            index
        };

        let second_index = {
            let mut schema_builder = Schema::builder();
            let text_field = schema_builder.add_text_field("text", TEXT);
            let index = Index::create_in_ram(schema_builder.build());
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field=>"texto3"))?;
            index_writer.add_document(doc!(text_field=>"texto4"))?;
            index_writer.delete_term(Term::from_field_text(text_field, "4"));

            index_writer.commit()?;
            index
        };

        let mut segments: Vec<Segment> = Vec::new();
        segments.extend(first_index.searchable_segments()?);
        segments.extend(second_index.searchable_segments()?);

        let target_settings = first_index.settings().clone();

        let mut demux_mapping = DemuxMapping::default();
        {
            let max_value = 2;
            // segment ordinal 0 mapping
            let mut doc_id_to_segment = DocIdToSegmentOrdinal::with_max_doc(max_value);
            doc_id_to_segment.set(0, 1);
            doc_id_to_segment.set(1, 0);
            demux_mapping.add(doc_id_to_segment);

            // segment ordinal 1 mapping
            let mut doc_id_to_segment = DocIdToSegmentOrdinal::with_max_doc(max_value);
            doc_id_to_segment.set(0, 1);
            doc_id_to_segment.set(1, 1);
            demux_mapping.add(doc_id_to_segment);
        }
        assert_eq!(demux_mapping.get_old_num_segments(), 2);

        let demuxed_indices = demux(
            &segments,
            &demux_mapping,
            target_settings,
            vec![
                Box::new(RamDirectory::default()),
                Box::new(RamDirectory::default()),
            ],
        )?;

        {
            let index = &demuxed_indices[0];

            let segments = index.searchable_segments()?;
            assert_eq!(segments.len(), 1);

            let segment_metas = segments[0].meta();
            assert_eq!(segment_metas.num_deleted_docs(), 0);
            assert_eq!(segment_metas.num_docs(), 1);

            let searcher = index.reader().unwrap().searcher();
            {
                let text_field = index.schema().get_field("text").unwrap();

                let do_search = |term: &str| {
                    let query = QueryParser::for_index(index, vec![text_field])
                        .parse_query(term)
                        .unwrap();
                    let top_docs: Vec<(f32, DocAddress)> =
                        searcher.search(&query, &TopDocs::with_limit(3)).unwrap();

                    top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>()
                };

                assert_eq!(do_search("texto1"), vec![] as Vec<u32>);
                assert_eq!(do_search("texto2"), vec![0]);
            }
        }

        {
            let index = &demuxed_indices[1];

            let segments = index.searchable_segments()?;
            assert_eq!(segments.len(), 1);

            let segment_metas = segments[0].meta();
            assert_eq!(segment_metas.num_deleted_docs(), 0);
            assert_eq!(segment_metas.num_docs(), 3);

            let searcher = index.reader().unwrap().searcher();
            {
                let text_field = index.schema().get_field("text").unwrap();

                let do_search = |term: &str| {
                    let query = QueryParser::for_index(index, vec![text_field])
                        .parse_query(term)
                        .unwrap();
                    let top_docs: Vec<(f32, DocAddress)> =
                        searcher.search(&query, &TopDocs::with_limit(3)).unwrap();

                    top_docs.iter().map(|el| el.1.doc_id).collect::<Vec<_>>()
                };

                assert_eq!(do_search("texto1"), vec![0]);
                assert_eq!(do_search("texto2"), vec![] as Vec<u32>);
                assert_eq!(do_search("texto3"), vec![1]);
                assert_eq!(do_search("texto4"), vec![2]);
            }
        }

        Ok(())
    }
}
