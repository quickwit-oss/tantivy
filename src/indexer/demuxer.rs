use common::BitSet;
use itertools::Itertools;

use crate::fastfield::DeleteBitSet;
use crate::{
    merge_filtered_segments, Directory, Index, IndexSettings, Segment, SegmentOrdinal, TantivyError,
};

/// DemuxMapping can be used to reorganize data from multiple segments.
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
/// Semantically you can define a mapping from [old segment ordinal, old docid] -> [new segment ordinal].
#[derive(Debug, Default)]
pub struct DemuxMapping {
    /// [index old segment ordinal] -> [index docid] = new segment ordinal
    mapping: Vec<DocidToSegmentOrdinal>,
}

/// DocidToSegmentOrdinal maps from docid within a segment to the new segment ordinal for demuxing.
#[derive(Debug, Default)]
pub struct DocidToSegmentOrdinal {
    docid_index_to_segment_ordinal: Vec<SegmentOrdinal>,
}

impl DocidToSegmentOrdinal {
    /// Creates a new DocidToSegmentOrdinal with size of num_docids.
    /// Initially all docids point to segment ordinal 0 and need to be set
    /// the via `set` method.
    pub fn new(num_docids: usize) -> Self {
        let mut vec = vec![];
        vec.resize(num_docids, 0);

        DocidToSegmentOrdinal {
            docid_index_to_segment_ordinal: vec,
        }
    }

    /// Associated a docids with a the new SegmentOrdinal.
    pub fn set(&mut self, docid: u32, segment_ordinal: SegmentOrdinal) {
        self.docid_index_to_segment_ordinal[docid as usize] = segment_ordinal;
    }

    /// Iterates over the new SegmentOrdinal in the order of the docid.
    pub fn iter(&self) -> impl Iterator<Item = &SegmentOrdinal> {
        self.docid_index_to_segment_ordinal.iter()
    }
}

impl DemuxMapping {
    /// Creates a new empty DemuxMapping.
    pub fn empty() -> Self {
        DemuxMapping {
            mapping: Default::default(),
        }
    }

    /// Creates a DemuxMapping from existing mapping data.
    pub fn new(mapping: Vec<DocidToSegmentOrdinal>) -> Self {
        DemuxMapping { mapping }
    }

    /// Adds a DocidToSegmentOrdinal. The order of the pus calls
    /// defines the old segment ordinal. e.g. first push = ordinal 0.
    pub fn add(&mut self, segment_mapping: DocidToSegmentOrdinal) {
        self.mapping.push(segment_mapping);
    }

    /// Returns the old number of segments.
    pub fn get_old_num_segments(&self) -> usize {
        self.mapping.len()
    }
}

fn get_delete_bitsets(
    demux_mapping: &DemuxMapping,
    target_segment_ordinal: SegmentOrdinal,
    max_value_per_segment: &[u32],
) -> Vec<DeleteBitSet> {
    let mut bitsets: Vec<_> = max_value_per_segment
        .iter()
        .map(|max_value| BitSet::with_max_value(*max_value))
        .collect();

    for (old_segment_ordinal, docid_to_new_segment) in demux_mapping.mapping.iter().enumerate() {
        let bitset_for_segment = &mut bitsets[old_segment_ordinal];
        for docid in docid_to_new_segment
            .iter()
            .enumerate()
            .filter(|(_docid, new_segment_ordinal)| **new_segment_ordinal != target_segment_ordinal)
            .map(|(docid, _)| docid)
        {
            // mark document as deleted if segment ordinal is not target segment ordinal
            bitset_for_segment.insert(docid as u32);
        }
    }

    bitsets
        .iter()
        .map(|bitset| DeleteBitSet::from_bitset(bitset, bitset.max_value()))
        .collect_vec()
}

/// Demux the segments according to `demux_mapping`. See `DemuxMapping`.
/// The number of output_directories need to match max new segment ordinal from `demux_mapping`.
///
/// The ordinal of `segments` need to match the ordinals in `demux_mapping`.
pub fn demux<Dir: Directory>(
    segments: &[Segment],
    demux_mapping: &DemuxMapping,
    target_settings: IndexSettings,
    mut output_directories: Vec<Dir>,
) -> crate::Result<Vec<Index>> {
    output_directories.reverse();
    let max_value_per_segment = segments
        .iter()
        .map(|seg| seg.meta().max_doc())
        .collect_vec();

    let mut indices = vec![];
    for target_segment_ordinal in 0..output_directories.len() {
        let delete_bitsets = get_delete_bitsets(
            demux_mapping,
            target_segment_ordinal as u32,
            &max_value_per_segment,
        )
        .into_iter()
        .map(|bitset| Some(bitset))
        .collect_vec();

        let index = merge_filtered_segments(
            segments,
            target_settings.clone(),
            delete_bitsets,
            output_directories.pop().ok_or_else(|| {
                TantivyError::InvalidArgument("not enough output_directories provided".to_string())
            })?,
        )?;
        indices.push(index);
    }
    Ok(indices)
}

#[cfg(test)]
mod tests {
    use crate::{
        collector::TopDocs,
        directory::RamDirectory,
        query::QueryParser,
        schema::{Schema, TEXT},
        DocAddress, Term,
    };

    use super::*;

    #[test]
    fn demux_map_to_deletebitset_test() {
        let max_value = 2;
        let mut demux_mapping = DemuxMapping::default();
        //segment ordinal 0 mapping
        let mut docid_to_segment = DocidToSegmentOrdinal::new(max_value);
        docid_to_segment.set(0, 1);
        docid_to_segment.set(1, 0);
        demux_mapping.add(docid_to_segment);

        //segment ordinal 1 mapping
        let mut docid_to_segment = DocidToSegmentOrdinal::new(max_value);
        docid_to_segment.set(0, 1);
        docid_to_segment.set(1, 1);
        demux_mapping.add(docid_to_segment);
        {
            let bit_sets_for_demuxing_to_segment_ordinal_0 =
                get_delete_bitsets(&demux_mapping, 0, &[max_value as u32, max_value as u32]);

            assert_eq!(
                bit_sets_for_demuxing_to_segment_ordinal_0[0].is_deleted(0),
                true
            );
            assert_eq!(
                bit_sets_for_demuxing_to_segment_ordinal_0[0].is_deleted(1),
                false
            );
            assert_eq!(
                bit_sets_for_demuxing_to_segment_ordinal_0[1].is_deleted(0),
                true
            );
            assert_eq!(
                bit_sets_for_demuxing_to_segment_ordinal_0[1].is_deleted(1),
                true
            );
        }

        {
            let bit_sets_for_demuxing_to_segment_ordinal_1 =
                get_delete_bitsets(&demux_mapping, 1, &[max_value as u32, max_value as u32]);

            assert_eq!(
                bit_sets_for_demuxing_to_segment_ordinal_1[0].is_deleted(0),
                false
            );
            assert_eq!(
                bit_sets_for_demuxing_to_segment_ordinal_1[0].is_deleted(1),
                true
            );
            assert_eq!(
                bit_sets_for_demuxing_to_segment_ordinal_1[1].is_deleted(0),
                false
            );
            assert_eq!(
                bit_sets_for_demuxing_to_segment_ordinal_1[1].is_deleted(1),
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
            index_writer.add_document(doc!(text_field=>"texto1"));
            index_writer.add_document(doc!(text_field=>"texto2"));
            index_writer.commit()?;
            index
        };

        let second_index = {
            let mut schema_builder = Schema::builder();
            let text_field = schema_builder.add_text_field("text", TEXT);
            let index = Index::create_in_ram(schema_builder.build());
            let mut index_writer = index.writer_for_tests()?;
            index_writer.add_document(doc!(text_field=>"texto3"));
            index_writer.add_document(doc!(text_field=>"texto4"));
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
            //segment ordinal 0 mapping
            let mut docid_to_segment = DocidToSegmentOrdinal::new(max_value);
            docid_to_segment.set(0, 1);
            docid_to_segment.set(1, 0);
            demux_mapping.add(docid_to_segment);

            //segment ordinal 1 mapping
            let mut docid_to_segment = DocidToSegmentOrdinal::new(max_value);
            docid_to_segment.set(0, 1);
            docid_to_segment.set(1, 1);
            demux_mapping.add(docid_to_segment);
        }
        assert_eq!(demux_mapping.get_old_num_segments(), 2);

        let demuxed_indices = demux(
            &segments,
            &demux_mapping,
            target_settings,
            vec![RamDirectory::default(), RamDirectory::default()],
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
                    let query = QueryParser::for_index(&index, vec![text_field])
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
                    let query = QueryParser::for_index(&index, vec![text_field])
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
