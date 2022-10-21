use std::sync::Arc;

use fastfield_codecs::Column;
use itertools::Itertools;

use crate::indexer::doc_id_mapping::SegmentDocIdMapping;
use crate::schema::Field;
use crate::SegmentReader;

pub(crate) struct RemappedDocIdColumn<'a> {
    doc_id_mapping: &'a SegmentDocIdMapping,
    fast_field_readers: Vec<Arc<dyn Column<u64>>>,
    min_value: u64,
    max_value: u64,
    num_vals: u32,
}

fn compute_min_max_val(
    u64_reader: &dyn Column<u64>,
    segment_reader: &SegmentReader,
) -> Option<(u64, u64)> {
    if segment_reader.max_doc() == 0 {
        return None;
    }

    if segment_reader.alive_bitset().is_none() {
        // no deleted documents,
        // we can use the previous min_val, max_val.
        return Some((u64_reader.min_value(), u64_reader.max_value()));
    }
    // some deleted documents,
    // we need to recompute the max / min
    segment_reader
        .doc_ids_alive()
        .map(|doc_id| u64_reader.get_val(doc_id))
        .minmax()
        .into_option()
}

impl<'a> RemappedDocIdColumn<'a> {
    pub(crate) fn new(
        readers: &'a [SegmentReader],
        doc_id_mapping: &'a SegmentDocIdMapping,
        field: Field,
    ) -> Self {
        let (min_value, max_value) = readers
            .iter()
            .filter_map(|reader| {
                let u64_reader: Arc<dyn Column<u64>> =
                    reader.fast_fields().typed_fast_field_reader(field).expect(
                        "Failed to find a reader for single fast field. This is a tantivy bug and \
                         it should never happen.",
                    );
                compute_min_max_val(&*u64_reader, reader)
            })
            .reduce(|a, b| (a.0.min(b.0), a.1.max(b.1)))
            .expect("Unexpected error, empty readers in IndexMerger");

        let fast_field_readers = readers
            .iter()
            .map(|reader| {
                let u64_reader: Arc<dyn Column<u64>> =
                    reader.fast_fields().typed_fast_field_reader(field).expect(
                        "Failed to find a reader for single fast field. This is a tantivy bug and \
                         it should never happen.",
                    );
                u64_reader
            })
            .collect::<Vec<_>>();

        RemappedDocIdColumn {
            doc_id_mapping,
            fast_field_readers,
            min_value,
            max_value,
            num_vals: doc_id_mapping.len() as u32,
        }
    }
}

impl<'a> Column for RemappedDocIdColumn<'a> {
    fn get_val(&self, _doc: u32) -> u64 {
        unimplemented!()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        Box::new(
            self.doc_id_mapping
                .iter_old_doc_addrs()
                .map(|old_doc_addr| {
                    let fast_field_reader =
                        &self.fast_field_readers[old_doc_addr.segment_ord as usize];
                    fast_field_reader.get_val(old_doc_addr.doc_id)
                }),
        )
    }
    fn min_value(&self) -> u64 {
        self.min_value
    }

    fn max_value(&self) -> u64 {
        self.max_value
    }

    fn num_vals(&self) -> u32 {
        self.num_vals
    }
}
