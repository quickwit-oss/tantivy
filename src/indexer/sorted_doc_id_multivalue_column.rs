use std::cmp;

use fastfield_codecs::Column;

use crate::fastfield::{MultiValueLength, MultiValuedFastFieldReader};
use crate::indexer::doc_id_mapping::SegmentDocIdMapping;
use crate::schema::Field;
use crate::{DocId, SegmentReader};

// We can now initialize our serializer, and push it the different values
pub(crate) struct SortedDocIdMultiValueColumn<'a> {
    doc_id_mapping: &'a SegmentDocIdMapping,
    fast_field_readers: Vec<MultiValuedFastFieldReader<u64>>,
    offsets: &'a [u64],
    min_value: u64,
    max_value: u64,
    num_vals: u64,
}

impl<'a> SortedDocIdMultiValueColumn<'a> {
    pub(crate) fn new(
        readers: &'a [SegmentReader],
        doc_id_mapping: &'a SegmentDocIdMapping,
        offsets: &'a [u64],
        field: Field,
    ) -> Self {
        // Our values are bitpacked and we need to know what should be
        // our bitwidth and our minimum value before serializing any values.
        //
        // Computing those is non-trivial if some documents are deleted.
        // We go through a complete first pass to compute the minimum and the
        // maximum value and initialize our Serializer.
        let mut num_vals = 0;
        let mut min_value = u64::MAX;
        let mut max_value = u64::MIN;
        let mut vals = Vec::new();
        let mut fast_field_readers = Vec::with_capacity(readers.len());
        for reader in readers {
            let ff_reader: MultiValuedFastFieldReader<u64> = reader
                .fast_fields()
                .typed_fast_field_multi_reader::<u64>(field)
                .expect(
                    "Failed to find multivalued fast field reader. This is a bug in tantivy. \
                     Please report.",
                );
            for doc in reader.doc_ids_alive() {
                ff_reader.get_vals(doc, &mut vals);
                for &val in &vals {
                    min_value = cmp::min(val, min_value);
                    max_value = cmp::max(val, max_value);
                }
                num_vals += vals.len();
            }
            fast_field_readers.push(ff_reader);
            // TODO optimize when no deletes
        }
        if min_value > max_value {
            min_value = 0;
            max_value = 0;
        }
        SortedDocIdMultiValueColumn {
            doc_id_mapping,
            fast_field_readers,
            offsets,
            min_value,
            max_value,
            num_vals: num_vals as u64,
        }
    }
}

impl<'a> Column for SortedDocIdMultiValueColumn<'a> {
    fn get_val(&self, pos: u64) -> u64 {
        // use the offsets index to find the doc_id which will contain the position.
        // the offsets are strictly increasing so we can do a binary search on it.

        let new_doc_id: DocId = self.offsets.partition_point(|&offset| offset <= pos) as DocId - 1; // Offsets start at 0, so -1 is safe

        // now we need to find the position of `pos` in the multivalued bucket
        let num_pos_covered_until_now = self.offsets[new_doc_id as usize];
        let pos_in_values = pos - num_pos_covered_until_now;

        let old_doc_addr = self.doc_id_mapping.get_old_doc_addr(new_doc_id);
        let num_vals =
            self.fast_field_readers[old_doc_addr.segment_ord as usize].get_len(old_doc_addr.doc_id);
        assert!(num_vals >= pos_in_values);
        let mut vals = Vec::new();
        self.fast_field_readers[old_doc_addr.segment_ord as usize]
            .get_vals(old_doc_addr.doc_id, &mut vals);

        vals[pos_in_values as usize]
    }

    fn iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        Box::new(
            self.doc_id_mapping
                .iter_old_doc_addrs()
                .flat_map(|old_doc_addr| {
                    let ff_reader = &self.fast_field_readers[old_doc_addr.segment_ord as usize];
                    let mut vals = Vec::new();
                    ff_reader.get_vals(old_doc_addr.doc_id, &mut vals);
                    vals.into_iter()
                }),
        )
    }
    fn min_value(&self) -> u64 {
        self.min_value
    }

    fn max_value(&self) -> u64 {
        self.max_value
    }

    fn num_vals(&self) -> u64 {
        self.num_vals
    }
}
