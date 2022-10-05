use std::cmp;

use fastfield_codecs::Column;

use crate::fastfield::MultiValuedFastFieldReader;
use crate::indexer::doc_id_mapping::SegmentDocIdMapping;
use crate::indexer::flat_map_with_buffer;
use crate::schema::Field;
use crate::{DocAddress, SegmentReader};

pub(crate) struct RemappedDocIdMultiValueColumn<'a> {
    doc_id_mapping: &'a SegmentDocIdMapping,
    fast_field_readers: Vec<MultiValuedFastFieldReader<u64>>,
    min_value: u64,
    max_value: u64,
    num_vals: u64,
}

impl<'a> RemappedDocIdMultiValueColumn<'a> {
    pub(crate) fn new(
        readers: &'a [SegmentReader],
        doc_id_mapping: &'a SegmentDocIdMapping,
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
        RemappedDocIdMultiValueColumn {
            doc_id_mapping,
            fast_field_readers,
            min_value,
            max_value,
            num_vals: num_vals as u64,
        }
    }
}

impl<'a> Column for RemappedDocIdMultiValueColumn<'a> {
    fn get_val(&self, _pos: u64) -> u64 {
        unimplemented!()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        Box::new(flat_map_with_buffer(
            self.doc_id_mapping.iter_old_doc_addrs(),
            |old_doc_addr: DocAddress, buffer| {
                let ff_reader = &self.fast_field_readers[old_doc_addr.segment_ord as usize];
                ff_reader.get_vals(old_doc_addr.doc_id, buffer);
            },
        ))
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
