use std::cmp;

use fastfield_codecs::Column;

use super::flat_map_with_buffer::FlatMapWithBufferIter;
use crate::fastfield::{MultiValueIndex, MultiValuedFastFieldReader};
use crate::indexer::doc_id_mapping::SegmentDocIdMapping;
use crate::schema::Field;
use crate::{DocAddress, SegmentReader};

pub(crate) struct RemappedDocIdMultiValueColumn<'a> {
    doc_id_mapping: &'a SegmentDocIdMapping,
    fast_field_readers: Vec<MultiValuedFastFieldReader<u64>>,
    min_value: u64,
    max_value: u64,
    num_vals: u32,
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
            num_vals: num_vals as u32,
        }
    }
}

impl<'a> Column for RemappedDocIdMultiValueColumn<'a> {
    fn get_val(&self, _pos: u32) -> u64 {
        unimplemented!()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        Box::new(
            self.doc_id_mapping
                .iter_old_doc_addrs()
                .flat_map_with_buffer(|old_doc_addr: DocAddress, buffer| {
                    let ff_reader = &self.fast_field_readers[old_doc_addr.segment_ord as usize];
                    ff_reader.get_vals(old_doc_addr.doc_id, buffer);
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

pub(crate) struct RemappedDocIdMultiValueIndexColumn<'a> {
    doc_id_mapping: &'a SegmentDocIdMapping,
    multi_value_length_readers: Vec<&'a MultiValueIndex>,
    min_value: u64,
    max_value: u64,
    num_vals: u32,
}

impl<'a> RemappedDocIdMultiValueIndexColumn<'a> {
    pub(crate) fn new(
        segment_and_ff_readers: &'a [(&'a SegmentReader, &'a MultiValueIndex)],
        doc_id_mapping: &'a SegmentDocIdMapping,
    ) -> Self {
        // We go through a complete first pass to compute the minimum and the
        // maximum value and initialize our Column.
        let mut num_vals = 0;
        let min_value = 0;
        let mut max_value = 0;
        let mut multi_value_length_readers = Vec::with_capacity(segment_and_ff_readers.len());
        for segment_and_ff_reader in segment_and_ff_readers {
            let segment_reader = segment_and_ff_reader.0;
            let multi_value_length_reader = segment_and_ff_reader.1;
            if !segment_reader.has_deletes() {
                max_value += multi_value_length_reader.total_num_vals();
            } else {
                for doc in segment_reader.doc_ids_alive() {
                    max_value += multi_value_length_reader.num_vals_for_doc(doc) as u64;
                }
            }
            num_vals += segment_reader.num_docs();
            multi_value_length_readers.push(multi_value_length_reader);
        }
        Self {
            doc_id_mapping,
            multi_value_length_readers,
            min_value,
            max_value,
            num_vals,
        }
    }
}

impl<'a> Column for RemappedDocIdMultiValueIndexColumn<'a> {
    fn get_val(&self, _pos: u32) -> u64 {
        unimplemented!()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        let mut offset = 0;
        Box::new(
            std::iter::once(0).chain(self.doc_id_mapping.iter_old_doc_addrs().map(
                move |old_doc_addr| {
                    let ff_reader =
                        &self.multi_value_length_readers[old_doc_addr.segment_ord as usize];
                    offset += ff_reader.num_vals_for_doc(old_doc_addr.doc_id);
                    offset as u64
                },
            )),
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
