use fastfield_codecs::{Column, ColumnReader};

use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::DocId;

pub(crate) struct MultivalueStartIndex<'a, C: Column> {
    column: &'a C,
    doc_id_map: &'a DocIdMapping,
    min_value: u64,
    max_value: u64,
}

struct MultivalueStartIndexReader<'a, C: Column> {
    column: &'a C,
    doc_id_map: &'a DocIdMapping,
    idx: u64,
    val: u64,
    len: u64,
}

impl<'a, C: Column> MultivalueStartIndexReader<'a, C> {
    fn new(column: &'a C, doc_id_map: &'a DocIdMapping) -> Self {
        Self {
            column,
            doc_id_map,
            idx: u64::MAX,
            val: 0,
            len: doc_id_map.num_new_doc_ids() as u64 + 1,
        }
    }

    fn reset(&mut self) {
        self.idx = u64::MAX;
        self.val = 0;
    }
}

impl<'a, C: Column> ColumnReader for MultivalueStartIndexReader<'a, C> {
    fn seek(&mut self, idx: u64) -> u64 {
        if self.idx > idx {
            self.reset();
            self.advance();
        }
        for _ in self.idx..idx {
            self.advance();
        }
        self.get()
    }

    fn advance(&mut self) -> bool {
        if self.idx == u64::MAX {
            self.idx = 0;
            self.val = 0;
            return true;
        }
        let new_doc_id: DocId = self.idx as DocId;
        self.idx += 1;
        if self.idx >= self.len {
            self.idx = self.len;
            return false;
        }
        let old_doc: DocId = self.doc_id_map.get_old_doc_id(new_doc_id);
        let num_vals_for_doc =
            self.column.get_val(old_doc as u64 + 1) - self.column.get_val(old_doc as u64);
        self.val += num_vals_for_doc;
        true
    }

    fn get(&self) -> u64 {
        self.val
    }
}

impl<'a, C: Column> MultivalueStartIndex<'a, C> {
    pub fn new(column: &'a C, doc_id_map: &'a DocIdMapping) -> Self {
        assert_eq!(column.num_vals(), doc_id_map.num_old_doc_ids() as u64 + 1);
        let iter = MultivalueStartIndexIter::new(column, doc_id_map);
        let (min_value, max_value) = tantivy_bitpacker::minmax(iter).unwrap_or((0, 0));
        MultivalueStartIndex {
            column,
            doc_id_map,
            min_value,
            max_value,
        }
    }

    fn specialized_reader(&self) -> MultivalueStartIndexReader<'a, C> {
        MultivalueStartIndexReader::new(self.column, self.doc_id_map)
    }
}
impl<'a, C: Column> Column for MultivalueStartIndex<'a, C> {
    fn reader(&self) -> Box<dyn ColumnReader + '_> {
        Box::new(self.specialized_reader())
    }

    fn get_val(&self, idx: u64) -> u64 {
        let mut reader = self.specialized_reader();
        reader.seek(idx)
    }

    fn min_value(&self) -> u64 {
        self.min_value
    }

    fn max_value(&self) -> u64 {
        self.max_value
    }

    fn num_vals(&self) -> u64 {
        (self.doc_id_map.num_new_doc_ids() + 1) as u64
    }
}

struct MultivalueStartIndexIter<'a, C: Column> {
    column: &'a C,
    doc_id_map: &'a DocIdMapping,
    new_doc_id: usize,
    offset: u64,
}

impl<'a, C: Column> MultivalueStartIndexIter<'a, C> {
    fn new(column: &'a C, doc_id_map: &'a DocIdMapping) -> Self {
        Self {
            column,
            doc_id_map,
            new_doc_id: 0,
            offset: 0,
        }
    }
}

impl<'a, C: Column> Iterator for MultivalueStartIndexIter<'a, C> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.new_doc_id > self.doc_id_map.num_new_doc_ids() {
            return None;
        }
        let new_doc_id = self.new_doc_id;
        self.new_doc_id += 1;
        let start_offset = self.offset;
        if new_doc_id < self.doc_id_map.num_new_doc_ids() {
            let old_doc = self.doc_id_map.get_old_doc_id(new_doc_id as u32) as u64;
            let num_vals_for_doc = self.column.get_val(old_doc + 1) - self.column.get_val(old_doc);
            self.offset += num_vals_for_doc;
        }
        Some(start_offset)
    }
}

#[cfg(test)]
mod tests {
    use fastfield_codecs::VecColumn;

    use super::*;

    #[test]
    fn test_multivalue_start_index() {
        let doc_id_mapping = DocIdMapping::from_new_id_to_old_id(vec![4, 1, 2]);
        assert_eq!(doc_id_mapping.num_old_doc_ids(), 5);
        let col = VecColumn::from(&[0u64, 3, 5, 10, 12, 16][..]);
        let multivalue_start_index = MultivalueStartIndex::new(
            &col, // 3, 2, 5, 2, 4
            &doc_id_mapping,
        );
        assert_eq!(multivalue_start_index.num_vals(), 4);
        assert_eq!(
            fastfield_codecs::iter_from_reader(multivalue_start_index.reader())
                .collect::<Vec<u64>>(),
            vec![0, 4, 6, 11]
        ); // 4, 2, 5
    }

    #[test]
    fn test_multivalue_get_vals() {
        let doc_id_mapping =
            DocIdMapping::from_new_id_to_old_id(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(doc_id_mapping.num_old_doc_ids(), 10);
        let col = VecColumn::from(&[0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55][..]);
        let multivalue_start_index = MultivalueStartIndex::new(&col, &doc_id_mapping);
        assert_eq!(
            fastfield_codecs::iter_from_reader(multivalue_start_index.reader())
                .collect::<Vec<u64>>(),
            vec![0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55]
        );
        assert_eq!(multivalue_start_index.num_vals(), 11);
        let mut multivalue_start_index_reader = multivalue_start_index.reader();
        assert_eq!(multivalue_start_index_reader.seek(3), 2);
        assert_eq!(multivalue_start_index_reader.seek(5), 5);
        assert_eq!(multivalue_start_index_reader.seek(8), 21);
        assert_eq!(multivalue_start_index_reader.seek(4), 3);
        assert_eq!(multivalue_start_index_reader.seek(0), 0);
        assert_eq!(multivalue_start_index_reader.seek(10), 55);
    }
}
