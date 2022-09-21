use fastfield_codecs::{Column, ColumnReader};
use tantivy_bitpacker::BlockedBitpacker;

use crate::indexer::doc_id_mapping::DocIdMapping;
use crate::DocId;

#[derive(Clone)]
pub(crate) struct WriterFastFieldColumn<'map, 'bitp> {
    pub(crate) doc_id_mapping_opt: Option<&'map DocIdMapping>,
    pub(crate) vals: &'bitp BlockedBitpacker,
    pub(crate) min_value: u64,
    pub(crate) max_value: u64,
    pub(crate) num_vals: u64,
}

impl<'map, 'bitp> Column for WriterFastFieldColumn<'map, 'bitp> {
    /// Return the value associated to the given doc.
    ///
    /// Whenever possible use the Iterator passed to the fastfield creation instead, for performance
    /// reasons.
    ///
    /// # Panics
    ///
    /// May panic if `doc` is greater than the index.
    fn get_val(&self, doc: u64) -> u64 {
        if let Some(doc_id_map) = self.doc_id_mapping_opt {
            self.vals
                .get(doc_id_map.get_old_doc_id(doc as u32) as usize) // consider extra
                                                                     // FastFieldReader wrapper for
                                                                     // non doc_id_map
        } else {
            self.vals.get(doc as usize)
        }
    }

    fn reader(&self) -> Box<dyn ColumnReader + '_> {
        if let Some(doc_id_mapping) = self.doc_id_mapping_opt {
            Box::new(RemappedColumnReader {
                doc_id_mapping,
                vals: self.vals,
                idx: u64::MAX,
                len: doc_id_mapping.num_new_doc_ids() as u64,
            })
        } else {
            Box::new(BitpackedColumnReader {
                vals: self.vals,
                idx: u64::MAX,
                len: self.num_vals,
            })
        }
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

struct RemappedColumnReader<'a> {
    doc_id_mapping: &'a DocIdMapping,
    vals: &'a BlockedBitpacker,
    idx: u64,
    len: u64,
}

impl<'a> ColumnReader for RemappedColumnReader<'a> {
    fn seek(&mut self, target_idx: u64) -> u64 {
        assert!(target_idx < self.len);
        self.idx = target_idx;
        self.get()
    }

    fn advance(&mut self) -> bool {
        self.idx = self.idx.wrapping_add(1);
        self.idx < self.len
    }

    fn get(&self) -> u64 {
        let old_doc_id: DocId = self.doc_id_mapping.get_old_doc_id(self.idx as DocId);
        self.vals.get(old_doc_id as usize)
    }
}

struct BitpackedColumnReader<'a> {
    vals: &'a BlockedBitpacker,
    idx: u64,
    len: u64,
}

impl<'a> ColumnReader for BitpackedColumnReader<'a> {
    fn seek(&mut self, target_idx: u64) -> u64 {
        assert!(target_idx < self.len);
        self.idx = target_idx;
        self.get()
    }

    fn advance(&mut self) -> bool {
        self.idx = self.idx.wrapping_add(1);
        self.idx < self.len
    }

    fn get(&self) -> u64 {
        self.vals.get(self.idx as usize)
    }
}
