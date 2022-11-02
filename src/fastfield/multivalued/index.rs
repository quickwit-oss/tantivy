use std::ops::Range;
use std::sync::Arc;

use fastfield_codecs::Column;

use crate::DocId;

#[derive(Clone)]
/// Index to resolve value range for given doc_id.
/// Starts at 0.
pub struct MultiValueIndex {
    idx: Arc<dyn Column<u64>>,
}

impl MultiValueIndex {
    pub(crate) fn new(idx: Arc<dyn Column<u64>>) -> Self {
        Self { idx }
    }

    /// Returns `[start, end)`, such that the values associated with
    /// the given document are `start..end`.
    #[inline]
    pub(crate) fn range(&self, doc: DocId) -> Range<u32> {
        let start = self.idx.get_val(doc) as u32;
        let end = self.idx.get_val(doc + 1) as u32;
        start..end
    }

    /// returns the num of values associated with a doc_id
    pub(crate) fn num_vals_for_doc(&self, doc: DocId) -> u32 {
        let range = self.range(doc);
        range.end - range.start
    }

    /// Returns the overall number of values in this field.
    #[inline]
    pub fn total_num_vals(&self) -> u64 {
        self.idx.max_value()
    }

    /// Returns the number of documents in the index.
    #[inline]
    pub fn num_docs(&self) -> u32 {
        self.idx.num_vals() - 1
    }

    /// Converts a list of positions of values in a 1:n index to the corresponding list of DocIds.
    ///
    /// Since there is no index for value pos -> docid, but docid -> value pos range, we scan the
    /// index.
    ///
    /// Correctness: positions needs to be sorted. idx_reader needs to contain monotonically
    /// increasing positions.
    ///
    /// TODO: Instead of a linear scan we can employ a exponential search into binary search to
    /// match a docid to its value position.
    pub(crate) fn positions_to_docids(&self, docid_start: u32, positions: &[u32]) -> Vec<DocId> {
        let mut docs = vec![];
        let mut cur_doc = docid_start;
        let mut last_doc = None;

        for pos in positions {
            loop {
                let end = self.idx.get_val(cur_doc + 1) as u32;
                if end > *pos {
                    // avoid duplicates
                    if Some(cur_doc) == last_doc {
                        break;
                    }
                    docs.push(cur_doc);
                    last_doc = Some(cur_doc);
                    break;
                }
                cur_doc += 1;
            }
        }

        docs
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use fastfield_codecs::IterColumn;

    use crate::fastfield::MultiValueIndex;

    #[test]
    fn test_positions_to_docid() {
        let offsets = vec![0, 10, 12, 15, 22, 23]; // docid values are [0..10, 10..12, 12..15, etc.]
        let column = IterColumn::from(offsets.into_iter());
        let index = MultiValueIndex::new(Arc::new(column));
        assert_eq!(index.num_docs(), 5);
        {
            let positions = vec![10u32, 11, 15, 20, 21, 22];

            assert_eq!(index.positions_to_docids(0, &positions), vec![1, 3, 4]);
            assert_eq!(index.positions_to_docids(1, &positions), vec![1, 3, 4]);
            assert_eq!(index.positions_to_docids(0, &[9]), vec![0]);
            assert_eq!(index.positions_to_docids(1, &[10]), vec![1]);
            assert_eq!(index.positions_to_docids(1, &[11]), vec![1]);
            assert_eq!(index.positions_to_docids(2, &[12]), vec![2]);
            assert_eq!(index.positions_to_docids(2, &[12, 14]), vec![2]);
            assert_eq!(index.positions_to_docids(2, &[12, 14, 15]), vec![2, 3]);
        }
    }
}
