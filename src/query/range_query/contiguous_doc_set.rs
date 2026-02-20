use crate::{DocId, DocSet, COLLECT_BLOCK_BUFFER_LEN, TERMINATED};

/// A `DocSet` representing a contiguous range of DocIds `[start, end)`.
///
/// This is used by the sorted-index range query optimization. When an index is sorted
/// by the query field, binary search on the column identifies the first and last
/// matching DocIds. Since documents are contiguous in a sorted segment, we can
/// represent the result as a simple integer range instead of scanning the column.
pub(crate) struct ContiguousDocSet {
    start: DocId,
    end: DocId,
    current: DocId,
}

impl ContiguousDocSet {
    /// Creates a new `ContiguousDocSet` for the half-open range `[start, end)`.
    ///
    /// If `start >= end`, the docset is immediately empty (TERMINATED).
    pub fn new(start: DocId, end: DocId) -> Self {
        if start >= end {
            return Self {
                start,
                end,
                current: TERMINATED,
            };
        }
        Self {
            start,
            end,
            current: start,
        }
    }
}

impl DocSet for ContiguousDocSet {
    #[inline]
    fn advance(&mut self) -> DocId {
        if self.current == TERMINATED {
            return TERMINATED;
        }
        self.current += 1;
        if self.current >= self.end {
            self.current = TERMINATED;
        }
        self.current
    }

    #[inline]
    fn seek(&mut self, target: DocId) -> DocId {
        if target >= self.end {
            self.current = TERMINATED;
            return TERMINATED;
        }
        if target > self.current {
            self.current = target;
        }
        self.current
    }

    fn fill_buffer(&mut self, buffer: &mut [DocId; COLLECT_BLOCK_BUFFER_LEN]) -> usize {
        if self.current == TERMINATED {
            return 0;
        }
        let remaining = (self.end - self.current) as usize;
        let count = remaining.min(COLLECT_BLOCK_BUFFER_LEN);
        for (i, slot) in buffer[..count].iter_mut().enumerate() {
            *slot = self.current + i as DocId;
        }
        self.current += count as DocId;
        if self.current >= self.end {
            self.current = TERMINATED;
        }
        count
    }

    #[inline]
    fn doc(&self) -> DocId {
        self.current
    }

    fn size_hint(&self) -> u32 {
        self.end.saturating_sub(self.start)
    }

    fn cost(&self) -> u64 {
        self.size_hint() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::docset::SeekDangerResult;

    #[test]
    fn contiguous_doc_set_empty_range_returns_terminated() {
        let ds = ContiguousDocSet::new(5, 5);
        assert_eq!(ds.doc(), TERMINATED);

        let ds2 = ContiguousDocSet::new(10, 5);
        assert_eq!(ds2.doc(), TERMINATED);
    }

    #[test]
    fn contiguous_doc_set_single_doc_lifecycle() {
        let mut ds = ContiguousDocSet::new(3, 4);
        assert_eq!(ds.doc(), 3);
        assert_eq!(ds.advance(), TERMINATED);
        assert_eq!(ds.doc(), TERMINATED);
    }

    #[test]
    fn contiguous_doc_set_basic_advance_sequence() {
        let mut ds = ContiguousDocSet::new(0, 5);
        assert_eq!(ds.doc(), 0);
        assert_eq!(ds.advance(), 1);
        assert_eq!(ds.advance(), 2);
        assert_eq!(ds.advance(), 3);
        assert_eq!(ds.advance(), 4);
        assert_eq!(ds.advance(), TERMINATED);
    }

    #[test]
    fn contiguous_doc_set_advance_after_terminated_is_idempotent() {
        let mut ds = ContiguousDocSet::new(0, 3);
        assert_eq!(ds.advance(), 1);
        assert_eq!(ds.advance(), 2);
        assert_eq!(ds.advance(), TERMINATED);
        assert_eq!(ds.advance(), TERMINATED);
        assert_eq!(ds.advance(), TERMINATED);
        assert_eq!(ds.doc(), TERMINATED);
    }

    #[test]
    fn contiguous_doc_set_seek_before_within_after_range() {
        // Seek before range: clamps to start
        let mut ds = ContiguousDocSet::new(5, 10);
        assert_eq!(ds.seek(2), 5);
        assert_eq!(ds.doc(), 5);

        // Seek within range: returns exact target
        let mut ds = ContiguousDocSet::new(0, 10);
        assert_eq!(ds.seek(3), 3);
        assert_eq!(ds.doc(), 3);

        // Seek past range: TERMINATED
        let mut ds = ContiguousDocSet::new(5, 10);
        assert_eq!(ds.seek(15), TERMINATED);
        assert_eq!(ds.doc(), TERMINATED);

        // Seek exact end (exclusive): TERMINATED
        let mut ds = ContiguousDocSet::new(5, 10);
        assert_eq!(ds.seek(10), TERMINATED);
        assert_eq!(ds.doc(), TERMINATED);
    }

    #[test]
    fn contiguous_doc_set_seek_then_advance_continues_correctly() {
        let mut ds = ContiguousDocSet::new(5, 10);
        assert_eq!(ds.seek(7), 7);
        assert_eq!(ds.advance(), 8);
        assert_eq!(ds.advance(), 9);
        assert_eq!(ds.advance(), TERMINATED);
    }

    #[test]
    fn contiguous_doc_set_fill_buffer_cases() {
        let cases: [(&str, u32, u32, usize); 3] = [
            ("full_block", 0, 100, COLLECT_BLOCK_BUFFER_LEN),
            ("partial_block", 0, 10, 10),
            ("empty", 5, 5, 0),
        ];
        for (name, start, end, expected_count) in cases {
            let mut ds = ContiguousDocSet::new(start, end);
            let mut buffer = [0u32; COLLECT_BLOCK_BUFFER_LEN];
            let count = ds.fill_buffer(&mut buffer);
            assert_eq!(count, expected_count, "{name}");
            for (i, &val) in buffer[..count].iter().enumerate() {
                assert_eq!(val, start + i as u32, "{name}");
            }
        }
    }

    #[test]
    fn contiguous_doc_set_fill_buffer_multiple_calls_are_sequential() {
        let mut ds = ContiguousDocSet::new(100, 200);
        let mut all_docs = Vec::new();
        loop {
            let mut buffer = [0u32; COLLECT_BLOCK_BUFFER_LEN];
            let count = ds.fill_buffer(&mut buffer);
            if count == 0 {
                break;
            }
            all_docs.extend_from_slice(&buffer[..count]);
        }
        let expected: Vec<u32> = (100..200).collect();
        assert_eq!(all_docs, expected);
    }

    #[test]
    fn contiguous_doc_set_size_hint_equals_range_len() {
        let ds = ContiguousDocSet::new(100, 600);
        assert_eq!(ds.size_hint(), 500);
    }

    #[test]
    fn contiguous_doc_set_size_hint_stable_after_iteration() {
        let mut ds = ContiguousDocSet::new(0, 5);
        assert_eq!(ds.size_hint(), 5);
        ds.advance();
        ds.advance();
        assert_eq!(ds.size_hint(), 5);
        // Exhaust the docset.
        while ds.advance() != TERMINATED {}
        assert_eq!(ds.size_hint(), 5);
    }

    #[test]
    fn contiguous_doc_set_cost_cases() {
        let cases: [(&str, u32, u32, u64); 3] = [
            ("normal_range", 100, 600, 500),
            ("inverted_range", 10, 5, 0),
            ("equal_range", 5, 5, 0),
        ];
        for (name, start, end, expected_cost) in cases {
            let ds = ContiguousDocSet::new(start, end);
            assert_eq!(ds.cost(), expected_cost, "{name}");
        }
    }

    #[test]
    fn contiguous_doc_set_seek_danger_found() {
        let mut ds = ContiguousDocSet::new(0, 10);
        assert_eq!(ds.seek_danger(5), SeekDangerResult::Found);
        assert_eq!(ds.doc(), 5);
    }

    #[test]
    fn contiguous_doc_set_seek_danger_not_found() {
        let mut ds = ContiguousDocSet::new(0, 10);
        assert_eq!(
            ds.seek_danger(15),
            SeekDangerResult::SeekLowerBound(TERMINATED)
        );
    }
}
