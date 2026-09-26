//! Merge per-segment postings into increasing mapped document id order.
//!
//! One cursor is kept per input segment. Positions are read only for the
//! current document, into a buffer supplied by the caller.

use std::cmp::{Ordering, Reverse};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use crate::docset::{DocSet, TERMINATED};
use crate::postings::{Postings, SegmentPostings};
use crate::DocId;

/// Skip deleted or filtered documents, leaving the cursor on the next mapped posting.
pub(crate) fn next_mapped_doc(
    postings: &mut SegmentPostings,
    mapping: &[Option<DocId>],
) -> Option<DocId> {
    while postings.doc() != TERMINATED {
        if let Some(doc) = mapping[postings.doc() as usize] {
            return Some(doc);
        }
        postings.advance();
    }
    None
}

/// One segment's postings, positioned on a document that survives the merge mapping.
struct MappedPostings<'a> {
    postings: SegmentPostings,
    mapping: &'a [Option<DocId>],
    current_doc: DocId,
}

impl<'a> MappedPostings<'a> {
    fn new(mut postings: SegmentPostings, mapping: &'a [Option<DocId>]) -> Option<Self> {
        let current_doc = next_mapped_doc(&mut postings, mapping)?;
        Some(Self {
            postings,
            mapping,
            current_doc,
        })
    }

    /// Move to the next mapped doc. `false` when this segment is exhausted.
    fn advance(&mut self) -> bool {
        self.postings.advance();
        match next_mapped_doc(&mut self.postings, self.mapping) {
            Some(doc) => {
                debug_assert!(
                    doc > self.current_doc,
                    "merge mapping must preserve per-segment order"
                );
                self.current_doc = doc;
                true
            }
            None => false,
        }
    }
}

impl Ord for MappedPostings<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.current_doc.cmp(&other.current_doc)
    }
}

impl PartialOrd for MappedPostings<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MappedPostings<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.current_doc == other.current_doc
    }
}

impl Eq for MappedPostings<'_> {}

/// Streams postings from several segments in increasing mapped document id.
///
/// Built once per term for a shuffled merge. Memory is one [`SegmentPostings`]
/// cursor per input segment that still contains the term, plus the positions
/// buffer the caller passes to [`Self::positions`].
pub(crate) struct PostingsMerger<'a> {
    /// Max-heap of reversed cursors, so the smallest mapped doc sits on top.
    ///
    /// Cursors are boxed so a sift swaps a pointer. `MappedPostings` embeds the
    /// postings block decoders, which are about 2KB.
    heap: BinaryHeap<Reverse<Box<MappedPostings<'a>>>>,
    /// `advance` reports the heap's first doc before moving any cursor.
    primed: bool,
}

impl<'a> PostingsMerger<'a> {
    /// `segments` are `(segment_ord, postings)` for segments that contain the term.
    /// `doc_id_map[segment_ord][local_doc]` is the mapped doc, or `None` when that
    /// document is deleted or filtered out.
    pub(crate) fn new(
        segments: impl IntoIterator<Item = (usize, SegmentPostings)>,
        doc_id_map: &'a [Vec<Option<DocId>>],
    ) -> Self {
        let segments = segments.into_iter();
        let (lower, _) = segments.size_hint();
        let mut heap = BinaryHeap::with_capacity(lower);
        for (segment_ord, postings) in segments {
            if let Some(cursor) = MappedPostings::new(postings, &doc_id_map[segment_ord]) {
                heap.push(Reverse(Box::new(cursor)));
            }
        }
        Self {
            heap,
            primed: false,
        }
    }

    /// Advance to the next mapped document.
    ///
    /// Returns `true` when a document is available. [`Self::doc`], [`Self::term_freq`],
    /// and [`Self::positions`] may be called only after this returns `true`.
    pub(crate) fn advance(&mut self) -> bool {
        if !self.primed {
            self.primed = true;
            return !self.heap.is_empty();
        }
        let Some(previous) = self.heap.peek().map(|cursor| cursor.0.current_doc) else {
            return false;
        };
        {
            let mut top = self.heap.peek_mut().expect("heap top exists");
            if !top.0.advance() {
                PeekMut::pop(top);
            }
        }
        if let Some(top) = self.heap.peek() {
            debug_assert!(
                top.0.current_doc > previous,
                "mapped doc ids must be strictly increasing"
            );
        }
        !self.heap.is_empty()
    }

    pub(crate) fn doc(&self) -> DocId {
        self.heap
            .peek()
            .expect("advance() returned true")
            .0
            .current_doc
    }

    pub(crate) fn term_freq(&self) -> u32 {
        self.heap
            .peek()
            .expect("advance() returned true")
            .0
            .postings
            .term_freq()
    }

    /// Fill `output` with the current document's positions.
    pub(crate) fn positions(&mut self, output: &mut Vec<u32>) {
        self.heap
            .peek_mut()
            .expect("advance() returned true")
            .0
            .postings
            .positions(output);
    }
}

#[cfg(test)]
mod tests {
    use super::PostingsMerger;
    use crate::postings::SegmentPostings;
    use crate::DocId;

    fn collect(merger: &mut PostingsMerger<'_>) -> Vec<(DocId, u32)> {
        let mut docs = Vec::new();
        let mut positions = vec![7, 7, 7];
        while merger.advance() {
            merger.positions(&mut positions);
            assert!(positions.is_empty());
            docs.push((merger.doc(), merger.term_freq()));
        }
        assert!(!merger.advance());
        docs
    }

    #[test]
    fn test_merges_segments_skipping_deletes() {
        // Segment 2 is fully deleted. Segment 3 has no postings, so its map is never read.
        let doc_id_map = vec![
            vec![Some(1), None, Some(3)],
            vec![Some(0), Some(2)],
            vec![None, None],
            Vec::new(),
        ];
        let segments = [
            (
                0,
                SegmentPostings::create_from_docs_and_tfs(&[(0, 1), (1, 2), (2, 3)], None),
            ),
            (
                1,
                SegmentPostings::create_from_docs_and_tfs(&[(0, 4), (1, 5)], None),
            ),
            (
                2,
                SegmentPostings::create_from_docs_and_tfs(&[(0, 9), (1, 9)], None),
            ),
            (3, SegmentPostings::empty()),
        ];
        let mut merger = PostingsMerger::new(segments, &doc_id_map);
        assert_eq!(collect(&mut merger), vec![(0, 4), (1, 1), (2, 5), (3, 3)]);
    }

    #[test]
    fn test_empty_term_yields_nothing() {
        let doc_id_map = vec![vec![Some(0)]];
        let mut merger = PostingsMerger::new([(0, SegmentPostings::empty())], &doc_id_map);
        assert_eq!(collect(&mut merger), Vec::<(DocId, u32)>::new());
    }

    #[test]
    fn test_crosses_postings_block_boundary() {
        const N: u32 = 200;
        let seg0: Vec<(u32, u32)> = (0..N).map(|doc| (doc, doc + 1)).collect();
        let seg1: Vec<(u32, u32)> = (0..N).map(|doc| (doc, 1_000 + doc)).collect();
        let map0: Vec<Option<DocId>> = (0..N)
            .map(|doc| if doc % 10 == 0 { None } else { Some(doc * 2) })
            .collect();
        let map1: Vec<Option<DocId>> = (0..N).map(|doc| Some(doc * 2 + 1)).collect();
        let doc_id_map = vec![map0, map1];
        let mut merger = PostingsMerger::new(
            [
                (0, SegmentPostings::create_from_docs_and_tfs(&seg0, None)),
                (1, SegmentPostings::create_from_docs_and_tfs(&seg1, None)),
            ],
            &doc_id_map,
        );

        let mut expected = Vec::new();
        for doc in 0..N {
            if doc % 10 != 0 {
                expected.push((doc * 2, doc + 1));
            }
            expected.push((doc * 2 + 1, 1_000 + doc));
        }
        expected.sort_unstable();
        assert_eq!(collect(&mut merger), expected);
    }
}
