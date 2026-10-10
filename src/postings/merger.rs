//! Merge per-segment postings into increasing mapped document id order.

use std::cmp::Reverse;
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use crate::docset::{DocSet, TERMINATED};
use crate::postings::{Postings, SegmentPostings};
use crate::DocId;

/// Skip to the next posting whose document is present in `mapping`.
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

struct MappedPostings<'a> {
    postings: SegmentPostings,
    mapping: &'a [Option<DocId>],
}

impl MappedPostings<'_> {
    fn advance(&mut self) -> Option<DocId> {
        self.postings.advance();
        next_mapped_doc(&mut self.postings, self.mapping)
    }
}

/// Streams postings from several segments in increasing mapped document id.
///
/// Create once per field and call [`Self::reset`] for each term.
pub(crate) struct PostingsMerger<'a> {
    doc_id_map: &'a [Vec<Option<DocId>>],
    cursors: Vec<MappedPostings<'a>>,
    /// Min-heap of `(current mapped doc, index into cursors)`.
    heap: BinaryHeap<Reverse<(DocId, usize)>>,
    primed: bool,
}

impl<'a> PostingsMerger<'a> {
    /// `doc_id_map[segment_ord][local_doc]` is the mapped doc, or `None` if dropped.
    pub(crate) fn new(doc_id_map: &'a [Vec<Option<DocId>>]) -> Self {
        Self {
            doc_id_map,
            cursors: Vec::new(),
            heap: BinaryHeap::new(),
            primed: false,
        }
    }

    /// Start merging a new term from `(segment_ord, postings)` pairs.
    pub(crate) fn reset(&mut self, segments: impl IntoIterator<Item = (usize, SegmentPostings)>) {
        self.cursors.clear();
        self.heap.clear();
        self.primed = false;
        let doc_id_map = self.doc_id_map;
        for (segment_ord, mut postings) in segments {
            let mapping = &doc_id_map[segment_ord][..];
            if let Some(doc) = next_mapped_doc(&mut postings, mapping) {
                self.heap.push(Reverse((doc, self.cursors.len())));
                self.cursors.push(MappedPostings { postings, mapping });
            }
        }
    }

    /// Advance to the next document. Must return `true` before the accessors are called.
    pub(crate) fn advance(&mut self) -> bool {
        if !self.primed {
            self.primed = true;
            return !self.heap.is_empty();
        }
        let previous = {
            let Some(mut top) = self.heap.peek_mut() else {
                return false;
            };
            let Reverse((previous, cursor_ord)) = *top;
            match self.cursors[cursor_ord].advance() {
                Some(doc) => {
                    debug_assert!(
                        doc > previous,
                        "merge mapping must preserve per-segment order"
                    );
                    *top = Reverse((doc, cursor_ord));
                }
                None => {
                    PeekMut::pop(top);
                }
            }
            previous
        };
        if let Some(Reverse((next, _))) = self.heap.peek() {
            debug_assert!(
                *next > previous,
                "mapped doc ids must be strictly increasing"
            );
        }
        !self.heap.is_empty()
    }

    fn current(&self) -> (DocId, usize) {
        self.heap.peek().expect("advance() returned true").0
    }

    pub(crate) fn doc(&self) -> DocId {
        self.current().0
    }

    pub(crate) fn term_freq(&self) -> u32 {
        self.cursors[self.current().1].postings.term_freq()
    }

    pub(crate) fn positions(&mut self, output: &mut Vec<u32>) {
        let cursor_ord = self.current().1;
        self.cursors[cursor_ord].postings.positions(output);
    }
}

#[cfg(test)]
mod tests {
    use super::PostingsMerger;
    use crate::postings::SegmentPostings;
    use crate::schema::{IndexRecordOption, Schema, TEXT};
    use crate::{DocId, Index, IndexWriter, Term};

    fn collect(merger: &mut PostingsMerger<'_>) -> Vec<(DocId, u32, Vec<u32>)> {
        let mut docs = Vec::new();
        let mut positions = vec![7, 7, 7];
        while merger.advance() {
            merger.positions(&mut positions);
            docs.push((merger.doc(), merger.term_freq(), positions.clone()));
        }
        assert!(!merger.advance());
        docs
    }

    fn without_positions(docs: &[(DocId, u32)]) -> Vec<(DocId, u32, Vec<u32>)> {
        docs.iter()
            .map(|&(doc, tf)| (doc, tf, Vec::new()))
            .collect()
    }

    /// Postings with positions for each token, one segment per inner slice.
    fn postings_with_positions(
        segments: &[&[&str]],
        tokens: &[&str],
    ) -> crate::Result<Vec<Vec<(usize, SegmentPostings)>>> {
        let mut schema_builder = Schema::builder();
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let mut readers = Vec::new();
        for docs in segments {
            let index = Index::create_in_ram(schema.clone());
            let mut writer: IndexWriter = index.writer_for_tests()?;
            for body in *docs {
                writer.add_document(doc!(text => *body))?;
            }
            writer.commit()?;
            let searcher = index.reader()?.searcher();
            assert_eq!(searcher.segment_readers().len(), 1);
            readers.push(searcher.segment_reader(0).inverted_index(text)?);
        }
        let mut terms = Vec::new();
        for token in tokens {
            let term = Term::from_field_text(text, token);
            let mut postings = Vec::new();
            for (segment_ord, reader) in readers.iter().enumerate() {
                if let Some(segment_postings) =
                    reader.read_postings(&term, IndexRecordOption::WithFreqsAndPositions)?
                {
                    postings.push((segment_ord, segment_postings));
                }
            }
            terms.push(postings);
        }
        Ok(terms)
    }

    #[test]
    fn test_merges_segments_skipping_deletes() {
        // Segment 2 is fully deleted and segment 3 has no postings.
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
        let mut merger = PostingsMerger::new(&doc_id_map);
        merger.reset(segments);
        assert_eq!(
            collect(&mut merger),
            without_positions(&[(0, 4), (1, 1), (2, 5), (3, 3)])
        );
    }

    #[test]
    fn test_empty_term_yields_nothing() {
        let doc_id_map = vec![vec![Some(0)]];
        let mut merger = PostingsMerger::new(&doc_id_map);
        merger.reset([(0, SegmentPostings::empty())]);
        assert_eq!(collect(&mut merger), Vec::new());
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
        let mut merger = PostingsMerger::new(&doc_id_map);
        merger.reset([
            (0, SegmentPostings::create_from_docs_and_tfs(&seg0, None)),
            (1, SegmentPostings::create_from_docs_and_tfs(&seg1, None)),
        ]);

        let mut expected = Vec::new();
        for doc in 0..N {
            if doc % 10 != 0 {
                expected.push((doc * 2, doc + 1));
            }
            expected.push((doc * 2 + 1, 1_000 + doc));
        }
        expected.sort_unstable();
        assert_eq!(collect(&mut merger), without_positions(&expected));
    }

    #[test]
    fn test_positions_follow_their_document() -> crate::Result<()> {
        let segments: [&[&str]; 2] = [&["a b a", "b", "b a b a a"], &["a", "b b a", "a b", "b"]];
        let doc_id_map = vec![
            vec![Some(1), None, Some(4)],
            vec![Some(0), Some(2), None, Some(3)],
        ];
        let mut terms = postings_with_positions(&segments, &["a", "b"])?.into_iter();
        let mut merger = PostingsMerger::new(&doc_id_map);

        merger.reset(terms.next().unwrap());
        assert_eq!(
            collect(&mut merger),
            vec![
                (0, 1, vec![0]),
                (1, 2, vec![0, 2]),
                (2, 1, vec![2]),
                (4, 3, vec![1, 3, 4]),
            ]
        );

        merger.reset(terms.next().unwrap());
        assert_eq!(
            collect(&mut merger),
            vec![
                (1, 1, vec![1]),
                (2, 2, vec![0, 1]),
                (3, 1, vec![0]),
                (4, 2, vec![0, 2]),
            ]
        );
        Ok(())
    }

    #[test]
    fn test_reset_discards_unfinished_term() -> crate::Result<()> {
        let segments: [&[&str]; 2] = [&["a b", "a"], &["b a", "a b", "a"]];
        let doc_id_map = vec![vec![Some(0), Some(2)], vec![Some(1), Some(3), Some(4)]];
        let mut terms = postings_with_positions(&segments, &["a", "b"])?.into_iter();
        let mut merger = PostingsMerger::new(&doc_id_map);

        merger.reset(terms.next().unwrap());
        assert!(merger.advance());
        assert_eq!(merger.doc(), 0);

        merger.reset(terms.next().unwrap());
        assert_eq!(
            collect(&mut merger),
            vec![(0, 1, vec![1]), (1, 1, vec![0]), (3, 1, vec![1])]
        );
        Ok(())
    }
}
