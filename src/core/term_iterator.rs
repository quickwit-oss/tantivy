use fst::Streamer;
use std::mem;
use std::collections::BinaryHeap;
use fst::map::Keys;
use schema::Field;
use schema::Term;
use core::SegmentReader;
use std::cmp::Ordering;


#[derive(PartialEq, Eq, Debug)]
struct HeapItem {
    term: Term,
    segment_ord: usize,
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &HeapItem) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &HeapItem) -> Ordering {
        (&other.term, &other.segment_ord).cmp(&(&self.term, &self.segment_ord))
    }
}

/// Given a list of sorted term streams,
/// returns an iterator over sorted unique terms.
///
/// The item yield is actually a pair with
/// - the term
/// - a slice with the ordinal of the segments containing
/// the terms.
pub struct TermIterator<'a> {
    key_streams: Vec<Keys<'a>>,
    heap: BinaryHeap<HeapItem>,
    // Buffer hosting the list of segment ordinals containing
    // the current term.
    current_term: Term,
    current_segment_ords: Vec<usize>,
}

impl<'a> TermIterator<'a> {
    fn new(key_streams: Vec<Keys<'a>>) -> TermIterator<'a> {
        let key_streams_len = key_streams.len();
        TermIterator {
            key_streams: key_streams,
            heap: BinaryHeap::new(),
            current_term: Term::from_field_text(Field(0), ""),
            current_segment_ords: (0..key_streams_len).collect(),
        }
    }

    /// Advance the term iterator to the next term.
    /// Returns true if there is indeed another term
    /// False if there is none.
    pub fn advance(&mut self) -> bool {
        self.advance_segments();
        if let Some(mut head) = self.heap.pop() {
            mem::swap(&mut self.current_term, &mut head.term);
            self.current_segment_ords.push(head.segment_ord);
            loop {
                match self.heap.peek() {
                    Some(&ref next_heap_it) if next_heap_it.term == self.current_term => {}
                    _ => {
                        break;
                    }
                }
                let next_heap_it = self.heap.pop().unwrap(); // safe : we peeked beforehand
                self.current_segment_ords.push(next_heap_it.segment_ord);
            }
            true
        } else {
            false
        }
    }


    /// Returns the current term.
    ///
    /// This method may be called
    /// iff advance() has been called before
    /// and "true" was returned.
    pub fn term(&self) -> &Term {
        &self.current_term
    }

    /// Returns the sorted list of segment ordinals
    /// that include the current term.
    ///
    /// This method may be called
    /// iff advance() has been called before
    /// and "true" was returned.
    pub fn segment_ords(&self) -> &[usize] {
        &self.current_segment_ords[..]
    }

    fn advance_segments(&mut self) {
        for segment_ord in self.current_segment_ords.drain(..) {
            if let Some(term) = self.key_streams[segment_ord].next() {
                self.heap
                    .push(HeapItem {
                              term: Term::from_bytes(term),
                              segment_ord: segment_ord,
                          });
            }
        }
    }
}

impl<'a, 'f> Streamer<'a> for TermIterator<'f> {
    type Item = &'a Term;

    fn next(&'a mut self) -> Option<Self::Item> {
        if self.advance() {
            Some(&self.current_term)
        } else {
            None
        }
    }
}

impl<'a> From<&'a [SegmentReader]> for TermIterator<'a> {
    fn from(segment_readers: &'a [SegmentReader]) -> TermIterator<'a> {
        TermIterator::new(segment_readers
                              .iter()
                              .map(|reader| reader.term_infos().keys())
                              .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{SchemaBuilder, Document, TEXT};
    use core::Index;

    #[test]
    fn test_term_iterator() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "a b d f");
                    index_writer.add_document(doc);
                }
                index_writer.commit().unwrap();
            }
            {
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "a b c d f");
                    index_writer.add_document(doc);
                }
                index_writer.commit().unwrap();
            }
            {
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "e f");
                    index_writer.add_document(doc);
                }
                index_writer.commit().unwrap();
            }
        }
        index.load_searchers().unwrap();
        let searcher = index.searcher();
        let mut term_it = searcher.terms();
        let mut terms = String::new();
        while let Some(term) = term_it.next() {
            terms.push_str(term.text());
        }
        assert_eq!(terms, "abcdef");
    }

}
