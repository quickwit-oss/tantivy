use std::collections::BinaryHeap;
use termdict::TermStreamer;
use std::cmp::Ordering;
use schema::Term;

pub struct HeapItem<'a> {
    pub streamer: TermStreamer<'a>,
    pub segment_ord: usize,
}

impl<'a> PartialEq for HeapItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.segment_ord == other.segment_ord
    }
}

impl<'a> Eq for HeapItem<'a> {}

impl<'a> PartialOrd for HeapItem<'a> {
    fn partial_cmp(&self, other: &HeapItem<'a>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for HeapItem<'a> {
    fn cmp(&self, other: &HeapItem<'a>) -> Ordering {
        (&other.streamer.key(), &other.segment_ord).cmp(&(&self.streamer.key(), &self.segment_ord))
    }
}

/// Given a list of sorted term streams,
/// returns an iterator over sorted unique terms.
///
/// The item yield is actually a pair with
/// - the term
/// - a slice with the ordinal of the segments containing
/// the terms.
pub struct TermMerger<'a> {
    heap: BinaryHeap<HeapItem<'a>>,
    current_streamers: Vec<HeapItem<'a>>,
}

impl<'a> TermMerger<'a> {
    /// Stream of merged term dictionary
    ///
    ///
    pub fn new(streams: Vec<TermStreamer<'a>>) -> TermMerger<'a> {
        TermMerger {
            heap: BinaryHeap::new(),
            current_streamers: streams
                .into_iter()
                .enumerate()
                .map(|(ord, streamer)| HeapItem {
                    streamer,
                    segment_ord: ord,
                })
                .collect(),
        }
    }

    fn advance_segments(&mut self) {
        let streamers = &mut self.current_streamers;
        let heap = &mut self.heap;
        for mut heap_item in streamers.drain(..) {
            if heap_item.streamer.advance() {
                heap.push(heap_item);
            }
        }
    }

    /// Advance the term iterator to the next term.
    /// Returns true if there is indeed another term
    /// False if there is none.
    #[allow(while_let_loop)]
    pub fn advance(&mut self) -> bool {
        self.advance_segments();
        if let Some(head) = self.heap.pop() {
            self.current_streamers.push(head);
            loop {
                if let Some(next_streamer) = self.heap.peek() {
                    if self.current_streamers[0].streamer.key() != next_streamer.streamer.key() {
                        break;
                    }
                } else {
                    break;
                } // no more streamer.
                let next_heap_it = self.heap.pop().unwrap(); // safe : we peeked beforehand
                self.current_streamers.push(next_heap_it);
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
    pub fn key(&self) -> &[u8] {
        self.current_streamers[0].streamer.key()
    }

    /// Returns the sorted list of segment ordinals
    /// that include the current term.
    ///
    /// This method may be called
    /// iff advance() has been called before
    /// and "true" was returned.
    pub fn current_kvs(&self) -> &[HeapItem<'a>] {
        &self.current_streamers[..]
    }

    /// Iterates through terms
    #[allow(should_implement_trait)]
    pub fn next(&mut self) -> Option<Term<&[u8]>> {
        if self.advance() {
            Some(Term::wrap(self.current_streamers[0].streamer.key()))
        } else {
            None
        }
    }
}
