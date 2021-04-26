use crate::termdict::TermOrdinal;
use crate::termdict::TermStreamer;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

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

    pub(crate) fn matching_segments<'b: 'a>(
        &'b self,
    ) -> impl 'b + Iterator<Item = (usize, TermOrdinal)> {
        self.current_streamers
            .iter()
            .map(|heap_item| (heap_item.segment_ord, heap_item.streamer.term_ord()))
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
    pub fn advance(&mut self) -> bool {
        self.advance_segments();
        if let Some(head) = self.heap.pop() {
            self.current_streamers.push(head);
            while let Some(next_streamer) = self.heap.peek() {
                if self.current_streamers[0].streamer.key() != next_streamer.streamer.key() {
                    break;
                }
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
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use super::TermMerger;
    use crate::directory::FileSlice;
    use crate::postings::TermInfo;
    use crate::termdict::{TermDictionary, TermDictionaryBuilder};
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use test::{self, Bencher};

    fn make_term_info(term_ord: u64) -> TermInfo {
        let offset = |term_ord: u64| (term_ord * 100 + term_ord * term_ord) as usize;
        TermInfo {
            doc_freq: term_ord as u32,
            postings_range: offset(term_ord)..offset(term_ord + 1),
            positions_idx: offset(term_ord) as u64 * 2u64,
        }
    }

    /// Create a dictionary of random strings.
    fn rand_dict(size: usize) -> crate::Result<TermDictionary> {
        let buffer: Vec<u8> = {
            let mut terms = vec![];
            for _i in 0..size {
                let rand_string: String = thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .map(char::from)
                    .collect();
                terms.push(rand_string);
            }
            terms.sort();

            let mut term_dictionary_builder = TermDictionaryBuilder::create(Vec::new())?;
            for i in 0..size {
                term_dictionary_builder.insert(terms[i].as_bytes(), &make_term_info(i as u64))?;
            }
            term_dictionary_builder.finish()?
        };
        let file = FileSlice::from(buffer);
        TermDictionary::open(file)
    }

    #[bench]
    fn bench_termmerger_baseline(b: &mut Bencher) -> crate::Result<()> {
        let dict1 = rand_dict(100000)?;
        let dict2 = rand_dict(100000)?;
        b.iter(|| {
            let stream1 = dict1.stream().unwrap();
            let stream2 = dict2.stream().unwrap();
            let mut merger = TermMerger::new(vec![stream1, stream2]);
            let mut count = 0;
            while merger.advance() {
                count += 1;
            }
            count
        });
        Ok(())
    }
}
