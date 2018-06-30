use super::stacker::{ExpUnrolledLinkedList, MemoryArena};
use postings::FieldSerializer;
use std::{self, io};
use DocId;

const EMPTY_ARRAY: [u32; 0] = [0u32; 0];
const POSITION_END: u32 = std::u32::MAX;

/// Recorder is in charge of recording relevant information about
/// the presence of a term in a document.
///
/// Depending on the `TextIndexingOptions` associated to the
/// field, the recorder may records
///   * the document frequency
///   * the document id
///   * the term frequency
///   * the term positions
pub trait Recorder: Copy {
    ///
    fn new(heap: &mut MemoryArena) -> Self;
    /// Returns the current document
    fn current_doc(&self) -> u32;
    /// Starts recording information about a new document
    /// This method shall only be called if the term is within the document.
    fn new_doc(&mut self, doc: DocId, heap: &mut MemoryArena);
    /// Record the position of a term. For each document,
    /// this method will be called `term_freq` times.
    fn record_position(&mut self, position: u32, heap: &mut MemoryArena);
    /// Close the document. It will help record the term frequency.
    fn close_doc(&mut self, heap: &mut MemoryArena);
    /// Pushes the postings information to the serializer.
    fn serialize(&self, serializer: &mut FieldSerializer, heap: &MemoryArena) -> io::Result<()>;
}

/// Only records the doc ids
#[derive(Clone, Copy)]
pub struct NothingRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
}

impl Recorder for NothingRecorder {
    fn new(heap: &mut MemoryArena) -> Self {
        NothingRecorder {
            stack: ExpUnrolledLinkedList::new(heap),
            current_doc: u32::max_value(),
        }
    }

    fn current_doc(&self) -> DocId {
        self.current_doc
    }

    fn new_doc(&mut self, doc: DocId, heap: &mut MemoryArena) {
        self.current_doc = doc;
        self.stack.push(doc, heap);
    }

    fn record_position(&mut self, _position: u32, _heap: &mut MemoryArena) {}

    fn close_doc(&mut self, _heap: &mut MemoryArena) {}

    fn serialize(&self, serializer: &mut FieldSerializer, heap: &MemoryArena) -> io::Result<()> {
        for doc in self.stack.iter(heap) {
            serializer.write_doc(doc, 0u32, &EMPTY_ARRAY)?;
        }
        Ok(())
    }
}

/// Recorder encoding document ids, and term frequencies
#[derive(Clone, Copy)]
pub struct TermFrequencyRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
    current_tf: u32,
}

impl Recorder for TermFrequencyRecorder {
    fn new(heap: &mut MemoryArena) -> Self {
        TermFrequencyRecorder {
            stack: ExpUnrolledLinkedList::new(heap),
            current_doc: u32::max_value(),
            current_tf: 0u32,
        }
    }

    fn current_doc(&self) -> DocId {
        self.current_doc
    }

    fn new_doc(&mut self, doc: DocId, heap: &mut MemoryArena) {
        self.current_doc = doc;
        self.stack.push(doc, heap);
    }

    fn record_position(&mut self, _position: u32, _heap: &mut MemoryArena) {
        self.current_tf += 1;
    }

    fn close_doc(&mut self, heap: &mut MemoryArena) {
        debug_assert!(self.current_tf > 0);
        self.stack.push(self.current_tf, heap);
        self.current_tf = 0;
    }

    fn serialize(&self, serializer: &mut FieldSerializer, heap: &MemoryArena) -> io::Result<()> {
        // the last document has not been closed...
        // its term freq is self.current_tf.
        let mut doc_iter = self.stack
            .iter(heap)
            .chain(Some(self.current_tf).into_iter());

        while let Some(doc) = doc_iter.next() {
            let term_freq = doc_iter
                .next()
                .expect("The IndexWriter recorded a doc without a term freq.");
            serializer.write_doc(doc, term_freq, &EMPTY_ARRAY)?;
        }
        Ok(())
    }
}

/// Recorder encoding term frequencies as well as positions.
#[derive(Clone, Copy)]
pub struct TFAndPositionRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
}

impl Recorder for TFAndPositionRecorder {
    fn new(heap: &mut MemoryArena) -> Self {
        TFAndPositionRecorder {
            stack: ExpUnrolledLinkedList::new(heap),
            current_doc: u32::max_value(),
        }
    }

    fn current_doc(&self) -> DocId {
        self.current_doc
    }

    fn new_doc(&mut self, doc: DocId, heap: &mut MemoryArena) {
        self.current_doc = doc;
        self.stack.push(doc, heap);
    }

    fn record_position(&mut self, position: u32, heap: &mut MemoryArena) {
        self.stack.push(position, heap);
    }

    fn close_doc(&mut self, heap: &mut MemoryArena) {
        self.stack.push(POSITION_END, heap);
    }

    fn serialize(&self, serializer: &mut FieldSerializer, heap: &MemoryArena) -> io::Result<()> {
        let mut doc_positions = Vec::with_capacity(100);
        let mut positions_iter = self.stack.iter(heap);
        while let Some(doc) = positions_iter.next() {
            let mut prev_position = 0;
            doc_positions.clear();
            for position in &mut positions_iter {
                if position == POSITION_END {
                    break;
                } else {
                    doc_positions.push(position - prev_position);
                    prev_position = position;
                }
            }
            serializer.write_doc(doc, doc_positions.len() as u32, &doc_positions)?;
        }
        Ok(())
    }
}
