use datastruct::stacker::{Addr, ExpUnrolledLinkedList, Heap, HeapAllocable};
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
pub trait Recorder: HeapAllocable {
    /// Returns the current document
    fn current_doc(&self) -> u32;
    /// Starts recording information about a new document
    /// This method shall only be called if the term is within the document.
    fn new_doc(&mut self, doc: DocId, heap: &Heap);
    /// Record the position of a term. For each document,
    /// this method will be called `term_freq` times.
    fn record_position(&mut self, position: u32, heap: &Heap);
    /// Close the document. It will help record the term frequency.
    fn close_doc(&mut self, heap: &Heap);
    /// Pushes the postings information to the serializer.
    fn serialize(
        &self,
        self_addr: Addr,
        serializer: &mut FieldSerializer,
        heap: &Heap,
    ) -> io::Result<()>;
}

/// Only records the doc ids
pub struct NothingRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
}

impl HeapAllocable for NothingRecorder {
    fn with_addr(addr: Addr) -> NothingRecorder {
        NothingRecorder {
            stack: ExpUnrolledLinkedList::with_addr(addr),
            current_doc: u32::max_value(),
        }
    }
}

impl Recorder for NothingRecorder {
    fn current_doc(&self) -> DocId {
        self.current_doc
    }

    fn new_doc(&mut self, doc: DocId, heap: &Heap) {
        self.current_doc = doc;
        self.stack.push(doc, heap);
    }

    fn record_position(&mut self, _position: u32, _heap: &Heap) {}

    fn close_doc(&mut self, _heap: &Heap) {}

    fn serialize(
        &self,
        self_addr: Addr,
        serializer: &mut FieldSerializer,
        heap: &Heap,
    ) -> io::Result<()> {
        for doc in self.stack.iter(self_addr, heap) {
            serializer.write_doc(doc, 0u32, &EMPTY_ARRAY)?;
        }
        Ok(())
    }
}

/// Recorder encoding document ids, and term frequencies
pub struct TermFrequencyRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
    current_tf: u32,
}

impl HeapAllocable for TermFrequencyRecorder {
    fn with_addr(addr: Addr) -> TermFrequencyRecorder {
        TermFrequencyRecorder {
            stack: ExpUnrolledLinkedList::with_addr(addr),
            current_doc: u32::max_value(),
            current_tf: 0u32,
        }
    }
}

impl Recorder for TermFrequencyRecorder {
    fn current_doc(&self) -> DocId {
        self.current_doc
    }

    fn new_doc(&mut self, doc: DocId, heap: &Heap) {
        self.current_doc = doc;
        self.stack.push(doc, heap);
    }

    fn record_position(&mut self, _position: u32, _heap: &Heap) {
        self.current_tf += 1;
    }

    fn close_doc(&mut self, heap: &Heap) {
        debug_assert!(self.current_tf > 0);
        self.stack.push(self.current_tf, heap);
        self.current_tf = 0;
    }

    fn serialize(
        &self,
        self_addr: Addr,
        serializer: &mut FieldSerializer,
        heap: &Heap,
    ) -> io::Result<()> {
        // the last document has not been closed...
        // its term freq is self.current_tf.
        let mut doc_iter = self.stack
            .iter(self_addr, heap)
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
pub struct TFAndPositionRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
}

impl HeapAllocable for TFAndPositionRecorder {
    fn with_addr(addr: Addr) -> TFAndPositionRecorder {
        TFAndPositionRecorder {
            stack: ExpUnrolledLinkedList::with_addr(addr),
            current_doc: u32::max_value(),
        }
    }
}

impl Recorder for TFAndPositionRecorder {
    fn current_doc(&self) -> DocId {
        self.current_doc
    }

    fn new_doc(&mut self, doc: DocId, heap: &Heap) {
        self.current_doc = doc;
        self.stack.push(doc, heap);
    }

    fn record_position(&mut self, position: u32, heap: &Heap) {
        self.stack.push(position, heap);
    }

    fn close_doc(&mut self, heap: &Heap) {
        self.stack.push(POSITION_END, heap);
    }

    fn serialize(
        &self,
        self_addr: Addr,
        serializer: &mut FieldSerializer,
        heap: &Heap,
    ) -> io::Result<()> {
        let mut doc_positions = Vec::with_capacity(100);
        let mut positions_iter = self.stack.iter(self_addr, heap);
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
