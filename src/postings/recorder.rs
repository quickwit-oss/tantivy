use DocId;
use std::io;
use postings::PostingsSerializer;
use datastruct::stacker::{ExpUnrolledLinkedList, Heap, HeapAllocable};

const EMPTY_ARRAY: [u32; 0] = [0u32; 0];
const POSITION_END: u32 = 4294967295;

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
    fn serialize(&self,
                 self_addr: u32,
                 serializer: &mut PostingsSerializer,
                 heap: &Heap)
                 -> io::Result<()>;
}

/// Only records the doc ids
#[repr(C, packed)]
pub struct NothingRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
}

impl HeapAllocable for NothingRecorder {
    fn with_addr(addr: u32) -> NothingRecorder {
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

    fn serialize(&self,
                 self_addr: u32,
                 serializer: &mut PostingsSerializer,
                 heap: &Heap)
                 -> io::Result<()> {
        for doc in self.stack.iter(self_addr, heap) {
            try!(serializer.write_doc(doc, 0u32, &EMPTY_ARRAY));
        }
        Ok(())
    }
}

/// Recorder encoding document ids, and term frequencies
#[repr(C, packed)]
pub struct TermFrequencyRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
    current_tf: u32,
}

impl HeapAllocable for TermFrequencyRecorder {
    fn with_addr(addr: u32) -> TermFrequencyRecorder {
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


    fn serialize(&self,
                 self_addr: u32,
                 serializer: &mut PostingsSerializer,
                 heap: &Heap)
                 -> io::Result<()> {
        let mut doc_iter = self.stack.iter(self_addr, heap);
        loop {
            if let Some(doc) = doc_iter.next() {
                if let Some(term_freq) = doc_iter.next() {
                    serializer.write_doc(doc, term_freq, &EMPTY_ARRAY)?;
                    continue;
                }
                else {
                    // the last document has not been closed...
                    // its term freq is self.current_tf.
                    serializer.write_doc(doc, self.current_tf, &EMPTY_ARRAY)?;
                    break;
                }
            }
            break;
        }
        Ok(())
    }
}

/// Recorder encoding term frequencies as well as positions.
#[repr(C, packed)]
pub struct TFAndPositionRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
}

impl HeapAllocable for TFAndPositionRecorder {
    fn with_addr(addr: u32) -> TFAndPositionRecorder {
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

    fn serialize(&self,
                 self_addr: u32,
                 serializer: &mut PostingsSerializer,
                 heap: &Heap)
                 -> io::Result<()> {
        let mut doc_positions = Vec::with_capacity(100);
        let mut positions_iter = self.stack.iter(self_addr, heap);
        while let Some(doc) = positions_iter.next() {
            let mut prev_position = 0;
            doc_positions.clear();
            loop {
                match positions_iter.next() {
                    Some(position) => {
                        if position == POSITION_END {
                            break;
                        } else {
                            doc_positions.push(position - prev_position);
                            prev_position = position;
                        }
                    }
                    None => {
                        // the last document has not been closed...
                        break;
                    }
                }
            }
            try!(serializer.write_doc(doc, doc_positions.len() as u32, &doc_positions));
        }
        Ok(())
    }
}
