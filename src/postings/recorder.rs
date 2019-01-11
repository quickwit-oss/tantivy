use super::stacker::{ExpUnrolledLinkedList, MemoryArena};
use common::VInt;
use postings::FieldSerializer;
use std::io;
use DocId;
use common::write_u32_vint;

const EMPTY_ARRAY: [u32; 0] = [0u32; 0];
const POSITION_END: u32 = 0;

#[derive(Default)]
pub struct BufferLender {
    buffer_u8: Vec<u8>,
    buffer_u32: Vec<u32>,
}

impl BufferLender {
    pub fn lend_u8(&mut self) -> &mut Vec<u8> {
        self.buffer_u8.clear();
        &mut self.buffer_u8
    }
    pub fn lend_all(&mut self) -> (&mut Vec<u8>, &mut Vec<u32>) {
        self.buffer_u8.clear();
        self.buffer_u32.clear();
        (&mut self.buffer_u8, &mut self.buffer_u32)
    }
}

/// Recorder is in charge of recording relevant information about
/// the presence of a term in a document.
///
/// Depending on the `TextIndexingOptions` associated to the
/// field, the recorder may records
///   * the document frequency
///   * the document id
///   * the term frequency
///   * the term positions
pub trait Recorder: Copy + 'static {
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
    fn serialize(
        &self,
        buffer_lender: &mut BufferLender,
        serializer: &mut FieldSerializer,
        heap: &MemoryArena,
    ) -> io::Result<()>;
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
        let _ = write_u32_vint(doc, &mut self.stack.writer(heap));
    }

    fn record_position(&mut self, _position: u32, _heap: &mut MemoryArena) {}

    fn close_doc(&mut self, _heap: &mut MemoryArena) {}

    fn serialize(
        &self,
        buffer_lender: &mut BufferLender,
        serializer: &mut FieldSerializer,
        heap: &MemoryArena,
    ) -> io::Result<()> {
        let buffer = buffer_lender.lend_u8();
        self.stack.read_to_end(heap, buffer);
        let mut data = &buffer[..];
        while !data.is_empty() {
            let doc = VInt::deserialize_u64(&mut data).unwrap();
            serializer.write_doc(doc as u32, 0u32, &EMPTY_ARRAY)?;
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
        let _ = write_u32_vint(doc, &mut self.stack.writer(heap));
    }

    fn record_position(&mut self, _position: u32, _heap: &mut MemoryArena) {
        self.current_tf += 1;
    }

    fn close_doc(&mut self, heap: &mut MemoryArena) {
        debug_assert!(self.current_tf > 0);
        let _ = write_u32_vint(self.current_tf, &mut self.stack.writer(heap));
        self.current_tf = 0;
    }

    fn serialize(
        &self,
        buffer_lender: &mut BufferLender,
        serializer: &mut FieldSerializer,
        heap: &MemoryArena,
    ) -> io::Result<()> {
        let buffer = buffer_lender.lend_u8();
        self.stack.read_to_end(heap, buffer);
        let mut data = &buffer[..];
        while !data.is_empty() {
            let doc = VInt::deserialize_u64(&mut data).unwrap();
            let term_freq = if data.is_empty() {
                self.current_tf
            } else {
                VInt::deserialize_u64(&mut data).unwrap() as u32
            };
            serializer.write_doc(doc as u32, term_freq, &EMPTY_ARRAY)?;
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
        let _ = write_u32_vint(doc, &mut self.stack.writer(heap));
    }

    fn record_position(&mut self, position: u32, heap: &mut MemoryArena) {
        let _ = write_u32_vint(position + 1u32, &mut self.stack.writer(heap));
    }

    fn close_doc(&mut self, heap: &mut MemoryArena) {
        let _ = write_u32_vint(POSITION_END, &mut self.stack.writer(heap));
    }

    fn serialize(
        &self,
        buffer_lender: &mut BufferLender,
        serializer: &mut FieldSerializer,
        heap: &MemoryArena,
    ) -> io::Result<()> {
        let (buffer_u8, buffer_positions) = buffer_lender.lend_all();
        self.stack.read_to_end(heap, buffer_u8);
        let mut data = &buffer_u8[..];
        while !data.is_empty() {
            let mut prev_position_plus_one = 1u32;
            let doc = VInt::deserialize_u64(&mut data).unwrap() as u32;
            buffer_positions.clear();
            while !data.is_empty() {
                let position_plus_one = VInt::deserialize_u64(&mut data).unwrap() as u32;
                if position_plus_one == POSITION_END {
                    break;
                } else {
                    let delta_position = position_plus_one - prev_position_plus_one;
                    buffer_positions.push(delta_position);
                    prev_position_plus_one = position_plus_one;
                }
            }
            serializer.write_doc(doc, buffer_positions.len() as u32, &buffer_positions)?;
        }
        Ok(())
    }
}
