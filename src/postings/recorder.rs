use super::stacker::{ExpUnrolledLinkedList, MemoryArena};
use crate::common::{read_u32_vint, write_u32_vint};
use crate::postings::FieldSerializer;
use crate::DocId;
use std::io;

const POSITION_END: u32 = 0;

#[derive(Default)]
pub(crate) struct BufferLender {
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

pub struct VInt32Reader<'a> {
    data: &'a [u8],
}

impl<'a> VInt32Reader<'a> {
    fn new(data: &'a [u8]) -> VInt32Reader<'a> {
        VInt32Reader { data }
    }
}

impl<'a> Iterator for VInt32Reader<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        if self.data.is_empty() {
            None
        } else {
            Some(read_u32_vint(&mut self.data))
        }
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
pub(crate) trait Recorder: Copy + 'static {
    ///
    fn new() -> Self;
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
        serializer: &mut FieldSerializer<'_>,
        heap: &MemoryArena,
    ) -> io::Result<()>;
    /// Returns the number of document containing this term.
    ///
    /// Returns `None` if not available.
    fn term_doc_freq(&self) -> Option<u32>;
}

/// Only records the doc ids
#[derive(Clone, Copy)]
pub struct NothingRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
}

impl Recorder for NothingRecorder {
    fn new() -> Self {
        NothingRecorder {
            stack: ExpUnrolledLinkedList::new(),
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
        serializer: &mut FieldSerializer<'_>,
        heap: &MemoryArena,
    ) -> io::Result<()> {
        let buffer = buffer_lender.lend_u8();
        self.stack.read_to_end(heap, buffer);
        // TODO avoid reading twice.
        for doc in VInt32Reader::new(&buffer[..]) {
            serializer.write_doc(doc as u32, 0u32, &[][..])?;
        }
        Ok(())
    }

    fn term_doc_freq(&self) -> Option<u32> {
        None
    }
}

/// Recorder encoding document ids, and term frequencies
#[derive(Clone, Copy)]
pub struct TermFrequencyRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
    current_tf: u32,
    term_doc_freq: u32,
}

impl Recorder for TermFrequencyRecorder {
    fn new() -> Self {
        TermFrequencyRecorder {
            stack: ExpUnrolledLinkedList::new(),
            current_doc: u32::max_value(),
            current_tf: 0u32,
            term_doc_freq: 0u32,
        }
    }

    fn current_doc(&self) -> DocId {
        self.current_doc
    }

    fn new_doc(&mut self, doc: DocId, heap: &mut MemoryArena) {
        self.term_doc_freq += 1;
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
        serializer: &mut FieldSerializer<'_>,
        heap: &MemoryArena,
    ) -> io::Result<()> {
        let buffer = buffer_lender.lend_u8();
        self.stack.read_to_end(heap, buffer);
        let mut u32_it = VInt32Reader::new(&buffer[..]);
        while let Some(doc) = u32_it.next() {
            let term_freq = u32_it.next().unwrap_or(self.current_tf);
            serializer.write_doc(doc as u32, term_freq, &[][..])?;
        }

        Ok(())
    }

    fn term_doc_freq(&self) -> Option<u32> {
        Some(self.term_doc_freq)
    }
}

/// Recorder encoding term frequencies as well as positions.
#[derive(Clone, Copy)]
pub struct TFAndPositionRecorder {
    stack: ExpUnrolledLinkedList,
    current_doc: DocId,
    term_doc_freq: u32,
}
impl Recorder for TFAndPositionRecorder {
    fn new() -> Self {
        TFAndPositionRecorder {
            stack: ExpUnrolledLinkedList::new(),
            current_doc: u32::max_value(),
            term_doc_freq: 0u32,
        }
    }

    fn current_doc(&self) -> DocId {
        self.current_doc
    }

    fn new_doc(&mut self, doc: DocId, heap: &mut MemoryArena) {
        self.current_doc = doc;
        self.term_doc_freq += 1u32;
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
        serializer: &mut FieldSerializer<'_>,
        heap: &MemoryArena,
    ) -> io::Result<()> {
        let (buffer_u8, buffer_positions) = buffer_lender.lend_all();
        self.stack.read_to_end(heap, buffer_u8);
        let mut u32_it = VInt32Reader::new(&buffer_u8[..]);
        while let Some(doc) = u32_it.next() {
            let mut prev_position_plus_one = 1u32;
            buffer_positions.clear();
            loop {
                match u32_it.next() {
                    Some(POSITION_END) | None => {
                        break;
                    }
                    Some(position_plus_one) => {
                        let delta_position = position_plus_one - prev_position_plus_one;
                        buffer_positions.push(delta_position);
                        prev_position_plus_one = position_plus_one;
                    }
                }
            }
            serializer.write_doc(doc, buffer_positions.len() as u32, &buffer_positions)?;
        }
        Ok(())
    }

    fn term_doc_freq(&self) -> Option<u32> {
        Some(self.term_doc_freq)
    }
}

#[cfg(test)]
mod tests {

    use super::write_u32_vint;
    use super::BufferLender;
    use super::VInt32Reader;

    #[test]
    fn test_buffer_lender() {
        let mut buffer_lender = BufferLender::default();
        {
            let buf = buffer_lender.lend_u8();
            assert!(buf.is_empty());
            buf.push(1u8);
        }
        {
            let buf = buffer_lender.lend_u8();
            assert!(buf.is_empty());
            buf.push(1u8);
        }
        {
            let (_, buf) = buffer_lender.lend_all();
            assert!(buf.is_empty());
            buf.push(1u32);
        }
        {
            let (_, buf) = buffer_lender.lend_all();
            assert!(buf.is_empty());
            buf.push(1u32);
        }
    }

    #[test]
    fn test_vint_u32() {
        let mut buffer = vec![];
        let vals = [0, 1, 324_234_234, u32::max_value()];
        for &i in &vals {
            assert!(write_u32_vint(i, &mut buffer).is_ok());
        }
        assert_eq!(buffer.len(), 1 + 1 + 5 + 5);
        let res: Vec<u32> = VInt32Reader::new(&buffer[..]).collect();
        assert_eq!(&res[..], &vals[..]);
    }
}
