use crate::postings::compression::COMPRESSION_BLOCK_SIZE;
use crate::DocId;

pub struct Block {
    doc_ids: [DocId; COMPRESSION_BLOCK_SIZE],
    term_freqs: [u32; COMPRESSION_BLOCK_SIZE],
    len: usize,
}

impl Block {
    pub fn new() -> Self {
        Block {
            doc_ids: [0u32; COMPRESSION_BLOCK_SIZE],
            term_freqs: [0u32; COMPRESSION_BLOCK_SIZE],
            len: 0,
        }
    }

    pub fn doc_ids(&self) -> &[DocId] {
        &self.doc_ids[..self.len]
    }

    pub fn term_freqs(&self) -> &[u32] {
        &self.term_freqs[..self.len]
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn append_doc(&mut self, doc: DocId, term_freq: u32) {
        let len = self.len;
        self.doc_ids[len] = doc;
        self.term_freqs[len] = term_freq;
        self.len = len + 1;
    }

    pub fn is_full(&self) -> bool {
        self.len == COMPRESSION_BLOCK_SIZE
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn last_doc(&self) -> DocId {
        assert_eq!(self.len, COMPRESSION_BLOCK_SIZE);
        self.doc_ids[COMPRESSION_BLOCK_SIZE - 1]
    }
}
