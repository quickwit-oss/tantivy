use postings::Postings;
use compression::{NUM_DOCS_PER_BLOCK, Block128Decoder};
use DocId;
use std::cmp::Ordering;
use std::mem;
use postings::SkipResult;
use std::num::Wrapping;

// No Term Frequency, no postings.
pub struct SegmentPostings<'a> {
    doc_freq: usize,
    block_decoder: Block128Decoder,
    remaining_data: &'a [u8],
    cur: Wrapping<usize>,
}

const EMPTY_ARRAY: [u8; 0] = [];

impl<'a> SegmentPostings<'a> {
    
    pub fn empty() -> SegmentPostings<'a> {
        SegmentPostings {
            doc_freq: 0,
            block_decoder: Block128Decoder::new(),
            remaining_data: &EMPTY_ARRAY,
            cur: Wrapping(usize::max_value()),
        }
    }
    
    pub fn load_next_block(&mut self,) {
        if self.doc_freq - self.cur.0 >= NUM_DOCS_PER_BLOCK {
            self.remaining_data = self.block_decoder.decode_sorted(self.remaining_data);
        }
        else {
            self.block_decoder.decode_sorted_remaining(self.remaining_data);
        }
    }
    
    pub fn from_data(doc_freq: u32, data: &'a [u8]) -> SegmentPostings<'a> {
        SegmentPostings {
            doc_freq: doc_freq as usize,
            block_decoder: Block128Decoder::new(),
            remaining_data: data,
            cur: Wrapping(usize::max_value()),
        }
        // let mut data_u32: &[u32] = unsafe { mem::transmute(data) };
        // let mut doc_ids: Vec<u32> = Vec::with_capacity(doc_freq as usize);
        // {
        //     let mut block_decoder = Block128Decoder::new();
        //     let num_blocks = doc_freq / (NUM_DOCS_PER_BLOCK as u32);
        //     for _ in 0..num_blocks {
        //         let (remaining = block_decoder.decode_sorted(data_u32);
        //         doc_ids.extend_from_slice(uncompressed);
        //         data_u32 = remaining;
        //     }
        //     if doc_freq % 128 != 0 {
        //         let data_u8: &[u8] = unsafe { mem::transmute(data_u32) };
        //         let mut cursor = Cursor::new(data_u8);
        //         let vint_len: usize = VInt::deserialize(&mut cursor).unwrap().val() as usize;
        //         let cursor_pos = cursor.position() as usize;
        //         let vint_data: &[u32] = unsafe { mem::transmute(&data_u8[cursor_pos..]) };
        //         let mut vints_decoder = VIntsDecoder::new();
        //         doc_ids.extend_from_slice(vints_decoder.decode_sorted(&vint_data[..vint_len]));
        //     }
        // }
        // SegmentPostings(doc_ids)

    }
}

impl<'a> Postings for SegmentPostings<'a> {
    
    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    fn next(&mut self,) -> bool {
        self.cur += Wrapping(1);
        if self.cur.0 >= self.doc_freq {
            return false;
        }
        if self.cur.0 % NUM_DOCS_PER_BLOCK == 0 {
            self.load_next_block();
        }
        return true;
    }
    
    fn doc(&self,) -> DocId {
        self.block_decoder.output()[self.cur.0 % NUM_DOCS_PER_BLOCK]
    }
    
    // after skipping position
    // the iterator in such a way that doc() will return a
    // value greater or equal to target.
    fn skip_next(&mut self, target: DocId) -> SkipResult {
        loop {
            match self.doc().cmp(&target) {
                Ordering::Equal => {
                    return SkipResult::Reached;
                }
                Ordering::Greater => {
                    return SkipResult::OverStep;
                }
                Ordering::Less => {}
            }
            if !self.next() {
                return SkipResult::End;
            }
        }
    }
    
    fn doc_freq(&self,) -> usize {
        self.doc_freq
    }
}