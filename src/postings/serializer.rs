use datastruct::FstMapBuilder;
use super::TermInfo;
use schema::Term;
use directory::WritePtr;
use compression::{NUM_DOCS_PER_BLOCK, Block128Encoder, VIntsEncoder, S4BP128Encoder};
use DocId;
use core::index::Segment;
use std::io;
use core::index::SegmentComponent;
use common::BinarySerializable;
use common::VInt;


pub struct PostingsSerializer {
    terms_fst_builder: FstMapBuilder<WritePtr, TermInfo>, // TODO find an alternative to work around the "move"
    postings_write: WritePtr,
    positions_write: WritePtr,
    written_bytes_postings: usize,
    written_bytes_positions: usize,
    positions_encoder: S4BP128Encoder,
    block_encoder: Block128Encoder,
    vints_encoder: VIntsEncoder,
    doc_ids: Vec<DocId>,
    term_freqs: Vec<u32>,
    position_deltas: Vec<u32>,
    is_termfreq_enabled: bool,
    is_positions_enabled: bool,
}

impl PostingsSerializer {

    pub fn open(segment: &Segment) -> io::Result<PostingsSerializer> {
        let terms_write = try!(segment.open_write(SegmentComponent::TERMS));
        let terms_fst_builder = try!(FstMapBuilder::new(terms_write));
        let postings_write = try!(segment.open_write(SegmentComponent::POSTINGS));
        let positions_write = try!(segment.open_write(SegmentComponent::POSITIONS));
        Ok(PostingsSerializer {
            terms_fst_builder: terms_fst_builder,
            postings_write: postings_write,
            positions_write: positions_write,
            written_bytes_postings: 0,
            written_bytes_positions: 0,
            positions_encoder: S4BP128Encoder::new(),
            block_encoder: Block128Encoder::new(),
            vints_encoder: VIntsEncoder::new(),
            doc_ids: Vec::new(),
            term_freqs: Vec::new(),
            position_deltas: Vec::new(),
            is_positions_enabled: false,
            is_termfreq_enabled: false,
        })
    }

    pub fn new_term(&mut self, term: &Term, doc_freq: DocId) -> io::Result<()> {
        try!(self.close_term());
        self.doc_ids.clear();
        self.term_freqs.clear();
        self.position_deltas.clear();
        let term_info = TermInfo {
            doc_freq: doc_freq,
            postings_offset: self.written_bytes_postings as u32,
        };
        self.terms_fst_builder
            .insert(term.as_slice(), &term_info)
    }

    pub fn close_term(&mut self,) -> io::Result<()> {
        if !self.doc_ids.is_empty() {
            {
                let block_encoded = self.vints_encoder.encode_sorted(&self.doc_ids[..]);
                self.written_bytes_postings += try!(VInt(block_encoded.len() as u64).serialize(&mut self.postings_write));
                for num in block_encoded {
                    self.written_bytes_postings += try!(num.serialize(&mut self.postings_write));
                }
            }
            {
                let block_encoded = self.vints_encoder.encode_sorted(&self.term_freqs[..]);
                self.written_bytes_postings += try!(VInt(block_encoded.len() as u64).serialize(&mut self.postings_write));
                for num in block_encoded {
                    self.written_bytes_postings += try!(num.serialize(&mut self.postings_write));
                }
            }
            if self.is_positions_enabled {
                let mut num_blocks = self.position_deltas.len() / NUM_DOCS_PER_BLOCK;
                let mut offset = 0;
                for _ in 0..num_blocks {
                    let block_encoded = self.positions_encoder.encode(&self.position_deltas[offset..offset + NUM_DOCS_PER_BLOCK]);
                    offset += NUM_DOCS_PER_BLOCK;
                    // self.positions_write.wr
                }
                // self.position_deltas.extend_from_slice(position_deltas);
                // let block_encoded &[u32] = self.positions_encoder.encode(&self.positions[..]);
            }
            self.doc_ids.clear();
            self.term_freqs.clear();
        }
        Ok(())
    }

    pub fn write_doc(&mut self, doc_id: DocId, term_freq: u32, position_deltas: &[u32]) -> io::Result<()> {
        self.doc_ids.push(doc_id);
        self.term_freqs.push(term_freq as u32);
        if self.doc_ids.len() == 128 { 
            {
                let block_encoded: &[u32] = self.block_encoder.encode_sorted(&self.doc_ids);
                for num in block_encoded {
                    self.written_bytes_postings += try!(num.serialize(&mut self.postings_write));
                }
            }
            if self.is_termfreq_enabled {
                let block_encoded: &[u32] = self.block_encoder.encode_sorted(&self.term_freqs);
                for num in block_encoded {
                    self.written_bytes_postings += try!(num.serialize(&mut self.postings_write));
                }
            }
            if self.is_positions_enabled {
                self.position_deltas.extend_from_slice(position_deltas);
            }
            self.doc_ids.clear();
            self.term_freqs.clear();
        }
        Ok(())
    }
    
    pub fn close(mut self,) -> io::Result<()> {
        try!(self.close_term());
        try!(self.terms_fst_builder.finish());
        try!(self.postings_write.flush());
        try!(self.positions_write.flush());
        Ok(())
    }
}
