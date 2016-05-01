use datastruct::FstMapBuilder;
use super::TermInfo;
use schema::Term;
use directory::WritePtr;
use compression;
use DocId;
use core::index::Segment;
use std::io;
use core::index::SegmentComponent;
use common::BinarySerializable;

pub struct PostingsSerializer {
    terms_fst_builder: FstMapBuilder<WritePtr, TermInfo>, // TODO find an alternative to work around the "move"
    postings_write: WritePtr,
    positions_write: WritePtr,
    written_bytes_postings: usize,
    written_bytes_positions: usize,
    encoder: compression::Encoder,
    doc_ids: Vec<DocId>,
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
            encoder: compression::Encoder::new(),
            doc_ids: Vec::new(),
        })
    }

    pub fn new_term(&mut self, term: &Term, doc_freq: DocId) -> io::Result<()> {
        try!(self.close_term());
        self.doc_ids.clear();
        let term_info = TermInfo {
            doc_freq: doc_freq,
            postings_offset: self.written_bytes_postings as u32,
        };
        self.terms_fst_builder
            .insert(term.as_slice(), &term_info)
    }

    pub fn close_term(&mut self,) -> io::Result<()> {
        if !self.doc_ids.is_empty() {
            let docs_data = self.encoder.encode_sorted(&self.doc_ids);
            self.written_bytes_postings += try!((docs_data.len() as u32).serialize(&mut self.postings_write));
            for num in docs_data {
                self.written_bytes_postings += try!(num.serialize(&mut self.postings_write));
            }
        }
        Ok(())
    }

    pub fn write_doc(&mut self, doc_id: DocId, positions: Option<&[u32]>) -> io::Result<()> {
        self.doc_ids.push(doc_id);
        Ok(())
    }


    pub fn close(mut self,) -> io::Result<()> {
        try!(self.close_term());
        try!(self.terms_fst_builder.finish());
        try!(self.postings_write.flush());
        Ok(())
    }
}
