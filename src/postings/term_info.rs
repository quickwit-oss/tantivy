use common::BinarySerializable;
use std::io;


// `TermInfo` contains all of the information 
// associated to terms in the `.term` file.
// 
// It consists of
// * doc_freq : the number of document in the segment
// containing this term. It is also the length of the
// posting list associated to this term
// * postings_offset: an offset in the `.idx` file 
// addressing the start of the posting list associated
// to this term.
#[derive(Debug,Ord,PartialOrd,Eq,PartialEq,Clone)]
pub struct TermInfo {
    pub doc_freq: u32,
    pub postings_offset: u32,
}


impl BinarySerializable for TermInfo {
    fn serialize(&self, writer: &mut io::Write) -> io::Result<usize> {
        Ok(
            try!(self.doc_freq.serialize(writer)) +
            try!(self.postings_offset.serialize(writer))
        )
    }
    fn deserialize(reader: &mut io::Read) -> io::Result<Self> {
        let doc_freq = try!(u32::deserialize(reader));
        let offset = try!(u32::deserialize(reader));
        Ok(TermInfo {
            doc_freq: doc_freq,
            postings_offset: offset,
        })
    }
}
