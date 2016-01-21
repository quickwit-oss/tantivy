use core::global::*;
use core::schema::*;
use core::error::{Result, Error};

// change the API to remove the lifetime, by
// "pushing" the data to a SegmentSerializer.

struct DebugSegmentSerialize {
    text: String,
}

impl DebugSegmentSerialize {
    pub fn to_string(&self,) -> &String {
        &self.text
    }
}

impl SegmentSerializer for DebugSegmentSerialize {
    fn new_term(&mut self, term: &Term, doc_freq: DocId) -> Result<()> {
        self.text.push_str(&format!("{:?}\n", term));
        Ok(())
    }

    fn add_doc(&mut self, doc_id: DocId) -> Result<()> {
        self.text.push_str(&format!("   - Doc {:?}\n", doc_id));
        Ok(())
    }
}

pub trait SegmentSerializer {
    fn new_term(&mut self, term: &Term, doc_freq: DocId) -> Result<()>;
    fn add_doc(&mut self, doc_id: DocId) -> Result<()>;
}

pub trait SerializableSegment {
    fn write<SegSer: SegmentSerializer>(&self, serializer: &mut SegSer) -> Result<()>;
}

// TODO make iteration over Fields somehow sorted
