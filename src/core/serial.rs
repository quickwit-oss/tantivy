use core::global::*;
use core::schema::*;
use core::error::{Result, Error};

// change the API to remove the lifetime, by
// "pushing" the data to a SegmentSerializer.

#[derive(Debug)]
pub struct DebugSegmentSerialize {
    text: String,
}

impl DebugSegmentSerialize {
    pub fn to_string(&self,) -> &String {
        &self.text
    }

    pub fn new() -> DebugSegmentSerialize {
        DebugSegmentSerialize {
            text: String::new(),
        }
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

    fn close(&mut self,) -> Result<()> {
        Ok(())
    }
}

pub trait SegmentSerializer {
    fn new_term(&mut self, term: &Term, doc_freq: DocId) -> Result<()>;
    fn add_doc(&mut self, doc_id: DocId) -> Result<()>;
    fn close(&mut self,) -> Result<()>;
}

pub trait SerializableSegment {
    fn write<SegSer: SegmentSerializer>(&self, serializer: &mut SegSer) -> Result<()>;
}

// TODO make iteration over Fields somehow sorted
