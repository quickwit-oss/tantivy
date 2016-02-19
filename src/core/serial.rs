use core::global::*;
use core::schema::*;
use core::error::{Result, Error};
use std::fmt;


pub trait SegmentSerializer<Output> {
    fn new_term(&mut self, term: &Term, doc_freq: DocId) -> Result<()>;
    fn write_docs(&mut self, docs: &[DocId]) -> Result<()>; // TODO add size
    fn store_doc(&mut self, field: &mut Iterator<Item=&FieldValue>);
    fn close(self,) -> Result<Output>;
}

pub trait SerializableSegment {
    fn write<Output, SegSer: SegmentSerializer<Output>>(&self, serializer: &mut SegSer) -> Result<Output>;
}


pub struct DebugSegmentSerializer {
    text: String,
    num_docs: u32,
}

impl fmt::Debug for DebugSegmentSerializer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.text)
    }
}

impl DebugSegmentSerializer {

    pub fn debug_string<S: SerializableSegment>(index: &S) -> String {
        let mut serializer = DebugSegmentSerializer::new();
        index.write(&mut serializer).unwrap()
    }

    pub fn new() -> DebugSegmentSerializer {
        DebugSegmentSerializer {
            text: String::new(),
            num_docs: 0,
        }
    }
}


impl SegmentSerializer<String> for DebugSegmentSerializer {
    fn new_term(&mut self, term: &Term, doc_freq: DocId) -> Result<()> {
        self.text.push_str(&format!("{:?}\n", term));
        Ok(())
    }

    fn store_doc(&mut self, fields: &mut Iterator<Item=&FieldValue>) {
        if self.num_docs == 0 {
            self.text.push_str(&format!("# STORED DOC\n======\n"))
        }
        self.text.push_str(&format!("doc {}", self.num_docs));
        for field_value in fields {
            self.text.push_str(&format!("field {:?} |", field_value.field));
            self.text.push_str(&format!("value {:?}\n", field_value.text));
        }
    }

    fn write_docs(&mut self, docs: &[DocId]) -> Result<()> {
        for doc in docs {
            self.text.push_str(&format!("   - Doc {:?}\n", doc));
        }
        Ok(())
    }

    fn close(self,) -> Result<String> {
        Ok(self.text)
    }
}

pub fn serialize_eq<L: SerializableSegment, R: SerializableSegment>(left: &L, right: &R) -> bool{
    let str_left = DebugSegmentSerializer::debug_string(left);
    let str_right = DebugSegmentSerializer::debug_string(right);
    str_left == str_right
}
