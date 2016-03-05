use core::schema::*;
use std::fmt;
use std::io;
use core::index::SegmentInfo;

pub trait SegmentSerializer<Output> {
    fn new_term(&mut self, term: &Term, doc_freq: DocId) -> Result<(), io::Error>;
    fn write_docs(&mut self, docs: &[DocId]) -> Result<(), io::Error>; // TODO add size
    fn store_doc(&mut self, field: &mut Iterator<Item=&FieldValue>);
    fn close(self,) -> Result<Output, io::Error>;
    fn write_segment_info(&self, segment_info: &SegmentInfo) -> io::Result<()>;
}

pub trait SerializableSegment {
    fn write<Output, SegSer: SegmentSerializer<Output>>(&self, serializer: SegSer) -> io::Result<Output>;
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
        let serializer = DebugSegmentSerializer::new();
        index.write(serializer).unwrap()
    }

    pub fn new() -> DebugSegmentSerializer {
        DebugSegmentSerializer {
            text: String::new(),
            num_docs: 0,
        }
    }
}

impl SegmentSerializer<String> for DebugSegmentSerializer {
    fn new_term(&mut self, term: &Term, doc_freq: DocId) -> Result<(), io::Error> {
        self.text.push_str(&format!("{:?} - docfreq{}\n", term, doc_freq));
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

    fn write_docs(&mut self, docs: &[DocId]) -> Result<(), io::Error> {
        for doc in docs {
            self.text.push_str(&format!("   - Doc {:?}\n", doc));
        }
        Ok(())
    }

    fn close(self,) -> Result<String, io::Error> {
        Ok(self.text)
    }

    fn write_segment_info(&self, segment_info: &SegmentInfo) -> io::Result<()> {
        Ok(())
    }
}

pub fn serialize_eq<L: SerializableSegment, R: SerializableSegment>(left: &L, right: &R) -> bool{
    let str_left = DebugSegmentSerializer::debug_string(left);
    let str_right = DebugSegmentSerializer::debug_string(right);
    str_left == str_right
}
