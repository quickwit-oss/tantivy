use core::schema::DocId;
use core::schema::Term;
use core::schema::FieldValue;
use std::io;
use core::index::SegmentInfo;

pub trait SegmentSerializer<Output> {
    fn new_term(&mut self, term: &Term, doc_freq: DocId) -> io::Result<()>;
    fn write_docs(&mut self, docs: &[DocId]) -> io::Result<()>; // TODO add size
    fn store_doc(&mut self, field: &mut Iterator<Item=&FieldValue>) -> io::Result<()>;
    fn close(self,) -> Result<Output, io::Error>;
    fn write_segment_info(&mut self, segment_info: &SegmentInfo) -> io::Result<()>;
}

pub trait SerializableSegment {
    fn write<Output, SegSer: SegmentSerializer<Output>>(&self, serializer: SegSer) -> io::Result<Output>;
}

#[cfg(test)]
pub mod tests {

    use core::schema::DocId;
    use core::schema::FieldValue;
    use core::schema::Term;
    use std::fmt;
    use super::SegmentSerializer;
    use super::SerializableSegment;
    use core::index::SegmentInfo;
    use std::io;

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

        fn store_doc(&mut self, fields: &mut Iterator<Item=&FieldValue>) -> io::Result<()> {
            if self.num_docs == 0 {
                self.text.push_str(&format!("# STORED DOC\n======\n"))
            }
            self.text.push_str(&format!("doc {}", self.num_docs));
            for field_value in fields {
                self.text.push_str(&format!("field {:?} |", field_value.field));
                self.text.push_str(&format!("value {:?}\n", field_value.text));
            }
            Ok(())
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

        fn write_segment_info(&mut self, segment_info: &SegmentInfo) -> io::Result<()> {
            self.text.push_str(&format!("\n segmentinfo({:?})", segment_info));
            Ok(())
        }
    }

}
