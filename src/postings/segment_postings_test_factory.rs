use super::FreqHandler;
use DocId;
use std::mem;
use std::path::{Path, PathBuf};
use super::SegmentPostings;
use super::serializer::PostingsSerializer;
use schema::{SchemaBuilder, STRING};
use directory::{RAMDirectory, Directory};
use schema::Term;


const EMPTY_POSITIONS: [DocId; 0] = [0u32; 0];

pub struct SegmentPostingsTestFactory {
    directory: RAMDirectory,
    i: usize,
}

impl Default for SegmentPostingsTestFactory {
    fn default() -> SegmentPostingsTestFactory {
        SegmentPostingsTestFactory {
            directory: RAMDirectory::create(),
            i: 0
        }
    }
}


//data: Vec<u8>,
//len: u32,

impl SegmentPostingsTestFactory {
    pub fn from_data<'a>(&'a self, doc_ids: Vec<DocId>) -> SegmentPostings<'a> {
        let mut schema_builder = SchemaBuilder::default();
        let field = schema_builder.add_text_field("text", STRING);
        let schema = schema_builder.build();
        
        let postings_path = PathBuf::from(format!("postings{}", self.i));
        let terms_path = PathBuf::from(format!("terms{}", self.i));
        let positions_path = PathBuf::from(format!("positions{}", self.i));
        self.i += 1;
    
        let mut directory = self.directory.clone();
        let mut postings_serializer = PostingsSerializer::new(
            directory.open_write(&terms_path).unwrap(),
            directory.open_write(&postings_path).unwrap(),
            directory.open_write(&positions_path).unwrap(),
            schema
        ).unwrap();
        let term = Term::from_field_text(field, "dummy");
        postings_serializer.new_term(&term, doc_ids.len() as u32);
        for doc_id in &doc_ids {
            postings_serializer.write_doc(*doc_id, 1u32, &EMPTY_POSITIONS);
        }
        postings_serializer.close_term();
        postings_serializer.close();
        let postings_data = self.directory.open_read(&postings_path).unwrap();
        let ref_postings_data = unsafe {
            mem::transmute::<&[u8], &'a [u8]>(postings_data.as_slice())
        };
        SegmentPostings::from_data(doc_ids.len() as u32, ref_postings_data, FreqHandler::new_without_freq())
    }
}


#[cfg(test)]
mod tests {
    
    use super::*;
    use postings::DocSet;
      
    #[test]
    pub fn test_segment_postings_tester() {
        let segment_postings_tester = SegmentPostingsTestFactory::default();
        let mut postings = segment_postings_tester.from_data(vec!(1,2,17,32));
        assert!(postings.advance());
        assert_eq!(postings.doc(), 1);
        assert!(postings.advance());
        assert_eq!(postings.doc(), 2);
        assert!(postings.advance());
        assert_eq!(postings.doc(), 17);
        assert!(postings.advance());
        assert_eq!(postings.doc(), 32);
        assert!(!postings.advance());
    }

}
