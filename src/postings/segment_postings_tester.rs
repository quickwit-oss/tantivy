use super::FreqHandler;
use DocId;
use std::path::Path;
use super::SegmentPostings;
use super::serializer::PostingsSerializer;
use schema::{SchemaBuilder, STRING};
use directory::{RAMDirectory, Directory};
use schema::Term;


const EMPTY_POSITIONS: [DocId; 0] = [0u32; 0];

pub struct SegmentPostingsTester {
    data: Vec<u8>,
    len: u32,    
}

impl SegmentPostingsTester {
    pub fn get(&self) -> SegmentPostings {
        SegmentPostings::from_data(self.len, &self.data, FreqHandler::new_without_freq())
    }
}

impl From<Vec<DocId>> for SegmentPostingsTester {
    
    fn from(doc_ids: Vec<DocId>) -> SegmentPostingsTester {
        let mut directory = RAMDirectory::create();
        let mut schema_builder = SchemaBuilder::default();
        let field = schema_builder.add_text_field("text", STRING);
        let schema = schema_builder.build();
        let mut postings_serializer = PostingsSerializer::new(
            directory.open_write(Path::new("terms")).unwrap(),
            directory.open_write(Path::new("postings")).unwrap(),
            directory.open_write(Path::new("positions")).unwrap(),
            schema
        ).unwrap();
        let term = Term::from_field_text(field, "dummy");
        postings_serializer.new_term(&term, doc_ids.len() as u32);
        for doc_id in &doc_ids {
            postings_serializer.write_doc(*doc_id, 1u32, &EMPTY_POSITIONS);
        }
        postings_serializer.close_term();
        postings_serializer.close();
        let postings_data = directory.open_read(Path::new("postings")).unwrap();
        SegmentPostingsTester {
            data: Vec::from(postings_data.as_slice()), 
            len: doc_ids.len() as u32,
        }
    }
    
}



#[cfg(test)]
mod tests {
    
    use super::*;
    use postings::DocSet;
      
    #[test]
    pub fn test_segment_postings_tester() {
        let segment_postings_tester = SegmentPostingsTester::from(vec!(1,2,17,32));
        let mut postings = segment_postings_tester.get();
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
