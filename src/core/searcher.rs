use core::reader::SegmentReader;
use core::directory::Directory;
use core::collector::Collector;
use core::schema::Term;
use core::postings::Postings;

pub struct Searcher {
    segments: Vec<SegmentReader>,
}

impl Searcher {
    pub fn for_directory(directory: Directory) -> Searcher {
        let mut segment_readers: Vec<SegmentReader> = Vec::new();
        for segment in directory.segments().into_iter() {
            match SegmentReader::open(segment.clone()) {
                Ok(segment_reader) => {
                    segment_readers.push(segment_reader);
                }
                Err(err) => {
                    // TODO return err
                    println!("Error while opening {:?}, {:?}", segment, err);
                }
            }
        }
        Searcher {
            segments: segment_readers
        }
    }
}


impl Searcher {

    pub fn search(&self, terms: &Vec<Term>, collector: &mut Collector) {
        for segment in &self.segments {
            collector.set_segment(&segment);
            let postings = segment.search(terms);
            for doc_id in postings {
                collector.collect(doc_id);
            }
        }
    }

}
