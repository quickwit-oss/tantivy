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
        println!("direct {}", directory.segments().len());
        Searcher {
            segments: directory.segments()
                .into_iter()
                .map(|segment| SegmentReader::open(segment).unwrap() ) // TODO error handling
                .collect()
        }
    }
}



impl Searcher {

    pub fn search(&self, terms: &Vec<Term>, collector: &mut Collector) {
        println!("Searcher");
        for segment in &self.segments {
            let postings = segment.search(terms);
            for doc_id in postings.iter() {
                collector.collect(doc_id);
            }
            collector.set_segment(&segment);
        }
    }

}
