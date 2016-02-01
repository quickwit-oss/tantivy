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
        for segment in &self.segments {
            collector.set_segment(&segment);
            let postings = segment.search(terms);
            for doc_id in postings {
                collector.collect(doc_id);
            }

        }
    }

}
