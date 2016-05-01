// pub mod postings;
// pub mod schema;
// pub mod directory;
// pub mod writer;
// pub mod analyzer;
// pub mod reader;
// pub mod codec;
// pub mod searcher;
// pub mod collector;
// pub mod serialize;
// pub mod store;
// pub mod simdcompression;
// pub mod fstmap;
// pub mod index;
// pub mod fastfield;
// pub mod fastdivide;
// pub mod merger;
// pub mod timer;

// use std::error;
// use std::io;

// pub fn convert_to_ioerror<E: 'static + error::Error + Send + Sync>(err: E) -> io::Error {
//     io::Error::new(
//         io::ErrorKind::InvalidData,
//         err
//     )
// }

mod serializer;
mod writer;
mod term_info;

use DocId;
pub use self::serializer::PostingsSerializer;
pub use self::writer::PostingsWriter;
pub use self::term_info::TermInfo;

pub trait Postings: Iterator<Item=DocId> {
    // after skipping position
    // the iterator in such a way that the
    // next call to next() will return a
    // value greater or equal to target.
    fn skip_next(&mut self, target: DocId) -> Option<DocId>;
}


#[cfg(test)]
mod tests {

    use super::*;
    use DocId;


    #[derive(Debug)]
    pub struct VecPostings {
        doc_ids: Vec<DocId>,
    	cursor: usize,
    }

    impl VecPostings {
        pub fn new(vals: Vec<DocId>) -> VecPostings {
            VecPostings {
                doc_ids: vals,
    			cursor: 0,
            }
        }
    }

    impl Postings for VecPostings {
        // after skipping position
        // the iterator in such a way that the
        // next call to next() will return a
        // value greater or equal to target.
        fn skip_next(&mut self, target: DocId) -> Option<DocId> {
            loop {
                match Iterator::next(self) {
                    Some(val) if val >= target => {
                        return Some(val);
                    },
                    None => {
                        return None;
                    },
                    _ => {}
                }
            }
        }
    }

    impl Iterator for VecPostings {
    	type Item = DocId;
    	fn next(&mut self,) -> Option<DocId> {
    		if self.cursor >= self.doc_ids.len() {
    			None
    		}
    		else {
                self.cursor += 1;
    			Some(self.doc_ids[self.cursor - 1])
    		}
    	}
    }


    // use test::Bencher;
    // #[test]
    // fn test_intersection() {
    //     {
    //         let left = VecPostings::new(vec!(1, 3, 9));
    //         let right = VecPostings::new(vec!(3, 4, 9, 18));
    //         let inter = IntersectionPostings::from_postings(vec!(left, right));
    //         let vals: Vec<DocId> = inter.collect();
    //         assert_eq!(vals, vec!(3, 9));
    //     }
    //     {
    //         let a = VecPostings::new(vec!(1, 3, 9));
    //         let b = VecPostings::new(vec!(3, 4, 9, 18));
    //         let c = VecPostings::new(vec!(1, 5, 9, 111));
    //         let inter = IntersectionPostings::from_postings(vec!(a, b, c));
    //         let vals: Vec<DocId> = inter.collect();
    //         assert_eq!(vals, vec!(9));
    //     }
    // }
    //
    // #[bench]
    // fn bench_single_intersection(b: &mut Bencher) {
    //     b.iter(|| {
    //         let docs = VecPostings::new((0..1_000_000).collect());
    //         let intersection = IntersectionPostings::from_postings(vec!(docs));
    //         intersection.count()
    //     });
    // }
}
