extern crate parici;
extern crate itertools;

use parici::core::DocId;
use parici::core::postings::{VecPostings, intersection};
use parici::core::postings::Postings;

#[test]
fn test_intersection() {
    let left = VecPostings::new(vec!(1, 3, 9));
    let right = VecPostings::new(vec!(3, 4, 9, 18));
    let inter = intersection(&left, &right);
    let vals: Vec<DocId> = inter.iter().collect();
    itertools::assert_equal(vals, vec!(3, 9));
}
