extern crate tantivy;
use tantivy::core::simdcompression::Encoder;


fn main() {
    let data: Vec<u32> = vec!(2,3,3,4,12,32,34);
    let mut encoder = Encoder::new();
    let output = encoder.encode(&data);
    println!("{}", output.len());
}
