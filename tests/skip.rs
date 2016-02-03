extern crate tantivy;

use std::io::Write;
use tantivy::core::skip::SkipListBuilder;

#[test]
fn test_skip_list_builder() {
    {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder = SkipListBuilder::new(10);
        skip_list_builder.insert(2, 3);
        skip_list_builder.write::<Vec<u8>>(&mut output);
        assert_eq!(output.len(), 17);
        assert_eq!(output[0], 1);
    }
    {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder = SkipListBuilder::new(3);
        for i in (0..9) {
            skip_list_builder.insert(i, i);
        }
        skip_list_builder.write::<Vec<u8>>(&mut output);
        assert_eq!(output.len(), 129);
        assert_eq!(output[0], 3);
    }
    {
        // checking that void gets serialized to nothing.
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder = SkipListBuilder::new(3);
        for i in (0..9) {
            skip_list_builder.insert(i, ());
        }
        skip_list_builder.write::<Vec<u8>>(&mut output);
        assert_eq!(output.len(), 93);
        assert_eq!(output[0], 3);
    }
}
