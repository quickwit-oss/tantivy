extern crate tantivy;
extern crate byteorder;
use std::io::{Write, Seek};
use std::io::SeekFrom;
use tantivy::core::skip::{SkipListBuilder, SkipList};
use std::io::Cursor;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

#[test]
fn test_skip_list_builder() {
    {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder = SkipListBuilder::new(10);
        skip_list_builder.insert(2, &3);
        skip_list_builder.write::<Vec<u8>>(&mut output);
        assert_eq!(output.len(), 17);
        assert_eq!(output[0], 1);
    }
    {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder = SkipListBuilder::new(3);
        for i in (0..9) {
            skip_list_builder.insert(i, &i);
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
            skip_list_builder.insert(i, &());
        }
        skip_list_builder.write::<Vec<u8>>(&mut output);
        assert_eq!(output.len(), 93);
        assert_eq!(output[0], 3);
    }
}

#[test]
fn test_skip_list_reader() {
    {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder = SkipListBuilder::new(10);
        skip_list_builder.insert(2, &3);
        skip_list_builder.write::<Vec<u8>>(&mut output);
        let mut skip_list: SkipList<u32> = SkipList::read(&mut output);
        assert_eq!(skip_list.next(), Some((2, 3)));
        assert_eq!(skip_list.next(), None);
    }
    {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder = SkipListBuilder::new(10);
        skip_list_builder.write::<Vec<u8>>(&mut output);
        let mut skip_list: SkipList<u32> = SkipList::read(&mut output);
        assert_eq!(skip_list.next(), None);
    }
    {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder = SkipListBuilder::new(2);
        skip_list_builder.insert(2, &());
        skip_list_builder.insert(3, &());
        skip_list_builder.insert(5, &());
        skip_list_builder.insert(7, &());
        skip_list_builder.insert(9, &());
        skip_list_builder.write::<Vec<u8>>(&mut output);
        let mut skip_list: SkipList<()> = SkipList::read(&mut output);
        assert_eq!(skip_list.next().unwrap(), (2, ()));
        assert_eq!(skip_list.next().unwrap(), (3, ()));
        assert_eq!(skip_list.next().unwrap(), (5, ()));
        assert_eq!(skip_list.next().unwrap(), (7, ()));
        assert_eq!(skip_list.next().unwrap(), (9, ()));
        assert_eq!(skip_list.next(), None);
    }
}
