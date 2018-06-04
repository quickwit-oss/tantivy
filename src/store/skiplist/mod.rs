#![allow(dead_code)]

mod skiplist;
mod skiplist_builder;

pub use self::skiplist::SkipList;
pub use self::skiplist_builder::SkipListBuilder;

#[cfg(test)]
mod tests {

    use super::{SkipList, SkipListBuilder};

    #[test]
    fn test_skiplist() {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder<u32> = SkipListBuilder::new(8);
        skip_list_builder.insert(2, &3).unwrap();
        skip_list_builder.write::<Vec<u8>>(&mut output).unwrap();
        let mut skip_list: SkipList<u32> = SkipList::from(output.as_slice());
        assert_eq!(skip_list.next(), Some((2, 3)));
    }

    #[test]
    fn test_skiplist2() {
        let mut output: Vec<u8> = Vec::new();
        let skip_list_builder: SkipListBuilder<u32> = SkipListBuilder::new(8);
        skip_list_builder.write::<Vec<u8>>(&mut output).unwrap();
        let mut skip_list: SkipList<u32> = SkipList::from(output.as_slice());
        assert_eq!(skip_list.next(), None);
    }

    #[test]
    fn test_skiplist3() {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder<()> = SkipListBuilder::new(2);
        skip_list_builder.insert(2, &()).unwrap();
        skip_list_builder.insert(3, &()).unwrap();
        skip_list_builder.insert(5, &()).unwrap();
        skip_list_builder.insert(7, &()).unwrap();
        skip_list_builder.insert(9, &()).unwrap();
        skip_list_builder.write::<Vec<u8>>(&mut output).unwrap();
        let mut skip_list: SkipList<()> = SkipList::from(output.as_slice());
        assert_eq!(skip_list.next().unwrap(), (2, ()));
        assert_eq!(skip_list.next().unwrap(), (3, ()));
        assert_eq!(skip_list.next().unwrap(), (5, ()));
        assert_eq!(skip_list.next().unwrap(), (7, ()));
        assert_eq!(skip_list.next().unwrap(), (9, ()));
        assert_eq!(skip_list.next(), None);
    }

    #[test]
    fn test_skiplist4() {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder<()> = SkipListBuilder::new(2);
        skip_list_builder.insert(2, &()).unwrap();
        skip_list_builder.insert(3, &()).unwrap();
        skip_list_builder.insert(5, &()).unwrap();
        skip_list_builder.insert(7, &()).unwrap();
        skip_list_builder.insert(9, &()).unwrap();
        skip_list_builder.write::<Vec<u8>>(&mut output).unwrap();
        let mut skip_list: SkipList<()> = SkipList::from(output.as_slice());
        assert_eq!(skip_list.next().unwrap(), (2, ()));
        skip_list.seek(5);
        assert_eq!(skip_list.next().unwrap(), (5, ()));
        assert_eq!(skip_list.next().unwrap(), (7, ()));
        assert_eq!(skip_list.next().unwrap(), (9, ()));
        assert_eq!(skip_list.next(), None);
    }

    #[test]
    fn test_skiplist5() {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder<()> = SkipListBuilder::new(4);
        skip_list_builder.insert(2, &()).unwrap();
        skip_list_builder.insert(3, &()).unwrap();
        skip_list_builder.insert(5, &()).unwrap();
        skip_list_builder.insert(6, &()).unwrap();
        skip_list_builder.write::<Vec<u8>>(&mut output).unwrap();
        let mut skip_list: SkipList<()> = SkipList::from(output.as_slice());
        assert_eq!(skip_list.next().unwrap(), (2, ()));
        skip_list.seek(6);
        assert_eq!(skip_list.next().unwrap(), (6, ()));
        assert_eq!(skip_list.next(), None);
    }

    #[test]
    fn test_skiplist6() {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder<()> = SkipListBuilder::new(2);
        skip_list_builder.insert(2, &()).unwrap();
        skip_list_builder.insert(3, &()).unwrap();
        skip_list_builder.insert(5, &()).unwrap();
        skip_list_builder.insert(7, &()).unwrap();
        skip_list_builder.insert(9, &()).unwrap();
        skip_list_builder.write::<Vec<u8>>(&mut output).unwrap();
        let mut skip_list: SkipList<()> = SkipList::from(output.as_slice());
        assert_eq!(skip_list.next().unwrap(), (2, ()));
        skip_list.seek(10);
        assert_eq!(skip_list.next(), None);
    }

    #[test]
    fn test_skiplist7() {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder<()> = SkipListBuilder::new(4);
        for i in 0..1000 {
            skip_list_builder.insert(i, &()).unwrap();
        }
        skip_list_builder.insert(1004, &()).unwrap();
        skip_list_builder.write::<Vec<u8>>(&mut output).unwrap();
        let mut skip_list: SkipList<()> = SkipList::from(output.as_slice());
        assert_eq!(skip_list.next().unwrap(), (0, ()));
        skip_list.seek(431);
        assert_eq!(skip_list.next().unwrap(), (431, ()));
        skip_list.seek(1003);
        assert_eq!(skip_list.next().unwrap(), (1004, ()));
        assert_eq!(skip_list.next(), None);
    }

    #[test]
    fn test_skiplist8() {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder<u64> = SkipListBuilder::new(8);
        skip_list_builder.insert(2, &3).unwrap();
        skip_list_builder.write::<Vec<u8>>(&mut output).unwrap();
        assert_eq!(output.len(), 11);
        assert_eq!(output[0], 1u8 + 128u8);
    }

    #[test]
    fn test_skiplist9() {
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder<u64> = SkipListBuilder::new(4);
        for i in 0..4 * 4 * 4 {
            skip_list_builder.insert(i, &i).unwrap();
        }
        skip_list_builder.write::<Vec<u8>>(&mut output).unwrap();
        assert_eq!(output.len(), 774);
        assert_eq!(output[0], 4u8 + 128u8);
    }

    #[test]
    fn test_skiplist10() {
        // checking that void gets serialized to nothing.
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder<()> = SkipListBuilder::new(4);
        for i in 0..((4 * 4 * 4) - 1) {
            skip_list_builder.insert(i, &()).unwrap();
        }
        skip_list_builder.write::<Vec<u8>>(&mut output).unwrap();
        assert_eq!(output.len(), 230);
        assert_eq!(output[0], 128u8 + 3u8);
    }

    #[test]
    fn test_skiplist11() {
        // checking that void gets serialized to nothing.
        let mut output: Vec<u8> = Vec::new();
        let mut skip_list_builder: SkipListBuilder<()> = SkipListBuilder::new(4);
        for i in 0..(4 * 4) {
            skip_list_builder.insert(i, &()).unwrap();
        }
        skip_list_builder.write::<Vec<u8>>(&mut output).unwrap();
        assert_eq!(output.len(), 65);
        assert_eq!(output[0], 128u8 + 3u8);
    }

}
