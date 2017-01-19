use bit_set::BitSet;
use directory::WritePtr;
use std::io::Write;
use std::io;
use directory::ReadOnlySource;
use DocId;

pub fn write_delete_bitset(delete_bitset: &BitSet, writer: &mut WritePtr) -> io::Result<()> {
    let max_doc = delete_bitset.capacity();
    let mut byte = 0u8;
    let mut shift = 0u8;
    for doc in 0..max_doc {
        if delete_bitset.contains(doc) {
            byte |= 1 << shift;
        }
        if shift == 7 {
            writer.write(&[byte])?;
            shift = 0;
            byte = 0;
        }
        else {
            shift += 1;
        }
    }
    if max_doc % 8 > 0 {
        writer.write(&[byte])?;
    }
    writer.flush()
}

pub struct DeleteBitSet(ReadOnlySource);

impl DeleteBitSet {

    pub fn open(data: ReadOnlySource) -> DeleteBitSet {
        DeleteBitSet(data)
    }

    pub fn is_deleted(&self, doc: DocId) -> bool {
        let byte_offset = doc / 8u32;
        let b: u8 = (*self.0)[byte_offset as usize];
        let shift = (doc & 7u32) as u8;
        b & (1u8 << shift) != 0
    }
}



#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use bit_set::BitSet;
    use directory::*;
    use super::*;

    fn test_delete_bitset_helper(bitset: &BitSet) {
        let test_path = PathBuf::from("test");
        let mut directory = RAMDirectory::create();
        {
            let mut writer = directory.open_write(&*test_path).unwrap();
            write_delete_bitset(bitset, &mut writer).unwrap();
        }
        {
            let source = directory.open_read(&test_path).unwrap();
            let delete_bitset = DeleteBitSet::open(source);
            let n = bitset.capacity();
            for doc in 0..n {
                assert_eq!(bitset.contains(doc), delete_bitset.is_deleted(doc as DocId));
            }
        }
    }

    #[test]
    fn test_delete_bitset() {
        {
            let mut bitset = BitSet::with_capacity(10);
            bitset.insert(1);
            bitset.insert(9);
            test_delete_bitset_helper(&bitset);
        }
        {
            let mut bitset = BitSet::with_capacity(8);
            bitset.insert(1);
            bitset.insert(2);
            bitset.insert(3);
            bitset.insert(5);
            bitset.insert(7);
            test_delete_bitset_helper(&bitset);
        }
    }
}