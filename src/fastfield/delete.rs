use bit_set::BitSet;
use directory::WritePtr;
use std::io::Write;
use std::io;
use directory::ReadOnlySource;
use DocId;
use common::HasLen;

/// Write a delete `BitSet`
///
/// where `delete_bitset` is the set of deleted `DocId`.
pub fn write_delete_bitset(delete_bitset: &BitSet, writer: &mut WritePtr) -> io::Result<()> {
    let max_doc = delete_bitset.capacity();
    let mut byte = 0u8;
    let mut shift = 0u8;
    for doc in 0..max_doc {
        if delete_bitset.contains(doc) {
            byte |= 1 << shift;
        }
        if shift == 7 {
            writer.write_all(&[byte])?;
            shift = 0;
            byte = 0;
        } else {
            shift += 1;
        }
    }
    if max_doc % 8 > 0 {
        writer.write_all(&[byte])?;
    }
    writer.flush()
}

/// Set of deleted `DocId`s.
#[derive(Clone)]
pub struct DeleteBitSet {
    data: ReadOnlySource,
    len: usize,
}


impl DeleteBitSet {
    /// Opens a delete bitset given its data source.
    pub fn open(data: ReadOnlySource) -> DeleteBitSet {
        let num_deleted: usize = data.as_slice()
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum();
        DeleteBitSet {
            data: data,
            len: num_deleted,
        }
    }

    /// Returns an empty delete bit set.
    pub fn empty() -> DeleteBitSet {
        DeleteBitSet {
            data: ReadOnlySource::empty(),
            len: 0,
        }
    }

    /// Returns true iff the segment has some deleted documents.
    pub fn has_deletes(&self) -> bool {
        self.len() > 0
    }

    /// Returns true iff the document is deleted.
    pub fn is_deleted(&self, doc: DocId) -> bool {
        if self.len == 0 {
            false
        } else {
            let byte_offset = doc / 8u32;
            let b: u8 = (*self.data)[byte_offset as usize];
            let shift = (doc & 7u32) as u8;
            b & (1u8 << shift) != 0
        }
    }
}


impl HasLen for DeleteBitSet {
    fn len(&self) -> usize {
        self.len
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
            assert_eq!(delete_bitset.len(), bitset.len());
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
