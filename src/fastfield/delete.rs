use crate::common::{BitSet, HasLen};
use crate::directory::ReadOnlySource;
use crate::directory::WritePtr;
use crate::space_usage::ByteCount;
use crate::DocId;
use std::io;
use std::io::Write;

/// Write a delete `BitSet`
///
/// where `delete_bitset` is the set of deleted `DocId`.
pub fn write_delete_bitset(
    delete_bitset: &BitSet,
    max_doc: u32,
    writer: &mut WritePtr,
) -> io::Result<()> {
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
    Ok(())
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
        let num_deleted: usize = data
            .as_slice()
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum();
        DeleteBitSet {
            data,
            len: num_deleted,
        }
    }

    /// Returns true iff the document is still "alive". In other words, if it has not been deleted.
    pub fn is_alive(&self, doc: DocId) -> bool {
        !self.is_deleted(doc)
    }

    /// Returns true iff the document has been marked as deleted.
    #[inline(always)]
    pub fn is_deleted(&self, doc: DocId) -> bool {
        let byte_offset = doc / 8u32;
        let b: u8 = (*self.data)[byte_offset as usize];
        let shift = (doc & 7u32) as u8;
        b & (1u8 << shift) != 0
    }

    /// Summarize total space usage of this bitset.
    pub fn space_usage(&self) -> ByteCount {
        self.data.len()
    }
}

impl HasLen for DeleteBitSet {
    fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::directory::*;
    use std::path::PathBuf;

    fn test_delete_bitset_helper(bitset: &BitSet, max_doc: u32) {
        let test_path = PathBuf::from("test");
        let mut directory = RAMDirectory::create();
        {
            let mut writer = directory.open_write(&*test_path).unwrap();
            write_delete_bitset(bitset, max_doc, &mut writer).unwrap();
            writer.terminate().unwrap();
        }
        let source = directory.open_read(&test_path).unwrap();
        let delete_bitset = DeleteBitSet::open(source);
        for doc in 0..max_doc {
            assert_eq!(bitset.contains(doc), delete_bitset.is_deleted(doc as DocId));
        }
        assert_eq!(delete_bitset.len(), bitset.len());
    }

    #[test]
    fn test_delete_bitset() {
        {
            let mut bitset = BitSet::with_max_value(10);
            bitset.insert(1);
            bitset.insert(9);
            test_delete_bitset_helper(&bitset, 10);
        }
        {
            let mut bitset = BitSet::with_max_value(8);
            bitset.insert(1);
            bitset.insert(2);
            bitset.insert(3);
            bitset.insert(5);
            bitset.insert(7);
            test_delete_bitset_helper(&bitset, 8);
        }
    }
}
