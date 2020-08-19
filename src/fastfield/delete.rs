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
/// Warning: this function does not call terminate. The caller is in charge of
/// closing the writer properly.
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
    #[cfg(test)]
    pub(crate) fn for_test(docs: &[DocId], max_doc: u32) -> DeleteBitSet {
        use crate::directory::{Directory, RAMDirectory, TerminatingWrite};
        use std::path::Path;
        assert!(docs.iter().all(|&doc| doc < max_doc));
        let mut bitset = BitSet::with_max_value(max_doc);
        for &doc in docs {
            bitset.insert(doc);
        }
        let mut directory = RAMDirectory::create();
        let path = Path::new("dummydeletebitset");
        let mut wrt = directory.open_write(path).unwrap();
        write_delete_bitset(&bitset, max_doc, &mut wrt).unwrap();
        wrt.terminate().unwrap();
        let source = directory.open_read(path).unwrap();
        Self::open(source)
    }

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
    use super::DeleteBitSet;
    use crate::common::HasLen;

    #[test]
    fn test_delete_bitset_empty() {
        let delete_bitset = DeleteBitSet::for_test(&[], 10);
        for doc in 0..10 {
            assert_eq!(delete_bitset.is_deleted(doc), !delete_bitset.is_alive(doc));
        }
        assert_eq!(delete_bitset.len(), 0);
    }

    #[test]
    fn test_delete_bitset() {
        let delete_bitset = DeleteBitSet::for_test(&[1, 9], 10);
        assert!(delete_bitset.is_alive(0));
        assert!(delete_bitset.is_deleted(1));
        assert!(delete_bitset.is_alive(2));
        assert!(delete_bitset.is_alive(3));
        assert!(delete_bitset.is_alive(4));
        assert!(delete_bitset.is_alive(5));
        assert!(delete_bitset.is_alive(6));
        assert!(delete_bitset.is_alive(6));
        assert!(delete_bitset.is_alive(7));
        assert!(delete_bitset.is_alive(8));
        assert!(delete_bitset.is_deleted(9));
        for doc in 0..10 {
            assert_eq!(delete_bitset.is_deleted(doc), !delete_bitset.is_alive(doc));
        }
        assert_eq!(delete_bitset.len(), 2);
    }
}
