use bit_set::BitSet;
use common::HasLen;
use common::TinySet;
use directory::ReadOnlySource;
use directory::WritePtr;
use std::io;
use std::io::Write;
use DocId;

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
    tinysets: Box<[TinySet]>,
    len: usize,
}

impl DeleteBitSet {
    /// Opens a delete bitset given its data source.
    pub fn open(data: ReadOnlySource) -> DeleteBitSet {
        let num_deleted: usize = data.as_slice()
            .iter()
            .map(|b| b.count_ones() as usize)
            .sum();
        let mut tinysets = Vec::new();
        let mut value_accumulator = 0u64;
        let byte_count = data.as_slice().len();
        for index in 0..byte_count {
            value_accumulator <<= 1;
            value_accumulator += data.as_slice()[byte_count - index - 1] as u64;
            if (index + 1) % 8 == 0 {
                tinysets.push(TinySet::from(value_accumulator).complement());
                value_accumulator = 0;
            }
        }
        if data.as_slice().len() % 8 != 0 {
            tinysets.push(TinySet::from(value_accumulator).complement());
        }
        DeleteBitSet {
            data,
            tinysets: tinysets.into_boxed_slice(),
            len: num_deleted,
        }
    }

    /// Returns whether the document has been marked as deleted.
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

    /// Returns the first non-empty `TinySet` associated to a bucket lower
    /// or greater than bucket.
    ///
    /// Reminder: the tiny set with the bucket `bucket`, represents the
    /// elements from `bucket * 64` to `(bucket+1) * 64`.
    pub(crate) fn first_non_empty_bucket(&self, bucket: u32) -> Option<u32> {
        self.tinysets[bucket as usize..]
            .iter()
            .cloned()
            .position(|tinyset| !tinyset.is_empty())
            .map(|delta_bucket| bucket + delta_bucket as u32)
    }

    /// Returns the tiny bitset representing the
    /// the set restricted to the number range from
    /// `bucket * 64` to `(bucket + 1) * 64`.
    pub(crate) fn tinyset(&self, bucket: u32) -> TinySet {
        self.tinysets[bucket as usize]
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
    use bit_set::BitSet;
    use directory::*;
    use std::path::PathBuf;

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
