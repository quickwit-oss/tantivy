use crate::directory::FileSlice;
use crate::directory::OwnedBytes;
use crate::space_usage::ByteCount;
use crate::DocId;
use common::BitSet;
use std::io;
use std::io::Write;

/// Write a delete `BitSet`
///
/// where `delete_bitset` is the set of deleted `DocId`.
/// Warning: this function does not call terminate. The caller is in charge of
/// closing the writer properly.
pub fn write_delete_bitset(delete_bitset: &BitSet, writer: &mut dyn Write) -> io::Result<()> {
    delete_bitset.serialize(writer)?;
    Ok(())
}

/// Set of deleted `DocId`s.
#[derive(Clone)]
pub struct DeleteBitSet {
    data: OwnedBytes,
    num_deleted: usize,
}

impl DeleteBitSet {
    #[cfg(test)]
    pub(crate) fn for_test(docs: &[DocId], max_doc: u32) -> DeleteBitSet {
        use crate::directory::{Directory, RamDirectory, TerminatingWrite};
        use std::path::Path;
        assert!(docs.iter().all(|&doc| doc < max_doc));
        let mut bitset = BitSet::with_max_value(max_doc);
        for &doc in docs {
            bitset.insert(doc);
        }
        let directory = RamDirectory::create();
        let path = Path::new("dummydeletebitset");
        let mut wrt = directory.open_write(path).unwrap();
        write_delete_bitset(&bitset, &mut wrt).unwrap();
        wrt.terminate().unwrap();
        let file = directory.open_read(path).unwrap();
        Self::open(file).unwrap()
    }

    /// Opens a delete bitset given its file.
    pub fn open(file: FileSlice) -> crate::Result<DeleteBitSet> {
        let bytes = file.read_bytes()?;
        let num_deleted = BitSet::iter_from_bytes(bytes.as_slice())
            .map(|tinyset| tinyset.len() as usize)
            .sum();

        Ok(DeleteBitSet {
            data: bytes,
            num_deleted,
        })
    }

    /// Returns true iff the document is still "alive". In other words, if it has not been deleted.
    #[inline]
    pub fn is_alive(&self, doc: DocId) -> bool {
        !self.is_deleted(doc)
    }

    /// Returns true iff the document has been marked as deleted.
    #[inline]
    pub fn is_deleted(&self, doc: DocId) -> bool {
        let data = self.data.as_slice();
        BitSet::contains_from_bytes(doc, data)
    }

    /// Iterate over the positions of the set elements
    #[inline]
    pub fn iter_unset(&self) -> impl Iterator<Item = u32> + '_ {
        let data = self.data.as_slice();
        BitSet::iter_unset_from_bytes(data)
    }

    /// The number of deleted docs
    pub fn num_deleted(&self) -> usize {
        self.num_deleted
    }
    /// Summarize total space usage of this bitset.
    pub fn space_usage(&self) -> ByteCount {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {

    use super::DeleteBitSet;

    #[test]
    fn test_delete_bitset_empty() {
        let delete_bitset = DeleteBitSet::for_test(&[], 10);
        for doc in 0..10 {
            assert_eq!(delete_bitset.is_deleted(doc), !delete_bitset.is_alive(doc));
        }
        assert_eq!(delete_bitset.num_deleted(), 0);
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
        assert_eq!(delete_bitset.num_deleted(), 2);
    }

    #[test]
    fn test_delete_bitset_iter_minimal() {
        let delete_bitset = DeleteBitSet::for_test(&[7], 8);

        let data: Vec<_> = delete_bitset.iter_unset().collect();
        assert_eq!(data, vec![0, 1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_delete_bitset_iter_small() {
        let delete_bitset = DeleteBitSet::for_test(&[0, 2, 3, 6], 7);

        let data: Vec<_> = delete_bitset.iter_unset().collect();
        assert_eq!(data, vec![1, 4, 5]);
    }
    #[test]
    fn test_delete_bitset_iter() {
        let delete_bitset = DeleteBitSet::for_test(&[0, 1, 1000], 1001);

        let data: Vec<_> = delete_bitset.iter_unset().collect();
        assert_eq!(data, (2..=999).collect::<Vec<_>>());
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use super::DeleteBitSet;
    use common::BitSet;
    use rand::prelude::IteratorRandom;
    use rand::prelude::SliceRandom;
    use rand::thread_rng;
    use test::Bencher;

    fn get_many_deleted() -> Vec<u32> {
        let mut data = (0..1_000_000_u32).collect::<Vec<u32>>();
        for _ in 0..(1_000_000) * 7 / 8 {
            remove_rand(&mut data);
        }
        data
    }

    fn remove_rand(raw: &mut Vec<u32>) {
        let i = (0..raw.len()).choose(&mut thread_rng()).unwrap();
        raw.remove(i);
    }

    #[bench]
    fn bench_deletebitset_iter_deser_on_fly(bench: &mut Bencher) {
        let delete_bitset = DeleteBitSet::for_test(&[0, 1, 1000, 10000], 1_000_000);

        bench.iter(|| delete_bitset.iter_unset().collect::<Vec<_>>());
    }

    #[bench]
    fn bench_deletebitset_access(bench: &mut Bencher) {
        let delete_bitset = DeleteBitSet::for_test(&[0, 1, 1000, 10000], 1_000_000);

        bench.iter(|| {
            (0..1_000_000_u32)
                .filter(|doc| delete_bitset.is_alive(*doc))
                .collect::<Vec<_>>()
        });
    }

    #[bench]
    fn bench_deletebitset_iter_deser_on_fly_1_8_alive(bench: &mut Bencher) {
        let delete_bitset = DeleteBitSet::for_test(&get_many_deleted(), 1_000_000);

        bench.iter(|| delete_bitset.iter_unset().collect::<Vec<_>>());
    }

    #[bench]
    fn bench_deletebitset_access_1_8_alive(bench: &mut Bencher) {
        let delete_bitset = DeleteBitSet::for_test(&get_many_deleted(), 1_000_000);

        bench.iter(|| {
            (0..1_000_000_u32)
                .filter(|doc| delete_bitset.is_alive(*doc))
                .collect::<Vec<_>>()
        });
    }
}
