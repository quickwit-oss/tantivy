use crate::directory::FileSlice;
use crate::directory::OwnedBytes;
use crate::space_usage::ByteCount;
use crate::DocId;
use common::BitSet;
use common::ReadSerializedBitSet;
use std::io;
use std::io::Write;

/// Write a alive `BitSet`
///
/// where `alive_bitset` is the set of alive `DocId`.
/// Warning: this function does not call terminate. The caller is in charge of
/// closing the writer properly.
pub fn write_alive_bitset<T: Write>(alive_bitset: &BitSet, writer: &mut T) -> io::Result<()> {
    alive_bitset.serialize(writer)?;
    Ok(())
}

/// Set of alive `DocId`s.
#[derive(Clone)]
pub struct AliveBitSet {
    data: OwnedBytes,
    num_deleted: usize,
    bitset: ReadSerializedBitSet,
}

impl AliveBitSet {
    #[cfg(test)]
    pub(crate) fn for_test(deleted_docs: &[DocId], max_doc: u32) -> AliveBitSet {
        use crate::directory::{Directory, RamDirectory, TerminatingWrite};
        use std::path::Path;
        assert!(deleted_docs.iter().all(|&doc| doc < max_doc));
        let mut bitset = BitSet::with_max_value_and_full(max_doc);
        for &doc in deleted_docs {
            bitset.remove(doc);
        }
        let directory = RamDirectory::create();
        let path = Path::new("dummydeletebitset");
        let mut wrt = directory.open_write(path).unwrap();
        write_alive_bitset(&bitset, &mut wrt).unwrap();
        wrt.terminate().unwrap();
        let file = directory.open_read(path).unwrap();
        Self::open(file).unwrap()
    }

    /// Opens a delete bitset given its file.
    pub fn open(file: FileSlice) -> crate::Result<AliveBitSet> {
        let bytes = file.read_bytes()?;
        let bitset = ReadSerializedBitSet::open(bytes.clone());
        let num_deleted = bitset.count_unset();

        Ok(AliveBitSet {
            data: bytes,
            num_deleted,
            bitset,
        })
    }

    /// Returns true iff the document is still "alive". In other words, if it has not been deleted.
    #[inline]
    pub fn is_alive(&self, doc: DocId) -> bool {
        self.bitset.contains(doc)
    }

    /// Returns true iff the document has been marked as deleted.
    #[inline]
    pub fn is_deleted(&self, doc: DocId) -> bool {
        !self.is_alive(doc)
    }

    /// Iterate over the alive docids.
    #[inline]
    pub fn iter_alive(&self) -> impl Iterator<Item = DocId> + '_ {
        self.bitset.iter()
    }

    /// Get underlying bitset
    #[inline]
    pub fn bitset(&self) -> &ReadSerializedBitSet {
        &self.bitset
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

    use super::AliveBitSet;

    #[test]
    fn test_alive_bitset_empty() {
        let alive_bitset = AliveBitSet::for_test(&[], 10);
        for doc in 0..10 {
            assert_eq!(alive_bitset.is_deleted(doc), !alive_bitset.is_alive(doc));
        }
        assert_eq!(alive_bitset.num_deleted(), 0);
    }

    #[test]
    fn test_alive_bitset() {
        let alive_bitset = AliveBitSet::for_test(&[1, 9], 10);
        assert!(alive_bitset.is_alive(0));
        assert!(alive_bitset.is_deleted(1));
        assert!(alive_bitset.is_alive(2));
        assert!(alive_bitset.is_alive(3));
        assert!(alive_bitset.is_alive(4));
        assert!(alive_bitset.is_alive(5));
        assert!(alive_bitset.is_alive(6));
        assert!(alive_bitset.is_alive(6));
        assert!(alive_bitset.is_alive(7));
        assert!(alive_bitset.is_alive(8));
        assert!(alive_bitset.is_deleted(9));
        for doc in 0..10 {
            assert_eq!(alive_bitset.is_deleted(doc), !alive_bitset.is_alive(doc));
        }
        assert_eq!(alive_bitset.num_deleted(), 2);
    }

    #[test]
    fn test_alive_bitset_iter_minimal() {
        let alive_bitset = AliveBitSet::for_test(&[7], 8);

        let data: Vec<_> = alive_bitset.iter_alive().collect();
        assert_eq!(data, vec![0, 1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_alive_bitset_iter_small() {
        let alive_bitset = AliveBitSet::for_test(&[0, 2, 3, 6], 7);

        let data: Vec<_> = alive_bitset.iter_alive().collect();
        assert_eq!(data, vec![1, 4, 5]);
    }
    #[test]
    fn test_alive_bitset_iter() {
        let alive_bitset = AliveBitSet::for_test(&[0, 1, 1000], 1001);

        let data: Vec<_> = alive_bitset.iter_alive().collect();
        assert_eq!(data, (2..=999).collect::<Vec<_>>());
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {

    use super::AliveBitSet;
    use rand::prelude::IteratorRandom;
    use rand::thread_rng;
    use test::Bencher;

    fn get_alive() -> Vec<u32> {
        let mut data = (0..1_000_000_u32).collect::<Vec<u32>>();
        for _ in 0..(1_000_000) * 1 / 8 {
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
        let alive_bitset = AliveBitSet::for_test(&[0, 1, 1000, 10000], 1_000_000);

        bench.iter(|| alive_bitset.iter_alive().collect::<Vec<_>>());
    }

    #[bench]
    fn bench_deletebitset_access(bench: &mut Bencher) {
        let alive_bitset = AliveBitSet::for_test(&[0, 1, 1000, 10000], 1_000_000);

        bench.iter(|| {
            (0..1_000_000_u32)
                .filter(|doc| alive_bitset.is_alive(*doc))
                .collect::<Vec<_>>()
        });
    }

    #[bench]
    fn bench_deletebitset_iter_deser_on_fly_1_8_alive(bench: &mut Bencher) {
        let alive_bitset = AliveBitSet::for_test(&get_alive(), 1_000_000);

        bench.iter(|| alive_bitset.iter_alive().collect::<Vec<_>>());
    }

    #[bench]
    fn bench_deletebitset_access_1_8_alive(bench: &mut Bencher) {
        let alive_bitset = AliveBitSet::for_test(&get_alive(), 1_000_000);

        bench.iter(|| {
            (0..1_000_000_u32)
                .filter(|doc| alive_bitset.is_alive(*doc))
                .collect::<Vec<_>>()
        });
    }
}
