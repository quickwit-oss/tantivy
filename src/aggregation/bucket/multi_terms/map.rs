use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::hash::Hash;

use rustc_hash::FxHashMap;

/// Maps fixed-length array keys and tracks the highest key for eviction.
#[derive(Clone, Debug)]
pub(super) struct ArrayHeapMap<K: Ord, V, const S: usize> {
    buckets: FxHashMap<[K; S], V>,
    heap: BinaryHeap<[K; S]>,
}

impl<K: Ord, V, const S: usize> Default for ArrayHeapMap<K, V, S> {
    fn default() -> Self {
        Self {
            buckets: FxHashMap::default(),
            heap: BinaryHeap::default(),
        }
    }
}

impl<K: Copy + Hash + Ord, V, const S: usize> ArrayHeapMap<K, V, S> {
    /// Returns an entry only when its key belongs in the first `size` keys.
    pub(super) fn get_or_insert_with(
        &mut self,
        key: &[K; S],
        size: usize,
        create: impl FnOnce() -> V,
    ) -> Option<&mut V> {
        if self.buckets.len() >= size {
            let highest = self.heap.peek().expect("page size is positive");
            if key > highest {
                return None;
            }
            if key < highest && !self.buckets.contains_key(key) {
                self.buckets.remove(&self.heap.pop().unwrap());
            }
        }
        Some(self.buckets.entry(*key).or_insert_with(|| {
            self.heap.push(*key);
            create()
        }))
    }

    pub(super) fn memory_consumption(&self) -> usize {
        let key_size = std::mem::size_of::<[K; S]>();
        let map_size = (key_size + std::mem::size_of::<V>()) * self.buckets.capacity();
        map_size + key_size * self.heap.capacity()
    }

    pub(super) fn into_iter(self) -> impl Iterator<Item = ([K; S], V)> {
        self.buckets.into_iter()
    }

    #[cfg(test)]
    pub(super) fn size(&self) -> usize {
        self.buckets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_heap_map() {
        let mut map = ArrayHeapMap::<u32, &str, 2>::default();
        let key1 = [1; 2];
        let key2 = [2; 2];
        map.get_or_insert_with(&key1, 2, || "a");
        map.get_or_insert_with(&key2, 2, || "b");
        assert_eq!(map.size(), 2);
        assert!(map.memory_consumption() > 0);
        // Existing keys, including the highest key, retain their entries.
        *map.get_or_insert_with(&key1, 2, || panic!("existing key"))
            .unwrap() = "updated";
        assert_eq!(
            map.get_or_insert_with(&key2, 2, || panic!("existing key")),
            Some(&mut "b")
        );
        assert!(map
            .get_or_insert_with(&[3; 2], 2, || panic!("rejected key"))
            .is_none());
        // A smaller key replaces the highest key, without changing the other entry.
        map.get_or_insert_with(&[0; 2], 2, || "new");
        assert_eq!(map.size(), 2);
        let mut entries: Vec<_> = map.into_iter().collect();
        entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(entries, vec![([0; 2], "new"), (key1, "updated")]);
    }
}
