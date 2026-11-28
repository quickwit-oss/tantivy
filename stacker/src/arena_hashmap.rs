use super::{Addr, MemoryArena};
use crate::shared_arena_hashmap::SharedArenaHashMap;

/// Customized `HashMap` with `&[u8]` keys
///
/// Its main particularity is that rather than storing its
/// keys in the heap, keys are stored in a memory arena
/// inline with the values.
///
/// The quirky API has the benefit of avoiding
/// the computation of the hash of the key twice,
/// or copying the key as long as there is no insert.
///
/// ArenaHashMap is like SharedArenaHashMap but takes ownership
/// of the memory arena. The memory arena stores the serialized
/// keys and values.
pub struct ArenaHashMap {
    shared_arena_hashmap: SharedArenaHashMap,
    pub memory_arena: MemoryArena,
}

impl Default for ArenaHashMap {
    fn default() -> Self {
        ArenaHashMap::with_capacity(4)
    }
}

impl ArenaHashMap {
    pub fn with_capacity(table_size: usize) -> ArenaHashMap {
        let memory_arena = MemoryArena::default();

        ArenaHashMap {
            shared_arena_hashmap: SharedArenaHashMap::with_capacity(table_size),
            memory_arena,
        }
    }

    #[inline]
    pub fn read<Item: Copy + 'static>(&self, addr: Addr) -> Item {
        self.memory_arena.read(addr)
    }

    #[inline]
    pub fn mem_usage(&self) -> usize {
        self.shared_arena_hashmap.mem_usage() + self.memory_arena.mem_usage()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.shared_arena_hashmap.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.shared_arena_hashmap.len()
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], Addr)> {
        self.shared_arena_hashmap.iter(&self.memory_arena)
    }

    /// Get a value associated to a key.
    #[inline]
    pub fn get<V>(&self, key: &[u8]) -> Option<V>
    where V: Copy + 'static {
        self.shared_arena_hashmap.get(key, &self.memory_arena)
    }

    /// `update` create a new entry for a given key if it does not exist
    /// or updates the existing entry.
    ///
    /// The actual logic for this update is define in the `updater`
    /// argument.
    ///
    /// If the key is not present, `updater` will receive `None` and
    /// will be in charge of returning a default value.
    /// If the key already as an associated value, then it will be passed
    /// `Some(previous_value)`.
    #[inline]
    pub fn mutate_or_create<V>(&mut self, key: &[u8], updater: impl FnMut(Option<V>) -> V)
    where V: Copy + 'static {
        self.shared_arena_hashmap
            .mutate_or_create(key, &mut self.memory_arena, updater);
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::ArenaHashMap;

    #[test]
    fn test_hash_map() {
        let mut hash_map: ArenaHashMap = ArenaHashMap::default();
        hash_map.mutate_or_create(b"abc", |opt_val: Option<u32>| {
            assert_eq!(opt_val, None);
            3u32
        });
        hash_map.mutate_or_create(b"abcd", |opt_val: Option<u32>| {
            assert_eq!(opt_val, None);
            4u32
        });
        hash_map.mutate_or_create(b"abc", |opt_val: Option<u32>| {
            assert_eq!(opt_val, Some(3u32));
            5u32
        });
        let mut vanilla_hash_map = HashMap::new();
        let iter_values = hash_map.iter();
        for (key, addr) in iter_values {
            let val: u32 = hash_map.memory_arena.read(addr);
            vanilla_hash_map.insert(key.to_owned(), val);
        }
        assert_eq!(vanilla_hash_map.len(), 2);
    }
    #[test]
    fn test_empty_hashmap() {
        let hash_map: ArenaHashMap = ArenaHashMap::default();
        assert_eq!(hash_map.get::<u32>(b"abc"), None);
    }

    #[test]
    fn test_many_terms() {
        let mut terms: Vec<String> = (0..20_000).map(|val| val.to_string()).collect();
        let mut hash_map: ArenaHashMap = ArenaHashMap::default();
        for term in terms.iter() {
            hash_map.mutate_or_create(term.as_bytes(), |_opt_val: Option<u32>| 5u32);
        }
        let mut terms_back: Vec<String> = hash_map
            .iter()
            .map(|(bytes, _)| String::from_utf8(bytes.to_vec()).unwrap())
            .collect();
        terms_back.sort();
        terms.sort();

        for pos in 0..terms.len() {
            assert_eq!(terms[pos], terms_back[pos]);
        }
    }

    #[test]
    fn test_empty_key_and_all_lengths_0_to_130() {
        let mut hash_map: ArenaHashMap = ArenaHashMap::with_capacity(4);
        let mut expected = HashMap::new();

        for len in 0usize..=130 {
            let key: Vec<u8> = (0usize..len)
                .map(|i| (i.wrapping_mul(31).wrapping_add(len)) as u8)
                .collect();
            hash_map.mutate_or_create(&key, |opt_val: Option<u64>| {
                assert_eq!(opt_val, None);
                len as u64
            });
            expected.insert(key, len as u64);
        }

        // Update every key and check previous value matches.
        for (key, val) in expected.iter_mut() {
            let old = *val;
            let new_val = old * 10 + 7;
            hash_map.mutate_or_create(key, |opt_val: Option<u64>| {
                assert_eq!(opt_val, Some(old));
                new_val
            });
            *val = new_val;
        }

        assert_eq!(hash_map.len(), expected.len());
        assert_eq!(hash_map.get::<u64>(b""), Some(7));
        for (key, &val) in &expected {
            assert_eq!(hash_map.get::<u64>(key), Some(val));
        }

        let mut seen = HashMap::new();
        for (key, addr) in hash_map.iter() {
            let val: u64 = hash_map.read(addr);
            seen.insert(key.to_vec(), val);
        }
        assert_eq!(seen, expected);
    }

    #[test]
    fn test_quickwit_long_json_keys_and_resizing() {
        let mut hash_map: ArenaHashMap = ArenaHashMap::with_capacity(4);
        let mut expected = HashMap::new();
        let prefixes: &[&[u8]] = &[
            b"attributes\x01k8s\x01container\x01pod_name\x01prod-us-east-1-",
            b"attributes\x01http\x01request\x01headers\x01x-forwarded-for\x01",
            b"resource\x01service\x01instance\x01id\x01",
        ];

        for i in 0u32..2000 {
            let mut key = prefixes[(i as usize) % prefixes.len()].to_vec();
            key.extend_from_slice(format!("worker-{:04}-shard-{}", i % 500, i % 13).as_bytes());
            let entry = expected.entry(key.clone()).or_insert(0u32);
            *entry += 1;
            let expected_count = *entry;
            hash_map.mutate_or_create(&key, |opt_val: Option<u32>| {
                let next = opt_val.unwrap_or(0) + 1;
                assert_eq!(next, expected_count);
                next
            });
        }

        assert_eq!(hash_map.len(), expected.len());
        for (key, &val) in &expected {
            assert_eq!(hash_map.get::<u32>(key), Some(val));
            let mut missing = key.clone();
            let last = missing.len() - 1;
            missing[last] ^= 0x01;
            if !expected.contains_key(&missing) {
                assert_eq!(hash_map.get::<u32>(&missing), None);
            }
        }
    }
}
