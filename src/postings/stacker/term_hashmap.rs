use std::convert::TryInto;
use std::{iter, mem, slice};

use murmurhash32::murmurhash2;

use super::{Addr, MemoryArena};
use crate::postings::stacker::memory_arena::store;
use crate::postings::UnorderedTermId;
use crate::Term;

/// `KeyValue` is the item stored in the hash table.
/// The key is actually a `BytesRef` object stored in an external memory arena.
/// The `value_addr` also points to an address in the memory arena.
#[derive(Copy, Clone)]
struct KeyValue {
    key_value_addr: Addr,
    hash: u32,
    unordered_term_id: UnorderedTermId,
}

impl Default for KeyValue {
    fn default() -> Self {
        KeyValue {
            key_value_addr: Addr::null_pointer(),
            hash: 0u32,
            unordered_term_id: UnorderedTermId::default(),
        }
    }
}

impl KeyValue {
    #[inline]
    fn is_empty(self) -> bool {
        self.key_value_addr.is_null()
    }
}

/// Customized `HashMap` with string keys
///
/// This `HashMap` takes String as keys. Keys are
/// stored in a user defined memory arena.
///
/// The quirky API has the benefit of avoiding
/// the computation of the hash of the key twice,
/// or copying the key as long as there is no insert.
pub struct TermHashMap {
    table: Box<[KeyValue]>,
    mask: usize,
    occupied: Vec<usize>,
    len: usize,
}

impl Default for TermHashMap {
    fn default() -> Self {
        Self::new(1 << 10)
    }
}

struct QuadraticProbing {
    hash: usize,
    i: usize,
    mask: usize,
}

impl QuadraticProbing {
    fn compute(hash: usize, mask: usize) -> QuadraticProbing {
        QuadraticProbing { hash, i: 0, mask }
    }

    #[inline]
    fn next_probe(&mut self) -> usize {
        self.i += 1;
        (self.hash + self.i) & self.mask
    }
}

pub struct Iter<'a, 'm> {
    hashmap: &'a TermHashMap,
    memory_arena: &'m MemoryArena,
    inner: slice::Iter<'a, usize>,
}

impl<'a, 'm> Iterator for Iter<'a, 'm> {
    type Item = (Term<&'m [u8]>, Addr, UnorderedTermId);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().cloned().map(move |bucket: usize| {
            let kv = self.hashmap.table[bucket];
            let (key, offset): (&'m [u8], Addr) = self
                .hashmap
                .get_key_value(kv.key_value_addr, self.memory_arena);
            (Term::wrap(key), offset, kv.unordered_term_id)
        })
    }
}

/// Returns the greatest power of two lower or equal to `n`.
/// Except if n == 0, in that case, return 1.
///
/// # Panics if n == 0
fn compute_previous_power_of_two(n: usize) -> usize {
    assert!(n > 0);
    let msb = (63u32 - n.leading_zeros()) as u8;
    1 << msb
}

impl TermHashMap {
    pub(crate) fn new(table_size: usize) -> TermHashMap {
        assert!(table_size > 0);
        let table_size_power_of_2 = compute_previous_power_of_two(table_size);
        let table: Vec<KeyValue> = iter::repeat(KeyValue::default())
            .take(table_size_power_of_2)
            .collect();
        TermHashMap {
            table: table.into_boxed_slice(),
            mask: table_size_power_of_2 - 1,
            occupied: Vec::with_capacity(table_size_power_of_2 / 2),
            len: 0,
        }
    }

    pub fn read<Item: Copy + 'static>(&self, addr: Addr, memory_arena: &MemoryArena) -> Item {
        memory_arena.read(addr)
    }

    fn probe(&self, hash: u32) -> QuadraticProbing {
        QuadraticProbing::compute(hash as usize, self.mask)
    }

    pub fn mem_usage(&self) -> usize {
        self.table.len() * mem::size_of::<KeyValue>()
            + self.occupied.len()
                * std::mem::size_of_val(&self.occupied.get(0).cloned().unwrap_or_default())
    }

    fn is_saturated(&self) -> bool {
        self.table.len() < self.occupied.len() * 3
    }

    #[inline]
    fn get_key_value<'m>(&self, addr: Addr, memory_arena: &'m MemoryArena) -> (&'m [u8], Addr) {
        let data = memory_arena.slice_from(addr);
        let (key_bytes_len_enc, data) = data.split_at(2);
        let key_bytes_len: u16 = u16::from_ne_bytes(key_bytes_len_enc.try_into().unwrap());
        let key_bytes: &[u8] = &data[..key_bytes_len as usize];
        (key_bytes, addr.offset(2u32 + key_bytes_len as u32))
    }

    #[inline]
    fn get_value_addr_if_key_match(
        &self,
        target_key: &[u8],
        addr: Addr,
        memory_arena: &mut MemoryArena,
    ) -> Option<Addr> {
        let (stored_key, value_addr) = self.get_key_value(addr, memory_arena);
        if stored_key == target_key {
            Some(value_addr)
        } else {
            None
        }
    }

    fn set_bucket(&mut self, hash: u32, key_value_addr: Addr, bucket: usize) -> UnorderedTermId {
        self.occupied.push(bucket);
        let unordered_term_id = self.len as UnorderedTermId;
        self.len += 1;
        self.table[bucket] = KeyValue {
            key_value_addr,
            hash,
            unordered_term_id,
        };
        unordered_term_id
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn iter<'a, 'm>(&'a self, memory_arena: &'m MemoryArena) -> Iter<'a, 'm> {
        Iter {
            inner: self.occupied.iter(),
            hashmap: self,
            memory_arena,
        }
    }

    fn resize(&mut self) {
        let new_len = self.table.len() * 2;
        let mask = new_len - 1;
        self.mask = mask;
        let new_table = vec![KeyValue::default(); new_len].into_boxed_slice();
        let old_table = mem::replace(&mut self.table, new_table);
        for old_pos in self.occupied.iter_mut() {
            let key_value: KeyValue = old_table[*old_pos];
            let mut probe = QuadraticProbing::compute(key_value.hash as usize, mask);
            loop {
                let bucket = probe.next_probe();
                if self.table[bucket].is_empty() {
                    *old_pos = bucket;
                    self.table[bucket] = key_value;
                    break;
                }
            }
        }
    }

    /// `update` create a new entry for a given key if it does not exists
    /// or updates the existing entry.
    ///
    /// The actual logic for this update is define in the the `updater`
    /// argument.
    ///
    /// If the key is not present, `updater` will receive `None` and
    /// will be in charge of returning a default value.
    /// If the key already as an associated value, then it will be passed
    /// `Some(previous_value)`.
    pub fn mutate_or_create<V, TMutator>(
        &mut self,
        key: &[u8],
        memory_arena: &mut MemoryArena,
        mut updater: TMutator,
    ) -> UnorderedTermId
    where
        V: Copy + 'static,
        TMutator: FnMut(Option<V>) -> V,
    {
        if self.is_saturated() {
            self.resize();
        }
        let hash = murmurhash2(key);

        let mut probe = self.probe(hash);
        loop {
            let bucket = probe.next_probe();
            let kv: KeyValue = self.table[bucket];

            if kv.is_empty() {
                // The key does not exists yet.
                let val = updater(None);
                let num_bytes = std::mem::size_of::<u16>() + key.len() + std::mem::size_of::<V>();
                let key_addr = memory_arena.allocate_space(num_bytes);
                {
                    let data = memory_arena.slice_mut(key_addr, num_bytes);
                    let (key_len, data) = data.split_at_mut(2);
                    key_len.copy_from_slice(&(key.len() as u16).to_le_bytes());
                    let stop = key.len();
                    data[..key.len()].copy_from_slice(key);
                    store(&mut data[stop..], val);
                }
                return self.set_bucket(hash, key_addr, bucket);
            } else if kv.hash == hash {
                if let Some(val_addr) =
                    self.get_value_addr_if_key_match(key, kv.key_value_addr, memory_arena)
                {
                    let v = memory_arena.read(val_addr);
                    let new_v = updater(Some(v));
                    memory_arena.write_at(val_addr, new_v);
                    return kv.unordered_term_id;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use super::{compute_previous_power_of_two, TermHashMap};
    use crate::postings::stacker::MemoryArena;

    #[test]
    fn test_hash_map() {
        let mut arena = MemoryArena::new();
        let mut hash_map: TermHashMap = TermHashMap::new(1 << 18);
        hash_map.mutate_or_create(b"abc", &mut arena, |opt_val: Option<u32>| {
            assert_eq!(opt_val, None);
            3u32
        });
        hash_map.mutate_or_create(b"abcd", &mut arena, |opt_val: Option<u32>| {
            assert_eq!(opt_val, None);
            4u32
        });
        hash_map.mutate_or_create(b"abc", &mut arena, |opt_val: Option<u32>| {
            assert_eq!(opt_val, Some(3u32));
            5u32
        });
        let mut vanilla_hash_map = HashMap::new();
        let iter_values = hash_map.iter(&arena);
        for (key, addr, _) in iter_values {
            let val: u32 = arena.read(addr);
            vanilla_hash_map.insert(key.to_owned(), val);
        }
        assert_eq!(vanilla_hash_map.len(), 2);
    }

    #[test]
    fn test_compute_previous_power_of_two() {
        assert_eq!(compute_previous_power_of_two(8), 8);
        assert_eq!(compute_previous_power_of_two(9), 8);
        assert_eq!(compute_previous_power_of_two(7), 4);
        assert_eq!(compute_previous_power_of_two(u64::MAX as usize), 1 << 63);
    }
}
