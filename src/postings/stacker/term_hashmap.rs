use super::murmurhash2;
use super::{Addr, ArenaStorable, MemoryArena};
use std::iter;
use std::mem;
use std::slice;

pub type BucketId = usize;

struct KeyBytesValue<'a, V> {
    key: &'a [u8],
    value: V,
}

impl<'a, V> KeyBytesValue<'a, V> {
    fn new(key: &'a [u8], value: V) -> KeyBytesValue<'a, V> {
        KeyBytesValue { key, value }
    }
}

impl<'a, V> ArenaStorable for KeyBytesValue<'a, V>
where
    V: ArenaStorable,
{
    fn num_bytes(&self) -> usize {
        0u16.num_bytes() + self.key.len() + self.value.num_bytes()
    }

    unsafe fn write_into(self, arena: &mut MemoryArena, addr: Addr) {
        arena.write(addr, self.key.len() as u16);
        arena.write_bytes(addr.offset(2), self.key);
        arena.write(addr.offset(2 + self.key.len() as u32), self.value);
    }
}

/// Returns the actual memory size in bytes
/// required to create a table of size $2^num_bits$.
pub fn compute_table_size(num_bits: usize) -> usize {
    (1 << num_bits) * mem::size_of::<KeyValue>()
}

/// `KeyValue` is the item stored in the hash table.
/// The key is actually a `BytesRef` object stored in an external heap.
/// The `value_addr` also points to an address in the heap.
///
/// The key and the value are actually stored contiguously.
/// For this reason, the (start, stop) information is actually redundant
/// and can be simplified in the future
#[derive(Copy, Clone)]
struct KeyValue {
    key_value_addr: Addr,
    hash: u32,
}

impl Default for KeyValue {
    fn default() -> Self {
        KeyValue {
            key_value_addr: Addr::null_pointer(),
            hash: 0u32,
        }
    }
}

impl KeyValue {
    fn is_empty(&self) -> bool {
        self.key_value_addr.is_null()
    }
}

/// Customized `HashMap` with string keys
///
/// This `HashMap` takes String as keys. Keys are
/// stored in a user defined heap.
///
/// The quirky API has the benefit of avoiding
/// the computation of the hash of the key twice,
/// or copying the key as long as there is no insert.
///
pub struct TermHashMap {
    table: Box<[KeyValue]>,
    pub heap: MemoryArena,
    mask: usize,
    occupied: Vec<usize>,
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

pub struct Iter<'a> {
    hashmap: &'a TermHashMap,
    inner: slice::Iter<'a, usize>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a [u8], Addr, BucketId);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().cloned().map(move |bucket: usize| {
            let kv = self.hashmap.table[bucket];
            let (key, offset): (&'a [u8], Addr) =
                unsafe { self.hashmap.get_key_value(kv.key_value_addr) };
            (key, offset, bucket as BucketId)
        })
    }
}

impl TermHashMap {
    pub fn new(num_bucket_power_of_2: usize) -> TermHashMap {
        let heap = MemoryArena::new();
        let table_size = 1 << num_bucket_power_of_2;
        let table: Vec<KeyValue> = iter::repeat(KeyValue::default()).take(table_size).collect();
        TermHashMap {
            table: table.into_boxed_slice(),
            heap,
            mask: table_size - 1,
            occupied: Vec::with_capacity(table_size / 2),
        }
    }

    fn probe(&self, hash: u32) -> QuadraticProbing {
        QuadraticProbing::compute(hash as usize, self.mask)
    }

    pub fn mem_usage(&self) -> usize {
        self.table.len() * mem::size_of::<KeyValue>()
    }

    fn is_saturated(&self) -> bool {
        self.table.len() < self.occupied.len() * 3
    }

    unsafe fn get_key_value(&self, addr: Addr) -> (&[u8], Addr) {
        let key_bytes_len = self.heap.read::<u16>(addr) as usize;
        let key_addr = addr.offset(2u32);
        let key_bytes: &[u8] = self.heap.read_slice(key_addr, key_bytes_len);
        let val_addr: Addr = key_addr.offset(key_bytes.len() as u32);
        (key_bytes, val_addr)
    }

    pub fn set_bucket(&mut self, hash: u32, key_value_addr: Addr, bucket: usize) {
        self.occupied.push(bucket);
        self.table[bucket] = KeyValue {
            key_value_addr,
            hash,
        };
    }

    pub fn iter(&self) -> Iter {
        Iter {
            inner: self.occupied.iter(),
            hashmap: &self,
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
    pub fn mutate_or_create<S, V, TMutator>(&mut self, key: S, mut updater: TMutator) -> BucketId
    where
        S: AsRef<[u8]>,
        V: Copy,
        TMutator: FnMut(Option<V>) -> V,
    {
        if self.is_saturated() {
            self.resize();
        }
        let key_bytes: &[u8] = key.as_ref();
        let hash = murmurhash2::murmurhash2(key.as_ref());
        let mut probe = self.probe(hash);
        loop {
            let bucket = probe.next_probe();
            let kv: KeyValue = self.table[bucket];
            if kv.is_empty() {
                let val = updater(None);
                let key_addr = self.heap.store(KeyBytesValue::new(key_bytes, val));
                self.set_bucket(hash, key_addr, bucket);
                return bucket as BucketId;
            } else if kv.hash == hash {
                let (key_matches, val_addr) = {
                    let (stored_key, val_addr): (&[u8], Addr) =
                        unsafe { self.get_key_value(kv.key_value_addr) };
                    (stored_key == key_bytes, val_addr)
                };
                if key_matches {
                    unsafe {
                        // logic
                        let v = self.heap.read(val_addr);
                        let new_v = updater(Some(v));
                        self.heap.write(val_addr, new_v);
                    };
                    return bucket as BucketId;
                }
            }
        }
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use super::murmurhash2::murmurhash2;
    use test::Bencher;

    #[bench]
    fn bench_murmurhash2(b: &mut Bencher) {
        let keys: [&'static str; 3] = ["wer qwe qwe qwe ", "werbq weqweqwe2 ", "weraq weqweqwe3 "];
        b.iter(|| {
            let mut s = 0;
            for &key in &keys {
                s ^= murmurhash2(key.as_bytes());
            }
            s
        });
    }
}

#[cfg(test)]
mod tests {

    use super::TermHashMap;
    use std::collections::HashMap;

    #[test]
    fn test_hash_map() {
        let mut hash_map: TermHashMap = TermHashMap::new(18);
        {
            hash_map.mutate_or_create("abc", |opt_val: Option<u32>| {
                assert_eq!(opt_val, None);
                3u32
            });
        }
        {
            hash_map.mutate_or_create("abcd", |opt_val: Option<u32>| {
                assert_eq!(opt_val, None);
                4u32
            });
        }
        {
            hash_map.mutate_or_create("abc", |opt_val: Option<u32>| {
                assert_eq!(opt_val, Some(3u32));
                5u32
            });
        }

        let mut vanilla_hash_map = HashMap::new();
        let mut iter_values = hash_map.iter();
        while let Some((key, addr, _)) = iter_values.next() {
            let val: u32 = unsafe {
                // test
                hash_map.heap.read(addr)
            };
            vanilla_hash_map.insert(key.to_owned(), val);
        }
        assert_eq!(vanilla_hash_map.len(), 2);
    }
}
