use super::heap::Heap;
use postings::UnorderedTermId;
use std::iter;
use std::mem;
use std::slice;
use datastruct::stacker::Addr;
use super::murmurhash2;


pub(crate) fn compute_table_size(num_bits: usize) -> usize  {
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
            hash: 0u32
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
    pub heap: Heap,
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
        (self.hash + self.i * self.i) & self.mask
    }
}


pub struct Iter<'a> {
    hashmap: &'a TermHashMap,
    inner: slice::Iter<'a, usize>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a [u8], Addr, UnorderedTermId);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().cloned().map(move |bucket: usize| {
            let kv = self.hashmap.table[bucket];
            let (key, offset): (&'a [u8], Addr) = self.hashmap.get_key_value(kv.key_value_addr);
            (key, offset, bucket as UnorderedTermId)
        })
    }
}

impl TermHashMap {
    pub fn new(num_bucket_power_of_2: usize) -> TermHashMap {
        let heap = Heap::new();
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

    #[inline(never)]
    fn get_key_value(&self, addr: Addr) -> (&[u8], Addr) {
        let key_bytes: &[u8] = self.heap.get_chunk(addr);
        let expull_addr: Addr = addr.offset(2u32 + key_bytes.len() as u32);
        (key_bytes, expull_addr)
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

    pub fn mutate<S, V, TGetOrCreateHandler>(
        &mut self,
        key: S,
        mut get_or_create_handler: TGetOrCreateHandler) -> UnorderedTermId
    where
        S: AsRef<[u8]>,
        V: Copy,
        TGetOrCreateHandler: FnMut(Option<V>) -> V
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
                let key_bytes_len = key_bytes.len();
                let key_value_len = key_bytes_len + 2 + mem::size_of::<V>();
                let key_addr = self.heap.allocate_space(key_value_len);
                self.set_bucket(hash, key_addr, bucket);
                let val = get_or_create_handler(None);
                unsafe { // logic - heap is not shared
                    self.heap.write_chunk(key_addr, key_bytes);
                    let val_addr = key_addr.offset(2 + key_bytes_len as u32);
                    self.heap.set(val_addr, val);
                }
                return bucket as UnorderedTermId;
            } else if kv.hash == hash {
                let (key_matches, expull_addr) = {
                    let (stored_key, expull_addr): (&[u8], Addr) = self.get_key_value(kv.key_value_addr);
                    (stored_key == key_bytes, expull_addr)
                };
                if key_matches {
                    unsafe { // logic
                        let v = self.heap.read(expull_addr);
                        let new_v = get_or_create_handler(Some(v));
                        self.heap.set(expull_addr, new_v);
                    };
                    return bucket as UnorderedTermId;
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

    use super::murmurhash2::murmurhash2;
    use std::collections::HashSet;
    use datastruct::stacker::TermHashMap;
    use std::collections::HashMap;


    #[test]
    fn test_hash_map() {
        let mut hash_map: TermHashMap = TermHashMap::new(18);
        {
            hash_map.mutate("abc", |opt_val: Option<u32>| {
                assert_eq!(opt_val, None);
                3u32
            });
        }
        {
            hash_map.mutate("abcd", |opt_val: Option<u32>| {
                assert_eq!(opt_val, None);
                4u32
            });
        }
        {
            hash_map.mutate("abc", |opt_val: Option<u32>| {
                assert_eq!(opt_val, Some(3u32));
                5u32
            });
        }

        let mut vanilla_hash_map = HashMap::new();
        let mut iter_values = hash_map.iter();
        while let Some((key, addr, _)) = iter_values.next() {
            let val: u32 = unsafe { // test
                hash_map.heap.read(addr)
            };
            vanilla_hash_map.insert(key.to_owned(), val);
        }
        assert_eq!(vanilla_hash_map.len(), 2);
    }

    
    #[test]
    fn test_murmur() {
        let s1 = "abcdef";
        let s2 = "abcdeg";
        for i in 0..5 {
            assert_eq!(
                murmurhash2(&s1[i..5].as_bytes()),
                murmurhash2(&s2[i..5].as_bytes())
            );
        }
    }

    #[test]
    fn test_murmur_against_reference_impl() {
        assert_eq!(murmurhash2("".as_bytes()), 3632506080);
        assert_eq!(murmurhash2("a".as_bytes()), 455683869);
        assert_eq!(murmurhash2("ab".as_bytes()), 2448092234);
        assert_eq!(murmurhash2("abc".as_bytes()), 2066295634);
        assert_eq!(murmurhash2("abcd".as_bytes()), 2588571162);
        assert_eq!(murmurhash2("abcde".as_bytes()), 2988696942);
        assert_eq!(murmurhash2("abcdefghijklmnop".as_bytes()), 2350868870);
    }

    #[test]
    fn test_murmur_collisions() {
        let mut set: HashSet<u32> = HashSet::default();
        for i in 0..10_000 {
            let s = format!("hash{}", i);
            let hash = murmurhash2(s.as_bytes());
            set.insert(hash);
        }
        assert_eq!(set.len(), 10_000);
    }

}
