use super::heap::Heap;
use postings::UnorderedTermId;
use std::iter;
use std::mem;
use std::slice;
use datastruct::stacker::Addr;

mod murmurhash2 {

    const SEED: u32 = 3_242_157_231u32;
    const M: u32 = 0x5bd1_e995;

    #[inline(always)]
    pub fn murmurhash2(key: &[u8]) -> u32 {
        let mut key_ptr: *const u32 = key.as_ptr() as *const u32;
        let len = key.len() as u32;
        let mut h: u32 = SEED ^ len;

        let num_blocks = len >> 2;
        for _ in 0..num_blocks {
            let mut k: u32 = unsafe { *key_ptr }; // ok because of num_blocks definition
            k = k.wrapping_mul(M);
            k ^= k >> 24;
            k = k.wrapping_mul(M);
            h = h.wrapping_mul(M);
            h ^= k;
            key_ptr = key_ptr.wrapping_offset(1);
        }

        // Handle the last few bytes of the input array
        let remaining: &[u8] = &key[key.len() & !3..];
        match remaining.len() {
            3 => {
                h ^= u32::from(remaining[2]) << 16;
                h ^= u32::from(remaining[1]) << 8;
                h ^= u32::from(remaining[0]);
                h = h.wrapping_mul(M);
            }
            2 => {
                h ^= u32::from(remaining[1]) << 8;
                h ^= u32::from(remaining[0]);
                h = h.wrapping_mul(M);
            }
            1 => {
                h ^= u32::from(remaining[0]);
                h = h.wrapping_mul(M);
            }
            _ => {}
        }
        h ^= h >> 13;
        h = h.wrapping_mul(M);
        h ^ (h >> 15)
    }
}

/// Split the thread memory budget into
/// - the heap size
/// - the hash table "table" itself.
///
/// Returns (the heap size in bytes, the hash table size in number of bits)
pub(crate) fn split_memory(per_thread_memory_budget: usize) -> (usize, usize) {
    let table_size_limit: usize = per_thread_memory_budget / 3;
    let compute_table_size = |num_bits: usize| (1 << num_bits) * mem::size_of::<KeyValue>();
    let table_num_bits: usize = (1..)
        .into_iter()
        .take_while(|num_bits: &usize| compute_table_size(*num_bits) < table_size_limit)
        .last()
        .expect(&format!(
            "Per thread memory is too small: {}",
            per_thread_memory_budget
        ));
    let table_size = compute_table_size(table_num_bits);
    let heap_size = per_thread_memory_budget - table_size;
    (heap_size, table_num_bits)
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

pub trait GetOrCreateHandler<V> {
    fn mutate(&mut self, value: &mut V);
    fn create(&mut self) -> V;
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

    pub fn get_or_create<S, V, TGetOrCreateHandler>(
        &mut self,
        key: S,
        mut get_or_create_handler: TGetOrCreateHandler) -> UnorderedTermId
    where
        S: AsRef<[u8]>,
        V: Copy,
        TGetOrCreateHandler: GetOrCreateHandler<V>
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
                let val = get_or_create_handler.create();
                unsafe {
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
                    unsafe {
                        let mut v = self.heap.read(expull_addr);
                        get_or_create_handler.mutate(&mut v);
                        self.heap.set(expull_addr, v);
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
    use super::split_memory;
    use std::collections::HashSet;
    use datastruct::stacker::TermHashMap;
    use std::collections::HashMap;
    use datastruct::stacker::hashmap::GetOrCreateHandler;

    #[derive(Copy, Default, Clone)]
    struct TestValue {
        val: u32
    }

    #[test]
    fn test_hashmap_size() {
        assert_eq!(split_memory(100_000), (67232, 12));
        assert_eq!(split_memory(1_000_000), (737856, 15));
        assert_eq!(split_memory(10_000_000), (7902848, 18));
    }


    struct GetOrCreate {
        previous_value: u32,
        val: u32,
        expect_create: bool
    }

    impl GetOrCreateHandler<TestValue> for GetOrCreate {
        fn mutate(&mut self, test_value: &mut TestValue) {
            if self.expect_create {
                panic!("expected create");
            }
            assert_eq!(self.previous_value, test_value.val);
            test_value.val = self.val;
        }

        fn create(&mut self) -> TestValue {
            if !self.expect_create {
                panic!("expected mutate");
            }
            let mut test_value = TestValue::default();
            test_value.val = self.val;
            test_value
        }
    }


    #[test]
    fn test_hash_map() {
        let mut hash_map: TermHashMap = TermHashMap::new(18);
        {
            let get_or_create = GetOrCreate {
                previous_value: 0u32,
                val: 3u32,
                expect_create: true
            };
            hash_map.get_or_create("abc", get_or_create);
        }
        {
            let get_or_create = GetOrCreate {
                previous_value: 0u32,
                val: 4u32,
                expect_create: true
            };
            hash_map.get_or_create("abcd", get_or_create);
        }
        {
            let get_or_create = GetOrCreate {
                previous_value: 3u32,
                val: 5u32,
                expect_create: false
            };
            hash_map.get_or_create("abc", get_or_create);
        }

        let mut vanilla_hash_map = HashMap::new();
        let mut iter_values = hash_map.iter();
        while let Some((key, addr, _)) = iter_values.next() {
            let val: TestValue = unsafe { hash_map.heap.read(addr) };
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
