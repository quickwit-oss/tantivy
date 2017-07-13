use std::iter;
use std::mem;
use super::heap::{Heap, HeapAllocable, BytesRef};



mod murmurhash2 {

    const SEED: u32 = 3_242_157_231u32;

    #[inline(always)]
    pub fn murmurhash2(key: &[u8]) -> u32 {
        let mut key_ptr: *const u32 = key.as_ptr() as *const u32;
        let m: u32 = 0x5bd1e995;
        let r = 24;
        let len = key.len() as u32;

        let mut h: u32 = SEED ^ len;
        let num_blocks = len >> 2;
        for _ in 0..num_blocks {
            let mut k: u32 = unsafe { *key_ptr };
            k = k.wrapping_mul(m);
            k ^= k >> r;
            k = k.wrapping_mul(m);
            k = k.wrapping_mul(m);
            h ^= k;
            key_ptr = key_ptr.wrapping_offset(1);
        }

        // Handle the last few bytes of the input array
        let remaining = len & 3;
        let key_ptr_u8: *const u8 = key_ptr as *const u8;
        match remaining {
            3 => {
                h ^= unsafe { *key_ptr_u8.wrapping_offset(2) as u32 } << 16;
                h ^= unsafe { *key_ptr_u8.wrapping_offset(1) as u32 } << 8;
                h ^= unsafe { *key_ptr_u8 as u32 };
                h = h.wrapping_mul(m);
            }
            2 => {
                h ^= unsafe { *key_ptr_u8.wrapping_offset(1) as u32 } << 8;
                h ^= unsafe { *key_ptr_u8 as u32 };
                h = h.wrapping_mul(m);
            }
            1 => {
                h ^= unsafe { *key_ptr_u8 as u32 };
                h = h.wrapping_mul(m);
            }
            _ => {}
        }
        h ^= h >> 13;
        h = h.wrapping_mul(m);
        h ^ (h >> 15)
    }
}

impl Default for BytesRef {
    fn default() -> BytesRef {
        BytesRef {
            start: 0u32,
            stop: 0u32,
        }
    }
}

/// Split the thread memory budget into
/// - the heap size
/// - the hash table "table" itself.
///
/// Returns (the heap size in bytes, the hash table size in number of bits)
pub(crate) fn split_memory(per_thread_memory_budget: usize) -> (usize, usize) {
    let table_size_limit: usize = per_thread_memory_budget / 6;
    let compute_table_size = |num_bits: usize| {
        let table_size: usize = (1 << num_bits) * mem::size_of::<KeyValue>();
        table_size * mem::size_of::<KeyValue>()
    };
    let table_num_bits: usize = (1..)
        .into_iter()
        .take_while(|num_bits: &usize| compute_table_size(*num_bits) < table_size_limit)
        .last()
        .expect(&format!("Per thread memory is too small: {}", per_thread_memory_budget));
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
#[derive(Copy, Clone, Default)]
struct KeyValue {
    key: BytesRef,
    value_addr: u32,
}

impl KeyValue {
    fn is_empty(&self) -> bool {
        self.key.stop == 0u32
    }
}

pub enum Entry {
    Vacant(usize),
    Occupied(u32),
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
pub struct HashMap<'a> {
    table: Box<[KeyValue]>,
    heap: &'a Heap,
    mask: usize,
    occupied: Vec<usize>,
}


struct QuadraticProbing {
    hash: usize,
    i: usize,
    mask: usize,
}

impl QuadraticProbing {
    fn compute(key: &[u8], mask: usize) -> QuadraticProbing {
        let hash = murmurhash2::murmurhash2(key) as usize;
        QuadraticProbing {
            hash: hash,
            i: 0,
            mask: mask,
        }
    }

    #[inline]
    fn next_probe(&mut self) -> usize {
        self.i += 1;
        (self.hash + self.i * self.i) & self.mask
    }
}


impl<'a> HashMap<'a> {
    pub fn new(num_bucket_power_of_2: usize, heap: &'a Heap) -> HashMap<'a> {
        let table_size = 1 << num_bucket_power_of_2;
        let table: Vec<KeyValue> = iter::repeat(KeyValue::default()).take(table_size).collect();
        HashMap {
            table: table.into_boxed_slice(),
            heap: heap,
            mask: table_size - 1,
            occupied: Vec::with_capacity(table_size / 2),
        }
    }

    fn probe(&self, key: &[u8]) -> QuadraticProbing {
        QuadraticProbing::compute(key, self.mask)
    }

    pub fn is_saturated(&self) -> bool {
        self.table.len() < self.occupied.len() * 10
    }

    fn get_key(&self, bytes_ref: BytesRef) -> &[u8] {
        self.heap.get_slice(bytes_ref)
    }

    pub fn set_bucket(&mut self, key_bytes: &[u8], bucket: usize, addr: u32) -> u32 {
        self.occupied.push(bucket);
        self.table[bucket] = KeyValue {
            key: self.heap.allocate_and_set(key_bytes),
            value_addr: addr,
        };
        addr
    }

    pub fn iter<'b: 'a>(&'b self) -> impl Iterator<Item = (&'a [u8], u32)> + 'b {
        let heap: &'a Heap = self.heap;
        let table: &'b [KeyValue] = &self.table;
        self.occupied
            .iter()
            .cloned()
            .map(move |bucket: usize| {
                     let kv = table[bucket];
                     let addr = kv.value_addr;
                     (heap.get_slice(kv.key), addr)
                 })
    }

    pub fn get_or_create<S: AsRef<[u8]>, V: HeapAllocable>(&mut self, key: S) -> &mut V {
        let entry = self.lookup(key.as_ref());
        match entry {
            Entry::Occupied(addr) => self.heap.get_mut_ref(addr),
            Entry::Vacant(bucket) => {
                let (addr, val): (u32, &mut V) = self.heap.allocate_object();
                self.set_bucket(key.as_ref(), bucket, addr);
                val
            }
        }
    }

    pub fn lookup<S: AsRef<[u8]>>(&self, key: S) -> Entry {
        let key_bytes: &[u8] = key.as_ref();
        let mut probe = self.probe(key_bytes);
        loop {
            let bucket = probe.next_probe();
            let kv: KeyValue = self.table[bucket];
            if kv.is_empty() {
                return Entry::Vacant(bucket);
            }
            if self.get_key(kv.key) == key_bytes {
                return Entry::Occupied(kv.value_addr);
            }
        }
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use super::super::heap::{Heap, HeapAllocable};
    use super::murmurhash2::murmurhash2;
    use test::Bencher;
    use std::collections::HashSet;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    use super::split_memory;

    struct TestValue {
        val: u32,
        _addr: u32,
    }

    impl HeapAllocable for TestValue {
        fn with_addr(addr: u32) -> TestValue {
            TestValue {
                val: 0u32,
                _addr: addr,
            }
        }
    }

    #[test]
    fn test_hashmap_size() {
        assert_eq!(split_memory(100_000), (90_784, 6));
        assert_eq!(split_memory(1_000_000), (852_544, 10));
        assert_eq!(split_memory(10_000_000), (8_820_352, 13));
    }


    #[test]
    fn test_hash_map() {
        let heap = Heap::with_capacity(2_000_000);
        let mut hash_map: HashMap = HashMap::new(18, &heap);
        {
            let v: &mut TestValue = hash_map.get_or_create("abc");
            assert_eq!(v.val, 0u32);
            v.val = 3u32;
        }
        {
            let v: &mut TestValue = hash_map.get_or_create("abcd");
            assert_eq!(v.val, 0u32);
            v.val = 4u32;
        }
        {
            let v: &mut TestValue = hash_map.get_or_create("abc");
            assert_eq!(v.val, 3u32);
        }
        {
            let v: &mut TestValue = hash_map.get_or_create("abcd");
            assert_eq!(v.val, 4u32);
        }
        let mut iter_values = hash_map.iter();
        {
            let (_, addr) = iter_values.next().unwrap();
            let val: &TestValue = heap.get_ref(addr);
            assert_eq!(val.val, 3u32);
        }
        {
            let (_, addr) = iter_values.next().unwrap();
            let val: &TestValue = heap.get_ref(addr);
            assert_eq!(val.val, 4u32);
        }
        assert!(iter_values.next().is_none());
    }

    #[test]
    fn test_murmur() {
        let s1 = "abcdef";
        let s2 = "abcdeg";
        for i in 0..5 {
            assert_eq!(murmurhash2(&s1[i..5].as_bytes()),
                       murmurhash2(&s2[i..5].as_bytes()));
        }
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

    #[bench]
    fn bench_murmurhash_2(b: &mut Bencher) {
        let keys: Vec<&'static str> =
            vec!["wer qwe qwe qwe ", "werbq weqweqwe2 ", "weraq weqweqwe3 "];
        b.iter(|| {
                   keys.iter()
                       .map(|&s| s.as_bytes())
                       .map(murmurhash2::murmurhash2)
                       .map(|h| h as u64)
                       .last()
                       .unwrap()
               });
    }

    #[bench]
    fn bench_siphasher(bench: &mut Bencher) {
        let v = String::from("abwer");
        bench.iter(|| {
                       let mut h = DefaultHasher::new();
                       h.write(v.as_bytes());
                       h.finish()
                   });
    }

}
