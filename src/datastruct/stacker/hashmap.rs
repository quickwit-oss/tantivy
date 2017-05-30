use std::iter;
use super::heap::{Heap, HeapAllocable, BytesRef};
use murmurhash64::murmur_hash64a;

const SEED: u64 = 2915580697u64;

fn hash(key: &[u8]) -> u64 {
    murmur_hash64a(key, SEED)
}


impl Default for BytesRef {
    fn default() -> BytesRef {
        BytesRef {
            start: 0u32,
            stop: 0u32,
        }
    }
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
    masked_hash: u32,
}

impl KeyValue {
    fn is_empty(&self) -> bool {
        self.key.stop == 0u32
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
pub struct HashMap<'a> {
    table: Box<[KeyValue]>,
    heap: &'a Heap,
    mask: usize,
    num_bucket_power_of_2: usize,
    occupied: Vec<usize>,
}


struct QuadraticProbing {
    hash: usize,
    i: usize,
    mask: usize,
}

impl QuadraticProbing {
    fn compute(hash: usize, mask: usize) -> QuadraticProbing {
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
            num_bucket_power_of_2: num_bucket_power_of_2,
            occupied: Vec::with_capacity(table_size / 2),
        }
    }

    fn probe(&self, hash: u64) -> QuadraticProbing {
        QuadraticProbing::compute(hash as usize, self.mask)
    }

    pub fn is_saturated(&self) -> bool {
        self.table.len() < self.occupied.len() * 3
    }

    #[inline(never)]
    fn get_key(&self, bytes_ref: BytesRef) -> &[u8] {
        self.heap.get_slice(bytes_ref)
    }

    pub fn set_bucket(&mut self, masked_hash: u32, key_bytes: &[u8], bucket: usize, addr: u32) -> u32 {
        self.occupied.push(bucket);
        self.table[bucket] = KeyValue {
            key: self.heap.allocate_and_set(key_bytes),
            value_addr: addr,
            masked_hash: masked_hash,
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


    pub fn mask_hash(&self, hash: u64) -> u32 {
        (hash >> self.num_bucket_power_of_2) as u32
    }

    pub fn get_or_create<S: AsRef<[u8]>, V: HeapAllocable>(&mut self, key: S) -> &mut V {
        let key_bytes: &[u8] = key.as_ref();
        let hash = hash(key.as_ref());
        let masked_hash = self.mask_hash(hash);
        let mut probe = self.probe(hash);
        loop {
            let bucket = probe.next_probe();
            let kv: KeyValue = self.table[bucket];
            if kv.is_empty() {
                let (addr, val): (u32, &mut V) = self.heap.allocate_object();
                self.set_bucket(masked_hash, key.as_ref(), bucket, addr);
                return val
            }
            if kv.masked_hash == masked_hash {
                if self.get_key(kv.key) == key_bytes {
                    return self.heap.get_mut_ref(kv.value_addr);
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use super::super::heap::{Heap, HeapAllocable};
    use test::Bencher;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

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

    // #[bench]
    // fn bench_djb2(bench: &mut Bencher) {
    //     let v = String::from("abwer");
    //     bench.iter(|| djb2(v.as_bytes()));
    // }

    // #[bench]
    // fn bench_siphasher(bench: &mut Bencher) {
    //     let v = String::from("abwer");
    //     bench.iter(|| {
    //                    let mut h = DefaultHasher::new();
    //                    h.write(v.as_bytes());
    //                    h.finish()
    //                });
    // }


}
