mod hashmap;
mod heap;
mod expull;

pub use self::heap::Heap;
pub use self::expull::ExpUnrolledLinkedList;
pub use self::hashmap::{HashMap, Entry};




#[test]
fn test_unrolled_linked_list() {
    let heap = Heap::with_capacity(30_000_000);
    {
        heap.clear();
        let mut ks: Vec<usize> = (1..5).map(|k| k * 100).collect();
        ks.push(2);
        ks.push(3);
        for k in (1..5).map(|k| k * 100) {        
            let mut hashmap: HashMap<ExpUnrolledLinkedList> = HashMap::new(10, &heap);
            for j in 0..k {
                for i in 0..500 {
                    let mut list = hashmap.get_or_create(i.to_string());
                    list.push(i*j, &heap);
                }
            }
            for i in 0..500 {
                match hashmap.lookup(i.to_string()) {
                    Entry::Occupied(addr) => {
                        let v: &mut ExpUnrolledLinkedList = heap.get_mut_ref(addr);
                        let mut it = v.iter(addr, &heap);
                        for j in 0..k {
                            assert_eq!(it.next().unwrap(), i*j);
                        }
                        assert!(!it.next().is_some());
                    }
                    _ => {
                        panic!("should never happen");
                    }
                }
            }
        }
        
    }
}