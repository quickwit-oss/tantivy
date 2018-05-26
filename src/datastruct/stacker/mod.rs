mod expull;

pub use self::expull::ExpUnrolledLinkedList;
pub(crate) mod hashmap;
mod heap;
pub use self::hashmap::TermHashMap;
pub use self::heap::{Heap, Addr};


#[cfg(test)]
mod tests {

    use super::Addr;
    use std::collections::HashMap;
    use super::Heap;
    use super::TermHashMap;
    use super::ExpUnrolledLinkedList;

    #[test]
    fn test_unrolled_linked_list() {
        {
            let heap = Heap::new();
            let mut ks: Vec<usize> = (1..5).map(|k| k * 100).collect();
            ks.push(2);
            ks.push(3);
            for k in (1..5).map(|k| k * 100) {
                let mut hashmap: TermHashMap = TermHashMap::new(10);
                for j in 0..k {
                    for i in 0..500 {
                        hashmap.get_or_create(i.to_string(),
                                              |v: &mut ExpUnrolledLinkedList| {v.push(i*j, &heap)},
                                              || {
                                                  let mut stack = ExpUnrolledLinkedList::new(&heap);
                                                  stack.push(i*j, &heap);
                                                  stack
                                              });
                    }
                }
                let mut map_addr: HashMap<Vec<u8>, Addr> = HashMap::new();
                for (key, addr, _) in hashmap.iter() {
                    map_addr.insert(Vec::from(key), addr);
                }

                for i in 0..500 {
                    let key: String = i.to_string();
                    let addr: Addr = *map_addr.get(key.as_bytes()).unwrap();
                    let exp_pull: ExpUnrolledLinkedList = hashmap.heap.read(addr);
                    let mut it = exp_pull.iter(&heap);
                    for j in 0..k {
                        assert_eq!(it.next().unwrap(), i * j);
                    }
                    assert!(!it.next().is_some());
                }
            }
        }
    }


}
