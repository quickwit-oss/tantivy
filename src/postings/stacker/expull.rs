use super::{MemoryArena, Addr};

use std::mem;
use common::is_power_of_2;

const MAX_BLOCK_LEN: u32 = 1u32 << 15;

const FIRST_BLOCK: u32 = 4u32;

#[inline]
pub fn jump_needed(len: u32) -> Option<usize> {
    match len {
        0...3 => None,
        4...MAX_BLOCK_LEN => {
            if is_power_of_2(len as usize) {
                Some(len as usize)
            } else {
                None
            }
        }
        n => {
            if n % MAX_BLOCK_LEN == 0 {
                Some(MAX_BLOCK_LEN as usize)
            } else {
                None
            }
        }
    }
}

/// An exponential unrolled link.
///
/// The use case is as follows. Tantivy's indexer conceptually acts like a
/// `HashMap<Term, Vec<u32>>`. As we come accross a given term in document
/// `D`, we lookup the term in the map and append the document id to its vector.
///
/// The vector is then only read when it is serialized.
///
/// The `ExpUnrolledLinkedList` offers a more efficient solution to this
/// problem.
///
/// It combines the idea of the unrolled linked list and tries to address the
/// problem of selecting an adequate block size using a strategy similar to
/// that of the `Vec` amortized resize strategy.
///
/// Data is stored in a linked list of blocks. The first block has a size of `4`
/// and each block has a length of twice that of the previous block up to
/// `MAX_BLOCK_LEN = 32768`.
///
/// This strategy is a good trade off to handle numerous very rare terms
/// and avoid wasting half of the memory for very frequent terms.
#[derive(Debug, Clone, Copy)]
pub struct ExpUnrolledLinkedList {
    len: u32,
    head: Addr,
    tail: Addr,
}

impl ExpUnrolledLinkedList {

    pub fn new(heap: &mut MemoryArena) -> ExpUnrolledLinkedList {
        let addr = heap.allocate_space((FIRST_BLOCK as usize) * mem::size_of::<u32>());
        ExpUnrolledLinkedList {
            len: 0u32,
            head: addr,
            tail: addr,
        }
    }

    pub fn iter<'a>(&self, heap: &'a MemoryArena) -> ExpUnrolledLinkedListIterator<'a> {
        ExpUnrolledLinkedListIterator {
            heap,
            addr: self.head,
            len: self.len,
            consumed: 0,
        }
    }

    /// Appends a new element to the current stack.
    ///
    /// If the current block end is reached, a new block is allocated.
    pub fn push(&mut self, val: u32, heap: &mut MemoryArena) {
        self.len += 1;
        if let Some(new_block_len) = jump_needed(self.len) {
            // We need to allocate another block.
            // We also allocate an extra `u32` to store the pointer
            // to the future next block.
            let new_block_size: usize = (new_block_len + 1) * mem::size_of::<u32>();
            let new_block_addr: Addr = heap.allocate_space(new_block_size);
            unsafe { // logic
                heap.write(self.tail, new_block_addr)
            };
            self.tail = new_block_addr;
        }
        unsafe { // logic
            heap.write(self.tail, val);
            self.tail = self.tail.offset(mem::size_of::<u32>() as u32);
        }
    }
}


pub struct ExpUnrolledLinkedListIterator<'a> {
    heap: &'a MemoryArena,
    addr: Addr,
    len: u32,
    consumed: u32,
}

impl<'a> Iterator for ExpUnrolledLinkedListIterator<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        if self.consumed == self.len {
            None
        } else {
            self.consumed += 1;
            let addr: Addr =
                if jump_needed(self.consumed).is_some() {
                    unsafe { // logic
                        self.heap.read(self.addr)
                    }
                } else {
                    self.addr
                };
            self.addr = addr.offset(mem::size_of::<u32>() as u32);
            Some(unsafe { // logic
                self.heap.read(addr)
            })
        }
    }
}

#[cfg(test)]
mod tests {

    use super::jump_needed;
    use super::super::MemoryArena;
    use super::*;

    #[test]
    fn test_stack() {
        let mut heap = MemoryArena::new();
        let mut stack = ExpUnrolledLinkedList::new(&mut heap);
        stack.push(1u32, &mut heap);
        stack.push(2u32, &mut heap);
        stack.push(4u32, &mut heap);
        stack.push(8u32, &mut heap);
        {
            let mut it = stack.iter(&heap);
            assert_eq!(it.next().unwrap(), 1u32);
            assert_eq!(it.next().unwrap(), 2u32);
            assert_eq!(it.next().unwrap(), 4u32);
            assert_eq!(it.next().unwrap(), 8u32);
            assert!(it.next().is_none());
        }
    }

    #[test]
    fn test_jump_if_needed() {
        let mut block_len = 4u32;
        let mut i = 0;
        while i < 10_000_000 {
            assert!(jump_needed(i + block_len - 1).is_none());
            assert!(jump_needed(i + block_len + 1).is_none());
            assert!(jump_needed(i + block_len).is_some());
            let new_block_len = jump_needed(i + block_len).unwrap();
            i += block_len;
            block_len = new_block_len as u32;
        }
    }
}


#[cfg(all(test, feature = "unstable"))]
mod bench {
    use super::ExpUnrolledLinkedList;
    use test::Bencher;
    use tantivy_memory_arena::MemoryArena;

    const NUM_STACK: usize = 10_000;
    const STACK_SIZE: u32 = 1000;




    #[bench]
    fn bench_push_vec(bench: &mut Bencher) {
        bench.iter(|| {
            let mut vecs = Vec::with_capacity(100);
            for _ in 0..NUM_STACK {
                vecs.push(Vec::new());
            }
            for s in 0..NUM_STACK {
                for i in 0u32..STACK_SIZE {
                    let t = s * 392017 % NUM_STACK;
                    vecs[t].push(i);
                }
            }
        });
    }

    #[bench]
    fn bench_push_stack(bench: &mut Bencher) {
        let heap = MemoryArena::new();
        bench.iter(|| {
            let mut stacks = Vec::with_capacity(100);
            for _ in 0..NUM_STACK {
                let (_, stack) = heap.allocate_object::<ExpUnrolledLinkedList>();
                stacks.push(stack);
            }
            for s in 0..NUM_STACK {
                for i in 0u32..STACK_SIZE {
                    let t = s * 392017 % NUM_STACK;
                    stacks[t].push(i, &heap);
                }
            }
            heap.clear();
        });
    }
}
