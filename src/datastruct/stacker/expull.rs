use super::heap::Heap;
use std::mem;
use datastruct::stacker::heap::Addr;

#[inline]
pub fn is_power_of_2(val: u32) -> bool {
    val & (val - 1) == 0
}

const MAX_BLOCK_LEN: u32 = 1u32 << 15;


const FIRST_BLOCK: u32 = 4u32;
#[inline]
pub fn jump_needed(len: u32) -> Option<usize> {
    match len {
        0...3 => None,
        val @ 4...MAX_BLOCK_LEN => {
            if is_power_of_2(val) {
                Some(val as usize)
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

#[derive(Debug, Clone, Copy)]
pub struct ExpUnrolledLinkedList {
    len: u32,
    head: Addr,
    tail: Addr,
}

impl ExpUnrolledLinkedList {

    pub fn new(heap: &Heap) -> ExpUnrolledLinkedList {
        let addr = heap.allocate_space((FIRST_BLOCK as usize) * mem::size_of::<u32>());
        ExpUnrolledLinkedList {
            len: 0u32,
            head: addr,
            tail: addr,
        }
    }

    pub fn iter<'a>(&self, heap: &'a Heap) -> ExpUnrolledLinkedListIterator<'a> {
        ExpUnrolledLinkedListIterator {
            heap,
            addr: self.head,
            len: self.len,
            consumed: 0,
        }
    }

    pub fn push(&mut self, val: u32, heap: &Heap) {
        self.len += 1;
        if let Some(new_block_len) = jump_needed(self.len) {
            // We need to allocate another block.
            // We also allocate an extra `u32` to store the pointer
            // to the future next block.
            let new_block_size: usize = (new_block_len + 1) * mem::size_of::<u32>();
            let new_block_addr: Addr = heap.allocate_space(new_block_size);
            unsafe { heap.set(self.tail, &new_block_addr) };
            self.tail = new_block_addr;
        }
        unsafe {
            heap.set(self.tail, &val);
            self.tail.0 += mem::size_of::<u32>() as u32;
        }
    }
}


pub struct ExpUnrolledLinkedListIterator<'a> {
    heap: &'a Heap,
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
            let addr: Addr;
            self.consumed += 1;
            if jump_needed(self.consumed).is_some() {
                addr = self.heap.read(self.addr);
            } else {
                addr = self.addr;
            }
            self.addr = Addr(addr.0 + mem::size_of::<u32>() as u32);
            Some(self.heap.read(addr))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::jump_needed;
    use super::super::heap::Heap;
    use super::*;

    #[test]
    fn test_stack() {
        let heap = Heap::new();
        let mut stack = ExpUnrolledLinkedList::new(&heap);
        stack.push(1u32, &heap);
        stack.push(2u32, &heap);
        stack.push(4u32, &heap);
        stack.push(8u32, &heap);
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
    use super::Heap;
    use test::Bencher;
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
        let heap = Heap::new();
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
