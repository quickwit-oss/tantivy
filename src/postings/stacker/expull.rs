use super::{Addr, MemoryArena};

use std::mem;
use std::io;

const MAX_BLOCK_LEN: u32 = 1u32 << 15;
const FIRST_BLOCK: u32 = 16u32;

enum CapacityResult {
    Available(u32),
    NeedAlloc(u32),
}

fn len_to_capacity(len: u32) -> CapacityResult {
    match len {
        0...15 => CapacityResult::Available(FIRST_BLOCK - len),
        16...MAX_BLOCK_LEN => {
            let cap = 1 << (32u32 - (len - 1u32).leading_zeros());
            let available = cap - len;
            if available == 0 {
                CapacityResult::NeedAlloc(len)
            } else {
                CapacityResult::Available(available)
            }
        }
        n => {
            let available = n % MAX_BLOCK_LEN;
            if available == 0 {
                CapacityResult::NeedAlloc(MAX_BLOCK_LEN)
            } else {
                CapacityResult::Available(MAX_BLOCK_LEN - available)
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

pub struct ExpUnrolledLinkedListWriter<'a> {
    eull: &'a mut ExpUnrolledLinkedList,
    heap: &'a mut MemoryArena
}

fn ensure_capacity(eull: &mut ExpUnrolledLinkedList, heap: &mut MemoryArena) -> u32 {
    match len_to_capacity(eull.len) {
        CapacityResult::NeedAlloc(new_block_len) => {
            let new_block_addr: Addr =
                heap.allocate_space(new_block_len as usize + mem::size_of::<u32>());
            heap.write_at(eull.tail, new_block_addr);
            eull.tail = new_block_addr;
            new_block_len
        }
        CapacityResult::Available(available) => available,
    }
}

impl<'a> ExpUnrolledLinkedListWriter<'a> {
    pub fn extend_from_slice(&mut self, mut buf: &[u8]) {
        if buf.is_empty() {
            // we need to cut early, because `ensure_capacity`
            // allocates if there is no capacity at all right now.
            return;
        }
        loop {
            let cap = ensure_capacity(self.eull, self.heap) as usize;
            if buf.len() <= cap {
                self.heap.write_slice(self.eull.tail, &buf);
                self.eull.len += buf.len() as u32;
                self.eull.tail = self.eull.tail.offset(buf.len() as u32);
                break;
            } else {
                self.heap.write_slice(self.eull.tail, &buf[..cap]);
                self.eull.len += cap as u32;
                self.eull.tail = self.eull.tail.offset(cap as u32);
                buf = &buf[cap..];
            }
        }
    }
}

impl<'a> io::Write for ExpUnrolledLinkedListWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // There is no use case to only write the capacity.
        // This is not IO after all, so we write the whole
        // buffer even if the contract of `.write` is looser.
        self.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.extend_from_slice(buf);
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
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

    #[inline(always)]
    pub fn get_writer<'a>(&'a mut self, heap: &'a mut MemoryArena) -> ExpUnrolledLinkedListWriter<'a> {
        ExpUnrolledLinkedListWriter {
            eull: self,
            heap
        }
    }

    pub fn read(&self, heap: &MemoryArena, output: &mut Vec<u8>) {
        output.clear();
        let mut cur = 0u32;
        let mut addr = self.head;
        let mut len = self.len;
        while len > 0 {
            let cap = match len_to_capacity(cur) {
                CapacityResult::Available(capacity) => capacity,
                CapacityResult::NeedAlloc(capacity) => capacity,
            };
            if cap < len {
                let data = heap.slice(addr, cap as usize);
                output.extend_from_slice(data);
                len -= cap;
                cur += cap;
            } else {
                let data = heap.slice(addr, len as usize);
                output.extend_from_slice(data);
                return;
            }
            addr = heap.read(addr.offset(cap));
        }
    }
}

#[cfg(test)]
mod tests {

    use super::super::MemoryArena;
    use super::len_to_capacity;
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
            let mut buffer = Vec::new();
            stack.read(&heap, &mut buffer);
            assert_eq!(&buffer[..], &[129u8, 130u8, 132u8, 136u8]);
        }
    }

    #[test]
    fn test_jump_if_needed() {
        let mut available = 16u32;
        for i in 0..10_000_000 {
            match len_to_capacity(i) {
                CapacityResult::NeedAlloc(cap) => {
                    assert_eq!(available, 0, "Failed len={}: Expected 0 got {}", i, cap);
                    available = cap;
                }
                CapacityResult::Available(cap) => {
                    assert_eq!(
                        available, cap,
                        "Failed len={}: Expected {} Got {}",
                        i, available, cap
                    );
                }
            }
            available -= 1;
        }
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use super::super::MemoryArena;
    use super::ExpUnrolledLinkedList;
    use test::Bencher;
    use byteorder::{NativeEndian, ByteOrder};

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
        bench.iter(|| {
            let mut heap = MemoryArena::new();
            let mut stacks = Vec::with_capacity(100);
            for _ in 0..NUM_STACK {
                let mut stack = ExpUnrolledLinkedList::new(&mut heap);
                stacks.push(stack);
            }
            for s in 0..NUM_STACK {
                for i in 0u32..STACK_SIZE {
                    let t = s * 392017 % NUM_STACK;
                    let _ = stacks[t].get_writer(&mut heap).write_u32::<NativeEndian>(i);
                }
            }
        });
    }
}
