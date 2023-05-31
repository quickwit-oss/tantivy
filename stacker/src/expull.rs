use std::mem;

use common::serialize_vint_u32;

use crate::fastcpy::fast_short_slice_copy;
use crate::{Addr, MemoryArena};

const FIRST_BLOCK_NUM: u16 = 2;

/// An exponential unrolled link.
///
/// The use case is as follows. Tantivy's indexer conceptually acts like a
/// `HashMap<Term, Vec<u32>>`. As we come across a given term in document
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
/// Data is stored in a linked list of blocks. The first block has a size of `8`
/// and each block has a length of twice that of the previous block up to
/// `MAX_BLOCK_LEN = 1<<15`.
///
/// This strategy is a good trade off to handle numerous very rare terms
/// and avoid wasting half of the memory for very frequent terms.
#[derive(Debug, Clone, Copy)]
pub struct ExpUnrolledLinkedList {
    // u16, since the max size of each block is (1<<next_cap_pow_2)
    // Limited to 15, so we don't overflow remaining_cap.
    remaining_cap: u16,
    // To get the current number of blocks: block_num - FIRST_BLOCK_NUM
    block_num: u16,
    head: Addr,
    tail: Addr,
}

impl Default for ExpUnrolledLinkedList {
    fn default() -> Self {
        Self {
            // 0 to trigger an initial allocation. Init with MemoryArena would be better.
            remaining_cap: 0,
            block_num: FIRST_BLOCK_NUM,
            head: Addr::null_pointer(),
            tail: Addr::null_pointer(),
        }
    }
}

pub struct ExpUnrolledLinkedListWriter<'a> {
    eull: &'a mut ExpUnrolledLinkedList,
    arena: &'a mut MemoryArena,
}

#[inline]
fn ensure_capacity<'a>(
    eull: &'a mut ExpUnrolledLinkedList,
    arena: &'a mut MemoryArena,
    allocate: u32,
) {
    let new_block_addr: Addr = arena.allocate_space(allocate as usize + mem::size_of::<Addr>());
    // Check first write
    if eull.head.is_null() {
        eull.head = new_block_addr;
    } else {
        arena.write_at(eull.tail, new_block_addr);
    }

    eull.tail = new_block_addr;
    eull.remaining_cap = allocate as u16;
}

impl<'a> ExpUnrolledLinkedListWriter<'a> {
    #[inline]
    pub fn write_u32_vint(&mut self, val: u32) {
        let mut buf = [0u8; 8];
        let data = serialize_vint_u32(val, &mut buf);
        self.extend_from_slice(data);
    }

    #[inline]
    pub fn extend_from_slice(&mut self, mut buf: &[u8]) {
        while !buf.is_empty() {
            let add_len: usize;
            {
                if self.eull.remaining_cap == 0 {
                    // Double the next cap
                    self.eull.increment_num_blocks();
                    let block_size = get_block_size(self.eull.block_num);
                    ensure_capacity(self.eull, self.arena, block_size as u32);
                }

                let output_buf = self
                    .arena
                    .slice_mut(self.eull.tail, self.eull.remaining_cap as usize);
                add_len = buf.len().min(output_buf.len());
                let output_buf = &mut output_buf[..add_len];
                let buf = &buf[..add_len];

                fast_short_slice_copy(buf, output_buf);
            }
            self.eull.remaining_cap -= add_len as u16;
            self.eull.tail = self.eull.tail.offset(add_len as u32);
            buf = &buf[add_len..];
        }
    }
}

// The block size is 2^block_num + 2, but max 2^15= 32k
// Inital size is 8, for the first block => block_num == 1
#[inline]
fn get_block_size(block_num: u16) -> u16 {
    1 << block_num.min(15)
}

impl ExpUnrolledLinkedList {
    pub fn increment_num_blocks(&mut self) {
        self.block_num += 1;
    }

    #[inline]
    pub fn writer<'a>(&'a mut self, arena: &'a mut MemoryArena) -> ExpUnrolledLinkedListWriter<'a> {
        ExpUnrolledLinkedListWriter { eull: self, arena }
    }

    pub fn read_to_end(&self, arena: &MemoryArena, output: &mut Vec<u8>) {
        let mut addr = self.head;
        if addr.is_null() {
            return;
        }
        let last_block_len = get_block_size(self.block_num) as usize - self.remaining_cap as usize;

        // Full Blocks
        for block_num in FIRST_BLOCK_NUM + 1..self.block_num {
            let cap = get_block_size(block_num) as usize;
            let data = arena.slice(addr, cap);
            output.extend_from_slice(data);
            addr = arena.read(addr.offset(cap as u32));
        }
        // Last Block
        let data = arena.slice(addr, last_block_len);
        output.extend_from_slice(data);
    }
}

#[cfg(test)]
mod tests {
    use common::{read_u32_vint, write_u32_vint};

    use super::super::MemoryArena;
    use super::*;

    #[test]
    fn test_eull_empty() {
        let arena = MemoryArena::default();
        let stack = ExpUnrolledLinkedList::default();
        {
            let mut buffer = Vec::new();
            stack.read_to_end(&arena, &mut buffer);
            assert_eq!(&buffer[..], &[]);
        }
    }

    #[test]
    fn test_eull1() {
        let mut arena = MemoryArena::default();
        let mut stack = ExpUnrolledLinkedList::default();
        stack.writer(&mut arena).extend_from_slice(&[1u8]);
        stack.writer(&mut arena).extend_from_slice(&[2u8]);
        stack.writer(&mut arena).extend_from_slice(&[3u8, 4u8]);
        stack.writer(&mut arena).extend_from_slice(&[5u8]);
        {
            let mut buffer = Vec::new();
            stack.read_to_end(&arena, &mut buffer);
            assert_eq!(&buffer[..], &[1u8, 2u8, 3u8, 4u8, 5u8]);
        }
    }

    #[test]
    fn test_eull_vint1() {
        let mut arena = MemoryArena::default();
        let mut stack = ExpUnrolledLinkedList::default();
        stack.writer(&mut arena).extend_from_slice(&[1u8]);
        stack.writer(&mut arena).extend_from_slice(&[2u8]);
        stack.writer(&mut arena).extend_from_slice(&[3u8, 4u8]);
        stack.writer(&mut arena).extend_from_slice(&[5u8]);
        {
            let mut buffer = Vec::new();
            stack.read_to_end(&arena, &mut buffer);
            assert_eq!(&buffer[..], &[1u8, 2u8, 3u8, 4u8, 5u8]);
        }
    }

    #[test]
    fn test_eull_first_write_extends_cap() {
        let mut arena = MemoryArena::default();
        let mut stack = ExpUnrolledLinkedList::default();
        stack
            .writer(&mut arena)
            .extend_from_slice(&[1u8, 2, 3, 4, 5, 6, 7, 8, 9]);
        {
            let mut buffer = Vec::new();
            stack.read_to_end(&arena, &mut buffer);
            assert_eq!(&buffer[..], &[1u8, 2, 3, 4, 5, 6, 7, 8, 9]);
        }
    }

    #[test]
    fn test_eull_long() {
        let mut arena = MemoryArena::default();
        let mut eull = ExpUnrolledLinkedList::default();
        let data: Vec<u32> = (0..100).collect();
        for &el in &data {
            eull.writer(&mut arena).write_u32_vint(el);
        }
        let mut buffer = Vec::new();
        eull.read_to_end(&arena, &mut buffer);
        let mut result = vec![];
        let mut remaining = &buffer[..];
        while !remaining.is_empty() {
            result.push(read_u32_vint(&mut remaining));
        }
        assert_eq!(&result[..], &data[..]);
    }

    #[test]
    fn test_eull_limit() {
        let mut eull = ExpUnrolledLinkedList::default();
        for _ in 0..100 {
            eull.increment_num_blocks();
        }
        assert_eq!(get_block_size(eull.block_num), 1 << 15);
    }

    #[test]
    fn test_eull_interlaced() {
        let mut arena = MemoryArena::default();
        let mut stack = ExpUnrolledLinkedList::default();
        let mut stack2 = ExpUnrolledLinkedList::default();

        let mut vec1: Vec<u8> = vec![];
        let mut vec2: Vec<u8> = vec![];

        for i in 0..9 {
            stack.writer(&mut arena).write_u32_vint(i);
            assert!(write_u32_vint(i, &mut vec1).is_ok());
            if i % 2 == 0 {
                stack2.writer(&mut arena).write_u32_vint(i);
                assert!(write_u32_vint(i, &mut vec2).is_ok());
            }
        }
        let mut res1 = vec![];
        let mut res2 = vec![];
        stack.read_to_end(&arena, &mut res1);
        stack2.read_to_end(&arena, &mut res2);
        assert_eq!(&vec1[..], &res1[..]);
        assert_eq!(&vec2[..], &res2[..]);
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use std::iter;

    use test::Bencher;

    use super::super::MemoryArena;
    use super::ExpUnrolledLinkedList;

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
            let mut arena = MemoryArena::default();
            let mut stacks: Vec<ExpUnrolledLinkedList> =
                iter::repeat_with(ExpUnrolledLinkedList::default)
                    .take(NUM_STACK)
                    .collect();
            for s in 0..NUM_STACK {
                for i in 0u32..STACK_SIZE {
                    let t = s * 392017 % NUM_STACK;
                    stacks[t]
                        .writer(&mut arena)
                        .extend_from_slice(&i.to_ne_bytes());
                }
            }
        });
    }
}
