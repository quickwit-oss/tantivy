use super::{Addr, MemoryArena};

use crate::postings::stacker::memory_arena::load;
use crate::postings::stacker::memory_arena::store;
use std::io;
use std::mem;

const MAX_BLOCK_LEN: u32 = 1u32 << 15;
const FIRST_BLOCK: usize = 16;
const INLINED_BLOCK_LEN: usize = FIRST_BLOCK + mem::size_of::<Addr>();

enum CapacityResult {
    Available(u32),
    NeedAlloc(u32),
}

fn len_to_capacity(len: u32) -> CapacityResult {
    match len {
        0..=15 => CapacityResult::Available(FIRST_BLOCK as u32 - len),
        16..=MAX_BLOCK_LEN => {
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
    tail: Addr,
    inlined_data: [u8; INLINED_BLOCK_LEN as usize],
}

pub struct ExpUnrolledLinkedListWriter<'a> {
    eull: &'a mut ExpUnrolledLinkedList,
    heap: &'a mut MemoryArena,
}

fn ensure_capacity<'a>(
    eull: &'a mut ExpUnrolledLinkedList,
    heap: &'a mut MemoryArena,
) -> &'a mut [u8] {
    if eull.len <= FIRST_BLOCK as u32 {
        // We are still hitting the inline block.
        if eull.len < FIRST_BLOCK as u32 {
            return &mut eull.inlined_data[eull.len as usize..FIRST_BLOCK];
        }
        // We need to allocate a new block!
        let new_block_addr: Addr = heap.allocate_space(FIRST_BLOCK + mem::size_of::<Addr>());
        store(&mut eull.inlined_data[FIRST_BLOCK..], new_block_addr);
        eull.tail = new_block_addr;
        return heap.slice_mut(eull.tail, FIRST_BLOCK);
    }
    let len = match len_to_capacity(eull.len) {
        CapacityResult::NeedAlloc(new_block_len) => {
            let new_block_addr: Addr =
                heap.allocate_space(new_block_len as usize + mem::size_of::<Addr>());
            heap.write_at(eull.tail, new_block_addr);
            eull.tail = new_block_addr;
            new_block_len
        }
        CapacityResult::Available(available) => available,
    };
    heap.slice_mut(eull.tail, len as usize)
}

impl<'a> ExpUnrolledLinkedListWriter<'a> {
    pub fn extend_from_slice(&mut self, mut buf: &[u8]) {
        if buf.is_empty() {
            // we need to cut early, because `ensure_capacity`
            // allocates if there is no capacity at all right now.
            return;
        }
        while !buf.is_empty() {
            let add_len: usize;
            {
                let output_buf = ensure_capacity(self.eull, self.heap);
                add_len = buf.len().min(output_buf.len());
                output_buf[..add_len].copy_from_slice(&buf[..add_len]);
            }
            self.eull.len += add_len as u32;
            self.eull.tail = self.eull.tail.offset(add_len as u32);
            buf = &buf[add_len..];
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
    pub fn new() -> ExpUnrolledLinkedList {
        ExpUnrolledLinkedList {
            len: 0u32,
            tail: Addr::null_pointer(),
            inlined_data: [0u8; INLINED_BLOCK_LEN as usize],
        }
    }

    #[inline(always)]
    pub fn writer<'a>(&'a mut self, heap: &'a mut MemoryArena) -> ExpUnrolledLinkedListWriter<'a> {
        ExpUnrolledLinkedListWriter { eull: self, heap }
    }

    pub fn read_to_end(&self, heap: &MemoryArena, output: &mut Vec<u8>) {
        let len = self.len as usize;
        if len <= FIRST_BLOCK {
            output.extend_from_slice(&self.inlined_data[..len]);
            return;
        }
        output.extend_from_slice(&self.inlined_data[..FIRST_BLOCK]);
        let mut cur = FIRST_BLOCK;
        let mut addr = load(&self.inlined_data[FIRST_BLOCK..]);
        loop {
            let cap = match len_to_capacity(cur as u32) {
                CapacityResult::Available(capacity) => capacity,
                CapacityResult::NeedAlloc(capacity) => capacity,
            } as usize;
            let data = heap.slice(addr, cap);
            if cur + cap >= len {
                output.extend_from_slice(&data[..(len - cur)]);
                return;
            }
            output.extend_from_slice(data);
            cur += cap;
            addr = heap.read(addr.offset(cap as u32));
        }
    }
}

#[cfg(test)]
mod tests {

    use super::super::MemoryArena;
    use super::len_to_capacity;
    use super::*;
    use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};

    #[test]
    #[test]
    fn test_stack() {
        let mut heap = MemoryArena::new();
        let mut stack = ExpUnrolledLinkedList::new();
        stack.writer(&mut heap).extend_from_slice(&[1u8]);
        stack.writer(&mut heap).extend_from_slice(&[2u8]);
        stack.writer(&mut heap).extend_from_slice(&[3u8, 4u8]);
        stack.writer(&mut heap).extend_from_slice(&[5u8]);
        {
            let mut buffer = Vec::new();
            stack.read_to_end(&heap, &mut buffer);
            assert_eq!(&buffer[..], &[1u8, 2u8, 3u8, 4u8, 5u8]);
        }
    }

    #[test]
    fn test_stack_long() {
        let mut heap = MemoryArena::new();
        let mut stack = ExpUnrolledLinkedList::new();
        let data: Vec<u32> = (0..100).collect();
        for &el in &data {
            assert!(stack
                .writer(&mut heap)
                .write_u32::<LittleEndian>(el)
                .is_ok());
        }
        let mut buffer = Vec::new();
        stack.read_to_end(&heap, &mut buffer);
        let mut result = vec![];
        let mut remaining = &buffer[..];
        while !remaining.is_empty() {
            result.push(LittleEndian::read_u32(&remaining[..4]));
            remaining = &remaining[4..];
        }
        assert_eq!(&result[..], &data[..]);
    }

    #[test]
    fn test_stack_interlaced() {
        let mut heap = MemoryArena::new();
        let mut stack = ExpUnrolledLinkedList::new();
        let mut stack2 = ExpUnrolledLinkedList::new();

        let mut vec1: Vec<u8> = vec![];
        let mut vec2: Vec<u8> = vec![];

        for i in 0..9 {
            assert!(stack.writer(&mut heap).write_u32::<LittleEndian>(i).is_ok());
            assert!(vec1.write_u32::<LittleEndian>(i).is_ok());
            if i % 2 == 0 {
                assert!(stack2
                    .writer(&mut heap)
                    .write_u32::<LittleEndian>(i)
                    .is_ok());
                assert!(vec2.write_u32::<LittleEndian>(i).is_ok());
            }
        }
        let mut res1 = vec![];
        let mut res2 = vec![];
        stack.read_to_end(&heap, &mut res1);
        stack2.read_to_end(&heap, &mut res2);
        assert_eq!(&vec1[..], &res1[..]);
        assert_eq!(&vec2[..], &res2[..]);
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

    #[test]
    fn test_jump_if_needed_progression() {
        let mut v = vec![];
        for i in 0.. {
            if v.len() >= 10 {
                break;
            }
            match len_to_capacity(i) {
                CapacityResult::NeedAlloc(cap) => {
                    v.push((i, cap));
                }
                _ => {}
            }
        }
        assert_eq!(
            &v[..],
            &[
                (16, 16),
                (32, 32),
                (64, 64),
                (128, 128),
                (256, 256),
                (512, 512),
                (1024, 1024),
                (2048, 2048),
                (4096, 4096),
                (8192, 8192)
            ]
        );
    }
}

#[cfg(all(test, feature = "unstable"))]
mod bench {
    use super::super::MemoryArena;
    use super::ExpUnrolledLinkedList;
    use byteorder::{NativeEndian, WriteBytesExt};
    use std::iter;
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
        bench.iter(|| {
            let mut heap = MemoryArena::new();
            let mut stacks: Vec<ExpUnrolledLinkedList> =
                iter::repeat_with(ExpUnrolledLinkedList::new)
                    .take(NUM_STACK)
                    .collect();
            for s in 0..NUM_STACK {
                for i in 0u32..STACK_SIZE {
                    let t = s * 392017 % NUM_STACK;
                    let _ = stacks[t].writer(&mut heap).write_u32::<NativeEndian>(i);
                }
            }
        });
    }
}
