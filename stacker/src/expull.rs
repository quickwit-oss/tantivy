use std::mem;

use common::serialize_vint_u32;

use crate::fastcpy::fast_short_slice_copy;
use crate::{Addr, MemoryArena};

const FIRST_BLOCK_NUM: u32 = 2;

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
    // Tracks the number of blocks allocated: block_num - FIRST_BLOCK_NUM
    block_num: u32,
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

impl ExpUnrolledLinkedListWriter<'_> {
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

// The block size is 2^block_num, but max 2^15 = 32KB
// Initial size is 8 bytes (2^3), for the first block => block_num == 2
// Block size caps at 32KB (2^15) regardless of how high block_num goes
#[inline]
fn get_block_size(block_num: u32) -> u16 {
    // Cap at 15 to prevent block sizes > 32KB
    // block_num can now be much larger than 15, but block size maxes out
    let exp: u32 = block_num.min(15u32);
    (1u32 << exp) as u16
}

/// Total size of the blocks preceding block `block_num`, i.e. blocks
/// `FIRST_BLOCK_NUM + 1..block_num`.
///
/// Precondition: `block_num >= FIRST_BLOCK_NUM + 1`.
#[inline]
fn full_blocks_len(block_num: u32) -> usize {
    // Sizes double up to 2^15, then stay at 2^15.
    const FIRST_BLOCK_SIZE: usize = 1 << (FIRST_BLOCK_NUM + 1);
    if block_num <= 15 {
        (1usize << block_num) - FIRST_BLOCK_SIZE
    } else {
        (1usize << 15) - FIRST_BLOCK_SIZE + (block_num as usize - 15) * (1usize << 15)
    }
}

impl ExpUnrolledLinkedList {
    #[inline(always)]
    pub fn increment_num_blocks(&mut self) {
        // Add overflow check as a safety measure
        // With u32, we can handle up to ~4 billion blocks before overflow
        // At 32KB per block (max size), that's 128 TB of data
        self.block_num = self
            .block_num
            .checked_add(1)
            .expect("ExpUnrolledLinkedList block count overflow - exceeded 4 billion blocks");
    }

    #[inline]
    pub fn writer<'a>(&'a mut self, arena: &'a mut MemoryArena) -> ExpUnrolledLinkedListWriter<'a> {
        ExpUnrolledLinkedListWriter { eull: self, arena }
    }

    /// Returns an iterator over the chunks of data stored in the list, in write order.
    ///
    /// Each chunk is the used part of one block. Concatenating the chunks gives the
    /// same bytes as `read_to_end`.
    #[inline]
    pub fn iter_chunks<'a>(&self, arena: &'a MemoryArena) -> ExpUnrolledLinkedListChunks<'a> {
        if self.head.is_null() {
            return ExpUnrolledLinkedListChunks {
                arena,
                current: &[],
                next_addr: Addr::null_pointer(),
                block_num: self.block_num,
                last_block_num: self.block_num,
                last_block_len: 0,
            };
        }

        let block_size = get_block_size(self.block_num) as usize;
        // Safety check: if remaining_cap > block_size, the metadata is corrupted
        assert!(
            self.remaining_cap as usize <= block_size,
            "ExpUnrolledLinkedList metadata corruption detected: remaining_cap ({}) > block_size \
             ({}). This indicates a serious bug, please report! (block_num={}, head={:?}, \
             tail={:?})",
            self.remaining_cap,
            block_size,
            self.block_num,
            self.head,
            self.tail
        );
        let last_block_len = block_size - self.remaining_cap as usize;

        // Note: Blocks are numbered starting from FIRST_BLOCK_NUM+1 (=3) after first allocation
        let mut chunks = ExpUnrolledLinkedListChunks {
            arena,
            current: &[],
            next_addr: Addr::null_pointer(),
            block_num: FIRST_BLOCK_NUM + 1,
            last_block_num: self.block_num,
            last_block_len,
        };
        chunks.load_block(self.head);
        chunks
    }

    pub fn read_to_end(&self, arena: &MemoryArena, output: &mut Vec<u8>) {
        // Size of all allocated blocks minus the unused capacity of the last one.
        // Also 0 for an empty list (block_num == FIRST_BLOCK_NUM, remaining_cap == 0).
        // `saturating_sub` because corrupted metadata is only detected later, in `iter_chunks`.
        let len = full_blocks_len(self.block_num + 1).saturating_sub(self.remaining_cap as usize);
        output.reserve(len);
        self.iter_chunks(arena).read_to_end(output);
    }
}

/// Iterator over the chunks of an `ExpUnrolledLinkedList`.
///
/// All blocks but the last one are full. The last block is only partially used.
pub struct ExpUnrolledLinkedListChunks<'a> {
    arena: &'a MemoryArena,
    // Unread part of the current block.
    // Empty if and only if the iterator is exhausted: blocks are never empty, and
    // a block is replaced by the next one as soon as it is fully read.
    current: &'a [u8],
    // Address of the block following the current one.
    // Only valid if `block_num < last_block_num`.
    next_addr: Addr,
    // Number of the current block.
    block_num: u32,
    // Number of the last block (the one `tail` points into).
    last_block_num: u32,
    // Number of bytes used in the last block.
    last_block_len: usize,
}

impl<'a> ExpUnrolledLinkedListChunks<'a> {
    /// Appends all the remaining chunks to `output`.
    ///
    /// Same result as extending `output` with each chunk, but faster for lists
    /// with few blocks: this loop avoids the per-chunk overhead of `next()`.
    #[inline]
    pub fn read_to_end(self, output: &mut Vec<u8>) {
        output.extend_from_slice(self.current);
        if self.block_num == self.last_block_num {
            return;
        }
        let mut addr = self.next_addr;
        // Full blocks. The last bytes of each block hold the address of the next block.
        for block_num in self.block_num + 1..self.last_block_num {
            let cap = get_block_size(block_num) as usize;
            output.extend_from_slice(self.arena.slice(addr, cap));
            addr = self.arena.read(addr.offset(cap as u32));
        }
        // Last block
        output.extend_from_slice(self.arena.slice(addr, self.last_block_len));
    }

    /// Appends the next `len` bytes to `output` and advances past them.
    ///
    /// Panics if fewer than `len` bytes remain.
    pub fn read_exact_into(&mut self, mut len: usize, output: &mut Vec<u8>) {
        output.reserve(len);
        while len > 0 {
            assert!(
                !self.current.is_empty(),
                "read_exact_into: {len} bytes missing at the end of the list"
            );
            let next_chunk_len = len.min(self.current.len());
            let (head, tail) = self.current.split_at(next_chunk_len);
            output.extend_from_slice(head);
            len -= next_chunk_len;
            self.current = tail;
            if self.current.is_empty() {
                self.advance_block();
            }
        }
    }

    /// Makes the block at `addr`, numbered `self.block_num`, the current block.
    #[inline]
    fn load_block(&mut self, addr: Addr) {
        if self.block_num == self.last_block_num {
            self.current = self.arena.slice(addr, self.last_block_len);
            return;
        }
        // Full block. Its last bytes hold the address of the next block.
        let cap = get_block_size(self.block_num) as usize;
        self.current = self.arena.slice(addr, cap);
        self.next_addr = self.arena.read(addr.offset(cap as u32));
    }

    /// Moves to the next block. Does nothing after the last block, leaving
    /// `current` empty.
    ///
    /// Precondition: `current` is empty.
    #[inline]
    fn advance_block(&mut self) {
        if self.block_num == self.last_block_num {
            return;
        }
        self.block_num += 1;
        self.load_block(self.next_addr);
    }
}

impl<'a> Iterator for ExpUnrolledLinkedListChunks<'a> {
    type Item = &'a [u8];

    #[inline(always)]
    fn next(&mut self) -> Option<&'a [u8]> {
        if self.current.is_empty() {
            return None;
        }
        let data = std::mem::take(&mut self.current);
        self.advance_block();
        Some(data)
    }
}

impl std::iter::FusedIterator for ExpUnrolledLinkedListChunks<'_> {}

#[cfg(test)]
mod tests {
    use common::{read_u32_vint, write_u32_vint};

    use super::*;

    fn eull_with_data(data: &[u8]) -> (MemoryArena, ExpUnrolledLinkedList) {
        let mut arena = MemoryArena::default();
        let mut eull = ExpUnrolledLinkedList::default();
        eull.writer(&mut arena).extend_from_slice(data);
        (arena, eull)
    }

    #[test]
    fn test_eull_empty() {
        let arena = MemoryArena::default();
        let stack = ExpUnrolledLinkedList::default();
        {
            let mut buffer = Vec::new();
            stack.read_to_end(&arena, &mut buffer);
            assert_eq!(&buffer[..], &[] as &[u8]);
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
    fn test_eull_iter_chunks() {
        let (arena, eull) = eull_with_data(&[]);
        assert_eq!(eull.iter_chunks(&arena).count(), 0);
        let data: Vec<u8> = (0..100u8).collect();
        let (arena, eull) = eull_with_data(&data);
        let chunks: Vec<&[u8]> = eull.iter_chunks(&arena).collect();
        // Blocks have size 8, 16, 32, 64.
        let chunk_lens: Vec<usize> = chunks.iter().map(|chunk| chunk.len()).collect();
        assert_eq!(&chunk_lens[..], &[8, 16, 32, 44]);
        assert_eq!(chunks.concat(), data);
        // `read_to_end` on a partially consumed iterator appends the remaining chunks.
        let mut chunks_iter = eull.iter_chunks(&arena);
        assert_eq!(chunks_iter.next().unwrap(), &data[..8]);
        let mut buffer = Vec::new();
        chunks_iter.read_to_end(&mut buffer);
        assert_eq!(buffer, &data[8..]);
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

    // Tests for u32 block_num fix (issue with large arrays)

    #[test]
    fn test_block_num_exceeds_u16_max() {
        // Test that we can handle more than 65,535 blocks (old u16 limit)
        let mut eull = ExpUnrolledLinkedList::default();

        // Simulate allocating 70,000 blocks (exceeds u16::MAX of 65,535)
        for _ in 0..70_000 {
            eull.increment_num_blocks();
        }

        // Verify block_num is correct
        assert_eq!(eull.block_num, FIRST_BLOCK_NUM + 70_000);

        // Verify we can still get block size (should be capped at 32KB)
        let block_size = get_block_size(eull.block_num);
        assert_eq!(block_size, 1 << 15); // 32KB max
    }

    #[test]
    #[allow(clippy::needless_range_loop)]
    fn test_large_dataset_simulation() {
        // Simulate the scenario: large arrays requiring many blocks
        // We write enough data to require thousands of blocks
        let mut arena = MemoryArena::default();
        let mut eull = ExpUnrolledLinkedList::default();

        // Write 100 MB of data (this will require ~3,200 blocks at 32KB each)
        // This is enough to validate the system works with large datasets
        // but not so much that the test is slow
        let bytes_per_write = 10_000;
        let num_writes = 10_000; // 10k * 10k = 100 MB

        let data: Vec<u8> = (0..bytes_per_write).map(|i| (i % 256) as u8).collect();
        for _ in 0..num_writes {
            eull.writer(&mut arena).extend_from_slice(&data);
        }

        // Verify we allocated many blocks (should be in the thousands)
        assert!(
            eull.block_num > 1000,
            "block_num ({}) should be > 1000 for this much data",
            eull.block_num
        );

        // Verify we can read back correctly
        let mut buffer = Vec::new();
        eull.read_to_end(&arena, &mut buffer);
        assert_eq!(buffer.len(), bytes_per_write * num_writes);

        // Verify data integrity on a sample
        for i in 0..bytes_per_write {
            assert_eq!(buffer[i], (i % 256) as u8);
        }
    }

    #[test]
    fn test_read_exact_into() {
        let data: Vec<u8> = (0..100u8).collect();
        let (arena, eull) = eull_with_data(&data);
        // Blocks have size 8, 16, 32, 64 (44 used).
        let mut chunks_iter = eull.iter_chunks(&arena);
        let mut buffer = Vec::new();
        // Within the first block.
        chunks_iter.read_exact_into(3, &mut buffer);
        assert_eq!(buffer, &data[..3]);
        // Up to the exact end of the first block.
        chunks_iter.read_exact_into(5, &mut buffer);
        assert_eq!(buffer, &data[..8]);
        // Across the second and third block.
        chunks_iter.read_exact_into(20, &mut buffer);
        assert_eq!(buffer, &data[..28]);
        chunks_iter.read_exact_into(0, &mut buffer);
        assert_eq!(buffer, &data[..28]);
        // `next()` returns the rest of the current block.
        assert_eq!(chunks_iter.next().unwrap(), &data[28..56]);
        // Bytes skipped by `next()` are not read again.
        chunks_iter.read_exact_into(4, &mut buffer);
        assert_eq!(&buffer[28..], &data[56..60]);
        // `read_to_end` appends the rest.
        chunks_iter.read_to_end(&mut buffer);
        assert_eq!(&buffer[32..], &data[60..]);
    }

    #[test]
    fn test_read_exact_into_consumes_len_bytes() {
        // List lengths around block boundaries (blocks of 8, 16, 32, ... bytes).
        for data_len in [1usize, 7, 8, 9, 23, 24, 25, 100, 1_000, 50_000] {
            // 251 is prime: the pattern does not line up with block sizes.
            let data: Vec<u8> = (0..data_len).map(|i| (i % 251) as u8).collect();
            let (arena, eull) = eull_with_data(&data);
            for step in [1usize, 2, 3, 5, 7, 8, 13, 64, 1_000] {
                let mut chunks_iter = eull.iter_chunks(&arena);
                let mut buffer = Vec::new();
                let mut pos = 0;
                while pos < data_len {
                    let len = step.min(data_len - pos);
                    chunks_iter.read_exact_into(len, &mut buffer);
                    pos += len;
                    // Also checks the position: a wrong position appends wrong bytes.
                    assert_eq!(buffer, &data[..pos], "data_len={data_len} step={step}");
                }
                assert!(chunks_iter.next().is_none());
            }
        }
    }

    #[test]
    #[should_panic(expected = "read_exact_into: 1 bytes missing")]
    fn test_read_exact_into_too_long() {
        let (arena, eull) = eull_with_data(&[1u8, 2, 3]);
        eull.iter_chunks(&arena).read_exact_into(4, &mut Vec::new());
    }

    #[test]
    fn test_full_blocks_len() {
        for block_num in FIRST_BLOCK_NUM + 1..40 {
            let expected: usize = (FIRST_BLOCK_NUM + 1..block_num)
                .map(|block_num| get_block_size(block_num) as usize)
                .sum();
            assert_eq!(
                full_blocks_len(block_num),
                expected,
                "block_num={block_num}"
            );
        }
    }

    #[test]
    fn test_get_block_size_with_large_block_num() {
        // Test that get_block_size handles large u32 values correctly

        // Small block numbers (under 15)
        assert_eq!(get_block_size(2), 4); // 2^2 = 4
        assert_eq!(get_block_size(3), 8); // 2^3 = 8
        assert_eq!(get_block_size(10), 1024); // 2^10 = 1KB

        // At the cap (15)
        assert_eq!(get_block_size(15), 32768); // 2^15 = 32KB

        // Beyond the cap (should stay at 32KB)
        assert_eq!(get_block_size(16), 32768);
        assert_eq!(get_block_size(100), 32768);
        assert_eq!(get_block_size(65_536), 32768); // Old u16::MAX + 1
        assert_eq!(get_block_size(100_000), 32768);
        assert_eq!(get_block_size(1_000_000), 32768);
    }

    #[test]
    fn test_increment_blocks_near_u16_boundary() {
        // Test incrementing around the old u16::MAX boundary
        let mut eull = ExpUnrolledLinkedList::default();

        // Set to just before old limit
        for _ in 0..65_533 {
            eull.increment_num_blocks();
        }
        assert_eq!(eull.block_num, FIRST_BLOCK_NUM + 65_533);

        // Cross the old u16::MAX boundary (this would have overflowed before)
        eull.increment_num_blocks(); // 65,534
        eull.increment_num_blocks(); // 65,535 (old max)
        eull.increment_num_blocks(); // 65,536 (would overflow u16)
        eull.increment_num_blocks(); // 65,537

        // Verify we're past the old limit
        assert_eq!(eull.block_num, FIRST_BLOCK_NUM + 65_537);
    }

    #[test]
    fn test_write_and_read_with_many_blocks() {
        // Test that write/read works correctly with many blocks
        let mut arena = MemoryArena::default();
        let mut eull = ExpUnrolledLinkedList::default();

        // Write data that will span many blocks
        let test_data: Vec<u8> = (0..50_000).map(|i| (i % 256) as u8).collect();
        eull.writer(&mut arena).extend_from_slice(&test_data);

        // Read it back
        let mut buffer = Vec::new();
        eull.read_to_end(&arena, &mut buffer);

        // Verify data integrity
        assert_eq!(buffer.len(), test_data.len());
        assert_eq!(&buffer[..], &test_data[..]);
    }

    #[test]
    fn test_multiple_eull_with_large_block_counts() {
        // Test multiple ExpUnrolledLinkedLists with high block counts
        // (simulates parallel columnar writes)
        let mut arena = MemoryArena::default();
        let mut eull1 = ExpUnrolledLinkedList::default();
        let mut eull2 = ExpUnrolledLinkedList::default();

        // Write different data to each
        for i in 0..10_000u32 {
            eull1.writer(&mut arena).write_u32_vint(i);
            eull2.writer(&mut arena).write_u32_vint(i * 2);
        }

        // Read back and verify
        let mut buf1 = Vec::new();
        let mut buf2 = Vec::new();
        eull1.read_to_end(&arena, &mut buf1);
        eull2.read_to_end(&arena, &mut buf2);

        // Deserialize and check
        let mut cursor1 = &buf1[..];
        let mut cursor2 = &buf2[..];
        for i in 0..10_000u32 {
            assert_eq!(read_u32_vint(&mut cursor1), i);
            assert_eq!(read_u32_vint(&mut cursor2), i * 2);
        }
    }

    #[test]
    fn test_block_size_stays_capped() {
        // Verify that even with massive block numbers, size stays at 32KB
        let mut eull = ExpUnrolledLinkedList::default();

        // Increment to a very large number
        for _ in 0..200_000 {
            eull.increment_num_blocks();
        }

        let block_size = get_block_size(eull.block_num);
        assert_eq!(block_size, 32768, "Block size should be capped at 32KB");
    }

    #[test]
    #[should_panic(expected = "ExpUnrolledLinkedList block count overflow")]
    fn test_increment_overflow_protection() {
        // Test that we panic gracefully if we somehow hit u32::MAX
        // This is extremely unlikely in practice (would require 128TB of data)
        let mut eull = ExpUnrolledLinkedList {
            block_num: u32::MAX,
            ..Default::default()
        };

        // This should panic with our custom error message
        eull.increment_num_blocks();
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
