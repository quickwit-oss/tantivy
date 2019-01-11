//! 32-bits Memory arena for types implementing `Copy`.
//! This Memory arena has been implemented to fit the use of tantivy's indexer
//! and has *twisted specifications*.
//!
//! - It works on stable rust.
//! - One can get an accurate figure of the memory usage of the arena.
//! - Allocation are very cheap.
//! - Allocation happening consecutively are very likely to have great locality.
//! - Addresses (`Addr`) are 32bits.
//! - Dropping the whole `MemoryArena` is cheap.
//!
//! # Limitations
//!
//! - Your object shall not implement `Drop`.
//! - `Addr` to the `Arena` are 32-bits. The maximum capacity of the arena
//! is 4GB. *(Tantivy's indexer uses one arena per indexing thread.)*
//! - The arena only works for objects much smaller than  `1MB`.
//! Allocating more than `1MB` at a time will result in a panic,
//! and allocating a lot of large object (> 500KB) will result in a fragmentation.
//! - Your objects are store in an unaligned fashion. For this reason,
//! the API does not let you access them as references.
//!
//! Instead, you store and access your data via `.write(...)` and `.read(...)`, which under the hood
//! stores your object using `ptr::write_unaligned` and `ptr::read_unaligned`.
use std::mem;
use std::ptr;

const NUM_BITS_PAGE_ADDR: usize = 20;
const PAGE_SIZE: usize = 1 << NUM_BITS_PAGE_ADDR; // pages are 1 MB large

/// Represents a pointer into the `MemoryArena`
/// .
/// Pointer are 32-bits and are split into
/// two parts.
///
/// The first 12 bits represent the id of a
/// page of memory.
///
/// The last 20 bits are an address within this page of memory.
#[derive(Copy, Clone, Debug)]
pub struct Addr(u32);

impl Addr {
    /// Creates a null pointer.
    pub fn null_pointer() -> Addr {
        Addr(u32::max_value())
    }

    /// Returns the `Addr` object for `addr + offset`
    pub fn offset(self, offset: u32) -> Addr {
        Addr(self.0.wrapping_add(offset))
    }

    /// Returns true if and only if the `Addr` is null.
    pub fn is_null(self) -> bool {
        self.0 == u32::max_value()
    }
}

pub fn store<Item: Copy + 'static>(dest: &mut [u8], val: Item) {
    assert_eq!(dest.len(), std::mem::size_of::<Item>());
    unsafe {
        ptr::write_unaligned(dest.as_mut_ptr() as *mut Item, val);
    }
}

/// The `MemoryArena`
pub struct MemoryArena {
    buffer: Vec<u8>,
}

impl MemoryArena {
    /// Creates a new memory arena.
    pub fn new(capacity: usize) -> MemoryArena {
        MemoryArena {
            buffer: Vec::with_capacity(capacity),
        }
    }

    /// Returns an estimate in number of bytes
    /// of resident memory consumed by the `MemoryArena`.
    ///
    /// Internally, it counts a number of `1MB` pages
    /// and therefore delivers an upperbound.
    pub fn mem_usage(&self) -> usize {
        self.buffer.len()
    }

    pub fn write_slice(&mut self, addr: Addr, data: &[u8]) {
        let start = addr.0 as usize;
        let stop = start + data.len();
        self.buffer[start..stop].copy_from_slice(data);
    }

    pub fn write_at<Item: Copy + 'static>(&mut self, addr: Addr, val: Item) {
        let dest = self.slice_mut(addr, std::mem::size_of::<Item>());
        store(dest, val);
    }

    /// Read an item in the heap at the given `address`.
    ///
    /// # Panics
    ///
    /// If the address is erroneous
    pub fn read<Item: Copy + 'static>(&self, addr: Addr) -> Item {
        let data = self.slice(addr, mem::size_of::<Item>());
        unsafe { ptr::read_unaligned(data.as_ptr() as *const Item) }
    }

    pub fn slice(&self, addr: Addr, len: usize) -> &[u8] {
        let start = addr.0 as usize;
        let stop = start + len;
        &self.buffer[start..stop]
    }

    pub fn slice_from(&self, addr: Addr) -> &[u8] {
        &self.buffer[addr.0 as usize..]
    }

    pub fn slice_mut(&mut self, addr: Addr, len: usize) -> &mut [u8] {
        let start = addr.0 as usize;
        let stop = start + len;
        &mut self.buffer[start..stop]
    }

    /// Allocates `len` bytes and returns the allocated address.
    pub fn allocate_space(&mut self, len: usize) -> Addr {
        let addr = self.buffer.len();
        self.buffer.resize(addr + len, 0u8);
        Addr(addr as u32)
    }
}

#[cfg(test)]
mod tests {

    use super::MemoryArena;

    #[test]
    fn test_arena_allocate_slice() {
        let mut arena = MemoryArena::new(10_000);
        let a = b"hello";
        let b = b"happy tax payer";

        let addr_a = arena.allocate_space(a.len());
        arena.slice_mut(addr_a, a.len()).copy_from_slice(a);

        let addr_b = arena.allocate_space(b.len());
        arena.slice_mut(addr_b, b.len()).copy_from_slice(b);

        assert_eq!(arena.slice(addr_a, a.len()), a);
        assert_eq!(arena.slice(addr_b, b.len()), b);
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct MyTest {
        pub a: usize,
        pub b: u8,
        pub c: u32,
    }

    #[test]
    fn test_store_object() {
        let mut arena = MemoryArena::new(10_000);
        let a = MyTest {
            a: 143,
            b: 21,
            c: 32,
        };
        let b = MyTest {
            a: 113,
            b: 221,
            c: 12,
        };

        let num_bytes = std::mem::size_of::<MyTest>();
        let addr_a = arena.allocate_space(num_bytes);
        arena.write_at(addr_a, a);

        let addr_b = arena.allocate_space(num_bytes);
        arena.write_at(addr_b, b);

        assert_eq!(arena.read::<MyTest>(addr_a), a);
        assert_eq!(arena.read::<MyTest>(addr_b), b);
    }
}
