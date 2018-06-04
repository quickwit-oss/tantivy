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
#[derive(Clone, Copy, Debug)]
pub struct Addr(u32);

impl Addr {

    /// Creates a null pointer.
    pub fn null_pointer() -> Addr {
        Addr(u32::max_value())
    }

    /// Returns the `Addr` object for `addr + offset`
    pub fn offset(&self, offset: u32) -> Addr {
        Addr(self.0.wrapping_add(offset))
    }

    fn new(page_id: usize, local_addr: usize) -> Addr {
        Addr( (page_id << NUM_BITS_PAGE_ADDR | local_addr) as u32)
    }

    fn page_id(&self) -> usize {
        (self.0 as usize) >> NUM_BITS_PAGE_ADDR
    }

    fn page_local_addr(&self) -> usize {
        (self.0 as usize) & (PAGE_SIZE - 1)
    }

    /// Returns true if and only if the `Addr` is null.
    pub fn is_null(&self) -> bool {
        self.0 == u32::max_value()
    }
}


/// Trait required for an object to be `storable`.
///
/// # Warning
///
/// Most of the time you should not implement this trait,
/// and only use the `MemoryArena` with object implementing `Copy`.
///
/// `ArenaStorable` is used in `tantivy` to force
/// a `Copy` object and a `slice` of data to be stored contiguously.
pub trait ArenaStorable {
    fn num_bytes(&self) -> usize;
    unsafe fn write_into(self, arena: &mut MemoryArena, addr: Addr);
}

impl<V> ArenaStorable for V where V: Copy {
    fn num_bytes(&self) -> usize {
        mem::size_of::<V>()
    }

    unsafe fn write_into(self, arena: &mut MemoryArena, addr: Addr) {
        let dst_ptr = arena.get_mut_ptr(addr) as *mut V;
        ptr::write_unaligned(dst_ptr, self);
    }
}

/// The `MemoryArena`
pub struct MemoryArena {
    pages: Vec<Page>,
}

impl MemoryArena {

    /// Creates a new memory arena.
    pub fn new() -> MemoryArena {
        let first_page = Page::new(0);
        MemoryArena {
            pages: vec![first_page]
        }
    }

    fn add_page(&mut self) -> &mut Page {
        let new_page_id = self.pages.len();
        self.pages.push(Page::new(new_page_id));
        &mut self.pages[new_page_id]
    }

    /// Returns an estimate in number of bytes
    /// of resident memory consumed by the `MemoryArena`.
    ///
    /// Internally, it counts a number of `1MB` pages
    /// and therefore delivers an upperbound.
    pub fn mem_usage(&self) -> usize {
        self.pages.len() * PAGE_SIZE
    }

    /// Writes a slice at the given address, assuming the
    /// memory was allocated beforehands.
    ///
    ///  # Panics
    ///
    /// May panic or corrupt the heap if he space was not
    /// properly allocated beforehands.
    pub fn write_bytes<B: AsRef<[u8]>>(&mut self, addr: Addr, data: B) {
        let bytes = data.as_ref();
        self.pages[addr.page_id()]
            .get_mut_slice(addr.page_local_addr(), bytes    .len())
            .copy_from_slice(bytes);
    }

    /// Returns the `len` bytes starting at `addr`
    ///
    /// # Panics
    ///
    /// Panics if the memory has not been allocated beforehands.
    pub fn read_slice(&self, addr: Addr, len: usize) -> &[u8] {
        self.pages[addr.page_id()]
            .get_slice(addr.page_local_addr(), len)
    }

    unsafe fn get_mut_ptr(&mut self, addr: Addr) -> *mut u8 {
        self.pages[addr.page_id()].get_mut_ptr(addr.page_local_addr())
    }

    /// Stores an item's data in the heap
    ///
    /// It allocates the `Item` beforehands.
    pub fn store<Item: ArenaStorable>(&mut self, val: Item) -> Addr {
        let num_bytes = val.num_bytes();
        let addr = self.allocate_space(num_bytes);
        unsafe { self.write(addr, val); };
        addr
    }

    pub unsafe fn write<Item: ArenaStorable>(&mut self, addr: Addr, val: Item) {
        val.write_into(self, addr)
    }

    /// Read an item in the heap at the given `address`.
    ///
    /// # Panics
    ///
    /// If the address is erroneous
    pub unsafe fn read<Item: Copy>(&self, addr: Addr) -> Item {
        let ptr = self.pages[addr.page_id()].get_ptr(addr.page_local_addr());
        ptr::read_unaligned(ptr as *const Item)
    }

    /// Allocates `len` bytes and returns the allocated address.
    pub fn allocate_space(&mut self, len: usize) -> Addr {
        let page_id = self.pages.len() - 1;
        if let Some(addr) = self.pages[page_id].allocate_space(len) {
            return addr;
        }
        self.add_page().allocate_space(len).unwrap()
    }

}


struct Page {
    page_id: usize,
    len: usize,
    data: Box<[u8]>
}

impl Page {
    fn new(page_id: usize) -> Page {
        let mut data: Vec<u8> = Vec::with_capacity(PAGE_SIZE);
        unsafe { data.set_len(PAGE_SIZE); } // avoid initializing page
        Page {
            page_id,
            len: 0,
            data: data.into_boxed_slice()
        }
    }

    #[inline(always)]
    fn is_available(&self, len: usize) -> bool {
        len + self.len <= PAGE_SIZE
    }

    fn get_mut_slice(&mut self, local_addr: usize, len: usize) -> &mut [u8] {
        &mut self.data[local_addr..][..len]
    }

    fn get_slice(&self, local_addr: usize, len: usize) -> &[u8] {
        &self.data[local_addr..][..len]
    }

    fn allocate_space(&mut self, len: usize) -> Option<Addr> {
        if self.is_available(len) {
            let addr = Addr::new(self.page_id, self.len);
            self.len += len;
            Some(addr)
        } else {
            None
        }
    }

    #[inline(always)]
    pub(crate) unsafe fn get_ptr(&self, addr: usize) -> *const u8 {
        self.data.as_ptr().offset(addr as isize)
    }

    #[inline(always)]
    pub(crate) unsafe fn get_mut_ptr(&mut self, addr: usize) -> *mut u8 {
        self.data.as_mut_ptr().offset(addr as isize)
    }
}

#[cfg(test)]
mod tests {

    use super::MemoryArena;

    #[test]
    fn test_arena_allocate_slice() {
        let mut arena = MemoryArena::new();
        let a = b"hello";
        let b = b"happy tax payer";

        let addr_a = arena.allocate_space(a.len());
        arena.write_bytes(addr_a, a);

        let addr_b= arena.allocate_space(b.len());
        arena.write_bytes(addr_b, b);

        assert_eq!(arena.read_slice(addr_a, a.len()), a);
        assert_eq!(arena.read_slice(addr_b, b.len()), b);
    }


    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct MyTest {
        pub a: usize,
        pub b: u8,
        pub c: u32
    }

    #[test]
    fn test_store_object() {
        let mut arena = MemoryArena::new();
        let a = MyTest { a: 143, b: 21, c: 32};
        let b = MyTest { a: 113, b: 221, c: 12};
        let addr_a = arena.store(a);
        let addr_b = arena.store(b);
        assert_eq!(unsafe { arena.read::<MyTest>(addr_a) }, a);
        assert_eq!(unsafe { arena.read::<MyTest>(addr_b) }, b);
    }
}