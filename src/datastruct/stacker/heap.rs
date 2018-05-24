use byteorder::{ByteOrder, NativeEndian};
use std::cell::UnsafeCell;
use std::ptr;

const NUM_BITS_PAGE_ADDR: usize = 20;
const PAGE_SIZE: usize = 1 << NUM_BITS_PAGE_ADDR; // pages are 1 MB large


/// Tantivy's custom `Heap`.
pub struct  Heap {
    inner: UnsafeCell<InnerHeap>,
}

#[cfg_attr(feature = "cargo-clippy", allow(mut_from_ref))]
impl Heap {

    /// Creates a new heap with a given capacity
    pub fn new() -> Heap {
        Heap {
            inner: UnsafeCell::new(InnerHeap::new()),
        }
    }

    fn inner(&self) -> &mut InnerHeap {
        unsafe { &mut *self.inner.get() }
    }

    pub fn mem_usage(&self) -> usize {
        self.inner().mem_usage()
    }

    /// Allocate a given amount of space and returns an address
    /// in the Heap.
    pub fn allocate_space(&self, num_bytes: usize) -> Addr {
        let (addr, _) = self.inner().allocate(num_bytes);
        addr
    }

    pub unsafe fn get_mut_ptr(&self, addr: Addr) -> *mut u8 {
        self.inner().get_mut_ptr(addr)
    }

    /// Stores a `&[u8]` in the heap and returns the destination BytesRef.
    pub fn allocate(&self, len: usize) -> (Addr, &mut [u8]) {
        self.inner().allocate(len)
    }

    /// Fetches the `&[u8]` stored on the slice defined by the `BytesRef`
    /// given as argumetn
    pub fn get_slice(&self, bytes_ref: Addr) -> &[u8] {
        self.inner().get_slice(bytes_ref)
    }

    /// Stores an item's data in the heap, at the given `address`.
    pub unsafe fn set<Item: Copy>(&self, addr: Addr, val: &Item) {
        let dst_ptr: *mut Item = (*self.inner.get()).get_mut_ptr(addr) as *mut Item;
        ptr::write_unaligned(dst_ptr, *val);
    }

    pub fn read<Item: Copy>(&self, addr: Addr) -> Item {
        unsafe {
            let ptr = self.inner().get_ptr(addr);
            ptr::read_unaligned(ptr as *const Item)
        }
    }

}


#[derive(Clone, Copy, Debug)]
pub struct Addr(pub u32);

impl Default for Addr {
    fn default() -> Self {
        Addr(u32::max_value())
    }
}

impl Addr {
    #[inline(always)]
    fn new(page_id: usize, local_addr: usize) -> Addr {
        Addr( (page_id << NUM_BITS_PAGE_ADDR | local_addr) as u32)
    }

    #[inline(always)]
    fn page_id(&self) -> usize {
        (self.0 as usize) >> NUM_BITS_PAGE_ADDR
    }

    #[inline(always)]
    fn page_local_addr(&self) -> usize {
        (self.0 as usize) & (PAGE_SIZE - 1)
    }

    #[inline(always)]
    pub fn is_null(&self) -> bool {
        self.0 == u32::max_value()
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
        unsafe { data.set_len(PAGE_SIZE); }
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

    #[inline(always)]
    fn allocate(&mut self, len: usize) -> Option<(Addr, &mut [u8])> {
        assert!(len <= PAGE_SIZE, "You may not allocate more than a page={} bytes", PAGE_SIZE);
        if self.is_available(len) {
            let local_addr = self.len;
            self.len += len;
            let addr = Addr::new(self.page_id, local_addr);
            Some((addr, &mut (*self.data)[local_addr..][..len]))
        } else {
            None
        }
    }


    fn get_slice(&self, local_addr: usize) -> &[u8] {
        let len = NativeEndian::read_u16(&self.data[local_addr..local_addr + 2]) as usize;
        &self.data[local_addr + 2..][..len]
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

    fn get_mut_slice(&mut self, addr: usize, len: usize) -> &mut [u8] {
        &mut (*self.data)[addr..addr+len]
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


pub struct InnerHeap {
    pages: Vec<Page>,
}

impl InnerHeap {

    pub fn new() -> InnerHeap {
        let first_page = Page::new(0);
        InnerHeap {
            pages: vec![first_page]
        }
    }

    fn add_page(&mut self) -> &mut Page {
        let new_page_id = self.pages.len();
        self.pages.push(Page::new(new_page_id));
        &mut self.pages[new_page_id]
    }

    pub fn mem_usage(&self) -> usize {
        self.pages.len() * PAGE_SIZE
    }

    pub fn get_slice(&self, addr: Addr) -> &[u8] {
        self.pages[addr.page_id()].get_slice(addr.page_local_addr())
    }

    pub unsafe fn get_ptr(&self, addr: Addr) -> *const u8 {
        self.pages[addr.page_id()].get_ptr(addr.page_local_addr())
    }

    pub unsafe fn get_mut_ptr(&mut self, addr: Addr) -> *mut u8 {
        self.pages[addr.page_id()].get_mut_ptr(addr.page_local_addr())
    }

    pub fn allocate_space(&mut self, len: usize) -> Addr {
        let page_id = self.pages.len() - 1;
        if let Some(addr) = self.pages[page_id].allocate_space(len) {
            return addr;
        }
        self.add_page().allocate_space(len).unwrap()
    }

    pub fn allocate(&mut self, len: usize) -> (Addr, &mut [u8]) {
        let page_id = self.pages.len() - 1;
        if self.pages[page_id].is_available(len) {
            return self.pages[page_id].allocate(len).unwrap();
        } else {
            return self.add_page().allocate(len).unwrap();
        }
    }

    pub fn get_mut_slice(&mut self, addr: Addr, len: usize) -> &mut [u8] {
        self.pages[addr.page_id()].get_mut_slice(addr.page_local_addr(), len)
    }
}


#[cfg(test)]
mod tests {

    use super::InnerHeap;

    #[test]
    fn test_arena_allocate() {
        let mut arena = InnerHeap::new();
        let a = b"hello";
        let b = b"happy tax payer";

        let addr_a = {
            let (addr_a, data) = arena.allocate(a.len());
            data.copy_from_slice(a);
            addr_a
        };
        let addr_b = {
            let (addr_b, data) = arena.allocate(b.len());
            data.copy_from_slice(b);
            addr_b
        };
        {
            let a_retrieve = arena.get_mut_slice(addr_a, a.len());
            assert_eq!(a_retrieve, a);
        }
        {
            let b_retrieve = arena.get_mut_slice(addr_b, b.len());
            assert_eq!(b_retrieve, b);
        }
    }

}