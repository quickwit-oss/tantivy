use byteorder::{ByteOrder, NativeEndian};
use std::ptr;

const NUM_BITS_PAGE_ADDR: usize = 20;
const PAGE_SIZE: usize = 1 << NUM_BITS_PAGE_ADDR; // pages are 1 MB large



#[derive(Clone, Copy, Debug)]
pub struct Addr(u32);


impl Addr {

    pub fn null_pointer() -> Addr {
        Addr(u32::max_value())
    }

    #[inline(always)]
    pub fn offset(&self, shift: u32) -> Addr {
        Addr(self.0.wrapping_add(shift))
    }

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



pub struct Heap {
    pages: Vec<Page>,
}

impl Heap {

    pub fn new() -> Heap {
        let first_page = Page::new(0);
        Heap {
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

    /// Returns a chunk stored at the addr `Addr`.
    ///
    /// A chunk is assumed to have been written using the
    /// `.write_chunk(..)` method.
    pub fn get_chunk(&self, addr: Addr) -> &[u8] {
        self.pages[addr.page_id()].get_chunk(addr.page_local_addr())
    }

    /// Assumes the space is preallocated.
    /// Write a slice at the given address. Prepends the content by its length
    /// encoded over 2 bytes.
    ///
    /// The slice is required to have a length of less than `u16::max_value()`.
    pub unsafe fn write_chunk(&mut self, addr: Addr, data: &[u8]) {
        assert!(data.len() < u16::max_value() as usize);
        let dest_bytes = self.pages[addr.page_id()].get_mut_slice(addr.page_local_addr(), data.len() + 2);
        NativeEndian::write_u16(&mut dest_bytes[0..2], data.len() as u16);
        dest_bytes[2..].copy_from_slice(data);
    }

    unsafe fn get_mut_ptr(&mut self, addr: Addr) -> *mut u8 {
        self.pages[addr.page_id()].get_mut_ptr(addr.page_local_addr())
    }

    /// Stores an item's data in the heap, at the given `address`.
    pub unsafe fn set<Item: Copy>(&mut self, addr: Addr, val: Item) {
        let dst_ptr: *mut Item = self.get_mut_ptr(addr) as *mut Item;
        ptr::write_unaligned(dst_ptr, val);
    }

    pub unsafe fn read<Item: Copy>(&self, addr: Addr) -> Item {
        let ptr = self.pages[addr.page_id()].get_ptr(addr.page_local_addr());
        ptr::read_unaligned(ptr as *const Item)
    }

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

    fn get_chunk(&self, local_addr: usize) -> &[u8] {
        let len = NativeEndian::read_u16(&self.data[local_addr..local_addr + 2]) as usize;
        &self.data[local_addr + 2..][..len]
    }

    fn get_mut_slice(&mut self, local_addr: usize, len: usize) -> &mut [u8] {
        &mut self.data[local_addr..][..len]
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

    use super::Heap;
    use std::slice;
    use std::ptr;

    #[test]
    fn test_arena_allocate() {
        let mut arena = Heap::new();
        let a = b"hello";
        let b = b"happy tax payer";

        unsafe {
            let addr_a = {
                let addr_a = arena.allocate_space(a.len());
                ptr::copy_nonoverlapping(a.as_ptr(), arena.get_mut_ptr(addr_a), a.len());
                addr_a
            };

            let addr_b = {
                let addr_b = arena.allocate_space(b.len());
                ptr::copy_nonoverlapping(b.as_ptr(), arena.get_mut_ptr(addr_b), b.len());
                addr_b
            };

            {
                let a_ptr = arena.get_mut_ptr(addr_a);
                let slice_a = slice::from_raw_parts(a_ptr, a.len());
                assert_eq!(slice_a, a);
            }

            {
                let b_ptr = arena.get_mut_ptr(addr_b);
                let slice_b = slice::from_raw_parts(b_ptr, b.len());
                assert_eq!(slice_b, b);
            }
        }
    }

}