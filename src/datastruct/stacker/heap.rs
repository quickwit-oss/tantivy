use byteorder::{ByteOrder, NativeEndian};
use std::cell::UnsafeCell;
use std::mem;
use std::ptr;


/// `BytesRef` refers to a slice in tantivy's custom `Heap`.
///
/// The slice will encode the length of the `&[u8]` slice
/// on 16-bits, and then the data is encoded.
#[derive(Copy, Clone)]
pub struct BytesRef(u32);

impl BytesRef {
    pub fn is_null(&self) -> bool {
        self.0 == u32::max_value()
    }

    pub fn addr(&self) -> u32 {
        self.0
    }
}

impl Default for BytesRef {
    fn default() -> BytesRef {
        BytesRef(u32::max_value())
    }
}


/// Object that can be allocated in tantivy's custom `Heap`.
pub trait HeapAllocable {
    fn with_addr(addr: Addr) -> Self;
}

/// Tantivy's custom `Heap`.
pub struct  Heap {
    inner: UnsafeCell<InnerHeap>,
}

#[cfg_attr(feature = "cargo-clippy", allow(mut_from_ref))]
impl Heap {

    pub fn with_capacity(cap: usize) -> Heap {
        Heap::new()
    }
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

    /// Clears the heap. All the underlying data is lost.
    ///
    /// This heap does not support deallocation.
    /// This method is the only way to free memory.
    pub fn clear(&self) {
        self.inner().clear();
    }

    /// Return amount of free space, in bytes.
    pub fn num_free_bytes(&self) -> u32 {
        panic!("num free bytes");
    }

    /// Allocate a given amount of space and returns an address
    /// in the Heap.
    pub fn allocate_space(&self, num_bytes: usize) -> Addr {
        let (addr, _) = self.inner().allocate(num_bytes);
        addr
    }

    /// Allocate an object in the heap
    pub fn allocate_object<V: HeapAllocable>(&self) -> (Addr, &mut V) {
        let addr = self.inner().allocate_space(mem::size_of::<V>());
        let v: V = V::with_addr(addr);
        unsafe {
            let v_mut_ptr = self.inner().get_mut_ptr(addr) as *mut V;
            ptr::write_unaligned(v_mut_ptr, v);
            (addr, &mut *v_mut_ptr)
        }

    }

    /// Stores a `&[u8]` in the heap and returns the destination BytesRef.
//    pub fn allocate_and_set(&self, data: &[u8]) -> BytesRef {
//        self.inner().allocate_and_set(data)
//    }

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

    /// Returns a mutable reference for an object at a given Item.
    pub fn get_mut_ref<Item>(&self, addr: Addr) -> &mut Item {
        unsafe {
            let item_ptr = self.inner().get_mut_ptr(addr) as *mut Item;
            &mut *item_ptr
        }
    }

    /// Returns a mutable reference to an `Item` at a given `addr`.
    #[cfg(test)]
    pub fn get_ref<Item>(&self, addr: Addr) -> &mut Item {
        self.get_mut_ref(addr)
    }
}

/*
struct InnerHeap {
    buffer: Vec<u8>,
    buffer_len: u32,
    used: u32,
    next_heap: Option<Box<InnerHeap>>,
}

impl InnerHeap {
    pub fn with_capacity(num_bytes: usize) -> InnerHeap {
        let buffer: Vec<u8> = vec![0u8; num_bytes];
        InnerHeap {
            buffer,
            buffer_len: num_bytes as u32,
            next_heap: None,
            used: 0u32,
        }
    }

    pub fn clear(&mut self) {
        self.used = 0u32;
        self.next_heap = None;
    }

    // Returns the number of free bytes. If the buffer
    // has reached it's capacity and overflowed to another buffer, return 0.
    pub fn num_free_bytes(&self) -> u32 {
        if self.next_heap.is_some() {
            0u32
        } else {
            self.buffer_len - self.used
        }
    }

    pub fn allocate_space(&mut self, num_bytes: usize) -> u32 {
        let addr = self.used;
        self.used += num_bytes as u32;
        if self.used <= self.buffer_len {
            addr
        } else {
            if self.next_heap.is_none() {
                info!(
                    r#"Exceeded heap size. The segment will be committed right
                         after indexing this document."#,
                );
                self.next_heap = Some(Box::new(InnerHeap::with_capacity(self.buffer_len as usize)));
            }
            self.next_heap.as_mut().unwrap().allocate_space(num_bytes) + self.buffer_len
        }
    }

    fn get_slice(&self, bytes_ref: BytesRef) -> &[u8] {
        let start = bytes_ref.0;
        if start >= self.buffer_len {
            self.next_heap
                .as_ref()
                .unwrap()
                .get_slice(BytesRef(start - self.buffer_len))
        } else {
            let start = start as usize;
            let len = NativeEndian::read_u16(&self.buffer[start..start + 2]) as usize;
            &self.buffer[start + 2..start + 2 + len]
        }
    }

    fn get_mut_slice(&mut self, start: u32, stop: u32) -> &mut [u8] {
        if start >= self.buffer_len {
            self.next_heap
                .as_mut()
                .unwrap()
                .get_mut_slice(start - self.buffer_len, stop - self.buffer_len)
        } else {
            &mut self.buffer[start as usize..stop as usize]
        }
    }

    fn allocate_and_set(&mut self, data: &[u8]) -> BytesRef {
        assert!(data.len() < u16::max_value() as usize);
        let total_len = 2 + data.len();
        let start = self.allocate_space(total_len);
        let total_buff = self.get_mut_slice(start, start + total_len as u32);
        NativeEndian::write_u16(&mut total_buff[0..2], data.len() as u16);
        total_buff[2..].clone_from_slice(data);
        BytesRef(start)
    }

    fn get_mut(&mut self, addr: u32) -> *mut u8 {
        if addr >= self.buffer_len {
            self.next_heap
                .as_mut()
                .unwrap()
                .get_mut(addr - self.buffer_len)
        } else {
            let addr_isize = addr as isize;
            unsafe { self.buffer.as_mut_ptr().offset(addr_isize) }
        }
    }

    fn get_mut_ref<Item>(&mut self, addr: u32) -> &mut Item {
        if addr >= self.buffer_len {
            self.next_heap
                .as_mut()
                .unwrap()
                .get_mut_ref(addr - self.buffer_len)
        } else {
            let v_ptr_u8 = self.get_mut(addr) as *mut u8;
            let v_ptr = v_ptr_u8 as *mut Item;
            unsafe { &mut *v_ptr }
        }
    }

    pub fn set<Item>(&mut self, addr: u32, val: &Item) {
        if addr >= self.buffer_len {
            self.next_heap
                .as_mut()
                .unwrap()
                .set(addr - self.buffer_len, val);
        } else {
            let v_ptr: *const Item = val as *const Item;
            let v_ptr_u8: *const u8 = v_ptr as *const u8;
            debug_assert!(addr + mem::size_of::<Item>() as u32 <= self.used);
            unsafe {
                let dest_ptr: *mut u8 = self.get_mut(addr);
                ptr::copy(v_ptr_u8, dest_ptr, mem::size_of::<Item>());
            }
        }
    }
}
*/

const NUM_BITS_PAGE_ADDR: usize = 20;
const PAGE_SIZE: usize = 1 << NUM_BITS_PAGE_ADDR; // pages are 1 MB large

#[derive(Clone, Copy, Debug, Default)]
pub struct Addr(pub u32);

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
        let mut first_page = Page::new(0);
        InnerHeap {
            pages: vec![]
        }
    }

    fn clear(&mut self) {
        self.pages.clear();
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
        assert!(len < PAGE_SIZE, "Can't allocate anything over {}", PAGE_SIZE);
        let page_id = self.pages.len() - 1;
        if let Some(addr) = self.pages[page_id].allocate_space(len) {
            return addr;
        }
        self.add_page().allocate_space(len).unwrap()
    }

    pub fn  allocate(&mut self, len: usize) -> (Addr, &mut [u8]) {
        assert!(len < PAGE_SIZE, "Can't allocate anything over {}", PAGE_SIZE);
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