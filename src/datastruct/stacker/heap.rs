use std::cell::UnsafeCell;
use std::mem;
use std::ptr;

/// `BytesRef` refers to a slice in tantivy's custom `Heap`.
#[derive(Copy, Clone)]
pub struct BytesRef {
    pub start: u32,
    pub stop: u32,
}

/// Object that can be allocated in tantivy's custom `Heap`.
pub trait HeapAllocable {
    fn with_addr(addr: u32) -> Self;
}

/// Tantivy's custom `Heap`.
pub struct Heap {
    inner: UnsafeCell<InnerHeap>,
}

#[cfg_attr(feature = "cargo-clippy", allow(mut_from_ref))]
impl Heap {
    /// Creates a new heap with a given capacity
    pub fn with_capacity(num_bytes: usize) -> Heap {
        Heap { inner: UnsafeCell::new(InnerHeap::with_capacity(num_bytes)) }
    }

    fn inner(&self) -> &mut InnerHeap {
        unsafe { &mut *self.inner.get() }
    }

    /// Clears the heap. All the underlying data is lost.
    ///
    /// This heap does not support deallocation.
    /// This method is the only way to free memory.
    pub fn clear(&self) {
        self.inner().clear();
    }

    /// Return the heap capacity.
    pub fn capacity(&self) -> u32 {
        self.inner().capacity()
    }

    /// Return amount of free space, in bytes.
    pub fn num_free_bytes(&self) -> u32 {
        self.inner().num_free_bytes()
    }

    /// Allocate a given amount of space and returns an address
    /// in the Heap.
    pub fn allocate_space(&self, num_bytes: usize) -> u32 {
        self.inner().allocate_space(num_bytes)
    }

    /// Allocate an object in the heap
    pub fn allocate_object<V: HeapAllocable>(&self) -> (u32, &mut V) {
        let addr = self.inner().allocate_space(mem::size_of::<V>());
        let v: V = V::with_addr(addr);
        self.inner().set(addr, &v);
        (addr, self.inner().get_mut_ref(addr))
    }

    /// Stores a `&[u8]` in the heap and returns the destination BytesRef.
    pub fn allocate_and_set(&self, data: &[u8]) -> BytesRef {
        self.inner().allocate_and_set(data)
    }

    /// Fetches the `&[u8]` stored on the slice defined by the `BytesRef`
    /// given as argumetn
    pub fn get_slice(&self, bytes_ref: BytesRef) -> &[u8] {
        self.inner().get_slice(bytes_ref.start, bytes_ref.stop)
    }

    /// Stores an item's data in the heap, at the given `address`.
    pub fn set<Item>(&self, addr: u32, val: &Item) {
        self.inner().set(addr, val);
    }

    /// Returns a mutable reference for an object at a given Item.
    pub fn get_mut_ref<Item>(&self, addr: u32) -> &mut Item {
        self.inner().get_mut_ref(addr)
    }

    /// Returns a mutable reference to an `Item` at a given `addr`.
    #[cfg(test)]
    pub fn get_ref<Item>(&self, addr: u32) -> &mut Item {
        self.get_mut_ref(addr)
    }
}



struct InnerHeap {
    buffer: Vec<u8>,
    used: u32,
    has_been_resized: bool,
}


impl InnerHeap {
    pub fn with_capacity(num_bytes: usize) -> InnerHeap {
        let buffer: Vec<u8> = vec![0u8; num_bytes];
        InnerHeap {
            buffer: buffer,
            used: 0u32,
            has_been_resized: false,
        }
    }

    pub fn clear(&mut self) {
        self.used = 0u32;
    }

    pub fn capacity(&self) -> u32 {
        self.buffer.len() as u32
    }

    // Returns the number of free bytes. If the buffer
    // has reached it's capacity and overflowed to another buffer, return 0.
    pub fn num_free_bytes(&self) -> u32 {
        if self.has_been_resized {
            0u32
        } else {
            (self.buffer.len() as u32) - self.used
        }
    }

    pub fn allocate_space(&mut self, num_bytes: usize) -> u32 {
        let addr = self.used;
        self.used += num_bytes as u32;
        let buffer_len = self.buffer.len();
        if self.used > buffer_len as u32 {
            self.buffer.resize(buffer_len * 2, 0u8);
            self.has_been_resized = true
        }
        addr
    }

    fn get_slice(&self, start: u32, stop: u32) -> &[u8] {
        &self.buffer[start as usize..stop as usize]
    }

    fn get_mut_slice(&mut self, start: u32, stop: u32) -> &mut [u8] {
        &mut self.buffer[start as usize..stop as usize]
    }

    fn allocate_and_set(&mut self, data: &[u8]) -> BytesRef {
        let start = self.allocate_space(data.len());
        let stop = start + data.len() as u32;
        self.get_mut_slice(start, stop).clone_from_slice(data);
        BytesRef {
            start: start as u32,
            stop: stop as u32,
        }
    }

    fn get_mut(&mut self, addr: u32) -> *mut u8 {
        let addr_isize = addr as isize;
        unsafe { self.buffer.as_mut_ptr().offset(addr_isize) }
    }

    fn get_mut_ref<Item>(&mut self, addr: u32) -> &mut Item {
        let v_ptr_u8 = self.get_mut(addr) as *mut u8;
        let v_ptr = v_ptr_u8 as *mut Item;
        unsafe { &mut *v_ptr }
    }

    fn set<Item>(&mut self, addr: u32, val: &Item) {
        let v_ptr: *const Item = val as *const Item;
        let v_ptr_u8: *const u8 = v_ptr as *const u8;
        debug_assert!(addr + mem::size_of::<Item>() as u32 <= self.used);
        unsafe {
            let dest_ptr: *mut u8 = self.get_mut(addr);
            ptr::copy(v_ptr_u8, dest_ptr, mem::size_of::<Item>());
        }
    }
}
