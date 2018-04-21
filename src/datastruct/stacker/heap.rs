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
        Heap {
            inner: UnsafeCell::new(InnerHeap::with_capacity(num_bytes)),
        }
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
        self.inner().get_slice(bytes_ref)
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
