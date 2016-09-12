use std::cell::UnsafeCell;
use std::mem;
use std::ptr;
use std::iter;

#[derive(Copy, Clone)]
pub struct BytesRef {
    pub start: u32,
    pub stop: u32,
}

struct InnerHeap {
    buffer: Vec<u8>,
    used: u32,
}

pub struct Heap {
    inner: UnsafeCell<InnerHeap>,
}

impl Heap {
    pub fn with_capacity(num_bytes: usize) -> Heap {
        Heap {
            inner: UnsafeCell::new(
                InnerHeap::with_capacity(num_bytes)
            ),
        }
    }

    fn inner(&self,) -> &mut InnerHeap {
        unsafe { &mut *self.inner.get() } 
    }

    pub fn clear(&self) {
        self.inner().clear();
    }

    pub fn capacity(&self,) -> u32 {
        self.inner().capacity()
    }

    pub fn len(&self,) -> u32 {
        self.inner().len()
    }

    pub fn free(&self,) -> u32 {
        self.inner().free()
    }

    pub fn allocate(&self, num_bytes: usize) -> u32 {
        self.inner().allocate(num_bytes)
    }
    
    pub fn new<V: From<u32>>(&self,) -> (u32, &mut V) {
        let addr = self.inner().allocate(mem::size_of::<V>());
        let v: V = V::from(addr);
        self.inner().set(addr, &v);
        (addr, self.inner().get_mut_ref(addr))
    }

    pub fn allocate_and_set(&self, data: &[u8]) -> BytesRef {
        self.inner().allocate_and_set(data)
    }

    pub fn get_slice(&self, bytes_ref: BytesRef) -> &[u8] {
        self.inner().get_slice(bytes_ref)
    }

    pub fn set<Item>(&self, addr: u32, val: &Item) {
        self.inner().set(addr, val);
    }

    pub fn get_mut_ref<Item>(&self, addr: u32) -> &mut Item {
        self.inner().get_mut_ref(addr)
    }
}


impl InnerHeap {

    pub fn with_capacity(num_bytes: usize) -> InnerHeap {
        InnerHeap {
            buffer: iter::repeat(0u8).take(num_bytes).collect(),
            used: 0u32,
        }
    }

    pub fn clear(&mut self) {
        self.used = 0u32;
    }

    pub fn capacity(&self,) -> u32 {
        self.buffer.len() as u32
    }

    pub fn len(&self,) -> u32 {
        self.used
    }

    pub fn free(&self,) -> u32 {
        (self.buffer.len() as u32) - self.used
    }

    pub fn allocate(&mut self, num_bytes: usize) -> u32 {
        let addr = self.used;
        self.used += num_bytes as u32;
        let len_buffer = self.buffer.len();
        if self.used > len_buffer as u32 {
            // TODO fix resizable heap
            panic!("Resizing heap is not working");
            // self.buffer.resize((self.used  * 2u32) as usize, 0u8);
        }
        addr
    }

    pub fn allocate_and_set(&mut self, data: &[u8]) -> BytesRef {
        let start = self.allocate(data.len()) as usize;
        let stop = start + data.len();
        &mut self.buffer[start..stop].clone_from_slice(data);
        BytesRef {
            start: start as u32,
            stop: stop as u32,
        }
    }

    pub fn get_mut(&mut self, addr: u32) -> *mut u8 {
        let addr_usize = addr as isize;
        debug_assert!(addr < self.used);
        unsafe { self.buffer.as_mut_ptr().offset(addr_usize) }
    }

    pub fn get_slice(&self, bytes_ref: BytesRef) -> &[u8] {
        &self.buffer[bytes_ref.start as usize .. bytes_ref.stop as usize]
    }

    pub fn get_mut_ref<Item>(&mut self, addr: u32) -> &mut Item {
        let v_ptr_u8 = self.get_mut(addr) as *mut u8;
        let v_ptr = v_ptr_u8 as *mut Item;
        unsafe { &mut *v_ptr }
    }

    pub fn set<Item>(&mut self, addr: u32, val: &Item) {
        let v_ptr: *const Item = val as *const Item;
        let v_ptr_u8: *const u8 = v_ptr as *const u8;
        debug_assert!(addr + mem::size_of::<Item>() as u32 <= self.used);
        unsafe {
            let dest_ptr: *mut u8 = self.get_mut(addr);
            ptr::copy(v_ptr_u8, dest_ptr, mem::size_of::<Item>());
        }
    }
}