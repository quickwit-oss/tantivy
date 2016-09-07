use std::iter;
use super::HashItem;

pub struct MemoryPool {
    data: Box<[u8]>,
    offset: usize,
}


impl MemoryPool {
    pub fn with_capacity(num_bytes: usize) -> MemoryPool {
        let buffer_vec: Vec<u8> = iter::repeat(0u8).take(num_bytes).collect();
        MemoryPool {
            data: buffer_vec.into_boxed_slice(),
            offset: 0,
        }
    }
    
    pub fn get(&self, item: HashItem) -> &[u8] {
        &self.data.as_ref()[item.start .. item.stop]
    }
    
    pub fn get_mut(&self, item: HashItem) -> &mut [u8] {
        &self.data.as_mut()[item.start .. item.stop]
    }
    
    pub fn allocate(&mut self, allocate_size: usize) -> usize {
        let offset = self.offset;
        self.offset += allocate_size;
        offset
    }
    
    pub fn store(&mut self, term: &[u8]) -> HashItem {
        let offset = self.offset;
        let term_len = term.len();
        self.data.as_mut()[offset..offset + term_len].clone_from_slice(term);
        self.offset += term_len;
        HashItem {
            start: offset,
            stop: self.offset, 
        }
    }
}