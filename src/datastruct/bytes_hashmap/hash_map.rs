use super::MemoryPool;
use core::hash::SipHasher;


const HASH_TABLE_SIZE: usize = 1 << 20;

#[derive(Copy)]
pub struct HashItem {
    pub start: u32,
    pub stop: u32,
}

impl Default for HashItem {
    fn default() -> HashItem {
        HashItem {
            start: 0,
            stop: 0,
        }
    }
}

impl HashItem {
    pub fn is_empty(&self,) -> bool {
        self.stop == self.start
    }
}

pub struct HashMap {
    sip_hasher: SipHasher,
    memory_pool: MemoryPool,
    hash_table: [HashItem; HASH_TABLE_SIZE],
}



impl HashMap {
    
    pub fn new(num_bytes: usize) {
        HashMap {
            sip_hasher: SipHasher::new(),
            memory_pool: MemoryPool::with_capacity(num_bytes),
            hash_table: [HashItem::default(); HASH_TABLE_SIZE],
        }
    }
    
    fn bucket_id(&mut self, term: &[u8]) -> usize {
        self.sip_hasher.write(term);
        let hash: u64 = self.finish();
        (hash as usize) % HASH_TABLE_SIZE
    } 
    
    pub fn get_or_put(&mut self, term: &[u8]) -> HashItem {
        let mut bucket_id = self.bucket_id(term);
        loop {
            let hash_item = &self.hash_table[bucket_id];
            if hash_item.is_empty() {
                return self.memory_pool.store(term)
            }
            else {
                let current_term = self.memory_pool.get(item);
                if current_term == term {
                    return hash_item;
                }
            }
            bucket_id = (bucket_id + 1) % HASH_TABLE_SIZE;            
        }
    }
    
}