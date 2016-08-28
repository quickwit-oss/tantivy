use compression::NUM_DOCS_PER_BLOCK;
use DocId;

const BLOCK_SIZE: u32 = NUM_DOCS_PER_BLOCK as u32;

struct Block {
    data: [u32; BLOCK_SIZE as usize],
    next: u32,
}

impl Block {
    fn new() -> Block {
        Block {
            data: [0u32; BLOCK_SIZE as usize],
            next: u32::max_value(), 
        }
    }
}

#[derive(Copy, Clone)]
struct ListInfo {
    first: u32,
    last: u32,
    len: u32,
}

pub struct BlockStore {
    lists: Vec<ListInfo>,
    blocks: Vec<Block>,
    free_blocks: Vec<u32>,
}

impl BlockStore {
    pub fn allocate(num_blocks: usize) -> BlockStore {
        BlockStore {
            lists: Vec::with_capacity(1_000_000),
            blocks: (0 .. num_blocks).map(|_| Block::new()).collect(),
            free_blocks: (0u32 .. num_blocks as u32).collect()
        }
    }
    
    fn new_list(&mut self, first_el: u32) -> u32 {
        let res = self.lists.len() as u32;
        let new_block_id = self.new_block().unwrap();
        self.blocks[new_block_id as usize].data[0] = first_el;
        self.lists.push(ListInfo {
            first: new_block_id,
            last: new_block_id,
            len: 1,
        });
        res
    }
    
    fn new_block(&mut self,) -> Option<u32> {
        self.free_blocks.pop()
            .map(|block_id| {
                self.blocks[block_id as usize].next = u32::max_value();
                block_id
            })
    }
        
    fn get_list_info(&mut self, list_id: u32) -> &mut ListInfo {
        &mut self.lists[list_id as usize]
    }
    
    
    fn block_id_to_append(&mut self, list_id: u32) -> u32 {
        let list_info: ListInfo = self.lists[list_id as usize];
        // get_list_info(list_id).len % BLOCK_SIZE == 0;
        // let new_block_required: bool = self.get_list_info(list_id).len % BLOCK_SIZE == 0;
        if list_info.len % BLOCK_SIZE == 0 {
            // we need to add a fresh new block.
            let new_block_id: u32 = { self.new_block().unwrap() };
            let last_block_id: usize;
            {
                // update the list info.
                let list_info: &mut ListInfo = self.get_list_info(list_id);
                last_block_id = list_info.last as usize;
                list_info.last = new_block_id;
            }
            self.blocks[last_block_id].next = new_block_id;          
            new_block_id
        }
        else {
            list_info.last
        }
    }
    
    pub fn push(&mut self, list_id: u32, val: u32) {
        let new_block_required: bool = self.get_list_info(list_id).len % BLOCK_SIZE == 0;
        let block_id: u32 = self.block_id_to_append(list_id);
        let list_len: u32;
        {
            let list_info: &mut ListInfo = self.get_list_info(list_id);
            list_len = list_info.len;
            list_info.len += 1u32;
        }
        self.blocks[block_id as usize].data[(list_len % BLOCK_SIZE) as usize] = val; 
    }
    
    pub fn iter_list(&self, list_id: u32) -> BlockIterator {
        let list_info = &self.lists[list_id as usize];
        BlockIterator {
            current_block: &self.blocks[list_info.first as usize],
            blocks: &self.blocks,
            cursor: 0,
            len: list_info.len as usize,
        }
    }
}


pub struct BlockIterator<'a> {
    current_block: &'a Block,
    blocks: &'a [Block],
    cursor: usize,
    len: usize,
}


impl<'a> Iterator for BlockIterator<'a> {
    
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        if self.cursor == self.len {
            None
        }
        else {
            let res = self.current_block.data[self.cursor % (BLOCK_SIZE as usize)];
            self.cursor += 1;
            if self.cursor % (BLOCK_SIZE as usize) == 0 {
                self.current_block = &self.blocks[self.current_block.next as usize]; 
            }
            Some(res)
        }
        
    }
}





pub struct BlockAppender {
    blocks: Vec<Box<[DocId; NUM_DOCS_PER_BLOCK]>>,
    doc_freq: usize,
}

impl BlockAppender {
    
    pub fn new() -> BlockAppender {
        BlockAppender {
            blocks: Vec::new(),
            doc_freq: 0,
        }
    }
    
    pub fn push(&mut self, doc_id: DocId) {
        if self.doc_freq % NUM_DOCS_PER_BLOCK == 0 {
            self.blocks.push(Box::new([0u32; NUM_DOCS_PER_BLOCK ]));
        }
        self.blocks[self.doc_freq / NUM_DOCS_PER_BLOCK][self.doc_freq % NUM_DOCS_PER_BLOCK] = doc_id;
        self.doc_freq += 1;
    }
    
    pub fn last(&self) -> Option<DocId> {
        if self.doc_freq == 0 {
            return None
        }
        else {
            Some(self.get(self.doc_freq - 1))
        }
    }
    
    pub fn len(&self,) -> usize {
        self.doc_freq
    }
    
    
    pub fn get(&self, cursor: usize) -> DocId {
        self.blocks[cursor / NUM_DOCS_PER_BLOCK][cursor % NUM_DOCS_PER_BLOCK]
    }
        
    
    pub fn iter(&self,) -> IterBlockAppender {
        IterBlockAppender {
            cursor: 0,
            block_appender: &self,
        }
    }
}



pub struct IterBlockAppender<'a> {
    cursor: usize,
    block_appender: &'a BlockAppender,
}


impl<'a> Iterator for IterBlockAppender<'a> {
    
    type Item = DocId;

    fn next(&mut self) -> Option<u32> {
        if self.cursor == self.block_appender.doc_freq {
            return None
        }
        else {
            let res = self.block_appender.get(self.cursor);
            self.cursor += 1;
            Some(res)    
        }
        
    }
}






#[cfg(test)]
mod tests {
    
    
    use super::*;
    

    #[test]
    pub fn test_block_store() {
        let mut block_store = BlockStore::allocate(1_000);
        let list_2 = block_store.new_list(0);
        let list_3 = block_store.new_list(0);
        let list_4 = block_store.new_list(0);
        let list_5 = block_store.new_list(0);
        for i in 1 .. 2_000 {
            block_store.push(list_2, i * 2);
            block_store.push(list_3, i * 3);
        }
        for i in 1 .. 10 {
            block_store.push(list_4, i * 4);
            block_store.push(list_5, i * 5);
        }
        
        let mut list2_iter = block_store.iter_list(list_2);
        let mut list3_iter = block_store.iter_list(list_3);
        let mut list4_iter = block_store.iter_list(list_4);
        let mut list5_iter = block_store.iter_list(list_5);
        for i in 0 .. 2_000 {
            assert_eq!(list2_iter.next().unwrap(), i * 2);
            assert_eq!(list3_iter.next().unwrap(), i * 3);
            
        }
        assert!(list2_iter.next().is_none());
        assert!(list3_iter.next().is_none());
        for i in 0 .. 10 {
            assert_eq!(list4_iter.next().unwrap(), i * 4);
            assert_eq!(list5_iter.next().unwrap(), i * 5);
        }
        assert!(list4_iter.next().is_none());
        assert!(list5_iter.next().is_none());
    }
}