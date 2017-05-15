use std::sync::Arc;


#[derive(Clone)]
pub struct SharedVecSlice {
    pub data: Arc<Vec<u8>>,
    pub start: usize,
    pub len: usize,
}

impl SharedVecSlice {
    pub fn empty() -> SharedVecSlice {
        SharedVecSlice::new(Arc::new(Vec::new()))
    }

    pub fn new(data: Arc<Vec<u8>>) -> SharedVecSlice {
        let data_len = data.len();
        SharedVecSlice {
            data: data,
            start: 0,
            len: data_len,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data[self.start..self.start + self.len]
    }

    pub fn slice(&self, from_offset: usize, to_offset: usize) -> SharedVecSlice {
        SharedVecSlice {
            data: self.data.clone(),
            start: self.start + from_offset,
            len: to_offset - from_offset,
        }
    }
}
