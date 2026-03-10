use common::VInt32Reader;

use crate::{ExpUnrolledLinkedList, MemoryArena};

/// A compact vector of `u32` values stored using variable-length integer encoding
/// backed by an arena-allocated exponential unrolled linked list.
///
/// Each `u32` is encoded as a vint (1–5 bytes) and appended to the linked list.
/// This is much more memory-efficient than `Vec<u32>` for storing many small integers,
/// particularly in the indexing pipeline where millions of per-term posting lists
/// must coexist in memory.
///
/// # Usage
///
/// ```ignore
/// use stacker::{MemoryArena, VecVint};
///
/// let mut arena = MemoryArena::default();
/// let mut col = VecVint::new();
/// col.push(42, &mut arena);
/// col.push(12345, &mut arena);
/// assert_eq!(col.to_vec(&arena), vec![42, 12345]);
/// ```
#[derive(Debug)]
pub struct VecVint {
    eull: ExpUnrolledLinkedList,
}

impl Default for VecVint {
    fn default() -> Self {
        Self::new()
    }
}

impl VecVint {
    /// Creates a new, empty `VecVint`.
    pub fn new() -> Self {
        Self {
            eull: ExpUnrolledLinkedList::default(),
        }
    }

    /// Creates a `VecVint` containing `count` copies of `val`.
    pub fn from_elem(val: u32, count: usize, arena: &mut MemoryArena) -> Self {
        let mut col = Self::new();
        for _ in 0..count {
            col.push(val, arena);
        }
        col
    }

    /// Appends a `u32` value, encoding it as a vint.
    #[inline]
    pub fn push(&mut self, val: u32, arena: &mut MemoryArena) {
        self.eull.writer(arena).write_u32_vint(val);
    }

    /// Appends all values from `other` into `self`.
    #[inline]
    pub fn extend(&mut self, other: &VecVint, arena: &mut MemoryArena) {
        let vals: Vec<u32> = other.to_vec(arena);
        for val in vals {
            self.push(val, arena);
        }
    }

    /// Reads the raw vint-encoded bytes into `buffer`.
    ///
    /// Use with [`VInt32Reader`] to iterate over the decoded values:
    /// ```ignore
    /// let mut buf = Vec::new();
    /// col.read_to_end(&arena, &mut buf);
    /// for val in VInt32Reader::new(&buf) {
    ///     // ...
    /// }
    /// ```
    pub fn read_to_end(&self, arena: &MemoryArena, buffer: &mut Vec<u8>) {
        self.eull.read_to_end(arena, buffer);
    }

    /// Collects all values into a `Vec<u32>`.
    pub fn to_vec(&self, arena: &MemoryArena) -> Vec<u32> {
        let mut buffer = Vec::new();
        self.read_to_end(arena, &mut buffer);
        VInt32Reader::new(&buffer).collect()
    }

    /// Retains only the values at positions where `keep(index)` returns `true`.
    ///
    /// This rebuilds the `VecVint` from scratch, discarding filtered values.
    pub fn retain_pos<F: FnMut(usize) -> bool>(&mut self, mut keep: F, arena: &mut MemoryArena) {
        let old_vals: Vec<u32> = self.to_vec(arena);
        *self = Self::new();
        for (idx, val) in old_vals.into_iter().enumerate() {
            if keep(idx) {
                self.push(val, arena);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use common::{VInt32Reader, read_u32_vint_no_advance, serialize_vint_u32};

    use super::*;

    #[test]
    fn roundtrip_basic() {
        let mut arena = MemoryArena::default();
        let mut col = VecVint::new();
        col.push(0, &mut arena);
        col.push(127, &mut arena);
        col.push(128, &mut arena);
        col.push(16383, &mut arena);
        col.push(16384, &mut arena);
        col.push(u32::MAX, &mut arena);
        let vals: Vec<u32> = col.to_vec(&arena);
        assert_eq!(vals, vec![0, 127, 128, 16383, 16384, u32::MAX]);
    }

    #[test]
    fn roundtrip_single_values() {
        for &val in &[0u32, 1, 127, 128, 16383, 16384, u32::MAX] {
            let mut buf = [0u8; 8];
            let encoded = serialize_vint_u32(val, &mut buf);
            let (decoded, consumed) = read_u32_vint_no_advance(encoded);
            assert_eq!(decoded, val);
            assert_eq!(consumed, encoded.len());
        }
    }

    #[test]
    fn roundtrip_many() {
        let mut arena = MemoryArena::default();
        let mut col = VecVint::new();
        let vals: Vec<u32> = (0..1024).collect();
        for &v in &vals {
            col.push(v, &mut arena);
        }
        assert_eq!(col.to_vec(&arena), vals);
    }

    #[test]
    fn test_empty() {
        let arena = MemoryArena::default();
        let col = VecVint::new();
        assert_eq!(col.to_vec(&arena), Vec::<u32>::new());
    }

    #[test]
    fn test_from_elem() {
        let mut arena = MemoryArena::default();
        let col = VecVint::from_elem(42, 5, &mut arena);
        assert_eq!(col.to_vec(&arena), vec![42, 42, 42, 42, 42]);
    }

    #[test]
    fn test_extend() {
        let mut arena = MemoryArena::default();
        let mut col1 = VecVint::new();
        col1.push(1, &mut arena);
        col1.push(2, &mut arena);
        let mut col2 = VecVint::new();
        col2.push(3, &mut arena);
        col2.push(4, &mut arena);
        col1.extend(&col2, &mut arena);
        assert_eq!(col1.to_vec(&arena), vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_retain_pos() {
        let mut arena = MemoryArena::default();
        let mut col = VecVint::new();
        for v in 0..10u32 {
            col.push(v, &mut arena);
        }
        col.retain_pos(|idx| idx % 2 == 0, &mut arena);
        assert_eq!(col.to_vec(&arena), vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn test_read_to_end_with_vint32reader() {
        let mut arena = MemoryArena::default();
        let mut col = VecVint::new();
        col.push(1, &mut arena);
        col.push(1000, &mut arena);
        col.push(u32::MAX, &mut arena);
        let mut buf = Vec::new();
        col.read_to_end(&arena, &mut buf);
        let vals: Vec<u32> = VInt32Reader::new(&buf).collect();
        assert_eq!(vals, vec![1, 1000, u32::MAX]);
    }
}
