pub(crate) const STRING_ARENA_CAPACITY: usize = 262_144;

/// Fixed-capacity storage for strings constructed while evaluating an expression.
pub(crate) struct StringArena {
    buffer: Box<[u8; STRING_ARENA_CAPACITY]>,
    cursor: usize,
}

impl StringArena {
    pub(crate) fn new() -> Self {
        let buffer = vec![0; STRING_ARENA_CAPACITY].into_boxed_slice();
        let buffer = buffer
            .try_into()
            .unwrap_or_else(|_| unreachable!("the arena buffer has the requested capacity"));
        Self { buffer, cursor: 0 }
    }

    pub(crate) fn clear(&mut self) {
        self.cursor = 0;
    }

    /// Reserves `len` contiguous bytes without growing the backing allocation.
    pub(crate) fn allocate(&mut self, len: usize) -> Option<*mut u8> {
        let end = self.cursor.checked_add(len)?;
        if end > STRING_ARENA_CAPACITY {
            return None;
        }
        // SAFETY: `cursor <= end <= STRING_ARENA_CAPACITY`.
        let allocation = unsafe { self.buffer.as_mut_ptr().add(self.cursor) };
        self.cursor = end;
        Some(allocation)
    }

    #[cfg(test)]
    pub(crate) fn used_bytes(&self) -> usize {
        self.cursor
    }
}

impl Default for StringArena {
    fn default() -> Self {
        Self::new()
    }
}
