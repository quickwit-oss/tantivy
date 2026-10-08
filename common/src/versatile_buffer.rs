use std::fmt::Debug;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};

/// A reusable allocation that can be lent as an empty `Vec<T>` for any `T` with the same size
/// and alignment as `A`.
///
/// Example: `VersatileBuffer<u64>` can lend `Vec<u64>`, `Vec<i64>`, `Vec<f64>`, ...
///
/// Hidden contract: `buffer.len()` is always 0. No `A` value is ever built: `A` only sets the
/// size and alignment of the allocation. Only the allocation is reused.
pub struct VersatileBuffer<A> {
    buffer: Vec<A>,
}

impl<A> Default for VersatileBuffer<A> {
    #[inline(always)]
    fn default() -> Self {
        VersatileBuffer { buffer: Vec::new() }
    }
}

impl<A> VersatileBuffer<A> {
    #[inline(always)]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    pub fn with_capacity(capacity: usize) -> Self {
        VersatileBuffer {
            buffer: Vec::with_capacity(capacity),
        }
    }

    /// Lends the allocation as an empty `Vec<T>`.
    ///
    /// The allocation goes back to `self` when the returned guard is dropped.
    /// If the guard is leaked (e.g. `mem::forget`), the allocation is leaked too and `self`
    /// starts again from an empty `Vec`.
    #[inline(always)]
    pub fn borrow<T>(&mut self) -> ClearOnDrop<'_, T, A> {
        const {
            assert!(std::mem::size_of::<T>() == std::mem::size_of::<A>());
            assert!(std::mem::align_of::<T>() == std::mem::align_of::<A>());
        }
        let mut storage = ManuallyDrop::new(std::mem::take(&mut self.buffer));
        let capacity = storage.capacity();
        let ptr = storage.as_mut_ptr().cast::<T>();
        // SAFETY: `ptr` and `capacity` come from a `Vec<A>` we own. `T` and `A` have the same
        // size and alignment (checked above), so the allocation is valid for a `Vec<T>` with the
        // same capacity. Length is 0, so nothing is read.
        let values: Vec<T> = unsafe { Vec::from_raw_parts(ptr, 0, capacity) };
        ClearOnDrop {
            home: &mut self.buffer,
            values,
        }
    }
}

/// An empty `Vec<T>` lent by a `VersatileBuffer`.
///
/// On drop, the values are dropped and the allocation goes back to the `VersatileBuffer`.
/// This also happens if the code using the vec panics.
pub struct ClearOnDrop<'a, T, A> {
    home: &'a mut Vec<A>,
    values: Vec<T>,
}

impl<T: Debug, A> Debug for ClearOnDrop<'_, T, A> {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.values.fmt(f)
    }
}

impl<T, A> Drop for ClearOnDrop<'_, T, A> {
    #[inline(always)]
    fn drop(&mut self) {
        self.values.clear();
        let mut values = ManuallyDrop::new(std::mem::take(&mut self.values));
        let capacity = values.capacity();
        let ptr = values.as_mut_ptr().cast::<A>();
        // SAFETY: same layout argument as in `VersatileBuffer::borrow`. `values` may have been
        // reallocated by `push`, but always with `T`'s layout, which equals `A`'s.
        let buffer: Vec<A> = unsafe { Vec::from_raw_parts(ptr, 0, capacity) };
        // `home` still holds the empty `Vec` left by `mem::take` in `borrow`. Forgetting it
        // instead of dropping it removes a dead dealloc branch, which the compiler cannot prove
        // dead on its own.
        std::mem::forget(std::mem::replace(self.home, buffer));
    }
}

impl<T, A> Deref for ClearOnDrop<'_, T, A> {
    type Target = Vec<T>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl<T, A> DerefMut for ClearOnDrop<'_, T, A> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use crate::versatile_buffer::{ClearOnDrop, VersatileBuffer};

    #[test]
    fn test_versatile_borrowable_buffer() {
        let mut my_buff: VersatileBuffer<u64> = VersatileBuffer::default();

        {
            let mut arr: ClearOnDrop<u64, u64> = my_buff.borrow();
            arr.push(1u64);
            arr.push(2u64);
            assert_eq!(&arr[..], &[1u64, 2u64]);
        }
        {
            let arr: ClearOnDrop<i64, u64> = my_buff.borrow();
            assert!(arr.is_empty());
        }
    }

    #[test]
    fn test_versatile_buffer_keeps_capacity() {
        let mut buffer: VersatileBuffer<u64> = VersatileBuffer::with_capacity(16);
        {
            let mut values = buffer.borrow::<u64>();
            assert!(values.capacity() >= 16);
            values.extend(0..100u64);
        }
        let values = buffer.borrow::<f64>();
        assert!(values.is_empty());
        assert!(values.capacity() >= 100);
    }

    #[test]
    fn test_versatile_buffer_forgotten_guard() {
        let mut buffer: VersatileBuffer<u64> = VersatileBuffer::new();
        let mut values = buffer.borrow::<u64>();
        values.extend([1u64, 2u64, 3u64]);
        std::mem::forget(values);
        let values = buffer.borrow::<f64>();
        assert!(values.is_empty());
    }

    #[test]
    fn test_versatile_buffer_16_bytes() {
        let mut buffer: VersatileBuffer<(u64, u64)> = VersatileBuffer::new();
        {
            let mut values = buffer.borrow::<(u64, u32, u32)>();
            values.push((1, 2, 3));
            assert_eq!(&values[..], &[(1, 2, 3)]);
        }
        let values = buffer.borrow::<[u64; 2]>();
        assert!(values.is_empty());
    }

    struct DropCounter<'a>(&'a Cell<u32>);

    impl Drop for DropCounter<'_> {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[test]
    fn test_versatile_buffer_drops_values() {
        let drop_count = Cell::new(0u32);
        let mut buffer: VersatileBuffer<usize> = VersatileBuffer::new();
        {
            let mut values = buffer.borrow::<DropCounter>();
            for _ in 0..5 {
                values.push(DropCounter(&drop_count));
            }
            assert_eq!(drop_count.get(), 0);
        }
        assert_eq!(drop_count.get(), 5);
    }
}
