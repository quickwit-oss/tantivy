use std::io;

/// A codec makes it possible to serialize a set of
/// elements, and open the resulting Set representation.
pub trait SetCodec {
    type Item: Copy + TryFrom<usize> + Eq + std::hash::Hash + std::fmt::Debug;
    type Reader<'a>: Set<Self::Item>;

    /// Serializes a set of unique sorted u16 elements.
    ///
    /// May panic if the elements are not sorted.
    fn serialize(els: impl Iterator<Item = Self::Item>, wrt: impl io::Write) -> io::Result<()>;
    fn open<'a>(data: &'a [u8]) -> Self::Reader<'a>;
}

pub trait Set<T> {
    /// Returns true if the elements is contained in the Set
    fn contains(&self, el: T) -> bool;

    /// If the set contains `el` returns its position in the sortd set of elements.
    /// If the set does not contain the element, it returns `None`.
    fn rank_if_exists(&self, el: T) -> Option<T>;

    /// Return the rank-th value stored in this bitmap.
    ///
    /// # Panics
    ///
    /// May panic if rank is greater than the number of elements in the Set.
    fn select(&self, rank: T) -> T;

    /// Batch version of select.
    /// `ranks` is assumed to be sorted.
    ///
    /// # Panics
    ///
    /// May panic if rank is greater than the number of elements in the Set.
    fn select_batch(&self, ranks: &[T], outputs: &mut [T]);
}
