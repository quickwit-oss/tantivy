use std::marker::PhantomData;
use std::ops::Range;

pub trait Iterable<T = u64> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = T> + '_>;
}

struct Mapped<U, Original, Transform> {
    original_iterable: Original,
    transform: Transform,
    input_type: PhantomData<U>,
}

impl<U, V, Original, Transform> Iterable<V> for Mapped<U, Original, Transform>
where
    Original: Iterable<U>,
    Transform: Fn(U) -> V,
{
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = V> + '_> {
        Box::new(self.original_iterable.boxed_iter().map(&self.transform))
    }
}

impl<U> Iterable<U> for &dyn Iterable<U> {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = U> + '_> {
        (*self).boxed_iter()
    }
}

pub fn map_iterable<U, V>(
    original_iterable: impl Iterable<U>,
    transform: impl Fn(U) -> V,
) -> impl Iterable<V> {
    Mapped {
        original_iterable,
        transform,
        input_type: PhantomData::<U>::default(),
    }
}

impl<'a, T: Copy> Iterable<T> for &'a [T] {
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        Box::new(self.iter().copied())
    }
}

impl<T: Copy> Iterable<T> for Range<T>
where Range<T>: Iterator<Item = T>
{
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        Box::new(self.clone())
    }
}
