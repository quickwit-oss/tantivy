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

impl<F, T> Iterable<T> for F
where F: Fn() -> Box<dyn Iterator<Item = T>>
{
    fn boxed_iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        self()
    }
}

pub fn map_iterable<U, V, F, I>(
    original_iterable: impl Fn() -> I,
    transform: F,
) -> impl Fn() -> std::iter::Map<I, F>
where
    F: Fn(U) -> V + Clone,
    I: Iterator<Item = U>,
{
    move || original_iterable().map(transform.clone())
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
