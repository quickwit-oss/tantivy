struct FlatMapWithgBuffer<T, F, Iter> {
    buffer: Vec<T>,
    fill_buffer: F,
    underlying_it: Iter,
}

impl<T, F, Iter, I> Iterator for FlatMapWithgBuffer<T, F, Iter>
where
    Iter: Iterator<Item = I>,
    F: Fn(I, &mut Vec<T>),
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        while self.buffer.is_empty() {
            let next_el = self.underlying_it.next()?;
            (self.fill_buffer)(next_el, &mut self.buffer);
            // We will pop elements, so we reverse the buffer first.
            self.buffer.reverse();
        }
        self.buffer.pop()
    }
}

/// FUnction similar to `flat_map`, but the generating function fills a buffer
/// instead of returning an Iterator.
pub fn flat_map_with_buffer<T, F, I, Iter>(
    underlying_it: Iter,
    fill_buffer: F,
) -> impl Iterator<Item = T>
where
    F: Fn(I, &mut Vec<T>),
    Iter: Iterator<Item = I>,
{
    FlatMapWithgBuffer {
        buffer: Vec::with_capacity(10),
        fill_buffer,
        underlying_it,
    }
}

#[cfg(test)]
mod tests {
    use super::flat_map_with_buffer;

    #[test]
    fn test_flat_map_with_buffer_empty() {
        let vals: Vec<usize> = flat_map_with_buffer(
            std::iter::empty::<usize>(),
            |_val: usize, _buffer: &mut Vec<usize>| {},
        )
        .collect();
        assert!(vals.is_empty());
    }

    #[test]
    fn test_flat_map_with_buffer_simple() {
        let vals: Vec<usize> = flat_map_with_buffer(1..5, |val: usize, buffer: &mut Vec<usize>| {
            buffer.extend(0..val)
        })
        .collect();
        assert_eq!(&[0, 0, 1, 0, 1, 2, 0, 1, 2, 3], &vals[..]);
    }

    #[test]
    fn test_flat_map_filling_no_elements_does_not_stop_iterator() {
        let vals: Vec<usize> = flat_map_with_buffer(
            [2, 0, 0, 3].into_iter(),
            |val: usize, buffer: &mut Vec<usize>| buffer.extend(0..val),
        )
        .collect();
        assert_eq!(&[0, 1, 0, 1, 2], &vals[..]);
    }
}
