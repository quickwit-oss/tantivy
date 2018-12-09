/*!
Stores an increasing mapping from naturals to naturals.

`CharFilter`s may make the original text longer or shorter.
Token's offset need to refer to their offset in the original
text.

This struct is in charge of doing an efficient book-keeping
a the possible shift in offsets and provide a mapping
from the transformed text to the original text.

We define the inverse of an increasing mapping `f` as:
g(i) = max {j | f(j) <= i}
     != min {j | f(i) >= i}

Note that having a single definition has some bad side effects.
For instance, when trying to convert a segment of chars to
its offset in the original string, the reverse mapping may
return an empty string.

We could use a different definition of the reverse mapping
when computing the lower bound and the upper bound of the segment,
but then non-overlapping tokens could have overlapping origins.

# Example

```
forward mapping
[0,1,2,5,6,7]
Encoded sparsely as (3, 2)

reverse mapping
[0,1,2,2,2,3,4,5]
Encoded sparsely as [(3, -1), (4,-1), (5,-1)]
```


*/

/// Builds a reverse mapping using a sparse representation of the
/// forward mapping.
pub struct OffsetIncrementsBuilder {
    prev_layer: OffsetIncrementsReader,
    cumulated: isize,
    incs: Vec<(usize, isize)>,
}

impl OffsetIncrementsBuilder {
    /// We require
    /// - `from_offset + delta >= 0`
    /// There is no need to call this function if delta = 0.
    pub fn register_inc(&mut self, from_offset: usize, delta: isize) {
        debug_assert_ne!(delta, 0);
        debug_assert!(delta>=-1);
        if delta > 0 {
            let from_offset_isize = from_offset as isize;
            let to_offset = (from_offset_isize + self.cumulated) as usize;
            println!("{} -> [{}..{}[ ", from_offset-1, to_offset-1, (to_offset as isize + delta));
            for i in 0..delta as usize {
                self.cumulated += 1;
                self.incs.push((to_offset + i, -self.cumulated));
            }
//            self.cumulated += delta;
        } else {
            unimplemented!();
        }
    }


    fn build(self) -> OffsetIncrements {
        println!("incs {:?}", self.incs);
        OffsetIncrements {
            incs: self.incs
        }
    }

    pub fn new_layer(&self) {
        panic!("wer")
    }
}


#[derive(Default)]
pub struct OffsetIncrementsReader {
    shifts: Vec<(usize, isize)>,
    current_shift: isize,
    idx: usize,
}

impl OffsetIncrementsReader {
    fn new(shifts: Vec<(usize, isize)>) -> OffsetIncrementsReader {
        OffsetIncrementsReader {
            shifts,
            current_shift: 0,
            idx: 0,
        }
    }

    fn convert_offset(&mut self, target: usize) -> usize {
        while self.idx < self.shifts.len() {
            let (offset, shift) = self.shifts[self.idx];
            if offset > target {
                break;
            } else {
                self.current_shift = shift;
            }
            self.idx += 1;
        }
        return (self.current_shift + target as isize) as usize;
    }
}

pub struct OffsetIncrements {
    incs: Vec<(usize, isize)>
}

impl OffsetIncrements {
    pub fn builder() -> OffsetIncrementsBuilder {
        OffsetIncrementsBuilder {
            prev_layer: OffsetIncrementsReader::default(),
            cumulated: 0,
            incs: Vec::new(),
        }
    }

    pub fn reader(&self) -> OffsetIncrementsReader {
        println!("{:?}", self.incs);
        OffsetIncrementsReader::new(self.incs.clone()) // TODO Fixme, no clone
    }
}

#[cfg(test)]
mod tests {
    use super::OffsetIncrements;
    use super::OffsetIncrementsReader;


    #[test]
    fn test_offset_increment_reader_empty() {
        let mut reader = OffsetIncrementsReader::new(vec![]);
        for i in 0..3 {
            assert_eq!(reader.convert_offset(i), i);
        }
    }

    #[test]
    fn test_offset_increment_reader_step() {
        let mut reader = OffsetIncrementsReader::new(vec![(1, 1), (3, 3), (6, 2), (7, 1), (8, 0), (9, -1)]);
        assert_eq!(reader.convert_offset(0), 0);
        assert_eq!(reader.convert_offset(1), 2);
        assert_eq!(reader.convert_offset(2), 3);
        assert_eq!(reader.convert_offset(3), 6);
        assert_eq!(reader.convert_offset(4), 7);
        assert_eq!(reader.convert_offset(5), 8);
        assert_eq!(reader.convert_offset(6), 8);
        assert_eq!(reader.convert_offset(7), 8);
        assert_eq!(reader.convert_offset(8), 8);
        assert_eq!(reader.convert_offset(9), 8);
    }

    #[test]
    fn test_offset_increment_reader_step_neg() {
        let mut reader = OffsetIncrementsReader::new(vec![(1, -1), (2, -2), (3, -3)]);
        assert_eq!(reader.convert_offset(0), 0);
        assert_eq!(reader.convert_offset(1), 0);
        assert_eq!(reader.convert_offset(2), 0);
        assert_eq!(reader.convert_offset(3), 0);
        assert_eq!(reader.convert_offset(4), 1);
        assert_eq!(reader.convert_offset(5), 2);
        assert_eq!(reader.convert_offset(6), 3);
        assert_eq!(reader.convert_offset(7), 4);
    }


    fn aux_test_increment(increments: OffsetIncrements, expected: Vec<usize>) {
        let mut reader = increments.reader();
        println!("EXPECT - {:?}", expected);
        for (i, el) in expected.into_iter().enumerate() {
            println!("{}: {} got {}", i, el, reader.convert_offset(i));
            assert_eq!(reader.convert_offset(i), el);
        }
    }


    fn assert_is_increasing(v: &[usize]) {
        assert!(v.len() > 0);
        assert_eq!(v[0], 0);
        let mut prec = 0;
        for &val in &v[1..] {
            assert!(val >= prec);
            prec = val;
        }
    }

    fn is_inverse(fwd: &[usize], rev: &[usize]) {
        assert_is_increasing(fwd);
        assert_is_increasing(rev);
        println!("fwd {:?} rev {:?}", fwd, rev);
        for (i, &antecedant) in rev.iter().enumerate() {
            let expected = fwd
                .iter()
                .enumerate()
                .filter(|(_, v)| **v <= i)
                .map(|(ord, _)| ord)
                .last()
                .unwrap();
            println!("i {}", i);
            assert_eq!(expected, antecedant);
        }
    }

    fn is_reciprocal(left: &[usize], right: &[usize]) {
        is_inverse(left, right);
        is_inverse(right, left);
    }

    #[test]
    fn test_is_inverse() {
        is_reciprocal(&[0,1,1,1,2], &[0, 3, 4]);
    }

    #[test]
    fn test_offset_increments_builder() {
        {
            let mut offset_increment_builder = OffsetIncrements::builder();
            offset_increment_builder.register_inc(2, 1);
            // [0, 1, 3, 4, 5]
            aux_test_increment(offset_increment_builder.build(), vec![0,1,1,2,3,4,5]);
        }
        {
            let mut offset_increment_builder = OffsetIncrements::builder();
            offset_increment_builder.register_inc(3, 2);
            // [0, 1, 2, 4, 5, 6]
            aux_test_increment(offset_increment_builder.build(), vec![0,1,2,2,2,3,4,5]);
        }
        {
            let mut offset_increment_builder = OffsetIncrements::builder();
            // 0, 0, 1, 2, 2, 2
            offset_increment_builder.register_inc(1, 1);
            offset_increment_builder.register_inc(3, 3);
            aux_test_increment(offset_increment_builder.build(), vec![0,0,1,2,2,2,2,3,4]);
        }
    }
}