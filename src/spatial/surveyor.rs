//! Dimension analysis for block kd-tree construction.
//!
//! Surveys triangle collections to identify the optimal partition dimension by tracking min/max
//! spreads and common byte prefixes across the four bounding box dimensions. The dimension with
//! maximum spread is selected for partitioning, and its prefix is used to optimize radix selection
//! by skipping shared leading bytes.
use crate::spatial::triangle::Triangle;

#[derive(Clone, Copy)]
struct Spread {
    min: i32,
    max: i32,
}

impl Spread {
    fn survey(&mut self, value: i32) {
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
    }
    fn spread(&self) -> i32 {
        self.max - self.min
    }
}

impl Default for Spread {
    fn default() -> Self {
        Spread {
            min: i32::MAX,
            max: i32::MIN,
        }
    }
}

#[derive(Debug)]
struct Prefix {
    words: Vec<u8>,
}

impl Prefix {
    fn prime(value: i32) -> Self {
        Prefix {
            words: value.to_be_bytes().to_vec(),
        }
    }
    fn survey(&mut self, value: i32) {
        let bytes = value.to_be_bytes();
        while !bytes.starts_with(&self.words) {
            self.words.pop();
        }
    }
}

/// Tracks dimension statistics for block kd-tree partitioning decisions.
///
/// Accumulates min/max spreads and common byte prefixes across the four bounding box dimensions
/// `(min_y, min_x, max_y, max_x)` of a triangle collection. Used during tree construction to
/// select the optimal partition dimension (maximum spread) and identify shared prefixes for radix
/// selection optimization.
pub struct Surveyor {
    spreads: [Spread; 4],
    prefixes: [Prefix; 4],
}

impl Surveyor {
    /// Creates a new surveyor using an initial triangle to establish byte prefixes.
    ///
    /// Prefixes require an initial value to begin comparison, so the first triangle's bounding box
    /// dimensions seed each prefix. Spreads initialize to defaults and will be updated as
    /// triangles are surveyed.
    pub fn new(triangle: &Triangle) -> Self {
        let mut prefixes = Vec::new();
        for &value in triangle.bbox() {
            prefixes.push(Prefix::prime(value));
        }
        Surveyor {
            spreads: [Spread::default(); 4],
            prefixes: prefixes.try_into().unwrap(),
        }
    }

    /// Updates dimension statistics with values from another triangle.
    ///
    /// Expands the min/max spread for each bounding box dimension and shrinks prefixes to only the
    /// bytes shared across all triangles surveyed so far.
    pub fn survey(&mut self, triangle: &Triangle) {
        for (i, &value) in triangle.bbox().iter().enumerate() {
            self.spreads[i].survey(value);
            self.prefixes[i].survey(value);
        }
    }

    /// Returns the dimension with maximum spread and its common byte prefix.
    ///
    /// Selects the optimal bounding box dimension for partitioning by finding which has the
    /// largest range (max - min). Returns both the dimension index and its prefix for use in radix
    /// selection.
    pub fn dimension(&self) -> (usize, &Vec<u8>) {
        let mut dimension = 0;
        let mut max_spread = self.spreads[0].spread();
        for (i, spread) in self.spreads.iter().enumerate().skip(1) {
            let current_spread = spread.spread();
            if current_spread > max_spread {
                dimension = i;
                max_spread = current_spread;
            }
        }
        (dimension, &self.prefixes[dimension].words)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::spatial::triangle::Triangle;

    #[test]
    fn survey_triangles() {
        let triangles = [
            Triangle::new(1, [0, 0, 8, 1, 10, 3], [false, false, false]),
            Triangle::new(1, [0, 0, 0, 11, 9, 0], [false, false, false]),
            Triangle::new(1, [0, 0, 5, 4, 5, 0], [false, false, false]),
        ];
        let mut surveyor = Surveyor::new(&triangles[0]);
        for triangle in &triangles {
            surveyor.survey(triangle);
        }
        let (dimension, prefix) = surveyor.dimension();
        assert_eq!(dimension, 3);
        assert_eq!(prefix.len(), 3);
    }
}
