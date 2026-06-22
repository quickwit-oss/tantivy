//! Trinary-projection-tree (TPT) partitioning, the candidate generator that
//! seeds the initial KNN graph before
//! [`refine`](super::RelativeNeighborhoodGraph::refine).
//!
//! A TPT recursively splits a slice of node ids along a sparse random
//! hyperplane — only the few highest-variance dimensions carry a weight, the
//! rest are implicitly zero (the "trinary" sparsity). Each split direction is
//! *fit on a sample* of the slice but *applied to the whole slice*, so the fit
//! cost is independent of slice size. Recursion bottoms out at
//! [`leaf_size`](TPTreeConfig::leaf_size); the leaves are small contiguous index
//! ranges the builder brute-forces into exact KNN edges.

use std::ops::Range;

use super::graph::NodeId;

/// Tuning knobs for [`TPTree`].
#[derive(Clone, Copy, Debug)]
pub struct TPTreeConfig {
    /// Max points in a leaf before recursion stops. The per-leaf exact KNN is
    /// quadratic in this, so it trades build cost for init-graph recall.
    pub leaf_size: usize,
    /// How many points of each slice to sample when fitting a split
    /// direction. The split is fit on the sample, then applied to the whole
    /// slice.
    pub samples: usize,
    /// How many of the highest-variance dimensions carry a nonzero projection
    /// weight; every other dimension is weighted zero.
    pub top_dims: usize,
    /// Random unit-norm projections tried per split; the one that spreads the
    /// sample most (max projected variance) wins, with the single
    /// highest-variance axis as the baseline.
    pub iterations: usize,
}

impl Default for TPTreeConfig {
    fn default() -> Self {
        TPTreeConfig {
            leaf_size: 2000,
            samples: 1000,
            top_dims: 5,
            iterations: 100,
        }
    }
}

/// A single TPT over a flat, `dim`-strided vector arena.
pub struct TPTree<'a> {
    vectors: &'a [f32],
    dim: usize,
    config: TPTreeConfig,
    rng: fastrand::Rng,
}

impl<'a> TPTree<'a> {
    /// `vectors` is the flat `dim`-strided buffer (its length must be a
    /// multiple of `dim`).
    pub fn new(config: TPTreeConfig, dim: usize, vectors: &'a [f32]) -> Self {
        debug_assert!(dim > 0, "dim must be non-zero");
        debug_assert_eq!(vectors.len() % dim, 0, "arena not a multiple of dim");
        TPTree {
            vectors,
            dim,
            config,
            rng: fastrand::Rng::new(),
        }
    }

    /// Partitions `indices` in place and returns the leaf ranges into it.
    /// Each returned range is a contiguous run of `indices` holding one
    /// leaf's node ids (at most [`leaf_size`](TPTreeConfig::leaf_size)).
    pub fn partition(&mut self, indices: &mut [NodeId]) -> Vec<Range<usize>> {
        let mut leaves = Vec::new();
        if !indices.is_empty() {
            self.subdivide(indices, 0, &mut leaves);
        }
        leaves
    }

    #[inline]
    fn coord(&self, node: NodeId, d: usize) -> f32 {
        self.vectors[node as usize * self.dim + d]
    }

    /// Recursively splits `indices` (whose first element sits at absolute
    /// `offset` in the original array), appending leaf ranges to `leaves`.
    fn subdivide(&mut self, indices: &mut [NodeId], offset: usize, leaves: &mut Vec<Range<usize>>) {
        if indices.len() <= self.config.leaf_size {
            leaves.push(offset..offset + indices.len());
            return;
        }
        let split = self.choose_split(indices);
        let (left, right) = indices.split_at_mut(split);
        self.subdivide(left, offset, leaves);
        self.subdivide(right, offset + split, leaves);
    }

    /// Picks a split hyperplane for `indices` and partitions the slice around
    /// it in place, returning the boundary `split` (left = `[0, split)`,
    /// right = `[split, len)`). The boundary is always in `1..len`, so each
    /// child is strictly smaller and the recursion terminates.
    fn choose_split(&mut self, indices: &mut [NodeId]) -> usize {
        let n = indices.len();
        let dim = self.dim;
        let sample = n.min(self.config.samples);
        let top_dims = self.config.top_dims.min(dim).max(1);

        let mut mean = vec![0.0f32; dim];
        for &node in &indices[..sample] {
            for (d, m) in mean.iter_mut().enumerate() {
                *m += self.coord(node, d);
            }
        }
        for m in &mut mean {
            *m /= sample as f32;
        }

        // Sum of squared deviations; comparisons only, so never normalized.
        let mut variance = vec![0.0f32; dim];
        for &node in &indices[..sample] {
            for (d, var) in variance.iter_mut().enumerate() {
                let diff = self.coord(node, d) - mean[d];
                *var += diff * diff;
            }
        }

        let mut dims: Vec<usize> = (0..dim).collect();
        dims.sort_unstable_by(|&a, &b| variance[b].total_cmp(&variance[a]));
        dims.truncate(top_dims);

        // Baseline: project onto the single highest-variance axis.
        let mut best_weight = vec![0.0f32; top_dims];
        best_weight[0] = 1.0;
        let mut best_mean = mean[dims[0]];
        let mut best_var = variance[dims[0]];

        // Random unit-norm projections; keep whichever spreads the sample most.
        let mut proj = vec![0.0f32; sample];
        let mut weight = vec![0.0f32; top_dims];
        for _ in 0..self.config.iterations {
            let mut norm = 0.0f32;
            for w in &mut weight {
                *w = self.rng.f32() * 2.0 - 1.0; // [-1, 1)
                norm += *w * *w;
            }
            let norm = norm.sqrt();
            if norm == 0.0 {
                continue;
            }
            for w in &mut weight {
                *w /= norm;
            }

            let mut m = 0.0f32;
            for (slot, &node) in proj.iter_mut().zip(&indices[..sample]) {
                let mut v = 0.0f32;
                for (k, &d) in dims.iter().enumerate() {
                    v += weight[k] * self.coord(node, d);
                }
                *slot = v;
                m += v;
            }
            m /= sample as f32;

            let mut var = 0.0f32;
            for &p in &proj {
                let diff = p - m;
                var += diff * diff;
            }
            if var > best_var {
                best_var = var;
                best_mean = m;
                best_weight.copy_from_slice(&weight);
            }
        }

        // Partition the whole slice (not just the sample) around the chosen
        // hyperplane. Signed so `j` can cross below zero.
        let mut i: isize = 0;
        let mut j: isize = n as isize - 1;
        while i <= j {
            let node = indices[i as usize];
            let mut val = 0.0f32;
            for (k, &d) in dims.iter().enumerate() {
                val += best_weight[k] * self.coord(node, d);
            }
            if val < best_mean {
                i += 1;
            } else {
                indices.swap(i as usize, j as usize);
                j -= 1;
            }
        }

        // Everything landed on one side (e.g. identical vectors): fall back to
        // a median split so the recursion still shrinks.
        let split = i as usize;
        if split == 0 || split == n {
            n / 2
        } else {
            split
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arena(pts: &[[f32; 3]]) -> Vec<f32> {
        pts.iter().flatten().copied().collect()
    }

    #[test]
    fn partition_separates_two_far_clusters() {
        // Two clusters far apart in the x–z plane (y is low-variance noise).
        // One split must cleanly separate them: max-variance projection puts
        // the threshold in the gap, so no leaf mixes the two.
        let pts = [
            [1., 5., 1.],
            [2., 5., 0.],
            [0., 4., 2.],
            [1., 6., 1.], // cluster A: ids 0..4
            [9., 5., 10.],
            [10., 5., 9.],
            [8., 4., 11.],
            [9., 6., 10.], // cluster B: ids 4..8
        ];
        let v = arena(&pts);
        let config = TPTreeConfig {
            leaf_size: 4,
            samples: 8,
            top_dims: 2,
            iterations: 100,
        };
        let mut tpt = TPTree::new(config, 3, &v);
        let mut indices: Vec<NodeId> = (0..8).collect();

        let leaves = tpt.partition(&mut indices);

        assert_eq!(leaves.len(), 2, "8 points / leaf_size 4 → one split");
        for leaf in leaves {
            let ids = &indices[leaf];
            let all_a = ids.iter().all(|&id| id < 4);
            let all_b = ids.iter().all(|&id| id >= 4);
            assert!(all_a || all_b, "leaf mixes clusters: {ids:?}");
        }
    }

    #[test]
    fn partition_terminates_on_identical_vectors() {
        // Every vector identical → no projection separates anything. The
        // median-split fallback must still drive recursion to leaves rather
        // than loop forever.
        let v = vec![0.0f32; 3 * 8];
        let config = TPTreeConfig {
            leaf_size: 2,
            samples: 8,
            top_dims: 2,
            iterations: 8,
        };
        let mut tpt = TPTree::new(config, 3, &v);
        let mut indices: Vec<NodeId> = (0..8).collect();

        let leaves = tpt.partition(&mut indices);

        assert!(leaves.iter().all(|l| l.len() <= 2));
        assert_eq!(leaves.iter().map(|l| l.len()).sum::<usize>(), 8);
    }
}
