//! KD-tree over boundary cell midpoints for nearest-neighbor queries during
//! distance search. Built once from boundary cells, queried per sweep cell.

/// A node in the kd-tree. Holds the XYZ center point of a boundary cell
/// and the query edge indices assigned to that cell.
pub struct BoundaryNode {
    pub point: [f64; 3],
    pub edge_indices: Vec<u32>,
}

/// KD-tree over boundary cell centers. The tree structure is implicit in the
/// Vec ordering. Node at slice midpoint is the root, left half is the left
/// subtree, right half is the right subtree. Axes cycle X, Y, Z.
pub struct BoundaryTree {
    nodes: Vec<BoundaryNode>,
}

impl BoundaryTree {
    pub fn build(mut nodes: Vec<BoundaryNode>) -> Self {
        Self::partition(&mut nodes, 0);
        BoundaryTree { nodes }
    }

    fn partition(nodes: &mut [BoundaryNode], depth: usize) {
        if nodes.len() <= 1 {
            return;
        }
        let axis = depth % 3;
        let mid = nodes.len() / 2;
        nodes.select_nth_unstable_by(mid, |a, b| {
            a.point[axis]
                .partial_cmp(&b.point[axis])
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Self::partition(&mut nodes[..mid], depth + 1);
        Self::partition(&mut nodes[mid + 1..], depth + 1);
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Find boundary nodes within max_chord_angle of the query point.
    /// Calls the visitor with each matching node's edge indices.
    /// The visitor returns true to continue, false to stop early.
    pub fn search_within(
        &self,
        query: &[f64; 3],
        max_chord_angle: f64,
        visitor: &mut impl FnMut(&[u32]) -> bool,
    ) {
        Self::search_slice(&self.nodes, query, max_chord_angle, 0, visitor);
    }

    fn search_slice(
        nodes: &[BoundaryNode],
        query: &[f64; 3],
        max_chord_angle: f64,
        depth: usize,
        visitor: &mut impl FnMut(&[u32]) -> bool,
    ) -> bool {
        if nodes.is_empty() {
            return true;
        }
        let mid = nodes.len() / 2;
        let node = &nodes[mid];
        let axis = depth % 3;

        let chord = chord_angle(query, &node.point);
        if chord <= max_chord_angle {
            if !visitor(&node.edge_indices) {
                return false;
            }
        }

        let diff = query[axis] - node.point[axis];
        let (first, second) = if diff < 0.0 {
            (&nodes[..mid], &nodes[mid + 1..])
        } else {
            (&nodes[mid + 1..], &nodes[..mid])
        };

        if !Self::search_slice(first, query, max_chord_angle, depth + 1, visitor) {
            return false;
        }

        if diff * diff <= max_chord_angle {
            if !Self::search_slice(second, query, max_chord_angle, depth + 1, visitor) {
                return false;
            }
        }

        true
    }

    /// Find the nearest boundary node to the query point. Returns the chord
    /// angle and a reference to the node, or None if the tree is empty.
    pub fn nearest(&self, query: &[f64; 3]) -> Option<(f64, &BoundaryNode)> {
        let mut best_dist = f64::INFINITY;
        let mut best_node: Option<&BoundaryNode> = None;
        Self::nearest_slice(&self.nodes, query, 0, &mut best_dist, &mut best_node);
        best_node.map(|n| (best_dist, n))
    }

    fn nearest_slice<'a>(
        nodes: &'a [BoundaryNode],
        query: &[f64; 3],
        depth: usize,
        best_dist: &mut f64,
        best_node: &mut Option<&'a BoundaryNode>,
    ) {
        if nodes.is_empty() {
            return;
        }
        let mid = nodes.len() / 2;
        let node = &nodes[mid];
        let axis = depth % 3;

        let chord = chord_angle(query, &node.point);
        if chord < *best_dist {
            *best_dist = chord;
            *best_node = Some(node);
        }

        let diff = query[axis] - node.point[axis];
        let (first, second) = if diff < 0.0 {
            (&nodes[..mid], &nodes[mid + 1..])
        } else {
            (&nodes[mid + 1..], &nodes[..mid])
        };

        Self::nearest_slice(first, query, depth + 1, best_dist, best_node);

        if diff * diff < *best_dist {
            Self::nearest_slice(second, query, depth + 1, best_dist, best_node);
        }
    }
}

#[inline]
fn chord_angle(a: &[f64; 3], b: &[f64; 3]) -> f64 {
    let dot = a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
    2.0 - 2.0 * dot
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(x: f64, y: f64, z: f64) -> BoundaryNode {
        let len = (x * x + y * y + z * z).sqrt();
        BoundaryNode {
            point: [x / len, y / len, z / len],
            edge_indices: vec![],
        }
    }

    #[test]
    fn nearest_finds_closest() {
        let nodes = vec![
            node(1.0, 0.0, 0.0),
            node(0.0, 1.0, 0.0),
            node(0.0, 0.0, 1.0),
            node(1.0, 1.0, 0.0),
        ];
        let tree = BoundaryTree::build(nodes);
        let query = [1.0, 0.0, 0.0];
        let (dist, nearest) = tree.nearest(&query).unwrap();
        assert!(dist < 0.001);
        assert!((nearest.point[0] - 1.0).abs() < 0.001);
    }

    #[test]
    fn search_within_finds_all() {
        let nodes = vec![
            node(1.0, 0.0, 0.0),
            node(0.0, 1.0, 0.0),
            node(0.0, 0.0, 1.0),
        ];
        let tree = BoundaryTree::build(nodes);
        let query = [1.0, 0.0, 0.0];
        let mut count = 0;
        tree.search_within(&query, 4.0, &mut |_| {
            count += 1;
            true
        });
        assert_eq!(count, 3);
    }

    #[test]
    fn search_within_prunes() {
        let nodes = vec![node(1.0, 0.0, 0.0), node(-1.0, 0.0, 0.0)];
        let tree = BoundaryTree::build(nodes);
        let query = [1.0, 0.0, 0.0];
        let mut count = 0;
        tree.search_within(&query, 0.1, &mut |_| {
            count += 1;
            true
        });
        assert_eq!(count, 1);
    }

    #[test]
    fn search_within_early_termination() {
        let nodes = vec![
            node(1.0, 0.0, 0.0),
            node(0.0, 1.0, 0.0),
            node(0.0, 0.0, 1.0),
        ];
        let tree = BoundaryTree::build(nodes);
        let query = [1.0, 0.0, 0.0];
        let mut count = 0;
        tree.search_within(&query, 4.0, &mut |_| {
            count += 1;
            false
        });
        assert_eq!(count, 1);
    }

    #[test]
    fn empty_tree() {
        let tree = BoundaryTree::build(vec![]);
        assert!(tree.nearest(&[1.0, 0.0, 0.0]).is_none());
        let mut count = 0;
        tree.search_within(&[1.0, 0.0, 0.0], 4.0, &mut |_| {
            count += 1;
            true
        });
        assert_eq!(count, 0);
    }
}
