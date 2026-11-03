//! Closed source bounds for prepared edge lookup.

use super::operand::Edge;
use crate::spatial::r2rect::R2Rect;

#[derive(Default)]
pub(super) struct EdgeIndex {
    edges: Vec<usize>,
    nodes: Vec<Node>,
}

struct Node {
    bound: R2Rect,
    range: std::ops::Range<usize>,
    children: Option<(usize, usize)>,
}

impl EdgeIndex {
    pub fn new(edges: &[Edge]) -> Self {
        let mut index = Self {
            edges: (0..edges.len()).collect(),
            nodes: Vec::new(),
        };
        if !edges.is_empty() {
            index.build(edges, 0..edges.len());
        }
        index
    }

    fn build(&mut self, edges: &[Edge], range: std::ops::Range<usize>) -> usize {
        let bound = self.edges[range.clone()]
            .iter()
            .fold(R2Rect::empty(), |bound, &edge| {
                bound.union(&edges[edge].bound)
            });
        let node = self.nodes.len();
        self.nodes.push(Node {
            bound,
            range: range.clone(),
            children: None,
        });
        if range.len() > 8 {
            let axis = usize::from(bound[1].hi() - bound[1].lo() > bound[0].hi() - bound[0].lo());
            let middle = range.len() / 2;
            self.edges[range.clone()].select_nth_unstable_by(middle, |&a, &b| {
                let center =
                    |edge: usize| edges[edge].bound[axis].lo() + edges[edge].bound[axis].hi();
                center(a).total_cmp(&center(b)).then_with(|| a.cmp(&b))
            });
            let middle = range.start + middle;
            let left = self.build(edges, range.start..middle);
            let right = self.build(edges, middle..range.end);
            self.nodes[node].children = Some((left, right));
        }
        node
    }

    pub fn intersecting(&self, edges: &[Edge], bound: &R2Rect) -> Vec<usize> {
        let mut found = Vec::new();
        if !self.nodes.is_empty() {
            self.visit(0, edges, bound, &mut found);
        }
        found
    }

    fn visit(&self, node: usize, edges: &[Edge], bound: &R2Rect, found: &mut Vec<usize>) {
        let node = &self.nodes[node];
        if !node.bound.intersects(bound) {
            return;
        }
        if let Some((left, right)) = node.children {
            self.visit(left, edges, bound, found);
            self.visit(right, edges, bound, found);
        } else {
            found.extend(
                self.edges[node.range.clone()]
                    .iter()
                    .copied()
                    .filter(|&edge| edges[edge].bound.intersects(bound)),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spatial::geometry::Geometry;
    use crate::spatial::geometry_set::to_geometry_set;
    use crate::spatial::plane::operand::Operand;
    use crate::spatial::plane::Plane;

    #[test]
    fn test_closed_lookup_matches_exhaustive_edge_bounds() {
        let lines = (0..64)
            .map(|i| {
                let x = (i % 8) as f64;
                let y = (i / 8) as f64;
                vec![[x, y], [x + 0.5, y + 1.0]]
            })
            .collect();
        let set = to_geometry_set(&Geometry::<Plane>::MultiLineString(lines), 0);
        let operand = Operand::new(&set);
        for x in 0..17 {
            for y in 0..17 {
                let p = [x as f64 * 0.5, y as f64 * 0.5];
                for bound in [
                    R2Rect::from_point(p),
                    R2Rect::from_point_pair(p, [p[0] + 0.5, p[1] + 0.5]),
                ] {
                    let mut found = operand.index.intersecting(&operand.edges, &bound);
                    found.sort_unstable();
                    let expected: Vec<_> = operand
                        .edges
                        .iter()
                        .enumerate()
                        .filter_map(|(i, edge)| edge.bound.intersects(&bound).then_some(i))
                        .collect();
                    assert_eq!(found, expected);
                }
            }
        }
    }
}
