//! Cell discovery followed by a decided whole-value relation.

use common::BitSet;

use super::operand::Operand;
use super::relation::{evaluate, Predicate, PreparedOperand};
use super::source_bound;
use crate::spatial::cell_index_reader::CellIndexReader;
use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipper::Clipper;
use crate::spatial::edge_resolver::EdgeResolver;
use crate::spatial::geometry_set::{GeometryKind, GeometrySet};
use crate::spatial::r2rect::R2Rect;
use crate::spatial::shape_index::ShapeIndex;
use crate::spatial::surface::{cell_bound_uv, Surface};

#[cfg(test)]
#[path = "../tests/plane_relation_tests.rs"]
mod tests;

pub struct PlaneQuery {
    index: ShapeIndex,
    operand: PreparedOperand,
    bound: R2Rect,
    known_nonempty: bool,
}

fn source_bounds<S: Surface>(set: &GeometrySet<S>) -> R2Rect {
    let mut bound = R2Rect::empty();
    for point in set.members.iter().flat_map(|member| &member.vertices) {
        let point = point.as_ref();
        bound = bound.union(&R2Rect::from_point([point[0], point[1]]));
    }
    bound
}

fn has_lower_dimension<S: Surface>(set: &GeometrySet<S>) -> bool {
    set.members
        .iter()
        .any(|member| member.kind != GeometryKind::Polygon && !member.vertices.is_empty())
}

impl PlaneQuery {
    #[cfg(test)]
    pub(crate) fn index(&self) -> &ShapeIndex {
        &self.index
    }

    pub fn prepare<S: Surface>(set: &GeometrySet<S>) -> Option<Self> {
        if S::DIMENSIONS != 2 {
            return None;
        }
        let index = Clipper::<S>::new(ClipOptions::default()).build(std::slice::from_ref(set));
        let known_nonempty = has_lower_dimension(set)
            || index
                .cells
                .iter()
                .any(|cell| cell.shapes.iter().any(|shape| shape.contains_center));
        Some(Self {
            index,
            operand: PreparedOperand::new(Operand::new(set)),
            bound: source_bounds(set),
            known_nonempty,
        })
    }

    pub fn search<S: Surface>(
        &self,
        reader: &CellIndexReader<'_>,
        terms_filter: Option<&BitSet>,
        edge_resolver: &EdgeResolver<'_, S>,
        max_doc: u32,
        predicate: Predicate,
        first_only: bool,
    ) -> BitSet {
        let mut hits = BitSet::with_max_value(max_doc);
        let mut seen = BitSet::with_max_value(edge_resolver.geometry_count(0));
        for query_cell in &self.index.cells {
            let query_interior = query_cell
                .shapes
                .iter()
                .any(|shape| shape.contains_center && shape.edge_indices.is_empty());
            let query_bound = source_bound(&cell_bound_uv::<super::Plane>(query_cell.cell_id));
            let (start, end) = reader.range_for_cell(query_cell.cell_id, 0, reader.cell_count());
            for position in start..end {
                let cell = reader.cell_at(position);
                for shape in &cell.shapes {
                    if seen.contains(shape.geometry_id.1) {
                        continue;
                    }
                    let doc_id = edge_resolver.doc_id_for(shape.geometry_id);
                    if hits.contains(doc_id)
                        || terms_filter.is_some_and(|filter| !filter.contains(doc_id))
                    {
                        continue;
                    }
                    let (head, set) = edge_resolver.read_geometry_set(shape.geometry_id);
                    let uniform_candidate = shape.contains_center && shape.edge_indices.is_empty();
                    let candidate_contains_query = uniform_candidate
                        && self.known_nonempty
                        && source_bound(&cell_bound_uv::<super::Plane>(cell.cell_id))
                            .contains(&self.bound);
                    let query_contains_candidate = query_interior
                        && (shape.contains_center || has_lower_dimension(&set))
                        && query_bound.contains(&source_bounds(&set));
                    let certified = match predicate {
                        Predicate::Intersects => {
                            candidate_contains_query || query_contains_candidate
                        }
                        Predicate::Contains | Predicate::Covers => candidate_contains_query,
                        Predicate::CoveredBy | Predicate::Within => query_contains_candidate,
                    };
                    // Discovery may include empty traces. Only a complete value decision
                    // or a material certificate can admit the document.
                    let matched = certified
                        || evaluate(
                            &PreparedOperand::new(Operand::new(&set)),
                            &self.operand,
                            predicate,
                        );
                    for member in 0..set.members.len() {
                        seen.insert(head + member as u32);
                    }
                    if matched {
                        hits.insert(doc_id);
                        if first_only {
                            return hits;
                        }
                    }
                }
            }
        }
        hits
    }
}
