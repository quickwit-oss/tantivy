//! Polygon-intersects-polygon query.
//!
//! Single-pass query using bitsets for bookkeeping. For each candidate geometry encountered during
//! the covering walk: first-encounter containment tests (both directions), then edge crossing tests
//! in every cell where the geometry appears. A hit bitset skips confirmed geometries. A
//! containment-tested bitset ensures the containment tests run at most once per geometry.

use common::BitSet;

use super::cell_index_reader::CellIndexReader;
use super::contains_query::QueryEdgeProvider;
use super::crossings::S2EdgeCrosser;
use super::edge_cache::EdgeCache;
use super::geometry_set::GeometrySet;
use super::region_coverer::{CovererOptions, RegionCoverer};
use super::s2cell_id::S2CellId;
use super::shape_index_region::{index_contains_point, CellIndexRegion};
use super::sphere::Sphere;
use crate::spatial::clip_options::ClipOptions;
use crate::spatial::clipper::Clipper;
use crate::spatial::shape_index::ShapeIndex;

/// Prepared intersects query, built once from a query polygon and applied per-segment.
pub struct IntersectsQuery {
    query_index: ShapeIndex,
    query_edges: QueryEdgeProvider,
    covering: Vec<S2CellId>,
}

impl IntersectsQuery {
    /// Build the query from a smashed GeometrySet.
    pub fn new(set: GeometrySet, options: CovererOptions) -> Self {
        let builder = Clipper::new(ClipOptions::default());
        let query_index = builder.build(std::slice::from_ref(&set));

        let query_edges = QueryEdgeProvider { set };

        let region = CellIndexRegion::new(&query_index, &query_edges);
        let coverer = RegionCoverer::new(options);
        let covering = coverer.get_covering(&region).into_cell_ids();

        Self {
            query_index,
            query_edges,
            covering,
        }
    }

    /// Search one segment for geometries that intersect the query polygon.
    pub fn search_segment<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, Sphere>,
    ) -> Vec<u32> {
        self.search_segment_inner(cell_reader, None, edge_cache)
    }

    /// Search one segment, skipping shapes whose doc_id is not in the filter bitset.
    pub fn search_segment_filtered<'a>(
        &self,
        cell_reader: &'a CellIndexReader<'a>,
        edge_cache: &mut EdgeCache<'a, Sphere>,
        terms_filter: &BitSet,
    ) -> Vec<u32> {
        self.search_segment_inner(cell_reader, Some(terms_filter), edge_cache)
    }

    fn search_segment_inner<'a>(
        &self,
        reader: &'a CellIndexReader<'a>,
        terms_filter: Option<&BitSet>,
        edge_cache: &mut EdgeCache<'a, Sphere>,
    ) -> Vec<u32> {
        let geometry_count = edge_cache.geometry_count(0);
        let mut include = BitSet::with_max_value(geometry_count);
        let mut exclude = BitSet::with_max_value(geometry_count);
        let mut containment_tested = BitSet::with_max_value(geometry_count);
        let mut doc_ids = Vec::new();
        let mut hits_candidate_contains = 0u64;
        let mut hits_query_contains = 0u64;
        let mut hits_crossing = 0u64;
        let mut total_tested = 0u64;

        // Pre-scan: find geometries that contain the query point. A closed geometry
        // with contains_center and no edges in the query point cell fully contains the
        // query point. Include these before the covering walk.
        let query_vertex = &self.query_edges.get_edge_set((0, 0)).vertices[0];
        let query_point_cell_id = S2CellId::from_point(query_vertex);
        if let Some(qpc) = reader.find(query_point_cell_id) {
            for shape in &qpc.shapes {
                let gid = shape.geometry_id.1;
                if shape.contains_center && shape.edge_indices.is_empty() {
                    let located = edge_cache.locate(shape.geometry_id);
                    if !located.closed {
                        continue;
                    }
                    if let Some(filter) = terms_filter {
                        if !filter.contains(located.doc_id) {
                            exclude.insert(gid);
                            continue;
                        }
                    }
                    hits_candidate_contains += 1;
                    include.insert(gid);
                    doc_ids.push(located.doc_id);
                }
            }
        }

        for &covering_cell_id in &self.covering {
            let query_cell = self.query_index.find_cell(covering_cell_id);
            for index_cell in reader.scan_range(covering_cell_id) {
                for clipped in &index_cell.shapes {
                    let gid = clipped.geometry_id.1;

                    if include.contains(gid) || exclude.contains(gid) {
                        continue;
                    }

                    // Locate once per iteration. The containment block uses it on
                    // first encounter. The crossing block uses it on every encounter.
                    let mut was_located = None;

                    // Containment tests: run once per geometry.
                    if !containment_tested.contains(gid) {
                        containment_tested.insert(gid);

                        let located = edge_cache.locate(clipped.geometry_id);

                        if let Some(filter) = terms_filter {
                            if !filter.contains(located.doc_id) {
                                exclude.insert(gid);
                                continue;
                            }
                        }

                        total_tested += 1;

                        // Does the query contain the candidate's first vertex?
                        let first_vertex = located.vertex(0);
                        if index_contains_point(
                            &self.query_index,
                            &self.query_edges,
                            (0, 0),
                            &first_vertex,
                        ) {
                            hits_query_contains += 1;
                            include.insert(gid);
                            doc_ids.push(located.doc_id);
                            continue;
                        }

                        was_located = Some(located);
                    }

                    // Edge crossing test.
                    if !clipped.edge_indices.is_empty() {
                        if let Some(qc) = query_cell {
                            let located = was_located
                                .unwrap_or_else(|| edge_cache.locate(clipped.geometry_id));
                            if located.vertex_count >= 2 {
                                let query_vertices =
                                    &self.query_edges.get_edge_set((0, 0)).vertices;
                                let crossed = 'crossing: {
                                    for &candidate_edge_idx in &clipped.edge_indices {
                                        let (cv0, cv1) = located.edge(candidate_edge_idx);
                                        let mut crosser = S2EdgeCrosser::new(&cv0, &cv1);
                                        for query_shape in &qc.shapes {
                                            for &query_edge_idx in &query_shape.edge_indices {
                                                let qi = query_edge_idx as usize;
                                                let qv0 = &query_vertices[qi];
                                                let qv1 = &query_vertices[qi + 1];
                                                if crosser.crossing_sign_two(qv0, qv1) > 0 {
                                                    break 'crossing true;
                                                }
                                            }
                                        }
                                    }
                                    false
                                };
                                if crossed {
                                    hits_crossing += 1;
                                    include.insert(gid);
                                    doc_ids.push(located.doc_id);
                                }
                            }
                        }
                    }
                }
            }
        }

        eprintln!(
            "intersects: {} tested, {} candidate_contains, {} query_contains, {} crossing, {} \
             total hits",
            total_tested,
            hits_candidate_contains,
            hits_query_contains,
            hits_crossing,
            hits_candidate_contains + hits_query_contains + hits_crossing,
        );

        doc_ids
    }
}
