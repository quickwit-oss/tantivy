//! Spatial query executor.
//!
//! Evaluates multi-stage spatial queries during Weight construction, producing per-segment
//! bitsets or scored result sets that per-segment scorers iterate.

use std::collections::HashMap;
use std::fmt;

use common::BitSet;

use super::cell_index_reader::CellIndexReader;
use super::distance::Distance;
use super::edge_cache::EdgeCache;
use super::edge_reader::EdgeReader;
use super::geometry_set::GeometrySet;
use super::intersects::Intersects;
use super::region_coverer::CovererOptions;
use super::sphere::Sphere;
use crate::core::Searcher;
use crate::docset::{DocSet, TERMINATED};
use crate::index::SegmentId;
use crate::query::{BitSetDocSet, EnableScoring, Explanation, Query, Scorer, Weight};
use crate::schema::Field;
use crate::{DocId, Score, SegmentReader};

/// Spatial relationship for a join predicate.
#[derive(Clone, Debug)]
pub enum SpatialRelation {
    /// Within a distance.
    Near(f64),
    /// Between two distances.
    Between(f64, f64),
    /// Geometries intersect.
    Intersects,
    /// One geometry contains the other.
    Contains,
}

/// A node in the execution plan tree.
pub enum PlanNode {
    /// A tantivy query evaluated to per-segment bitsets.
    Query(Box<dyn Query>),

    /// Find K nearest documents to a query geometry.
    Knn {
        /// The spatial field.
        field: Field,
        /// The query geometry.
        geometry: GeometrySet<Sphere>,
        /// Number of nearest neighbors.
        k: usize,
        /// If present, only documents passing this node are considered.
        filter: Option<Box<PlanNode>>,
    },

    /// Intersects query filtered by a terms bitset during traversal.
    Intersects {
        /// The spatial field.
        field: Field,
        /// The query geometry (smashed).
        geometry: GeometrySet<Sphere>,
        /// Documents passing this node are eligible candidates.
        filter: Box<PlanNode>,
    },

    /// Distance query filtered by a terms bitset during traversal.
    Within {
        /// The spatial field.
        field: Field,
        /// The query geometry (smashed).
        geometry: GeometrySet<Sphere>,
        /// Maximum distance (chord angle length2).
        radius: f64,
        /// Documents passing this node are eligible candidates.
        filter: Box<PlanNode>,
    },

    /// Spatial join between an outer and inner result set.
    Join {
        /// The spatial field.
        field: Field,
        /// Documents to test.
        outer: Box<PlanNode>,
        /// Documents to test against.
        inner: Box<PlanNode>,
        /// The spatial relationship.
        relation: SpatialRelation,
    },
}

/// The output of evaluating a plan node. Per-segment results keyed by segment ID.
pub struct StageOutput {
    /// Per-segment results.
    pub results: HashMap<SegmentId, SegmentResult>,
}

/// The precomputed results for a single segment.
pub enum SegmentResult {
    /// A bitset of matching doc_ids with a constant score (join, distance, text queries).
    Match(BitSet),
    /// A scored list of (doc_id, distance) pairs sorted by doc_id (kNN).
    Scored(Vec<(DocId, f32)>),
}

impl StageOutput {
    /// Extract the bitset for a segment, building one from scored results if necessary.
    fn bitset_for(&self, segment_id: &SegmentId, max_doc: u32) -> BitSet {
        match self.results.get(segment_id) {
            Some(SegmentResult::Match(bitset)) => bitset.clone(),
            Some(SegmentResult::Scored(results)) => {
                let mut bitset = BitSet::with_max_value(max_doc);
                for &(doc_id, _) in results {
                    bitset.insert(doc_id);
                }
                bitset
            }
            None => BitSet::with_max_value(max_doc),
        }
    }
}

/// Evaluate a plan node recursively, producing per-segment results.
fn evaluate(
    node: &PlanNode,
    searcher: &Searcher,
    segments: &[SegmentReader],
) -> crate::Result<StageOutput> {
    match node {
        PlanNode::Query(query) => {
            // Run the tantivy query to completion per segment, collect bitsets.
            let scoring = EnableScoring::disabled_from_searcher(searcher);
            let weight = query.weight(scoring)?;
            let mut results = HashMap::new();
            for reader in segments {
                let mut scorer = weight.scorer(reader, 1.0)?;
                let mut bitset = BitSet::with_max_value(reader.max_doc());
                let mut doc = scorer.doc();
                while doc != TERMINATED {
                    bitset.insert(doc);
                    doc = scorer.advance();
                }
                results.insert(reader.segment_id(), SegmentResult::Match(bitset));
            }
            Ok(StageOutput { results })
        }

        PlanNode::Intersects {
            field,
            geometry,
            filter,
        } => {
            // Evaluate the filter node -> per-segment bitsets.
            let filter_output = evaluate(filter, searcher, segments)?;

            // Build the Intersects once from the smashed geometry.
            let intersects = Intersects::new(geometry.clone(), CovererOptions::default());

            // For each segment, run the filtered spatial traversal.
            let mut results = HashMap::new();
            for reader in segments {
                let spatial = reader.spatial_fields().get_field(*field)?;
                if let Some(spatial_reader) = spatial {
                    let cell_reader = CellIndexReader::open(spatial_reader.cells_bytes());
                    let er = EdgeReader::<Sphere>::open(spatial_reader.edges_bytes());
                    let mut edge_cache = EdgeCache::new(vec![er], 100_000);

                    let filter_bitset =
                        filter_output.bitset_for(&reader.segment_id(), reader.max_doc());

                    let bitset = intersects.search(
                        &cell_reader,
                        Some(&filter_bitset),
                        &mut edge_cache,
                        reader.max_doc(),
                    );
                    results.insert(reader.segment_id(), SegmentResult::Match(bitset));
                }
            }
            Ok(StageOutput { results })
        }

        PlanNode::Within {
            field,
            geometry,
            radius,
            filter,
        } => {
            let filter_output = evaluate(filter, searcher, segments)?;

            let query = Distance::<Sphere>::new(geometry.clone(), *radius, CovererOptions::default());

            let mut results = HashMap::new();
            for reader in segments {
                let spatial = reader.spatial_fields().get_field(*field)?;
                if let Some(spatial_reader) = spatial {
                    let cell_reader = CellIndexReader::open(spatial_reader.cells_bytes());
                    let er = EdgeReader::<Sphere>::open(spatial_reader.edges_bytes());
                    let mut edge_cache = EdgeCache::new(vec![er], 100_000);

                    let filter_bitset =
                        filter_output.bitset_for(&reader.segment_id(), reader.max_doc());

                    let bitset = query.search(
                        &cell_reader,
                        Some(&filter_bitset),
                        &mut edge_cache,
                        reader.max_doc(),
                    );
                    results.insert(reader.segment_id(), SegmentResult::Match(bitset));
                }
            }
            Ok(StageOutput { results })
        }

        PlanNode::Knn { .. } => {
            todo!("kNN via flood fill tightening")
        }

        PlanNode::Join {
            field,
            outer,
            inner,
            relation,
        } => {
            // Collect inner bitsets across all segments.
            let inner_output = evaluate(inner, searcher, segments)?;

            // Collect outer bitsets across all segments.
            let outer_output = evaluate(outer, searcher, segments)?;

            // For each segment, walk its cell index for outer geometries, probe all segments for
            // inner matches.
            let mut results = HashMap::new();
            for reader in segments.iter() {
                let outer_bitset = outer_output.bitset_for(&reader.segment_id(), reader.max_doc());
                let mut result_bitset = BitSet::with_max_value(reader.max_doc());
                let mut visited = std::collections::HashSet::new();

                let spatial = reader.spatial_fields().get_field(*field)?;
                if spatial.is_none() {
                    results.insert(reader.segment_id(), SegmentResult::Match(result_bitset));
                    continue;
                }
                let spatial_reader = spatial.unwrap();
                let cell_reader = CellIndexReader::open(spatial_reader.cells_bytes());
                let er = EdgeReader::<Sphere>::open(spatial_reader.edges_bytes());
                let edge_cache = EdgeCache::new(vec![er], 100_000);

                for cell in cell_reader.iter() {
                    for clipped in &cell.shapes {
                        let geometry_id = clipped.geometry_id;
                        if !visited.insert(geometry_id) {
                            continue;
                        }
                        let doc_id = edge_cache.doc_id_for(geometry_id);
                        if !outer_bitset.contains(doc_id) {
                            continue;
                        }

                        // Read the outer geometry.
                        let outer_geometry = edge_cache.get(geometry_id).geometry_set().clone();

                        // Probe all segments for inner matches.
                        let mut found = false;
                        for probe_reader in segments.iter() {
                            let probe_spatial = probe_reader.spatial_fields().get_field(*field)?;
                            if let Some(probe_spatial_reader) = probe_spatial {
                                let probe_cell_reader =
                                    CellIndexReader::open(probe_spatial_reader.cells_bytes());
                                let probe_er =
                                    EdgeReader::<Sphere>::open(probe_spatial_reader.edges_bytes());
                                let mut probe_edge_cache = EdgeCache::new(vec![probe_er], 100_000);
                                let inner_bitset = inner_output
                                    .bitset_for(&probe_reader.segment_id(), probe_reader.max_doc());

                                found = match relation {
                                    SpatialRelation::Near(r) => {
                                        let probe = Distance::<Sphere>::any_within(
                                            outer_geometry.clone(),
                                            *r,
                                            CovererOptions::default(),
                                        );
                                        probe
                                            .search(
                                                &probe_cell_reader,
                                                Some(&inner_bitset),
                                                &mut probe_edge_cache,
                                                probe_reader.max_doc(),
                                            )
                                            .len()
                                            > 0
                                    }
                                    SpatialRelation::Intersects => {
                                        let probe = Intersects::new(
                                            outer_geometry.clone(),
                                            CovererOptions::default(),
                                        );
                                        probe
                                            .search(
                                                &probe_cell_reader,
                                                Some(&inner_bitset),
                                                &mut probe_edge_cache,
                                                probe_reader.max_doc(),
                                            )
                                            .len()
                                            > 0
                                    }
                                    SpatialRelation::Contains => {
                                        todo!("contains join probe")
                                    }
                                    SpatialRelation::Between(_, _) => {
                                        todo!("between is composed from distance queries")
                                    }
                                };
                                if found {
                                    break;
                                }
                            }
                        }

                        if found {
                            result_bitset.insert(doc_id);
                        }
                    }
                }

                results.insert(reader.segment_id(), SegmentResult::Match(result_bitset));
            }

            Ok(StageOutput { results })
        }
    }
}

/// Query that evaluates a spatial execution plan during Weight construction.
pub struct SpatialExecutor {
    /// The root of the plan tree.
    root: PlanNode,
}

impl SpatialExecutor {
    /// Create a new executor from a plan tree.
    pub fn new(root: PlanNode) -> Self {
        Self { root }
    }

    /// Replace the outer PlanNode in the root Join with the given query.
    pub fn set_outer(&mut self, query: Box<dyn Query>) {
        if let PlanNode::Join { ref mut outer, .. } = self.root {
            *outer = Box::new(PlanNode::Query(query));
        }
    }
}

impl fmt::Debug for SpatialExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SpatialExecutor")
    }
}

impl Clone for PlanNode {
    fn clone(&self) -> Self {
        match self {
            PlanNode::Query(q) => PlanNode::Query(q.box_clone()),
            PlanNode::Knn {
                field,
                geometry,
                k,
                filter,
            } => PlanNode::Knn {
                field: *field,
                geometry: geometry.clone(),
                k: *k,
                filter: filter.clone(),
            },
            PlanNode::Intersects {
                field,
                geometry,
                filter,
            } => PlanNode::Intersects {
                field: *field,
                geometry: geometry.clone(),
                filter: filter.clone(),
            },
            PlanNode::Within {
                field,
                geometry,
                radius,
                filter,
            } => PlanNode::Within {
                field: *field,
                geometry: geometry.clone(),
                radius: *radius,
                filter: filter.clone(),
            },
            PlanNode::Join {
                field,
                outer,
                inner,
                relation,
            } => PlanNode::Join {
                field: *field,
                outer: outer.clone(),
                inner: inner.clone(),
                relation: relation.clone(),
            },
        }
    }
}

impl Clone for SpatialExecutor {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
        }
    }
}

impl Query for SpatialExecutor {
    fn weight(&self, enable_scoring: EnableScoring<'_>) -> crate::Result<Box<dyn Weight>> {
        let searcher = enable_scoring
            .searcher()
            .expect("SpatialExecutor requires a Searcher");
        let segments = searcher.segment_readers();

        let output = evaluate(&self.root, searcher, segments)?;

        Ok(Box::new(ExecutorWeight {
            results: output.results,
        }))
    }
}

/// Holds precomputed per-segment results. Scorers iterate them.
struct ExecutorWeight {
    /// Precomputed results keyed by segment ID.
    results: HashMap<SegmentId, SegmentResult>,
}

impl Weight for ExecutorWeight {
    fn scorer(&self, reader: &SegmentReader, boost: Score) -> crate::Result<Box<dyn Scorer>> {
        match self.results.get(&reader.segment_id()) {
            Some(SegmentResult::Match(bitset)) => {
                Ok(Box::new(ReplayScorer::from_bitset(bitset.clone(), boost)))
            }
            Some(SegmentResult::Scored(results)) => {
                Ok(Box::new(ReplayScorer::from_scored(results.clone())))
            }
            None => Ok(Box::new(ReplayScorer::empty())),
        }
    }

    fn explain(&self, _reader: &SegmentReader, _doc: DocId) -> crate::Result<Explanation> {
        Ok(Explanation::new("SpatialExecutor", 1.0))
    }
}

/// Iterates precomputed results as a scorer.
struct ReplayScorer {
    /// The iteration state.
    inner: ReplayInner,
}

enum ReplayInner {
    /// Iterate a bitset with a constant score.
    BitSet {
        docs: BitSetDocSet,
        doc_id: DocId,
        score: Score,
    },
    /// Iterate a sorted vec of (doc_id, distance).
    Scored {
        results: Vec<(DocId, f32)>,
        index: usize,
    },
    /// No results for this segment.
    Empty,
}

impl ReplayScorer {
    fn from_bitset(bitset: BitSet, score: Score) -> Self {
        let docs = BitSetDocSet::from(bitset);
        let doc_id = docs.doc();
        Self {
            inner: ReplayInner::BitSet {
                docs,
                doc_id,
                score,
            },
        }
    }

    fn from_scored(results: Vec<(DocId, f32)>) -> Self {
        Self {
            inner: ReplayInner::Scored { results, index: 0 },
        }
    }

    fn empty() -> Self {
        Self {
            inner: ReplayInner::Empty,
        }
    }
}

impl Scorer for ReplayScorer {
    fn score(&mut self) -> Score {
        match &self.inner {
            ReplayInner::BitSet { score, .. } => *score,
            ReplayInner::Scored { results, index, .. } => {
                if *index < results.len() {
                    -results[*index].1
                } else {
                    0.0
                }
            }
            ReplayInner::Empty => 0.0,
        }
    }
}

impl DocSet for ReplayScorer {
    fn advance(&mut self) -> DocId {
        match &mut self.inner {
            ReplayInner::BitSet { docs, doc_id, .. } => {
                if *doc_id == TERMINATED {
                    return TERMINATED;
                }
                *doc_id = docs.advance();
                *doc_id
            }
            ReplayInner::Scored { results, index, .. } => {
                *index += 1;
                if *index < results.len() {
                    results[*index].0
                } else {
                    TERMINATED
                }
            }
            ReplayInner::Empty => TERMINATED,
        }
    }

    fn doc(&self) -> DocId {
        match &self.inner {
            ReplayInner::BitSet { doc_id, .. } => *doc_id,
            ReplayInner::Scored { results, index, .. } => {
                if *index < results.len() {
                    results[*index].0
                } else {
                    TERMINATED
                }
            }
            ReplayInner::Empty => TERMINATED,
        }
    }

    fn size_hint(&self) -> u32 {
        match &self.inner {
            ReplayInner::BitSet { docs, .. } => docs.size_hint(),
            ReplayInner::Scored { results, .. } => results.len() as u32,
            ReplayInner::Empty => 0,
        }
    }
}
