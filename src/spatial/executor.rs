//! Spatial query executor.
//!
//! Evaluates multi-stage spatial queries during Weight construction, producing per-segment
//! bitsets or scored result sets that per-segment scorers iterate.

use std::collections::HashMap;
use std::fmt;

use common::BitSet;

use super::cell_index_reader::CellIndexReader;
use super::closest_edge_query::ClosestEdgeQuery;
use super::contains_query::ContainsQuery;
use super::edge_cache::EdgeCache;
use super::edge_reader::EdgeReader;
use super::geometry_set::GeometrySet;
use super::intersects_query::IntersectsQuery;
use super::region_coverer::CovererOptions;
use super::s1chord_angle::S1ChordAngle;
use super::sphere::Sphere;
use crate::core::Searcher;
use crate::docset::{DocSet, TERMINATED};
use crate::index::SegmentId;
use crate::query::{
    does_not_match, BitSetDocSet, EnableScoring, Explanation, Query, Scorer, Weight,
};
use crate::schema::Field;
use crate::{DocId, Score, SegmentReader, TantivyError};

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
        geometry: GeometrySet,
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
        geometry: GeometrySet,
        /// Documents passing this node are eligible candidates.
        filter: Box<PlanNode>,
    },

    /// Distance query filtered by a terms bitset during traversal.
    Within {
        /// The spatial field.
        field: Field,
        /// The query geometry (smashed).
        geometry: GeometrySet,
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

/// Vertices each edge cache of a query may hold before it evicts geometries.
const QUERY_EDGE_CACHE_VERTICES: usize = 100_000;

/// Evaluate a plan node recursively, producing per-segment results. Every stage's documents are
/// alive: a Query stage leaves out deleted documents, and every other stage draws its documents
/// from stages below it, or applies the deletions itself.
fn evaluate(
    node: &PlanNode,
    searcher: &Searcher,
    segments: &[SegmentReader],
) -> crate::Result<StageOutput> {
    match node {
        PlanNode::Query(query) => {
            // Run the tantivy query to completion per segment, collect bitsets. Deleted documents
            // must not become the inner or outer documents of a join, or eligible candidates.
            let scoring = EnableScoring::disabled_from_searcher(searcher);
            let weight = query.weight(scoring)?;
            let mut results = HashMap::new();
            for reader in segments {
                let mut bitset = BitSet::with_max_value(reader.max_doc());
                let alive_bitset = reader.alive_bitset();
                weight.for_each_no_score(reader, &mut |docs| {
                    for &doc in docs {
                        if alive_bitset.is_none_or(|alive| alive.is_alive(doc)) {
                            bitset.insert(doc);
                        }
                    }
                })?;
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

            // Build the IntersectsQuery once from the smashed geometry.
            let intersects = IntersectsQuery::new(geometry.clone(), CovererOptions::default());

            // For each segment, run the filtered spatial traversal.
            let mut results = HashMap::new();
            for reader in segments {
                let spatial = reader.spatial_fields().get_field(*field)?;
                if let Some(spatial_reader) = spatial {
                    let cell_reader = CellIndexReader::open(spatial_reader.cells_bytes())?;
                    let er = EdgeReader::<Sphere>::open(spatial_reader.edges_bytes())?;
                    let mut edge_cache = EdgeCache::new(vec![er], QUERY_EDGE_CACHE_VERTICES);

                    let filter_bitset =
                        filter_output.bitset_for(&reader.segment_id(), reader.max_doc());

                    let doc_ids = intersects.search_segment_filtered(
                        &cell_reader,
                        &mut edge_cache,
                        &filter_bitset,
                    );

                    let mut bitset = BitSet::with_max_value(reader.max_doc());
                    for doc_id in doc_ids {
                        bitset.insert(doc_id);
                    }
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

            let query =
                ClosestEdgeQuery::within(geometry.clone(), S1ChordAngle::from_length2(*radius));

            let mut results = HashMap::new();
            for reader in segments {
                let spatial = reader.spatial_fields().get_field(*field)?;
                if let Some(spatial_reader) = spatial {
                    let cell_reader = CellIndexReader::open(spatial_reader.cells_bytes())?;
                    let er = EdgeReader::<Sphere>::open(spatial_reader.edges_bytes())?;
                    let mut edge_cache = EdgeCache::new(vec![er], QUERY_EDGE_CACHE_VERTICES);

                    let filter_bitset =
                        filter_output.bitset_for(&reader.segment_id(), reader.max_doc());

                    let hits = query.search_segment_filtered(
                        &cell_reader,
                        &mut edge_cache,
                        &filter_bitset,
                    );

                    let mut bitset = BitSet::with_max_value(reader.max_doc());
                    for result in hits {
                        bitset.insert(result.doc_id);
                    }
                    results.insert(reader.segment_id(), SegmentResult::Match(bitset));
                }
            }
            Ok(StageOutput { results })
        }

        PlanNode::Knn {
            field,
            geometry,
            k,
            filter,
        } => {
            let filter_output = match filter {
                Some(node) => Some(evaluate(node, searcher, segments)?),
                None => None,
            };

            let query = ClosestEdgeQuery::knn(geometry.clone(), *k);

            // Run branch-and-bound per segment, collecting up to K results with
            // distances. Each result is tagged with its segment ID for redistribution
            // after the cross-segment merge.
            let mut all_results: Vec<(SegmentId, u32, f64)> = Vec::new();
            for reader in segments {
                let spatial = reader.spatial_fields().get_field(*field)?;
                if let Some(spatial_reader) = spatial {
                    let cell_reader = CellIndexReader::open(spatial_reader.cells_bytes())?;
                    let er = EdgeReader::<Sphere>::open(spatial_reader.edges_bytes())?;
                    let mut edge_cache = EdgeCache::new(vec![er], QUERY_EDGE_CACHE_VERTICES);

                    // Deleted documents must be excluded during traversal, before they can
                    // consume either a segment's K slots or the global K slots. A filter's
                    // documents are alive already.
                    let filter_bitset = match &filter_output {
                        Some(fo) => Some(fo.bitset_for(&reader.segment_id(), reader.max_doc())),
                        None => reader.alive_bitset().map(|alive| {
                            let mut bitset = BitSet::with_max_value_and_full(reader.max_doc());
                            bitset.intersect_update(alive.bitset());
                            bitset
                        }),
                    };
                    let hits = match &filter_bitset {
                        Some(filter) => {
                            query.search_segment_filtered(&cell_reader, &mut edge_cache, filter)
                        }
                        None => query.search_segment(&cell_reader, &mut edge_cache),
                    };

                    for hit in hits {
                        all_results.push((reader.segment_id(), hit.doc_id, hit.distance.length2()));
                    }
                }
            }

            // Merge across segments: sort by distance, keep global top K.
            all_results.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
            all_results.truncate(*k);

            // Redistribute to per-segment scored results.
            let mut results: HashMap<SegmentId, Vec<(DocId, f32)>> = HashMap::new();
            for (seg_id, doc_id, distance) in all_results {
                results
                    .entry(seg_id)
                    .or_default()
                    .push((doc_id, distance as f32));
            }

            // Sort each segment's results by doc_id for DocSet ordering.
            let mut final_results = HashMap::new();
            for (seg_id, mut docs) in results {
                docs.sort_by_key(|&(doc_id, _)| doc_id);
                final_results.insert(seg_id, SegmentResult::Scored(docs));
            }

            Ok(StageOutput {
                results: final_results,
            })
        }

        PlanNode::Join {
            field,
            outer,
            inner,
            relation,
        } => {
            // Phase 1: collect inner bitsets across all segments.
            let inner_output = evaluate(inner, searcher, segments)?;

            // Phase 2: collect outer bitsets across all segments.
            let outer_output = evaluate(outer, searcher, segments)?;

            let spatial_readers = segments
                .iter()
                .map(|reader| reader.spatial_fields().get_field(*field))
                .collect::<crate::Result<Vec<_>>>()?;

            // Phase 3: open each segment that holds inner documents once, for every probe.
            let mut probes = Vec::new();
            for (segment_ord, (reader, spatial_reader)) in
                segments.iter().zip(&spatial_readers).enumerate()
            {
                let Some(spatial_reader) = spatial_reader else {
                    continue;
                };
                let inner = inner_output.bitset_for(&reader.segment_id(), reader.max_doc());
                if inner.len() == 0 {
                    continue;
                }
                let er = EdgeReader::<Sphere>::open(spatial_reader.edges_bytes())?;
                probes.push(ProbeSegment {
                    segment_ord,
                    cell_reader: CellIndexReader::open(spatial_reader.cells_bytes())?,
                    edge_cache: EdgeCache::new(vec![er], QUERY_EDGE_CACHE_VERTICES),
                    inner,
                });
            }

            // Phase 4: for each segment, walk its cell index for outer documents, and probe the
            // segments for inner matches with each one's geometry.
            let mut results = HashMap::new();
            for (segment_ord, (reader, spatial_reader)) in
                segments.iter().zip(&spatial_readers).enumerate()
            {
                let mut result_bitset = BitSet::with_max_value(reader.max_doc());
                // With no inner documents anywhere, nothing matches.
                if let Some(spatial_reader) = spatial_reader.as_ref().filter(|_| !probes.is_empty())
                {
                    let outer_bitset =
                        outer_output.bitset_for(&reader.segment_id(), reader.max_doc());
                    let cell_reader = CellIndexReader::open(spatial_reader.cells_bytes())?;
                    let er = EdgeReader::<Sphere>::open(spatial_reader.edges_bytes())?;
                    let edge_cache = EdgeCache::new(vec![er], QUERY_EDGE_CACHE_VERTICES);
                    // Each outer document is probed once, with all of its geometry.
                    let mut visited = BitSet::with_max_value(reader.max_doc());

                    for cell in cell_reader.iter() {
                        for clipped in &cell.shapes {
                            let doc_id = edge_cache.doc_id_for(clipped.geometry_id);
                            if visited.contains(doc_id) || !outer_bitset.contains(doc_id) {
                                continue;
                            }
                            visited.insert(doc_id);

                            let outer_geometry =
                                edge_cache.get(clipped.geometry_id).geometry_set().clone();
                            let probe = JoinProbe::new(relation, outer_geometry);
                            let found = probes.iter_mut().any(|segment| {
                                // A document is not joined to itself.
                                let is_self = segment.segment_ord == segment_ord
                                    && segment.inner.contains(doc_id);
                                if is_self {
                                    segment.inner.remove(doc_id);
                                }
                                let found = probe.matches(segment);
                                if is_self {
                                    segment.inner.insert(doc_id);
                                }
                                found
                            });
                            if found {
                                result_bitset.insert(doc_id);
                            }
                        }
                    }
                }
                results.insert(reader.segment_id(), SegmentResult::Match(result_bitset));
            }

            Ok(StageOutput { results })
        }
    }
}

/// A segment a join probes for inner matches: its readers, and its inner documents.
struct ProbeSegment<'a> {
    segment_ord: usize,
    cell_reader: CellIndexReader<'a>,
    edge_cache: EdgeCache<'a, Sphere>,
    inner: BitSet,
}

/// The query a join runs on the probed segments for one outer geometry, built once per outer
/// document.
enum JoinProbe {
    Distance(ClosestEdgeQuery),
    Intersects(IntersectsQuery),
    Contains(ContainsQuery),
}

impl JoinProbe {
    fn new(relation: &SpatialRelation, outer_geometry: GeometrySet) -> Self {
        match relation {
            SpatialRelation::Near(r) => JoinProbe::Distance(ClosestEdgeQuery::any_within(
                outer_geometry,
                S1ChordAngle::from_radians(*r),
            )),
            SpatialRelation::Between(inner_r, outer_r) => {
                JoinProbe::Distance(ClosestEdgeQuery::between(
                    outer_geometry,
                    S1ChordAngle::from_radians(*inner_r),
                    S1ChordAngle::from_radians(*outer_r),
                ))
            }
            SpatialRelation::Intersects => JoinProbe::Intersects(IntersectsQuery::new(
                outer_geometry,
                CovererOptions::default(),
            )),
            SpatialRelation::Contains => JoinProbe::Contains(ContainsQuery::new(
                outer_geometry,
                CovererOptions::default(),
            )),
        }
    }

    /// Whether any inner document of the segment matches.
    fn matches(&self, segment: &mut ProbeSegment<'_>) -> bool {
        let (cell_reader, edge_cache, inner) = (
            &segment.cell_reader,
            &mut segment.edge_cache,
            &segment.inner,
        );
        match self {
            JoinProbe::Distance(query) => !query
                .search_segment_filtered(cell_reader, edge_cache, inner)
                .is_empty(),
            JoinProbe::Intersects(query) => !query
                .search_segment_filtered(cell_reader, edge_cache, inner)
                .is_empty(),
            JoinProbe::Contains(query) => !query
                .search_segment_filtered(cell_reader, edge_cache, inner)
                .is_empty(),
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
            **outer = PlanNode::Query(query);
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
        let searcher = enable_scoring.searcher().ok_or_else(|| {
            TantivyError::InvalidArgument(
                "A spatial join or kNN query needs a searcher: it evaluates every segment at once"
                    .to_string(),
            )
        })?;
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
                Ok(Box::new(ReplayScorer::from_scored(results.clone(), boost)))
            }
            None => Ok(Box::new(ReplayScorer::empty())),
        }
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> crate::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) != doc {
            return Err(does_not_match(doc));
        }
        Ok(Explanation::new("SpatialExecutor", scorer.score()))
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
        boost: Score,
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

    fn from_scored(results: Vec<(DocId, f32)>, boost: Score) -> Self {
        Self {
            inner: ReplayInner::Scored {
                results,
                index: 0,
                boost,
            },
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
            ReplayInner::Scored {
                results,
                index,
                boost,
            } => {
                if *index < results.len() {
                    -results[*index].1 * *boost
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
