//! A generic, single-threaded *k*-nearest-neighbor graph: a flat vector arena
//! plus fixed-degree adjacency. It is the storage substrate for graph-based
//! approximate-nearest-neighbor indexes — the [`RelativeNeighborhoodGraph`]
//! built on top of it below — and carries no edge semantics of its own beyond
//! "node `i`'s nearest neighbors, in order".
//!
//! - Node ids are dense indices straight into the backing arrays. The node set is fixed at
//!   construction: the arena's length determines the node count, and every node starts with no
//!   edges.
//! - Adjacency is one flat array: node `i` owns the contiguous, best-first (most similar first),
//!   [`EMPTY`]-padded run `neighbors[i * max_edges ..][.. max_edges]`.
//! - Edges store only ids. [`Similarity`] scores drive bounded top-*k* insertion at build time but
//!   aren't durable — the order is baked in and search rescores against the live query. A graph
//!   reconstructed from disk ([`Graph::for_reload`]) carries no similarity buffer and is filled in
//!   stored order via [`Graph::push_edge`].
//!
//! `Graph<S>` never owns vector data of its own: `S` is any [`VectorArena`] —
//! a flat, `dim`-strided arena where node `i`'s vector is
//! `vectors[i * dim ..][.. dim]`. A build borrows the clusterer's matrix
//! (`S = &[f32]`); a reload can wrap owned or file-resident storage. Scoring
//! goes through [`VectorArena::similarity`]; the graph itself has no notion
//! of a metric and only ever *compares* the [`Similarity`] values handed to it.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::io::{self, Write};
use std::ops::Deref;

use common::{BinarySerializable, BitSet};

use super::partition;
use crate::schema::Metric;
use crate::vector::{Similarity, VectorArena, VectorElement};
use crate::Executor;

/// A dense node identifier, indexing straight into the backing arrays.
pub type NodeId = u32;

/// Sentinel marking an unused neighbor slot; node ids never reach [`NodeId::MAX`].
pub const EMPTY: NodeId = NodeId::MAX;

/// A single-threaded *k*-nearest-neighbor graph over `dim`-dimensional vectors
/// stored in the arena `S` (any [`VectorArena`], typed or byte-backed).
///
/// See the [module docs](self) for the layout and design rationale.
pub struct Graph<S> {
    /// Maximum out-degree per node (the *k* in *k*-NN).
    max_edges: usize,
    /// Vector dimensionality; the stride of the `vectors` arena.
    dim: usize,
    /// Flat vector arena: node `i`'s vector is `vectors[i * dim ..][.. dim]`.
    /// One contiguous buffer, indexed by node id, borrowed or owned via `S`.
    vectors: S,
    /// Flat adjacency: node `i` owns `neighbors[i * max_edges ..][.. max_edges]`,
    /// sorted best-first (most similar first) and [`EMPTY`]-padded. The durable
    /// search structure.
    neighbors: Vec<NodeId>,
    /// Per-edge similarities driving top-*k* eviction during construction.
    /// Empty for a graph reconstructed via [`for_reload`](Graph::for_reload).
    sims: Vec<Similarity>,
}

impl<S: VectorArena> Graph<S> {
    /// Creates a build graph over `vectors`, a flat `dim`-strided arena whose
    /// length fixes the node count. Every node starts with no edges; the flat
    /// edge arrays are allocated here, once. Panics if `vectors` is not a
    /// multiple of `dim` long.
    pub fn new(vectors: S, dim: usize, max_edges: usize) -> Self {
        let n = Self::node_count(&vectors, dim, max_edges);
        Graph {
            max_edges,
            dim,
            vectors,
            neighbors: vec![EMPTY; n * max_edges],
            sims: vec![Similarity::WORST; n * max_edges],
        }
    }

    /// Creates a graph for reconstruction from disk: same shape as
    /// [`new`](Graph::new) but with no similarity buffer. Edges are filled in
    /// their stored, best-first order via [`push_edge`](Graph::push_edge);
    /// [`add_edge`](Graph::add_edge) must not be used.
    pub fn for_reload(vectors: S, dim: usize, max_edges: usize) -> Self {
        let n = Self::node_count(&vectors, dim, max_edges);
        Graph {
            max_edges,
            dim,
            vectors,
            neighbors: vec![EMPTY; n * max_edges],
            sims: Vec::new(),
        }
    }

    /// Reconstructs a graph serialized by [`serialize`](Graph::serialize)
    /// over `vectors` — the arena is persisted separately, and its length
    /// fixes the node count the adjacency is validated against. The stored
    /// adjacency is exactly the in-memory layout, so this is a
    /// validate-and-decode, not a rebuild; the result carries no similarity
    /// buffer and is search-only.
    pub fn open(adjacency: &[u8], vectors: S, dim: usize) -> io::Result<Graph<S>> {
        let mut cursor = adjacency;
        let max_edges = u32::deserialize(&mut cursor)? as usize;
        if max_edges == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "serialized graph has zero max_edges",
            ));
        }
        let n = Self::node_count(&vectors, dim, max_edges);
        let expected = n * max_edges * std::mem::size_of::<NodeId>();
        if cursor.len() != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "serialized graph adjacency is {} bytes, expected {expected} for {n} nodes",
                    cursor.len()
                ),
            ));
        }
        let neighbors: Vec<NodeId> = cursor
            .chunks_exact(std::mem::size_of::<NodeId>())
            .map(|chunk| NodeId::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        Ok(Graph {
            max_edges,
            dim,
            vectors,
            neighbors,
            sims: Vec::new(),
        })
    }

    /// Validates the constructor arguments and derives the node count.
    fn node_count(vectors: &S, dim: usize, max_edges: usize) -> usize {
        assert!(max_edges > 0, "max_edges must be non-zero");
        assert!(dim > 0, "dim must be non-zero");
        let n = vectors.num_vectors(dim);
        // Ids must stay below the EMPTY sentinel (NodeId::MAX).
        assert!(n < NodeId::MAX as usize, "arena exceeds NodeId space");
        n
    }

    /// Considers the directed edge `from -> to`, keeping it only if `from` has a
    /// free slot or `sim` beats its least-similar neighbor (which is evicted) —
    /// so each node retains its `max_edges` most similar, best-first. Only
    /// `from`'s adjacency is touched; the builder adds the reverse edge for
    /// symmetry.
    ///
    /// Re-adding an existing `to` keeps the more similar score; self-edges are
    /// ignored. Only valid on a build graph ([`new`](Graph::new)); use
    /// [`push_edge`](Graph::push_edge) on a reloaded one.
    pub fn add_edge(&mut self, from: NodeId, to: NodeId, sim: Similarity) {
        debug_assert_eq!(
            self.sims.len(),
            self.neighbors.len(),
            "add_edge requires the build-time similarity buffer; use push_edge"
        );
        debug_assert!((from as usize) < self.len(), "from out of range");
        debug_assert!((to as usize) < self.len(), "to out of range");
        self.edge_list_mut(from).add_edge(to, sim);
    }

    /// Mutable view of `node`'s edge list.
    fn edge_list_mut(&mut self, node: NodeId) -> EdgeListMut<'_> {
        let k = self.max_edges;
        let start = node as usize * k;
        EdgeListMut {
            node,
            neighbors: &mut self.neighbors[start..start + k],
            sims: &mut self.sims[start..start + k],
        }
    }

    /// Mutable views of every node's edge list, in id order. The views are
    /// disjoint, so they can be split across threads and mutated concurrently
    /// without locks. Only valid on a build graph ([`new`](Graph::new)), which
    /// has the similarity buffer.
    pub(crate) fn edge_lists_mut(&mut self) -> impl Iterator<Item = EdgeListMut<'_>> {
        debug_assert_eq!(
            self.sims.len(),
            self.neighbors.len(),
            "edge_lists_mut requires the build-time similarity buffer"
        );
        let k = self.max_edges;
        self.neighbors
            .chunks_mut(k)
            .zip(self.sims.chunks_mut(k))
            .enumerate()
            .map(|(node, (neighbors, sims))| EdgeListMut {
                node: node as NodeId,
                neighbors,
                sims,
            })
    }

    /// Blindly appends `to` as `from`'s next neighbor, with no top-*k* or
    /// similarity rules. For reconstructing a graph whose edges are already
    /// stored in best-first order. Panics if `from` already has `max_edges`
    /// neighbors.
    pub fn push_edge(&mut self, from: NodeId, to: NodeId) {
        debug_assert!((from as usize) < self.len(), "from out of range");
        let k = self.max_edges;
        let degree = self.degree(from);
        assert!(degree < k, "node already has max_edges neighbors");
        self.neighbors[from as usize * k + degree] = to;
    }

    /// Overwrites `node`'s adjacency with `neighbors` (already in the desired,
    /// best-first order), padding the remaining slots with [`EMPTY`]. Used by
    /// the RNG rebuild to replace a node's edge set in one shot.
    ///
    /// Does not maintain the build-time similarity buffer, so it must not be
    /// interleaved with [`add_edge`](Graph::add_edge) on the same node.
    pub fn set_neighbors(&mut self, node: NodeId, neighbors: &[NodeId]) {
        let k = self.max_edges;
        assert!(neighbors.len() <= k, "too many neighbors for node");
        debug_assert!((node as usize) < self.len(), "node out of range");
        let base = node as usize * k;
        let run = &mut self.neighbors[base..base + k];
        run[..neighbors.len()].copy_from_slice(neighbors);
        run[neighbors.len()..].fill(EMPTY);
    }

    /// The number of neighbors currently recorded for `node`.
    #[inline]
    pub fn degree(&self, node: NodeId) -> usize {
        let base = node as usize * self.max_edges;
        self.neighbors[base..base + self.max_edges]
            .iter()
            .take_while(|&&n| n != EMPTY)
            .count()
    }

    /// Borrows `node`'s neighbor ids, best-first. Excludes empty slots.
    #[inline]
    pub fn neighbors(&self, node: NodeId) -> &[NodeId] {
        let base = node as usize * self.max_edges;
        &self.neighbors[base..base + self.degree(node)]
    }

    /// The number of nodes in the graph.
    #[inline]
    pub fn len(&self) -> usize {
        self.vectors.num_vectors(self.dim)
    }

    /// Whether the graph has no nodes.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The vector dimensionality.
    #[inline]
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// The maximum out-degree (the *k* in *k*-NN).
    #[inline]
    pub fn max_edges(&self) -> usize {
        self.max_edges
    }

    /// Writes the durable part of the graph — `max_edges`, then the flat
    /// adjacency exactly as held in memory — as little-endian `u32`s:
    ///
    /// ```text
    /// max_edges (u32) + neighbors (u32[len · max_edges], best-first,
    ///                              EMPTY-padded runs of max_edges per node)
    /// ```
    ///
    /// Neither the vectors nor the node count are written: the arena is
    /// persisted (and the count derived) elsewhere, and a reload wraps it via
    /// [`for_reload`](Graph::for_reload). Similarities aren't durable at all —
    /// see the [module docs](self).
    pub fn serialize<W: Write + ?Sized>(&self, out: &mut W) -> io::Result<()> {
        let max_edges = u32::try_from(self.max_edges)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "max_edges exceeds u32"))?;
        max_edges.serialize(out)?;
        for &neighbor in &self.neighbors {
            neighbor.serialize(out)?;
        }
        Ok(())
    }

    /// Borrows the arena storage. For `Copy` storage like `&[T]`, dereferencing
    /// the borrow yields a reference with the *arena's* lifetime, so the TPT
    /// build can read vectors while mutating edge lists.
    #[inline]
    pub fn arena(&self) -> &S {
        &self.vectors
    }
}

/// Typed-arena views: only `[T]`-shaped storage can hand out `&[T]` borrows
/// (file bytes have no alignment guarantee).
impl<T, S: Deref<Target = [T]>> Graph<S> {
    /// Borrows `node`'s vector — a contiguous `dim`-length slice of the arena.
    #[inline]
    pub fn payload(&self, node: NodeId) -> &[T] {
        let start = node as usize * self.dim;
        &self.vectors[start..start + self.dim]
    }

    /// Iterates every node's vector in id order; pair with
    /// [`Iterator::enumerate`] to recover the [`NodeId`].
    #[inline]
    pub fn iter(&self) -> std::slice::ChunksExact<'_, T> {
        self.vectors.chunks_exact(self.dim)
    }
}

impl<'a, T: 'a, S: Deref<Target = [T]>> IntoIterator for &'a Graph<S> {
    type Item = &'a [T];
    type IntoIter = std::slice::ChunksExact<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Mutable view of one node's edge list: its neighbor-id and similarity runs,
/// best-first. Views of different nodes are disjoint, so a set of them
/// (from [`Graph::edge_lists_mut`]) can be mutated by different threads
/// without locks.
pub(crate) struct EdgeListMut<'a> {
    /// The node this list belongs to; self-edges to it are rejected.
    node: NodeId,
    neighbors: &'a mut [NodeId],
    sims: &'a mut [Similarity],
}

impl EdgeListMut<'_> {
    /// Considers the directed edge `self.node -> to` — the same bounded
    /// best-first insert as [`Graph::add_edge`], which delegates here.
    pub(crate) fn add_edge(&mut self, to: NodeId, sim: Similarity) {
        if to == self.node {
            return;
        }

        // Reject when the list is full and this edge is no more similar than
        // the least-similar neighbor. (Empty slots hold `Similarity::WORST`,
        // which any real score beats.)
        let last = self.sims.len() - 1;
        if sim <= self.sims[last] {
            return;
        }

        // Deduplicate: if `to` is already a neighbor, keep only the more
        // similar copy and let it bubble back into sorted position.
        if let Some(pos) = self.neighbors.iter().position(|&n| n == to) {
            if sim <= self.sims[pos] {
                return;
            }
            self.sims[pos] = sim;
            let mut j = pos;
            while j > 0 && self.sims[j - 1] < self.sims[j] {
                self.neighbors.swap(j - 1, j);
                self.sims.swap(j - 1, j);
                j -= 1;
            }
            return;
        }

        // Insertion sort: slide `sim` into place from the back, shifting less
        // similar entries down and dropping whatever falls off the last slot.
        let mut j = last;
        while j > 0 && self.sims[j - 1] < sim {
            self.neighbors[j] = self.neighbors[j - 1];
            self.sims[j] = self.sims[j - 1];
            j -= 1;
        }
        self.neighbors[j] = to;
        self.sims[j] = sim;
    }
}

/// Why a [`RelativeNeighborhoodGraph::search`] stopped expanding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchTerminationReason {
    /// The best unexpanded candidate could not beat the worst kept result of
    /// a full beam: the search converged.
    SearchConverged,
    /// The frontier drained before the beam converged: every node reachable
    /// from the seeds was visited.
    GraphExhausted,
}

/// Per-query cost and convergence counters returned by
/// [`RelativeNeighborhoodGraph::search`].
#[derive(Clone, Copy, Debug)]
pub struct NeighborhoodGraphSearchMetrics {
    /// Nodes visited — and therefore scored — by the query; the search's
    /// navigation cost.
    pub visited_count: usize,
    /// Frontier candidates that survived the convergence check and had their
    /// adjacency scanned: the number of hops the search took. On a resumed
    /// [`SearchIterator`], evictions re-admitted through the frontier rescan
    /// their (fully visited) adjacency and are counted again.
    pub expanded_count: usize,
    /// Neighbor entries iterated across all expansions, including
    /// already-visited ones; `edges_scanned - visited_count` is traversal
    /// spent on redundant edges.
    pub edges_scanned: usize,
    /// Candidates that displaced a worse result after the beam filled. Low
    /// churn means the beam width was generous; constant churn means it was
    /// tight.
    pub evictions: usize,
    /// Candidates actually returned: after truncation to `k` for
    /// [`search`](RelativeNeighborhoodGraph::search), yielded so far for a
    /// [`SearchIterator`].
    pub result_count: usize,
    /// Why the expansion loop stopped.
    pub termination_reason: SearchTerminationReason,
}

impl Default for NeighborhoodGraphSearchMetrics {
    fn default() -> Self {
        Self {
            visited_count: 0,
            expanded_count: 0,
            edges_scanned: 0,
            evictions: 0,
            result_count: 0,
            termination_reason: SearchTerminationReason::GraphExhausted,
        }
    }
}

// ============================================================
// The relative neighborhood graph (RNG) index over a Graph.
// ============================================================
// `Graph` is the pure storage layer; `RelativeNeighborhoodGraph` layers on the
// metric and the search/build parameters. Per-query working buffers live in a
// caller-owned `Workspace` rather than being borrowed from the index, so
// `search` needs only `&self`: queries can run concurrently, which is what
// lets `build` and `refine` fan their per-leaf / per-node search work across
// an `Executor`, serializing only the cheap graph mutation.

/// Tuning knobs for a [`RelativeNeighborhoodGraph`].
#[derive(Clone, Copy, Debug)]
pub struct NeighborhoodGraphConfig {
    /// Maximum out-degree per node (the *k* in *k*-NN graph).
    pub max_edges: usize,
    /// Beam width for query-time search; the effective beam is `max(ef, k)`.
    pub ef: usize,
    /// Size of the candidate pool gathered per node during [`refine`]: each
    /// node runs a top-`num_candidates` search of the current graph, and those
    /// candidates feed RNG edge selection.
    ///
    /// [`refine`]: RelativeNeighborhoodGraph::refine
    pub num_candidates: usize,
    /// Number of independent TPT partitions [`build`] unions to seed the initial
    /// KNN graph. Each tree splits along different random directions; unioning
    /// their per-leaf edges stitches across any single tree's split boundaries,
    /// so more trees means fewer missed neighbors (better init recall) at linear
    /// build cost.
    ///
    /// [`build`]: RelativeNeighborhoodGraph::build
    pub num_trees: usize,
}

impl Default for NeighborhoodGraphConfig {
    fn default() -> Self {
        NeighborhoodGraphConfig {
            max_edges: 32,
            ef: 64,
            num_candidates: 256,
            num_trees: 32,
        }
    }
}

/// A beam search over a [`RelativeNeighborhoodGraph`] that yields
/// [`Candidate`]s on demand.
///
/// Results arrive in converged batches: each batch is one beam round run to
/// convergence, drained most similar first by [`next`](Iterator::next). Once
/// a batch is empty, pulling again initiates another round from where the
/// search stopped.
///
/// `RESUMABLE` selects the eviction policy at compile time. When `true` (the
/// [`ResumableSearchIterator`] returned by
/// [`search_iter`](RelativeNeighborhoodGraph::search_iter)), no scored node
/// is ever dropped — candidates evicted from the beam return to the frontier
/// — so every reachable node is yielded exactly once and the stream ends only
/// when the graph is exhausted. When `false`, evictions are dropped: the beam
/// minimum only rises within a round, so a search that stops at its first
/// converged round — the one-shot
/// [`search`](RelativeNeighborhoodGraph::search) — never needs them again and
/// skips the bookkeeping.
///
/// Each batch is yielded most similar first, but the stream as a whole is not
/// globally sorted: a resumed round can discover a node more similar than one
/// already yielded.
pub struct SearchIterator<'g, 'w, S: VectorArena, const RESUMABLE: bool> {
    rng: &'g RelativeNeighborhoodGraph<S>,
    workspace: &'w mut Workspace,
    query: &'g [S::Elem],
    /// Beam width of each round.
    ef: usize,
    /// The current converged batch, sorted ascending so popping from the back
    /// yields most similar first.
    batch: Vec<Candidate>,
    /// Counters accumulated across all rounds so far.
    metrics: NeighborhoodGraphSearchMetrics,
}

/// A [`SearchIterator`] that retains beam evictions so it can keep yielding
/// past its first converged round, to graph exhaustion. Returned by
/// [`search_iter`](RelativeNeighborhoodGraph::search_iter).
pub type ResumableSearchIterator<'g, 'w, S> = SearchIterator<'g, 'w, S, true>;

/// A [`SearchIterator`] that drops beam evictions; only sound for a single
/// converged round, which is exactly how the one-shot
/// [`search`](RelativeNeighborhoodGraph::search) drives it.
type OneShotSearchIterator<'g, 'w, S> = SearchIterator<'g, 'w, S, false>;

impl<'g, 'w, S: VectorArena, const RESUMABLE: bool> SearchIterator<'g, 'w, S, RESUMABLE> {
    fn new(
        rng: &'g RelativeNeighborhoodGraph<S>,
        workspace: &'w mut Workspace,
        query: &'g [S::Elem],
        seeds: &[NodeId],
        ef: usize,
    ) -> Self {
        debug_assert_eq!(query.len(), rng.graph.dim(), "query dimension mismatch");
        let n = rng.graph.len();
        workspace.begin_query(n);

        let arena = rng.graph.arena();
        let dim = rng.graph.dim();
        let mut metrics = NeighborhoodGraphSearchMetrics::default();

        for &node_id in seeds {
            if node_id as usize >= n || workspace.visited.contains(node_id) {
                continue;
            }
            workspace.visited.insert(node_id);
            metrics.visited_count += 1;
            let sim = arena.similarity(rng.metric, dim, node_id, query);
            workspace.frontier.push(Candidate { sim, node: node_id });
        }

        SearchIterator {
            rng,
            workspace,
            query,
            ef,
            batch: Vec::new(),
            metrics,
        }
    }

    /// The counters accumulated so far: totals across every round run to this
    /// point, with `result_count` counting candidates actually yielded and
    /// `termination_reason` describing why the latest round stopped.
    pub fn metrics(&self) -> NeighborhoodGraphSearchMetrics {
        self.metrics
    }

    /// Runs one beam round to convergence and drains it into `self.batch`.
    fn run_round(&mut self) {
        let graph = &self.rng.graph;
        let arena = graph.arena();
        let dim = graph.dim();
        let metric = self.rng.metric;
        let ws = &mut *self.workspace;

        self.metrics.termination_reason = SearchTerminationReason::GraphExhausted;

        while let Some(&candidate) = ws.frontier.peek() {
            // Stop once the best unexpanded candidate can't beat the worst
            // kept result and the result set is already full. Whole-candidate
            // comparisons (not raw sims) make the order strict: two distinct
            // nodes are never equal, so a node tied with the beam minimum
            // can't keep displacing it and cycle forever. Peek, don't pop:
            // the candidate stays for the next round to commit.
            if ws.results.len() >= self.ef
                && ws.results.peek().is_some_and(|worst| candidate < worst.0)
            {
                self.metrics.termination_reason = SearchTerminationReason::SearchConverged;
                break;
            }

            ws.frontier.pop();
            if ws.results.len() < self.ef {
                ws.results.push(Reverse(candidate));
            } else if let Some(mut worst) = ws.results.peek_mut() {
                let evicted = std::mem::replace(&mut *worst, Reverse(candidate)).0;
                drop(worst);
                if RESUMABLE {
                    ws.frontier.push(evicted);
                }
                self.metrics.evictions += 1;
            }

            // Expand. An eviction re-admitted through the frontier scans its
            // adjacency a second time, but every neighbor it could contribute
            // was already visited by its first expansion, so the rescan is a
            // no-op beyond the visited checks (still counted as scanned
            // edges, since the work is done).
            let neighbors = graph.neighbors(candidate.node);
            self.metrics.expanded_count += 1;
            self.metrics.edges_scanned += neighbors.len();

            for &neighbor in neighbors {
                if ws.visited.contains(neighbor) {
                    continue;
                }
                ws.visited.insert(neighbor);
                self.metrics.visited_count += 1;

                let sim = arena.similarity(metric, dim, neighbor, self.query);
                ws.frontier.push(Candidate {
                    sim,
                    node: neighbor,
                });
            }
        }

        self.batch.extend(ws.results.drain().map(|Reverse(c)| c));
        // Ascending similarity with descending-id ties, so popping from the
        // back yields descending similarity with ascending-id ties.
        self.batch
            .sort_unstable_by(|a, b| a.sim.cmp(&b.sim).then_with(|| b.node.cmp(&a.node)));
    }
}

impl<S: VectorArena, const RESUMABLE: bool> Iterator for SearchIterator<'_, '_, S, RESUMABLE> {
    type Item = Candidate;

    fn next(&mut self) -> Option<Self::Item> {
        if self.batch.is_empty() {
            // On an exhausted graph the frontier is empty and this is a cheap
            // no-op; the batch stays empty and the stream ends below.
            self.run_round();
        }
        let candidate = self.batch.pop()?;
        self.metrics.result_count += 1;
        Some(candidate)
    }
}

/// A relative neighborhood graph (RNG) index over vector storage `S` (any
/// [`VectorArena`]); queries are `&[S::Elem]` of the same dimension.
///
/// Scoring goes through [`VectorArena::similarity`], so each storage shape
/// uses its native kernel. This type owns the metric and parameters;
/// per-query scratch is supplied by the caller as a [`Workspace`].
pub struct RelativeNeighborhoodGraph<S> {
    /// Vector arena and directed adjacency.
    graph: Graph<S>,
    /// Similarity metric. Search results and stored edges share one ranking
    /// convention: descending [`Similarity`], best first.
    metric: Metric,
    /// Search, build, and refine tuning knobs.
    config: NeighborhoodGraphConfig,
}

impl<S: VectorArena> RelativeNeighborhoodGraph<S> {
    /// Creates an edge-less index over `vectors`, a flat `dim`-strided arena
    /// whose length fixes the node count, using `metric` and the given tuning
    /// `params`.
    pub fn new(vectors: S, dim: usize, metric: Metric, params: NeighborhoodGraphConfig) -> Self {
        RelativeNeighborhoodGraph {
            graph: Graph::new(vectors, dim, params.max_edges),
            metric,
            config: params,
        }
    }

    /// Greedy beam search for the `k` nodes most similar to `query`, expanding
    /// outward from `seeds`. Returns [`Candidate`]s most similar first, with a
    /// beam width of `max(ef, k)`, plus the query's
    /// [`NeighborhoodGraphSearchMetrics`].
    pub fn search(
        &self,
        ws: &mut Workspace,
        query: &[S::Elem],
        seeds: &[NodeId],
        k: usize,
    ) -> (Vec<Candidate>, NeighborhoodGraphSearchMetrics) {
        if self.graph.is_empty() || k == 0 {
            return (Vec::new(), NeighborhoodGraphSearchMetrics::default());
        }
        let mut iter = OneShotSearchIterator::new(self, ws, query, seeds, self.config.ef.max(k));
        let out: Vec<Candidate> = iter.by_ref().take(k).collect();
        let metrics = iter.metrics();
        (out, metrics)
    }

    /// Resumable beam search: like [`search`](Self::search), but instead of a
    /// fixed `k` the returned [`SearchIterator`] yields [`Candidate`]s on
    /// demand, resuming expansion whenever the caller pulls past what has been
    /// found so far — see [`SearchIterator`] for the batch and ordering
    /// semantics. Callers that don't know their cutoff up front (e.g.
    /// adaptive cluster probing) can start with a beam of
    /// [`ef`](NeighborhoodGraphConfig::ef) and keep pulling instead of paying
    /// for a worst-case beam width.
    ///
    /// `ws` is reset here and borrowed for the iterator's lifetime;
    /// [`SearchIterator::metrics`] exposes the accumulated counters at any
    /// point.
    pub fn search_iter<'g, 'w>(
        &'g self,
        ws: &'w mut Workspace,
        query: &'g [S::Elem],
        seeds: &[NodeId],
    ) -> ResumableSearchIterator<'g, 'w, S> {
        ResumableSearchIterator::new(self, ws, query, seeds, self.config.ef)
    }

    /// Writes the durable part of the index — the inner [`Graph`]'s adjacency;
    /// see [`Graph::serialize`] for the format. The metric and tuning knobs are
    /// configuration, not data, so they are not persisted.
    pub fn serialize<W: Write + ?Sized>(&self, out: &mut W) -> io::Result<()> {
        self.graph.serialize(out)
    }

    /// Opens a serialized RNG (see [`serialize`](Self::serialize)) over
    /// `vectors` — typically a
    /// [`FileSliceArena`](crate::vector::FileSliceArena) so search fetches centroid
    /// rows lazily. The metric and knobs were never persisted: supply the
    /// same `metric` the graph was built with; of `params`, only `ef`
    /// affects search (the persisted `max_edges` governs the adjacency).
    /// The result is search-only — see [`Graph::open`].
    pub fn open(
        adjacency: &[u8],
        vectors: S,
        dim: usize,
        metric: Metric,
        params: NeighborhoodGraphConfig,
    ) -> io::Result<Self> {
        Ok(RelativeNeighborhoodGraph {
            graph: Graph::open(adjacency, vectors, dim)?,
            metric,
            config: params,
        })
    }

    /// The number of nodes in the graph.
    pub fn len(&self) -> usize {
        self.graph.len()
    }

    /// Whether the graph has no nodes.
    pub fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }
}

/// Refinement requires typed storage: a node's stored vector doubles as its
/// search query, and edge selection scores stored vectors against each other.
/// A graph over raw file bytes is search-only.
impl<T: VectorElement, S: Deref<Target = [T]>> RelativeNeighborhoodGraph<S> {
    /// Refines every node against the current graph: each node searches from
    /// itself to gather a candidate pool, applies the RNG occlusion rule to
    /// reselect its edges, and the new adjacencies are written back. This pass
    /// is what turns a raw KNN graph into an RNG.
    ///
    /// The search-and-select phase is read-only over the graph, so it runs in
    /// parallel on the `executor`; the write-back is applied serially
    /// afterward. Every node reads the same pre-pass snapshot — a
    /// *synchronous* refinement, the shape that parallelizes.
    pub fn refine(&mut self, executor: &Executor)
    where S: Sync {
        let len = self.graph.len();
        if len == 0 {
            return;
        }

        // Phase 1 (parallel, read-only): each node searches the snapshot and
        // RNG-selects its new neighbors. One chunk per executor thread;
        // `max(1)` guards more threads than nodes.
        let chunk = (len / executor.num_threads()).max(1);
        let ranges = (0..len)
            .step_by(chunk)
            .map(|s| (s as NodeId, (s + chunk).min(len) as NodeId));
        let chunked_selected: Vec<Vec<Vec<NodeId>>> = {
            let rng = &*self;
            executor
                .map(
                    move |(start, end): (NodeId, NodeId)| {
                        let mut ws = Workspace::new();
                        let mut out = Vec::with_capacity((end - start) as usize);
                        for node in start..end {
                            let query = rng.graph.payload(node);
                            let (candidates, _) =
                                rng.search(&mut ws, query, &[node], rng.config.num_candidates);
                            out.push(rng.select_neighbors(node, &candidates));
                        }
                        Ok(out)
                    },
                    ranges,
                )
                .expect("refine search panicked")
        };

        // Phase 2 (serial): write each node's selection back. Disjoint per node
        // and a bounded copy each, so the serial cost is negligible.
        let mut node: NodeId = 0;
        for chunk in &chunked_selected {
            for selected in chunk {
                self.graph.set_neighbors(node, selected);
                node += 1;
            }
        }
    }

    /// Applies the relative-neighborhood-graph occlusion rule to `candidates`
    /// (nearest-first) and returns the survivors — `node`'s new adjacency, at most
    /// `max_edges`, skipping `node` itself. Read-only, so it can run concurrently
    /// across nodes; the caller writes the result back into the graph.
    ///
    /// Everything is in similarity space (higher is better): a candidate `c` is
    /// kept unless some already-selected neighbor `r` is *more* similar to `c`
    /// than `node` is — then `r` makes the direct `node -> c` edge redundant and
    /// occludes it (the classic RNG "lune" emptiness test). The comparison is
    /// non-strict (`<=`), so an `r` *exactly* as similar as `node` does not
    /// occlude — the canonical RNG definition, and what keeps duplicate vectors
    /// from wiping out a node's whole edge set.
    fn select_neighbors(&self, node: NodeId, candidates: &[Candidate]) -> Vec<NodeId> {
        let max_edges = self.config.max_edges;
        let mut selected: Vec<NodeId> = Vec::with_capacity(max_edges);
        for &Candidate { sim, node: cand } in candidates {
            if cand == node {
                continue; // the query node itself is never its own neighbor
            }
            if selected.len() >= max_edges {
                break;
            }
            let cand_vec = self.graph.payload(cand);
            let keep = selected
                .iter()
                .all(|&r| self.metric.similarity(self.graph.payload(r), cand_vec) <= sim);
            if keep {
                selected.push(cand);
            }
        }

        debug_assert!(!selected.is_empty(), "selected nodes should not be empty");
        selected
    }
}

/// Build is `f32`-only and borrow-only for now: the TPT partitioner does
/// floating-point math over the vectors, and `&[f32]` is `Copy`, so the arena
/// can be read while edge lists are mutated. The rest of the index stays
/// generic over [`VectorArena`] storage.
impl RelativeNeighborhoodGraph<&[f32]> {
    /// Builds the RNG index over the borrowed arena: seeds a raw KNN graph with
    /// a TPT forest, then prunes it into an RNG. Expects a freshly constructed,
    /// edge-less index.
    pub fn build(&mut self, executor: &Executor) {
        self.build_init_knn(executor);
        self.refine(executor);
    }

    /// Seeds the raw KNN graph: unions a forest of
    /// [`num_trees`](NeighborhoodGraphConfig::num_trees) TPT partitions,
    /// brute-forcing KNN within each leaf. Leaves run in parallel on the
    /// `executor`, writing edges directly into their members' edge lists —
    /// leaves partition the nodes, so the lists touched by different leaves are
    /// disjoint, and each list keeps only the node's nearest
    /// [`max_edges`](NeighborhoodGraphConfig::max_edges), so memory stays
    /// bounded by the graph itself.
    fn build_init_knn(&mut self, executor: &Executor) {
        let vectors = *self.graph.arena();
        let dim = self.graph.dim();
        let n = self.graph.len();
        if n == 0 {
            return;
        }

        // One TPTree reused across trees: its RNG advances between partitions,
        // so each tree splits along different directions.
        let metric = self.metric;
        let mut tpt = partition::TPTree::new(partition::TPTreeConfig::default(), dim, vectors);
        let mut indices: Vec<NodeId> = (0..n as NodeId).collect();
        for _ in 0..self.config.num_trees {
            let leaves = tpt.partition(&mut indices);

            // Rearrange the graph's edge-list borrows into `indices` order.
            // `indices` is a permutation, so every list is claimed exactly once,
            // and afterwards the leaf ranges partition `edge_lists` — the
            // disjointness the borrow checker needs to let leaves run in
            // parallel.
            let mut unclaimed: Vec<Option<EdgeListMut>> =
                self.graph.edge_lists_mut().map(Some).collect();
            let mut edge_lists: Vec<EdgeListMut> = indices
                .iter()
                .map(|&node| {
                    unclaimed[node as usize]
                        .take()
                        .expect("indices is a permutation")
                })
                .collect();

            // One task per leaf: its member ids and their edge lists.
            // `partition` returns the leaves in order, tiling `0..n`.
            let mut leaf_tasks: Vec<(&[NodeId], &mut [EdgeListMut])> =
                Vec::with_capacity(leaves.len());
            let mut rest = edge_lists.as_mut_slice();
            for leaf in &leaves {
                let (leaf_lists, tail) = std::mem::take(&mut rest).split_at_mut(leaf.len());
                leaf_tasks.push((&indices[leaf.clone()], leaf_lists));
                rest = tail;
            }
            debug_assert!(rest.is_empty(), "leaves must tile all of indices");

            // Each leaf brute-forces its pairwise similarities and inserts both
            // directions of each edge; the lists dedup re-encounters across
            // trees and keep only the best `max_edges`.
            executor
                .map(
                    move |(members, edge_lists): (&[NodeId], &mut [EdgeListMut])| {
                        for i in 0..members.len() {
                            let vec_a = &vectors[members[i] as usize * dim..][..dim];
                            for j in (i + 1)..members.len() {
                                let vec_b = &vectors[members[j] as usize * dim..][..dim];
                                let sim = metric.similarity(vec_a, vec_b);
                                edge_lists[i].add_edge(members[j], sim);
                                edge_lists[j].add_edge(members[i], sim);
                            }
                        }
                        Ok(())
                    },
                    leaf_tasks.into_iter(),
                )
                .expect("leaf KNN computation panicked");
        }
    }
}

/// Reusable per-query working buffers for
/// [`RelativeNeighborhoodGraph::search`]; reuse one across queries to avoid
/// reallocating.
pub struct Workspace {
    /// Nodes scored in the current query, 1 bit per node.
    visited: BitSet,
    /// Max-heap by similarity: every scored candidate not currently committed
    /// to `results` — the pool the search pops from, best first.
    frontier: BinaryHeap<Candidate>,
    /// Min-heap by similarity (via `Reverse`): the current beam — the best
    /// `width` committed results, with the least-similar on top for eviction.
    results: BinaryHeap<Reverse<Candidate>>,
}

impl Default for Workspace {
    fn default() -> Self {
        Workspace {
            visited: BitSet::with_max_value(0),
            frontier: BinaryHeap::new(),
            results: BinaryHeap::new(),
        }
    }
}

impl Workspace {
    /// Creates an empty workspace. It grows to fit on first use.
    pub fn new() -> Self {
        Workspace::default()
    }

    /// Prepares the workspace for a query over `n` nodes: zeroes the visited
    /// bitset (growing it if needed) and clears the heaps.
    fn begin_query(&mut self, n: usize) {
        if (self.visited.max_value() as usize) < n {
            self.visited = BitSet::with_max_value(n as u32);
        } else {
            self.visited.clear();
        }
        self.frontier.clear();
        self.results.clear();
    }
}

/// A `(similarity, node)` pair ordered by similarity (ties broken by node id for
/// determinism). Ordered ascending, so a plain max-heap yields most-similar
/// first and `Reverse<Candidate>` yields least-similar first. Also the element
/// type [`search`](RelativeNeighborhoodGraph::search) returns.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Candidate {
    /// Similarity to the query (higher is more similar).
    pub sim: Similarity,
    /// The graph node this candidate refers to.
    pub node: NodeId,
}

impl Eq for Candidate {}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sim
            .cmp(&other.sim)
            .then_with(|| self.node.cmp(&other.node))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a graph of `n` 1-dimensional nodes (vector = `[id]`), for terse
    /// edge tests that only care about topology.
    fn graph_with_nodes(n: NodeId, max_edges: usize) -> Graph<Vec<f32>> {
        Graph::new((0..n).map(|i| i as f32).collect(), 1, max_edges)
    }

    /// Shorthand for a raw similarity score (higher is better).
    fn sim(score: f32) -> Similarity {
        Similarity::new(score)
    }

    #[test]
    fn edge_lists_mut_allows_disjoint_parallel_writes() {
        // Two threads each own half the edge lists. The borrows are disjoint,
        // so this compiles without locks and behaves like serial add_edge.
        let mut g = graph_with_nodes(4, 2);
        let mut lists: Vec<EdgeListMut> = g.edge_lists_mut().collect();
        let (left, right) = lists.split_at_mut(2);
        std::thread::scope(|scope| {
            scope.spawn(move || {
                left[0].add_edge(1, sim(1.0));
                left[1].add_edge(0, sim(1.0));
            });
            scope.spawn(move || {
                right[0].add_edge(3, sim(1.0));
                right[1].add_edge(2, sim(1.0));
            });
        });
        drop(lists);
        assert_eq!(g.neighbors(0), &[1]);
        assert_eq!(g.neighbors(1), &[0]);
        assert_eq!(g.neighbors(2), &[3]);
        assert_eq!(g.neighbors(3), &[2]);
    }

    #[test]
    fn new_derives_nodes_from_the_arena() {
        let g: Graph<Vec<f32>> = Graph::new(vec![1.0, 2.0, 3.0, 4.0], 2, 8);
        assert_eq!(g.len(), 2);
        assert!(!g.is_empty());
        assert_eq!(g.payload(0), &[1.0, 2.0]);
        assert_eq!(g.payload(1), &[3.0, 4.0]);
        assert_eq!(g.degree(0), 0);
        assert!(g.neighbors(0).is_empty());

        let empty: Graph<Vec<f32>> = Graph::new(Vec::new(), 2, 8);
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn borrowed_storage_leaves_the_arena_with_the_caller() {
        // The merge-time shape: the graph borrows the caller's matrix, and
        // `arena` hands back a reference independent of the graph borrow — so
        // the vectors stay readable while edge lists are mutated, which is
        // exactly what the TPT build needs.
        let matrix: Vec<f32> = vec![0.0, 1.0, 2.0];
        let mut g: Graph<&[f32]> = Graph::new(&matrix, 1, 2);
        let vectors = *g.arena();
        g.add_edge(0, 1, sim(1.0)); // mutate while `vectors` is still borrowed
        assert_eq!(vectors, matrix.as_slice());
        assert_eq!(g.neighbors(0), &[1]);
    }

    #[test]
    #[should_panic(expected = "arena not a multiple of dim")]
    fn new_rejects_a_misaligned_arena() {
        let _ = Graph::new(vec![1.0f32, 2.0, 3.0], 2, 4);
    }

    #[test]
    fn edges_are_sorted_best_first() {
        let mut g = graph_with_nodes(5, 8);
        g.add_edge(0, 3, sim(0.1));
        g.add_edge(0, 1, sim(0.8));
        g.add_edge(0, 4, sim(0.5));
        g.add_edge(0, 2, sim(0.9));
        assert_eq!(g.neighbors(0), &[2, 1, 4, 3]);
        assert_eq!(g.degree(0), 4);
    }

    #[test]
    fn bounded_top_k_evicts_the_least_similar() {
        let mut g = graph_with_nodes(5, 2);
        g.add_edge(0, 1, sim(0.5));
        g.add_edge(0, 2, sim(0.6));
        // Full now with {2:0.6, 1:0.5}. A better edge evicts the worst (1).
        g.add_edge(0, 3, sim(0.9));
        assert_eq!(g.neighbors(0), &[3, 2]);
        // An edge worse than the current minimum is rejected outright.
        g.add_edge(0, 4, sim(0.1));
        assert_eq!(g.neighbors(0), &[3, 2]);
    }

    #[test]
    fn re_adding_keeps_the_more_similar_score() {
        let mut g = graph_with_nodes(4, 4);
        g.add_edge(0, 1, sim(0.2));
        g.add_edge(0, 2, sim(0.5));
        // Re-add 1 with a better score: it must move ahead of 2 and not duplicate.
        g.add_edge(0, 1, sim(0.9));
        assert_eq!(g.neighbors(0), &[1, 2]);
        // Re-add 1 with a worse score: ignored.
        g.add_edge(0, 1, sim(0.1));
        assert_eq!(g.neighbors(0), &[1, 2]);
    }

    #[test]
    fn edges_are_directed_and_self_edges_ignored() {
        let mut g = graph_with_nodes(3, 4);
        g.add_edge(0, 1, sim(0.3));
        assert_eq!(g.neighbors(0), &[1]);
        assert!(g.neighbors(1).is_empty());
        g.add_edge(2, 2, sim(1.0));
        assert!(g.neighbors(2).is_empty());
    }

    #[test]
    fn iter_yields_vectors_in_node_order() {
        let g: Graph<Vec<f32>> = Graph::new(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0], 2, 4);

        let collected: Vec<&[f32]> = g.iter().collect();
        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0], &[1.0, 2.0]);
        assert_eq!(collected[1], &[3.0, 4.0]);
        assert_eq!(collected[2], &[5.0, 6.0]);

        // `&graph` works as IntoIterator; enumerate recovers the node id.
        for (id, vector) in (&g).into_iter().enumerate() {
            assert_eq!(vector, g.payload(id as NodeId));
        }
    }

    #[test]
    fn for_reload_pushes_edges_in_stored_order() {
        let arena: Vec<f32> = (0..4).map(|i| i as f32).collect();
        let mut g = Graph::for_reload(arena, 1, 4);
        // Edges arrive already best-first; push them blindly, no similarities.
        g.push_edge(0, 1);
        g.push_edge(0, 2);
        g.push_edge(0, 3);
        assert_eq!(g.neighbors(0), &[1, 2, 3]);
        assert_eq!(g.degree(0), 3);
    }

    #[test]
    fn set_neighbors_overwrites_and_repads() {
        let mut g = graph_with_nodes(5, 4);
        g.set_neighbors(0, &[3, 1, 2]);
        assert_eq!(g.neighbors(0), &[3, 1, 2]);
        assert_eq!(g.degree(0), 3);
        // Overwriting with a SHORTER list must re-empty the freed tail slots,
        // not leave a stale id behind — the path the RNG refine relies on each
        // pass when a node's edge set shrinks.
        g.set_neighbors(0, &[4]);
        assert_eq!(g.neighbors(0), &[4]);
        assert_eq!(g.degree(0), 1);
        // The empty slice clears the adjacency entirely.
        g.set_neighbors(0, &[]);
        assert!(g.neighbors(0).is_empty());
        assert_eq!(g.degree(0), 0);
    }

    #[test]
    #[should_panic(expected = "too many neighbors")]
    fn set_neighbors_rejects_more_than_max_edges() {
        let mut g = graph_with_nodes(4, 2);
        g.set_neighbors(0, &[1, 2, 3]); // 3 > max_edges 2
    }

    /// Decodes a serialized graph back into (max_edges, neighbors) u32s.
    fn decode(bytes: &[u8]) -> (u32, Vec<NodeId>) {
        assert_eq!(
            bytes.len() % 4,
            0,
            "serialization is a whole number of u32s"
        );
        let mut words = bytes
            .chunks_exact(4)
            .map(|w| u32::from_le_bytes(w.try_into().unwrap()));
        let max_edges = words.next().expect("missing max_edges header");
        (max_edges, words.collect())
    }

    #[test]
    fn serialize_writes_max_edges_then_padded_adjacency() {
        let mut g = graph_with_nodes(3, 2);
        g.add_edge(0, 2, sim(0.2));
        g.add_edge(0, 1, sim(0.9)); // more similar: sorts ahead of 2
        g.add_edge(1, 0, sim(0.9));
        // node 2 keeps an all-EMPTY run

        let mut bytes = Vec::new();
        g.serialize(&mut bytes).unwrap();

        let (max_edges, neighbors) = decode(&bytes);
        assert_eq!(max_edges, 2);
        assert_eq!(neighbors, vec![1, 2, 0, EMPTY, EMPTY, EMPTY]);
    }

    #[test]
    fn reloaded_graph_serializes_byte_identically() {
        // The durable invariant behind slot reuse across merges: serialize →
        // reload (push edges in stored order) → serialize must be a fixed
        // point, so nothing drifts however many times a graph round-trips.
        let mut built = graph_with_nodes(4, 3);
        built.add_edge(0, 1, sim(0.9));
        built.add_edge(0, 3, sim(0.6));
        built.add_edge(2, 0, sim(0.8));

        let mut bytes = Vec::new();
        built.serialize(&mut bytes).unwrap();

        let arena: Vec<f32> = (0..4).map(|i| i as f32).collect();
        let mut reloaded = Graph::for_reload(arena, 1, built.max_edges());
        for node in 0..built.len() as NodeId {
            for &to in built.neighbors(node) {
                reloaded.push_edge(node, to);
            }
        }

        let mut reloaded_bytes = Vec::new();
        reloaded.serialize(&mut reloaded_bytes).unwrap();
        assert_eq!(bytes, reloaded_bytes);
    }
}

#[cfg(test)]
mod rng_tests {
    use super::*;

    /// A line of `n` 1-D points at positions `0..n`, each connected to its ±1 and
    /// ±2 neighbors, with edges scored by L2 similarity.
    fn line_index(n: NodeId) -> RelativeNeighborhoodGraph<Vec<f32>> {
        let params = NeighborhoodGraphConfig {
            max_edges: 4,
            ef: 8,
            num_candidates: 8,
            num_trees: 1,
        };
        let vectors: Vec<f32> = (0..n).map(|i| i as f32).collect();
        let mut rng = RelativeNeighborhoodGraph::new(vectors, 1, Metric::L2, params);
        for i in 0..n as i64 {
            for off in [-2i64, -1, 1, 2] {
                let nb = i + off;
                if (0..n as i64).contains(&nb) {
                    let sim = Metric::L2.similarity(&[i as f32], &[nb as f32]);
                    rng.graph.add_edge(i as NodeId, nb as NodeId, sim);
                }
            }
        }
        rng
    }

    #[test]
    fn search_finds_nearest_neighbors() {
        let rng = line_index(8);
        let mut ws = Workspace::new();
        // Query at 4.2 → nearest points are 4, 5, 3, in that order.
        let (res, metrics) = rng.search(&mut ws, &[4.2], &[0], 3);
        let ids: Vec<NodeId> = res.iter().map(|c| c.node).collect();
        assert_eq!(ids, vec![4, 5, 3]);
        assert_eq!(metrics.result_count, 3);
        assert!(metrics.visited_count >= 3);
        assert!(metrics.expanded_count >= 1);
        assert!(metrics.edges_scanned >= metrics.visited_count - 1);
        // Similarities are returned in descending order.
        assert!(res[0].sim >= res[1].sim && res[1].sim >= res[2].sim);
    }

    #[test]
    fn search_handles_degenerate_inputs() {
        let rng = line_index(5);
        let mut ws = Workspace::new();
        assert!(rng.search(&mut ws, &[1.0], &[0], 0).0.is_empty()); // k == 0
        assert!(rng.search(&mut ws, &[1.0], &[], 3).0.is_empty()); // no seeds

        let empty: RelativeNeighborhoodGraph<Vec<f32>> = RelativeNeighborhoodGraph::new(
            Vec::new(),
            1,
            Metric::L2,
            NeighborhoodGraphConfig::default(),
        );
        assert!(empty.search(&mut ws, &[1.0], &[0], 3).0.is_empty()); // empty graph
    }

    #[test]
    fn search_reuses_workspace_deterministically() {
        let rng = line_index(8);
        let mut ws = Workspace::new();
        let (a, _) = rng.search(&mut ws, &[4.2], &[0], 3);
        let (b, _) = rng.search(&mut ws, &[4.2], &[0], 3);
        assert_eq!(a, b); // epoch reset means repeated queries match exactly
    }

    #[test]
    fn search_iter_first_batch_matches_search() {
        let rng = line_index(8);
        let mut ws = Workspace::new();
        let (batch, _) = rng.search(&mut ws, &[4.2], &[0], 3);
        let iterated: Vec<Candidate> = rng.search_iter(&mut ws, &[4.2], &[0]).take(3).collect();
        assert_eq!(batch, iterated);
    }

    #[test]
    fn search_iter_first_batch_is_the_true_top_ef_sorted() {
        // Query at 10.2 on a 0..20 line with ef = 8: the first batch must be
        // the true top 8 in descending similarity, even though the search
        // seeds from the far end of the line.
        let rng = line_index(20);
        let mut ws = Workspace::new();
        let first_batch: Vec<NodeId> = rng
            .search_iter(&mut ws, &[10.2], &[0])
            .take(8)
            .map(|c| c.node)
            .collect();
        assert_eq!(first_batch, vec![10, 11, 9, 12, 8, 13, 7, 14]);
    }

    #[test]
    fn search_iter_resumes_past_the_first_batch() {
        // Pulling past ef (8) must resume the search and surface the next
        // nearest nodes — including beam evictions retained in the frontier.
        let rng = line_index(20);
        let mut ws = Workspace::new();
        let mut ids: Vec<NodeId> = rng
            .search_iter(&mut ws, &[10.2], &[0])
            .take(12)
            .map(|c| c.node)
            .collect();
        ids.sort_unstable();
        // The 12 pulled candidates are exactly the 12 nearest to 10.2.
        assert_eq!(ids, (5..=16).collect::<Vec<NodeId>>());
    }

    #[test]
    fn search_iter_yields_every_reachable_node_exactly_once() {
        // ef (8) < n (20): draining the iterator must keep resuming until the
        // whole connected line has been yielded, then stop.
        let n: NodeId = 20;
        let rng = line_index(n);
        let mut ws = Workspace::new();
        let mut iter = rng.search_iter(&mut ws, &[4.2], &[0]);
        let mut ids: Vec<NodeId> = iter.by_ref().map(|c| c.node).collect();

        ids.sort_unstable();
        assert_eq!(ids, (0..n).collect::<Vec<NodeId>>());

        let metrics = iter.metrics();
        assert_eq!(metrics.result_count, n as usize);
        assert_eq!(metrics.visited_count, n as usize);
        // Every node is expanded; re-admitted evictions may rescan.
        assert!(metrics.expanded_count >= n as usize);
        assert_eq!(
            metrics.termination_reason,
            SearchTerminationReason::GraphExhausted
        );
        // Exhausted iterators stay exhausted.
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn search_iter_handles_degenerate_inputs() {
        let rng = line_index(5);
        let mut ws = Workspace::new();
        assert_eq!(rng.search_iter(&mut ws, &[1.0], &[]).next(), None); // no seeds

        let empty: RelativeNeighborhoodGraph<Vec<f32>> = RelativeNeighborhoodGraph::new(
            Vec::new(),
            1,
            Metric::L2,
            NeighborhoodGraphConfig::default(),
        );
        assert_eq!(empty.search_iter(&mut ws, &[1.0], &[0]).next(), None); // empty graph
    }

    #[test]
    fn search_iter_survives_duplicate_vectors() {
        // Duplicate vectors produce boundary ties; the total order on
        // `Candidate` must keep resumed rounds from cycling and yield each
        // copy exactly once.
        let params = NeighborhoodGraphConfig {
            max_edges: 4,
            ef: 2,
            num_candidates: 4,
            num_trees: 1,
        };
        // Nodes 0..6 in a line, but every position duplicated: 0,0,1,1,2,2.
        let vectors: Vec<f32> = (0..6).map(|i| (i / 2) as f32).collect();
        let mut rng = RelativeNeighborhoodGraph::new(vectors, 1, Metric::L2, params);
        for i in 0..6u32 {
            for j in 0..6u32 {
                if i != j {
                    let sim = Metric::L2.similarity(&[(i / 2) as f32], &[(j / 2) as f32]);
                    rng.graph.add_edge(i, j, sim);
                }
            }
        }
        let mut ws = Workspace::new();
        let mut ids: Vec<NodeId> = rng
            .search_iter(&mut ws, &[0.9], &[0])
            .map(|c| c.node)
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, (0..6).collect::<Vec<NodeId>>());
    }

    #[test]
    fn search_from_a_node_returns_it_then_nearest() {
        let rng = line_index(8);
        let mut ws = Workspace::new();
        let (res, _) = rng.search(&mut ws, &[4.0], &[4], 4);
        let ids: Vec<NodeId> = res.iter().map(|c| c.node).collect();
        assert_eq!(ids[0], 4); // the query point itself ranks first
        assert!(ids[1] == 3 || ids[1] == 5); // then its nearest neighbors
    }

    fn sorted_neighbors<S: VectorArena>(
        rng: &RelativeNeighborhoodGraph<S>,
        node: NodeId,
    ) -> Vec<NodeId> {
        let mut v = rng.graph.neighbors(node).to_vec();
        v.sort_unstable();
        v
    }

    #[test]
    fn refine_applies_rng_occlusion() {
        // Colinear points 0,1,2. The RNG must drop the 0–2 edge: node 1 sits
        // between them, so 1 occludes 2 from 0 (and 0 from 2).
        let config = NeighborhoodGraphConfig {
            max_edges: 4, // room for both edges; RNG, not capacity, does the pruning
            ef: 4,
            num_candidates: 4,
            num_trees: 1,
        };
        let vectors: Vec<f32> = (0..3).map(|i| i as f32).collect();
        let mut rng = RelativeNeighborhoodGraph::new(vectors, 1, Metric::L2, config);
        // Start fully connected so each node's search sees every other node.
        for i in 0..3i64 {
            for j in 0..3i64 {
                if i != j {
                    let sim = Metric::L2.similarity(&[i as f32], &[j as f32]);
                    rng.graph.add_edge(i as NodeId, j as NodeId, sim);
                }
            }
        }

        rng.refine(&Executor::SingleThread);

        assert_eq!(sorted_neighbors(&rng, 0), vec![1]); // 0–2 occluded by 1
        assert_eq!(sorted_neighbors(&rng, 2), vec![1]); // 2–0 occluded by 1
        assert_eq!(sorted_neighbors(&rng, 1), vec![0, 2]); // middle keeps both
    }

    #[test]
    fn refine_prunes_full_mesh_to_the_optimal_path_graph() {
        // The exact RNG of n equally spaced colinear points is the path graph:
        // every node keeps only its immediate ±1 neighbors; ±2 and beyond are
        // occluded by the node in between. Starting from a full mesh, `refine`
        // must recover exactly that minimal, optimal edge set — proof the
        // occlusion rule prunes everything redundant and nothing it shouldn't.
        const N: NodeId = 6;
        let config = NeighborhoodGraphConfig {
            max_edges: 8, // far more room than the answer needs
            ef: 8,
            num_candidates: 8,
            num_trees: 1,
        };
        let vectors: Vec<f32> = (0..N).map(|i| i as f32).collect();
        let mut rng = RelativeNeighborhoodGraph::new(vectors, 1, Metric::L2, config);
        for i in 0..N as i64 {
            for j in 0..N as i64 {
                if i != j {
                    let sim = Metric::L2.similarity(&[i as f32], &[j as f32]);
                    rng.graph.add_edge(i as NodeId, j as NodeId, sim);
                }
            }
        }

        rng.refine(&Executor::SingleThread);

        assert_eq!(sorted_neighbors(&rng, 0), vec![1]);
        assert_eq!(sorted_neighbors(&rng, N - 1), vec![N - 2]);
        for i in 1..N - 1 {
            assert_eq!(sorted_neighbors(&rng, i), vec![i - 1, i + 1]);
        }
    }

    /// Fully connect every node to every other, scored by L2 similarity.
    fn fully_connect(rng: &mut RelativeNeighborhoodGraph<Vec<f32>>, pts: &[[f32; 2]]) {
        for i in 0..pts.len() {
            for j in 0..pts.len() {
                if i != j {
                    let sim = Metric::L2.similarity(&pts[i], &pts[j]);
                    rng.graph.add_edge(i as NodeId, j as NodeId, sim);
                }
            }
        }
    }

    #[test]
    fn refine_keeps_duplicate_vector_edges() {
        // Nodes 0 and 1 are identical. The occlusion is non-strict, so the
        // duplicate — exactly as similar to 2 as 0 is — must NOT occlude the
        // 0->2 edge. A strict `<` would wipe it, leaving 0 with just [1].
        let config = NeighborhoodGraphConfig {
            max_edges: 4,
            ef: 4,
            num_candidates: 4,
            num_trees: 1,
        };
        let pts = [[0.0f32, 0.0], [0.0, 0.0], [1.0, 0.0]];
        let vectors: Vec<f32> = pts.iter().flatten().copied().collect();
        let mut rng = RelativeNeighborhoodGraph::new(vectors, 2, Metric::L2, config);
        fully_connect(&mut rng, &pts);

        rng.refine(&Executor::SingleThread);

        assert_eq!(sorted_neighbors(&rng, 0), vec![1, 2]);
    }

    #[test]
    fn refine_caps_selected_neighbors_at_max_edges() {
        // Node 0 has four neighbors in four directions at distinct distances;
        // none occlude each other, so pure RNG would keep all four. With
        // max_edges = 2 the occlusion loop must stop after the two nearest.
        let config = NeighborhoodGraphConfig {
            max_edges: 2,
            ef: 8,
            num_candidates: 8,
            num_trees: 1,
        };
        let vectors: Vec<f32> = vec![
            0.0, 0.0, // 0: origin
            1.0, 0.0, // 1: dist 1  (nearest)
            0.0, 2.0, // 2: dist 4  (2nd)
            -3.0, 0.0, // 3: dist 9
            0.0, -4.0, // 4: dist 16
        ];
        let mut rng = RelativeNeighborhoodGraph::new(vectors, 2, Metric::L2, config);
        // Hand-wired connected init (max_edges = 2 each) so node 0's search can
        // still reach all four candidates despite the tight degree.
        rng.graph.set_neighbors(0, &[1, 2]);
        rng.graph.set_neighbors(1, &[0, 3]);
        rng.graph.set_neighbors(2, &[0, 4]);
        rng.graph.set_neighbors(3, &[1, 0]);
        rng.graph.set_neighbors(4, &[2, 0]);

        rng.refine(&Executor::SingleThread);

        // Nearest two kept; the farther two dropped despite being valid RNG edges.
        assert_eq!(sorted_neighbors(&rng, 0), vec![1, 2]);
    }

    #[test]
    fn build_init_knn_seeds_reciprocal_edges() {
        // The raw KNN seam before refine: 1-D line 0..6, single tree. The whole
        // set fits in one leaf, so build_init_knn does exact brute-force KNN —
        // each node's nearest is its ±1 neighbor, and every edge is inserted both
        // ways. (build() would then refine this down to the path graph.)
        let config = NeighborhoodGraphConfig {
            max_edges: 4,
            ef: 8,
            num_candidates: 8,
            num_trees: 1,
        };
        let vectors: Vec<f32> = (0..6).map(|i| i as f32).collect();
        let mut rng = RelativeNeighborhoodGraph::new(vectors.as_slice(), 1, Metric::L2, config);

        rng.build_init_knn(&Executor::single_thread());

        for i in 0..6u32 {
            let nbrs = rng.graph.neighbors(i);
            assert!(!nbrs.is_empty(), "node {i} has no edges");
            assert!(
                nbrs[0] == i.wrapping_sub(1) || nbrs[0] == i + 1,
                "node {i}'s nearest edge {} is not adjacent",
                nbrs[0]
            );
        }
        // Reciprocity: the 0–1 edge exists in both directions.
        assert!(rng.graph.neighbors(0).contains(&1));
        assert!(rng.graph.neighbors(1).contains(&0));
    }

    #[test]
    fn build_recovers_the_path_graph() {
        // Full pipeline through the single public call: build() seeds the init KNN
        // over a colinear line and refines it internally. The exact RNG of equally
        // spaced colinear points is the path graph — the same target as
        // refine_prunes_full_mesh_to_the_optimal_path_graph, but driven end-to-end
        // by build() with no separate refine().
        const N: NodeId = 6;
        let config = NeighborhoodGraphConfig {
            max_edges: 8,
            ef: 8,
            num_candidates: 8,
            num_trees: 1,
        };
        let vectors: Vec<f32> = (0..N).map(|i| i as f32).collect();
        let mut rng = RelativeNeighborhoodGraph::new(vectors.as_slice(), 1, Metric::L2, config);

        rng.build(&Executor::single_thread());

        assert_eq!(sorted_neighbors(&rng, 0), vec![1]);
        assert_eq!(sorted_neighbors(&rng, N - 1), vec![N - 2]);
        for i in 1..N - 1 {
            assert_eq!(sorted_neighbors(&rng, i), vec![i - 1, i + 1]);
        }
    }
}
