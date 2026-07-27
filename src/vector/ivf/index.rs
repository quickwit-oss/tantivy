//! The `.centroids` file and its reader, [`IvfIndex`] — the per-field IVF
//! routing index. This module owns the wire format end to end: the
//! serializers the merge calls and the [`IvfIndex::open`] that parses them
//! back sit side by side.
//!
//! The on-disk file is a 4-byte format-version stamp (see `vector::header`)
//! followed by a [`CompositeFile`](crate::directory::CompositeFile). Written
//! per field, only for IVF segments (⟺ the field's `.vec` `IdMap` is
//! `Explicit`). The composite has three slots per field:
//!
//! ```text
//! [0] num_centroids (u32) + num_docs (u32) + centroid_bytes (N · stride)
//! [1] cluster_offsets (u64[N+1], prefix sum)
//! [2] RNG over the centroids (see `Graph::serialize` for the layout;
//!     absent for degenerate centroid counts — routing then falls back to a
//!     linear scan of the centroids)
//! ```
//!
//! One dense `centroid_id = 0..N` indexes all three: `cluster_offsets[c]` is
//! the first row of cluster `c` in the parallel `.vec` rows/`IdMap`, and graph
//! node `c` is centroid `c` (its vector is row `c` of slot `[0]`, which is why
//! the graph slot stores no vectors of its own).

use std::io::{self, Write};
use std::mem;
use std::ops::Range;

use common::{BinarySerializable, HasLen, OwnedBytes};

use super::graph::{
    Candidate, NeighborhoodGraphConfig, NeighborhoodGraphSearchMetrics, NodeId,
    RelativeNeighborhoodGraph, ResumableSearchIterator, Workspace,
};
use crate::directory::FileSlice;
use crate::schema::{Metric, VectorDType, VectorOptions};
use crate::vector::{FileSliceArena, VectorArena};

/// The IVF routing index over one field's clusters: says which clusters —
/// contiguous row ranges of the `.vec` rows — a query should probe.
///
/// Pinned state is small and touched by every query: the cluster offsets and
/// the RNG adjacency (edges only, `num_centroids × max_edges × 4` bytes). The
/// centroid vectors stay behind a [`FileSliceArena`] and are fetched one node
/// at a time as routing visits them. Everything row-scale (the rows and
/// id-map) lives on [`VectorIndexReader`](crate::vector::VectorIndexReader).
pub struct IvfIndex {
    num_centroids: usize,
    /// Distinct documents with a vector in this field. Rows including
    /// replicas are [`Self::num_rows`].
    num_docs: usize,
    /// The centroid rows (slot `[0]` past the two count words).
    centroids_slice: FileSlice,
    /// Slot `[1]`: the `u64[N+1]` prefix sum, pinned.
    cluster_offsets: OwnedBytes,
    dim: usize,
    metric: Metric,
    /// The persisted RNG over the centroids (slot `[2]`). `None` for
    /// degenerate centroid counts, where routing falls back to a linear scan.
    graph: Option<RelativeNeighborhoodGraph<FileSliceArena<f32>>>,
}

impl IvfIndex {
    /// Write slot `[0]` of the `.centroids` composite for a field. `num_docs`
    /// is the number of distinct docs assigned — NOT the posting-row total,
    /// which replication can multiply.
    pub(crate) fn serialize_centroids<W: Write + ?Sized>(
        num_centroids: usize,
        num_docs: usize,
        centroid_bytes: &[u8],
        options: &VectorOptions,
        out: &mut W,
    ) -> io::Result<()> {
        let expected = num_centroids
            .checked_mul(options.bytes_per_vector())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "centroid byte length overflow")
            })?;
        if centroid_bytes.len() != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid IVF centroid byte length",
            ));
        }
        u32::try_from(num_centroids)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "centroid count exceeds u32"))?
            .serialize(out)?;
        u32::try_from(num_docs)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "doc count exceeds u32"))?
            .serialize(out)?;
        out.write_all(centroid_bytes)
    }

    /// Write slot `[1]` of the `.centroids` composite for a field.
    pub(crate) fn serialize_offsets<W: Write + ?Sized>(
        cluster_offsets: &[u64],
        out: &mut W,
    ) -> io::Result<()> {
        for offset in cluster_offsets {
            offset.serialize(out)?;
        }
        Ok(())
    }

    /// Parse a field's `.centroids` slots. Only the count words, the offsets,
    /// and the graph adjacency are materialized; the centroid rows stay
    /// behind a [`FileSlice`] for lazy per-node reads.
    pub(crate) fn open(
        options: &VectorOptions,
        centroids_slice: FileSlice,
        offsets_slice: FileSlice,
        graph_slice: Option<FileSlice>,
    ) -> crate::Result<Self> {
        let count_words = 2 * mem::size_of::<u32>();
        if centroids_slice.len() < count_words {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IVF centroids slot is smaller than its count words",
            )
            .into());
        }
        let header = centroids_slice.slice_to(count_words).read_bytes()?;
        let mut reader = header.as_slice();
        let num_centroids = u32::deserialize(&mut reader)? as usize;
        let num_docs = u32::deserialize(&mut reader)? as usize;
        let centroid_len = num_centroids
            .checked_mul(options.bytes_per_vector())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "centroid byte length overflow")
            })?;
        let centroids_slice = centroids_slice.slice_from(count_words);
        if centroids_slice.len() != centroid_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IVF centroid byte length mismatch",
            )
            .into());
        }

        let cluster_offsets = offsets_slice.read_bytes()?;
        let expected_offsets = (num_centroids + 1)
            .checked_mul(mem::size_of::<u64>())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "cluster offset length overflow")
            })?;
        if cluster_offsets.len() != expected_offsets {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IVF cluster offset byte length mismatch",
            )
            .into());
        }

        let graph = match graph_slice {
            Some(slice) => {
                let vectors = match options.dtype() {
                    VectorDType::F32 => FileSliceArena::<f32>::new(centroids_slice.clone()),
                };
                // Adjacency length is validated against the arena's node
                // count inside `Graph::open`.
                let adjacency = slice.read_bytes()?;
                Some(RelativeNeighborhoodGraph::open(
                    &adjacency,
                    vectors,
                    options.dim(),
                    options.metric(),
                    NeighborhoodGraphConfig::default(),
                )?)
            }
            None => None,
        };

        let index = IvfIndex {
            num_centroids,
            num_docs,
            centroids_slice,
            cluster_offsets,
            dim: options.dim(),
            metric: options.metric(),
            graph,
        };
        // Every distinct doc owns at least its primary row, so a doc count
        // above the row total means a corrupt file.
        if index.num_docs > index.num_rows() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "IVF doc count exceeds the posting-row total",
            )
            .into());
        }
        Ok(index)
    }

    pub fn num_clusters(&self) -> usize {
        self.num_centroids
    }

    /// Distinct docs with a vector; replication inflates the row total,
    /// [`Self::num_rows`].
    pub(crate) fn num_docs(&self) -> usize {
        self.num_docs
    }

    /// Total posting rows across all clusters — memberships, counting a
    /// replicated doc once per cell it lives in.
    pub fn num_rows(&self) -> usize {
        self.cluster_offset(self.num_centroids) as usize
    }

    fn cluster_offset(&self, cluster: usize) -> u64 {
        let start = cluster * mem::size_of::<u64>();
        let end = start + mem::size_of::<u64>();
        u64::from_le_bytes(self.cluster_offsets[start..end].try_into().unwrap())
    }

    /// The contiguous row range of `cluster` within the `.vec` rows.
    #[inline]
    pub fn cluster_range(&self, cluster: usize) -> Range<usize> {
        debug_assert!(cluster < self.num_centroids, "cluster out of bounds");
        self.cluster_offset(cluster) as usize..self.cluster_offset(cluster + 1) as usize
    }

    /// Per-cluster posting-list sizes, in cluster order — memberships, like
    /// [`Self::num_rows`].
    pub(crate) fn cluster_sizes(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.num_centroids).map(|cluster| {
            (self.cluster_offset(cluster + 1) - self.cluster_offset(cluster)) as usize
        })
    }

    /// The centroid rows, materialized in one read — for introspection and
    /// tests only. Routing fetches per-node ranges through the lazy arena.
    pub fn centroid_bytes(&self) -> crate::Result<OwnedBytes> {
        Ok(self.centroids_slice.read_bytes()?)
    }

    /// Clusters to probe for `query`, ranked lazily — a [`ClusterRanking`]
    /// yielding [`Candidate`]s best routing score first (graph node `c` *is*
    /// cluster `c`, so `Candidate::node` is the cluster id).
    ///
    /// With a persisted RNG this is a resumable beam search
    /// ([`RelativeNeighborhoodGraph::search_iter`]): the first batch is one
    /// converged round at the configured
    /// [`ef`](NeighborhoodGraphConfig::ef), and pulling past it resumes the
    /// search, so routing cost is paid only as far as probing actually
    /// reaches. Without one every centroid is scored exactly, up front. Both
    /// paths score through the same [`FileSliceArena`], so their rankings
    /// agree.
    ///
    /// `ws` holds the routing search's scratch and is borrowed for the
    /// ranking's lifetime; [`ClusterRanking::metrics`] reports the cost
    /// incurred so far (surfaced as `ProbeStats::routing`).
    pub(crate) fn rank_clusters<'a>(
        &'a self,
        ws: &'a mut Workspace,
        query: &'a [f32],
    ) -> ClusterRanking<'a> {
        match &self.graph {
            Some(graph) => {
                // TODO: Replace with proper seed generation
                let seeds: Vec<NodeId> = {
                    (0..graph.len())
                        .step_by((graph.len() / 8).max(1))
                        .take(8)
                        .map(|node| node as NodeId)
                        .collect()
                };
                ClusterRanking::Graph(graph.search_iter(ws, query, &seeds))
            }
            None => {
                let arena = FileSliceArena::<f32>::new(self.centroids_slice.clone());
                let mut ranked: Vec<Candidate> = (0..self.num_centroids)
                    .map(|cluster| Candidate {
                        sim: arena.similarity(self.metric, self.dim, cluster as NodeId, query),
                        node: cluster as NodeId,
                    })
                    .collect();
                ranked.sort_unstable_by(|a, b| b.cmp(a));
                ClusterRanking::Exact {
                    ranked: ranked.into_iter(),
                    num_centroids: self.num_centroids,
                }
            }
        }
    }
}

/// Lazily ranked clusters for one query, yielded best routing score first;
/// returned by [`IvfIndex::rank_clusters`], which documents the two paths.
pub(crate) enum ClusterRanking<'a> {
    /// Beam-searched routing over the persisted centroid RNG; pulling past a
    /// converged batch resumes the search.
    Graph(ResumableSearchIterator<'a, 'a, FileSliceArena<f32>>),
    /// Exact fallback for graph-less segments: every centroid scored and
    /// sorted up front.
    Exact {
        ranked: std::vec::IntoIter<Candidate>,
        num_centroids: usize,
    },
}

impl ClusterRanking<'_> {
    /// The routing cost incurred so far: fixed for the exact path, growing
    /// with each pull that resumes the beam search on the graph path — so
    /// take the snapshot after the last pull.
    pub(crate) fn metrics(&self) -> IvfSearchMetrics {
        match self {
            ClusterRanking::Graph(iter) => IvfSearchMetrics {
                visited_count: iter.metrics().visited_count,
                graph: Some(iter.metrics()),
            },
            ClusterRanking::Exact { num_centroids, .. } => IvfSearchMetrics {
                visited_count: *num_centroids,
                graph: None,
            },
        }
    }
}

impl Iterator for ClusterRanking<'_> {
    type Item = Candidate;

    fn next(&mut self) -> Option<Candidate> {
        match self {
            ClusterRanking::Graph(iter) => iter.next(),
            ClusterRanking::Exact { ranked, .. } => ranked.next(),
        }
    }
}

/// Routing cost of one [`IvfIndex::rank_clusters`] ranking (a
/// [`ClusterRanking::metrics`] snapshot): how many centroids were scored to
/// pick the probe order, and — when routing went through the centroid RNG —
/// the beam search's full [`NeighborhoodGraphSearchMetrics`].
#[derive(Clone, Copy, Debug, Default)]
pub struct IvfSearchMetrics {
    /// Centroids scored to route the query (the navigation cost):
    /// `num_centroids` on the exact path, the beam-visited count when routed
    /// via the RNG.
    pub visited_count: usize,
    /// The centroid-graph beam search's counters; `None` when routing fell
    /// back to a linear scan of the centroids.
    pub graph: Option<NeighborhoodGraphSearchMetrics>,
}
