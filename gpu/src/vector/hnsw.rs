//! HNSW (Hierarchical Navigable Small World) index for approximate nearest neighbor search.
//!
//! GPU-accelerated distance computation with CPU-side graph traversal.
//! The graph structure is navigated on CPU (branchy, sequential) while
//! distance computation for candidate evaluation is batched to GPU.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

use crate::device::GpuContext;
use crate::error::GpuResult;
use crate::kernel::GpuKernel;
use crate::vector::distance::{compute_distance_cpu, DistanceMetric, GpuDistanceKernel};

/// Maximum number of connections per node at level 0.
const M: usize = 16;
/// Maximum number of connections per node at levels > 0.
const M_MAX0: usize = 32;
/// Size multiplier for construction candidate list.
const EF_CONSTRUCTION: usize = 200;

/// HNSW graph index for approximate nearest neighbor search.
pub struct HnswIndex {
    /// Flat storage of all vectors: vectors[i] has dimension `dim`.
    vectors: Vec<Vec<f32>>,
    /// Adjacency lists per level: graph[level][node_id] = Vec<neighbor_id>
    graph: Vec<Vec<Vec<u32>>>,
    /// Maximum level for each node.
    levels: Vec<usize>,
    /// Entry point (node with highest level).
    entry_point: Option<u32>,
    /// Maximum level in the graph.
    max_level: usize,
    /// Vector dimension.
    dim: usize,
    /// Distance metric.
    metric: DistanceMetric,
    /// GPU distance kernel (None if GPU unavailable or not compiled).
    gpu_kernel: Option<GpuDistanceKernel>,
}

/// A scored candidate: (distance, node_id).
#[derive(Debug, Clone, Copy, PartialEq)]
struct Candidate {
    distance: f32,
    id: u32,
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl HnswIndex {
    /// Create a new empty HNSW index.
    pub fn new(dim: usize, metric: DistanceMetric) -> Self {
        Self {
            vectors: Vec::new(),
            graph: Vec::new(),
            levels: Vec::new(),
            entry_point: None,
            max_level: 0,
            dim,
            metric,
            gpu_kernel: None,
        }
    }

    /// Create a new HNSW index with GPU acceleration.
    pub fn with_gpu(dim: usize, metric: DistanceMetric, ctx: &GpuContext) -> GpuResult<Self> {
        let kernel = GpuDistanceKernel::compile(ctx)?;
        Ok(Self {
            vectors: Vec::new(),
            graph: Vec::new(),
            levels: Vec::new(),
            entry_point: None,
            max_level: 0,
            dim,
            metric,
            gpu_kernel: Some(kernel),
        })
    }

    /// Number of vectors in the index.
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    /// Vector dimension.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Distance metric.
    pub fn metric(&self) -> DistanceMetric {
        self.metric
    }

    /// Maximum level in the graph.
    pub fn max_level(&self) -> usize {
        self.max_level
    }

    /// Entry point node ID.
    pub fn entry_point_id(&self) -> Option<u32> {
        self.entry_point
    }

    /// Number of levels in the graph.
    pub fn num_levels(&self) -> usize {
        self.graph.len()
    }

    /// Access all vectors.
    pub fn vectors(&self) -> &[Vec<f32>] {
        &self.vectors
    }

    /// Access node levels.
    pub fn node_levels(&self) -> &[usize] {
        &self.levels
    }

    /// Access neighbors for a node at a given level.
    pub fn neighbors(&self, level: usize, node: usize) -> &[u32] {
        if level < self.graph.len() && node < self.graph[level].len() {
            &self.graph[level][node]
        } else {
            &[]
        }
    }

    /// Reconstruct an HNSW index from its serialized parts.
    pub fn from_parts(
        vectors: Vec<Vec<f32>>,
        graph: Vec<Vec<Vec<u32>>>,
        levels: Vec<usize>,
        entry_point: Option<u32>,
        max_level: usize,
        dim: usize,
        metric: DistanceMetric,
    ) -> Self {
        Self {
            vectors,
            graph,
            levels,
            entry_point,
            max_level,
            dim,
            metric,
            gpu_kernel: None,
        }
    }

    /// Insert a vector into the index.
    pub fn insert(&mut self, vector: Vec<f32>) -> GpuResult<u32> {
        if vector.len() != self.dim {
            return Err(crate::error::GpuError::ColumnTypeMismatch {
                expected: format!("dim={}", self.dim),
                actual: format!("dim={}", vector.len()),
            });
        }

        let id = self.vectors.len() as u32;
        let level = self.random_level();

        // Ensure graph has enough levels
        while self.graph.len() <= level {
            self.graph.push(Vec::new());
        }
        for lvl in self.graph.iter_mut() {
            while lvl.len() <= id as usize {
                lvl.push(Vec::new());
            }
        }
        self.levels.push(level);
        self.vectors.push(vector.clone());

        if self.entry_point.is_none() {
            self.entry_point = Some(id);
            self.max_level = level;
            return Ok(id);
        }

        // Safe: checked is_none above
        let entry = self.entry_point.expect("entry_point confirmed Some");

        // Phase 1: Greedy search from top level down to level+1
        let mut current = entry;
        for lev in (level + 1..=self.max_level).rev() {
            current = self.greedy_closest(lev, current, &vector);
        }

        // Phase 2: Insert at each level from `level` down to 0
        let mut ep = vec![Candidate {
            distance: self.distance(current as usize, &vector),
            id: current,
        }];

        for lev in (0..=level.min(self.max_level)).rev() {
            let max_conn = if lev == 0 { M_MAX0 } else { M };
            let candidates = self.search_layer(&vector, &ep, EF_CONSTRUCTION, lev);
            let neighbors = self.select_neighbors(&candidates, max_conn);

            // Connect new node to neighbors
            self.graph[lev][id as usize] = neighbors.iter().map(|c| c.id).collect();

            // Connect neighbors back to new node
            for &neighbor in &neighbors {
                let nid = neighbor.id as usize;
                self.graph[lev][nid].push(id);
                if self.graph[lev][nid].len() > max_conn {
                    // Prune: keep only closest `max_conn` neighbors
                    let nv = &self.vectors[nid];
                    let mut scored: Vec<Candidate> = self.graph[lev][nid]
                        .iter()
                        .map(|&nbr| Candidate {
                            distance: compute_distance_cpu(
                                nv,
                                &self.vectors[nbr as usize],
                                self.metric,
                            ),
                            id: nbr,
                        })
                        .collect();
                    scored.sort();
                    self.graph[lev][nid] = scored[..max_conn].iter().map(|c| c.id).collect();
                }
            }

            ep = candidates;
        }

        // Update entry point if new node has higher level
        if level > self.max_level {
            self.entry_point = Some(id);
            self.max_level = level;
        }

        Ok(id)
    }

    /// Search for the k nearest neighbors of a query vector.
    ///
    /// Returns Vec of (distance, doc_id) sorted by distance (ascending).
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<(f32, u32)> {
        if self.entry_point.is_none() {
            return Vec::new();
        }

        let entry = self.entry_point.expect("entry_point confirmed Some");
        let mut current = entry;

        // Phase 1: Greedy search from top level down to level 1
        for lev in (1..=self.max_level).rev() {
            current = self.greedy_closest(lev, current, query);
        }

        // Phase 2: Search at level 0 with ef candidates
        let ep = vec![Candidate {
            distance: self.distance(current as usize, query),
            id: current,
        }];
        let candidates = self.search_layer(query, &ep, ef.max(k), 0);

        // Return top k
        candidates
            .into_iter()
            .take(k)
            .map(|c| (c.distance, c.id))
            .collect()
    }

    /// Batch search using GPU for distance computation.
    ///
    /// For each search step, collects candidate vectors and computes
    /// distances on GPU in a single batch.
    pub fn search_gpu(&self, query: &[f32], k: usize, ef: usize) -> GpuResult<Vec<(f32, u32)>> {
        if self.gpu_kernel.is_none() || self.entry_point.is_none() {
            return Ok(self.search(query, k, ef));
        }

        let entry = self.entry_point.expect("entry_point confirmed Some");
        let mut current = entry;

        // Phase 1: Greedy search (small batches, CPU is fine)
        for lev in (1..=self.max_level).rev() {
            current = self.greedy_closest(lev, current, query);
        }

        // Phase 2: GPU-accelerated search at level 0
        let ep = vec![Candidate {
            distance: self.distance(current as usize, query),
            id: current,
        }];
        let candidates = self.search_layer_gpu(query, &ep, ef.max(k), 0)?;

        Ok(candidates
            .into_iter()
            .take(k)
            .map(|c| (c.distance, c.id))
            .collect())
    }

    // ─── Internal methods ───

    fn random_level(&self) -> usize {
        // ml = 1 / ln(M)
        let ml = 1.0 / (M as f64).ln();
        let r: f64 = rand_f64();
        (-r.ln() * ml).floor() as usize
    }

    fn distance(&self, node_id: usize, query: &[f32]) -> f32 {
        compute_distance_cpu(&self.vectors[node_id], query, self.metric)
    }

    fn greedy_closest(&self, level: usize, start: u32, query: &[f32]) -> u32 {
        let mut current = start;
        let mut best_dist = self.distance(current as usize, query);

        loop {
            let mut changed = false;
            if level < self.graph.len() && (current as usize) < self.graph[level].len() {
                for &neighbor in &self.graph[level][current as usize] {
                    let dist = self.distance(neighbor as usize, query);
                    if dist < best_dist {
                        best_dist = dist;
                        current = neighbor;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
        current
    }

    fn search_layer(
        &self,
        query: &[f32],
        entry_points: &[Candidate],
        ef: usize,
        level: usize,
    ) -> Vec<Candidate> {
        let mut visited = HashSet::new();
        let mut candidates: BinaryHeap<Reverse<Candidate>> = BinaryHeap::new();
        let mut results: BinaryHeap<Candidate> = BinaryHeap::new();

        for ep in entry_points {
            visited.insert(ep.id);
            candidates.push(Reverse(*ep));
            results.push(*ep);
        }

        while let Some(Reverse(current)) = candidates.pop() {
            let worst_result = results.peek().map(|c| c.distance).unwrap_or(f32::MAX);
            if current.distance > worst_result && results.len() >= ef {
                break;
            }

            if level < self.graph.len() && (current.id as usize) < self.graph[level].len() {
                for &neighbor in &self.graph[level][current.id as usize] {
                    if visited.insert(neighbor) {
                        let dist = self.distance(neighbor as usize, query);
                        let worst = results.peek().map(|c| c.distance).unwrap_or(f32::MAX);

                        if results.len() < ef || dist < worst {
                            let c = Candidate {
                                distance: dist,
                                id: neighbor,
                            };
                            candidates.push(Reverse(c));
                            results.push(c);
                            if results.len() > ef {
                                results.pop();
                            }
                        }
                    }
                }
            }
        }

        let mut result_vec: Vec<Candidate> = results.into_vec();
        result_vec.sort();
        result_vec
    }

    fn search_layer_gpu(
        &self,
        query: &[f32],
        entry_points: &[Candidate],
        ef: usize,
        level: usize,
    ) -> GpuResult<Vec<Candidate>> {
        let kernel = self.gpu_kernel.as_ref().expect("gpu_kernel confirmed Some");

        let mut visited = HashSet::new();
        let mut candidates: BinaryHeap<Reverse<Candidate>> = BinaryHeap::new();
        let mut results: BinaryHeap<Candidate> = BinaryHeap::new();

        for ep in entry_points {
            visited.insert(ep.id);
            candidates.push(Reverse(*ep));
            results.push(*ep);
        }

        while let Some(Reverse(current)) = candidates.pop() {
            let worst_result = results.peek().map(|c| c.distance).unwrap_or(f32::MAX);
            if current.distance > worst_result && results.len() >= ef {
                break;
            }

            if level < self.graph.len() && (current.id as usize) < self.graph[level].len() {
                // Collect unvisited neighbors for GPU batch
                let unvisited: Vec<u32> = self.graph[level][current.id as usize]
                    .iter()
                    .filter(|&&n| visited.insert(n))
                    .copied()
                    .collect();

                if unvisited.is_empty() {
                    continue;
                }

                // Batch compute distances on GPU
                let flat_vecs: Vec<f32> = unvisited
                    .iter()
                    .flat_map(|&id| self.vectors[id as usize].iter().copied())
                    .collect();

                let distances = kernel.compute(query, &flat_vecs, self.dim, self.metric)?;

                for (i, &neighbor) in unvisited.iter().enumerate() {
                    let dist = distances[i];
                    let worst = results.peek().map(|c| c.distance).unwrap_or(f32::MAX);

                    if results.len() < ef || dist < worst {
                        let c = Candidate {
                            distance: dist,
                            id: neighbor,
                        };
                        candidates.push(Reverse(c));
                        results.push(c);
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }

        let mut result_vec: Vec<Candidate> = results.into_vec();
        result_vec.sort();
        Ok(result_vec)
    }

    fn select_neighbors(&self, candidates: &[Candidate], max_conn: usize) -> Vec<Candidate> {
        // Simple heuristic: take the closest `max_conn` candidates
        candidates[..candidates.len().min(max_conn)].to_vec()
    }
}

/// Pseudo-random f64 in [0, 1) for HNSW level generation.
/// Uses thread-local xorshift64 seeded from time + thread ID on first use.
fn rand_f64() -> f64 {
    use std::cell::Cell;
    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0) };
    }
    STATE.with(|s| {
        let mut x = s.get();
        if x == 0 {
            // Seed from high-resolution clock + thread ID hash
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x12345678_9abcdef0);
            let tid = std::thread::current().id();
            let tid_hash = {
                let s = format!("{tid:?}");
                s.bytes()
                    .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64))
            };
            x = now ^ tid_hash;
            if x == 0 {
                x = 0x12345678_9abcdef0;
            }
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        s.set(x);
        (x as f64) / (u64::MAX as f64)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_insert_and_search() {
        let mut index = HnswIndex::new(3, DistanceMetric::L2);

        // Insert 100 random-ish vectors
        for i in 0..100u32 {
            let v = vec![i as f32, (i * 2) as f32, (i * 3) as f32];
            index.insert(v).unwrap();
        }

        assert_eq!(index.len(), 100);

        // Search for nearest to [50, 100, 150]
        let query = vec![50.0, 100.0, 150.0];
        let results = index.search(&query, 5, 50);

        assert_eq!(results.len(), 5);
        // The closest should be the vector [50, 100, 150] (id=50)
        assert_eq!(results[0].1, 50);
        assert_eq!(results[0].0, 0.0); // Exact match
    }

    #[test]
    fn test_hnsw_cosine() {
        let mut index = HnswIndex::new(2, DistanceMetric::Cosine);

        index.insert(vec![1.0, 0.0]).unwrap(); // id=0: east
        index.insert(vec![0.0, 1.0]).unwrap(); // id=1: north
        index.insert(vec![1.0, 1.0]).unwrap(); // id=2: northeast

        // Query northeast — closest by cosine should be id=2
        let results = index.search(&[0.9, 1.1], 2, 10);
        assert!(!results.is_empty());
        assert_eq!(results[0].1, 2);
    }

    #[test]
    fn test_hnsw_empty_search() {
        let index = HnswIndex::new(4, DistanceMetric::L2);
        let results = index.search(&[1.0, 2.0, 3.0, 4.0], 5, 50);
        assert!(results.is_empty());
    }

    #[test]
    fn test_hnsw_gpu_fallback() {
        let ctx = crate::device::GpuContext::cpu_fallback();
        let mut index = HnswIndex::with_gpu(2, DistanceMetric::L2, &ctx).unwrap();

        for i in 0..50u32 {
            index.insert(vec![i as f32, (i * 2) as f32]).unwrap();
        }

        let results = index.search_gpu(&[25.0, 50.0], 3, 30).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1, 25); // Exact match
    }
}
