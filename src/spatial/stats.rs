//! Spatial index shape statistics.
//!
//! This module reads the persisted spatial cell and edge indexes through their normal readers and
//! reports structure, not query results. It is intended for validating shape-changing index fixes.

use std::collections::BTreeMap;

use serde::Serialize;

use super::cell_index_reader::CellIndexReader;
use super::clip_options::ClipOptions;
use super::clipped_shape::GeometryId;
use super::clipper::get_edge_max_level;
use super::edge_cache::EdgeCache;
use super::edge_reader::EdgeReader;
use super::reader::SpatialReader;
use super::s2cell_id::S2CellId;
use super::sphere::Sphere;
use super::surface::Surface;

const TOP_CELL_COUNT: usize = 20;
const STATS_EDGE_CACHE_MAX_VERTICES: usize = 20_000_000;

/// Statistics for one persisted spatial field.
#[derive(Clone, Debug, Serialize)]
pub struct SpatialStats {
    /// Runtime options used to recompute threshold pressure.
    pub options: StatsOptions,
    /// Hashes and lengths of the field slices that were read.
    pub slices: SliceStats,
    /// Total number of cells in the persisted cell index.
    pub total_cells: u64,
    /// Cell counts by S2 level.
    pub levels: Vec<LevelStats>,
    /// Global edge reference counts across all cells.
    pub edge_references: EdgeReferenceStats,
    /// Cells at or above the recomputed threshold pressure.
    pub threshold: ThresholdStats,
    /// Heaviest cells by edge reference count.
    pub heaviest_cells: Vec<CellStats>,
}

/// Split options embedded in the stats output so runs are self-describing.
#[derive(Clone, Debug, Serialize)]
pub struct StatsOptions {
    /// Maximum edge threshold from `ClipOptions::default()`.
    pub max_edges_per_cell: usize,
    /// Minimum short-edge fraction from `ClipOptions::default()`.
    pub min_short_edge_fraction: f64,
}

/// Field-slice identity for the bytes that were actually read.
#[derive(Clone, Debug, Serialize)]
pub struct SliceStats {
    /// Cell index field slice identity.
    pub cells: SliceHash,
    /// Edge index field slice identity.
    pub edges: SliceHash,
    /// Doc-id field slice identity.
    pub doc_ids: SliceHash,
}

/// Length plus stable 64-bit hash of one byte slice.
#[derive(Clone, Debug, Serialize)]
pub struct SliceHash {
    /// Slice length in bytes.
    pub len: usize,
    /// Stable FNV-1a 64-bit hash encoded as lowercase hex.
    pub hash64: String,
}

/// Cell distribution for one S2 level.
#[derive(Clone, Debug, Serialize)]
pub struct LevelStats {
    /// S2 cell level.
    pub level: i32,
    /// Number of persisted cells at this level.
    pub cells: u64,
    /// Number of persisted cells at this level and all lower numeric levels.
    pub cumulative_cells: u64,
    /// Distribution of edge references per cell at this level.
    pub edges_per_cell: DistributionStats,
    /// Distribution of short-edge references per cell at this level.
    pub short_edges_per_cell: DistributionStats,
    /// Distribution of distinct geometries per cell at this level.
    pub geometries_per_cell: DistributionStats,
}

/// Distribution summary for integer values.
#[derive(Clone, Debug, Serialize)]
pub struct DistributionStats {
    /// Number of samples.
    pub count: u64,
    /// Maximum sample value.
    pub max: usize,
    /// Arithmetic mean.
    pub mean: f64,
    /// 50th percentile.
    pub p50: usize,
    /// 90th percentile.
    pub p90: usize,
    /// 95th percentile.
    pub p95: usize,
    /// 99th percentile.
    pub p99: usize,
    /// Power-of-two bucket counts.
    pub buckets: Vec<BucketStats>,
}

/// Count of values up to a bucket's inclusive upper bound.
#[derive(Clone, Debug, Serialize)]
pub struct BucketStats {
    /// Inclusive upper bound for this bucket.
    pub upper: usize,
    /// Number of samples in this bucket.
    pub count: u64,
}

/// Global duplication accounting for cell edge references.
#[derive(Clone, Debug, Serialize)]
pub struct EdgeReferenceStats {
    /// Total edge references across all cells.
    pub total: u64,
    /// Distinct `(geometry_id, edge_index)` pairs across all cells.
    pub distinct: u64,
    /// Total references divided by distinct references.
    pub duplication_factor: f64,
}

/// Recomputed threshold pressure over persisted cells.
#[derive(Clone, Debug, Serialize)]
pub struct ThresholdStats {
    /// Number of cells whose short-edge count is greater than the recomputed threshold.
    pub cells_over_threshold: u64,
    /// Number of cells whose short-edge count equals the recomputed threshold.
    pub cells_at_threshold: u64,
    /// Worst cells by `short_edge_refs - threshold`.
    pub worst_cells: Vec<CellStats>,
}

/// Per-cell stats used for top-N reports.
#[derive(Clone, Debug, Serialize)]
pub struct CellStats {
    /// S2 token for the cell.
    pub token: String,
    /// Raw S2CellId value.
    pub cell_id: u64,
    /// S2 cell level.
    pub level: i32,
    /// Number of persisted shapes in the cell.
    pub shapes: usize,
    /// Number of distinct geometries in the cell.
    pub distinct_geometries: usize,
    /// Total edge references in the cell.
    pub edge_refs: usize,
    /// Short-edge references in the cell.
    pub short_edge_refs: usize,
    /// Recomputed threshold for this persisted cell.
    pub threshold: usize,
    /// `short_edge_refs - threshold`.
    pub threshold_excess: isize,
}

#[derive(Default)]
struct LevelAccumulator {
    edges: Vec<usize>,
    short_edges: Vec<usize>,
    geometries: Vec<usize>,
}

#[derive(Clone, Copy)]
struct CellKey {
    cell_id: S2CellId,
    level: i32,
    shapes: usize,
    distinct_geometries: usize,
    edge_refs: usize,
    short_edge_refs: usize,
    threshold: usize,
    threshold_excess: isize,
}

/// Compute sphere spatial stats from an opened per-field spatial reader.
pub fn sphere_stats_for_reader(reader: &SpatialReader) -> SpatialStats {
    spatial_stats_for_slices::<Sphere>(
        reader.cells_bytes(),
        reader.edges_bytes(),
        reader.doc_ids_bytes(),
    )
}

/// Compute sphere spatial stats from field-level cells, edges, and doc_id slices.
pub fn sphere_stats_for_slices(
    cells_bytes: &[u8],
    edges_bytes: &[u8],
    doc_ids_bytes: &[u8],
) -> SpatialStats {
    spatial_stats_for_slices::<Sphere>(cells_bytes, edges_bytes, doc_ids_bytes)
}

fn spatial_stats_for_slices<S: Surface>(
    cells_bytes: &[u8],
    edges_bytes: &[u8],
    doc_ids_bytes: &[u8],
) -> SpatialStats {
    let options = ClipOptions::default();
    let cell_reader = CellIndexReader::open_with_doc_ids(cells_bytes, doc_ids_bytes);
    let edge_reader = EdgeReader::<S>::open(edges_bytes);
    let edge_cache = EdgeCache::new(vec![edge_reader], STATS_EDGE_CACHE_MAX_VERTICES);

    let mut levels: BTreeMap<i32, LevelAccumulator> = BTreeMap::new();
    let mut total_cells = 0u64;
    let mut total_edge_refs = 0u64;
    let mut distinct_edge_refs: Vec<(GeometryId, u32)> = Vec::new();
    let mut heaviest_cells = Vec::new();
    let mut threshold_cells = Vec::new();
    let mut cells_over_threshold = 0u64;
    let mut cells_at_threshold = 0u64;
    let mut geometry_ids: Vec<GeometryId> = Vec::new();

    for cell in cell_reader.iter() {
        total_cells += 1;
        let level = cell.cell_id.level();
        geometry_ids.clear();
        geometry_ids.extend(cell.shapes.iter().map(|shape| shape.geometry_id));
        geometry_ids.sort_unstable();
        geometry_ids.dedup();
        let distinct_geometries = geometry_ids.len();
        let mut edge_refs = 0usize;
        let mut short_edge_refs = 0usize;

        for shape in &cell.shapes {
            edge_refs += shape.edge_indices.len();
            if shape.edge_indices.is_empty() {
                continue;
            }
            let entry = edge_cache.get(shape.geometry_id);
            for &edge_index in &shape.edge_indices {
                distinct_edge_refs.push((shape.geometry_id, edge_index));
                let (v0, v1) = entry.edge(edge_index);
                if level < get_edge_max_level::<S>(&v0, &v1) {
                    short_edge_refs += 1;
                }
            }
        }

        total_edge_refs += edge_refs as u64;
        let threshold = options.max_edges_per_cell.max(
            (options.min_short_edge_fraction * (short_edge_refs + distinct_geometries) as f64)
                as usize,
        );
        let threshold_excess = short_edge_refs as isize - threshold as isize;

        let cell_key = CellKey {
            cell_id: cell.cell_id,
            level,
            shapes: cell.shapes.len(),
            distinct_geometries,
            edge_refs,
            short_edge_refs,
            threshold,
            threshold_excess,
        };

        if short_edge_refs > threshold {
            cells_over_threshold += 1;
            select_top_cell(&mut threshold_cells, cell_key, compare_threshold_cells);
        } else if short_edge_refs == threshold {
            cells_at_threshold += 1;
            select_top_cell(&mut threshold_cells, cell_key, compare_threshold_cells);
        }
        select_top_cell(&mut heaviest_cells, cell_key, compare_heavy_cells);

        let level_stats = levels.entry(level).or_default();
        level_stats.edges.push(edge_refs);
        level_stats.short_edges.push(short_edge_refs);
        level_stats.geometries.push(distinct_geometries);
    }

    heaviest_cells.sort_unstable_by(compare_heavy_cells);
    let heaviest_cells = heaviest_cells
        .into_iter()
        .map(CellKey::into_stats)
        .collect();

    threshold_cells.sort_unstable_by(compare_threshold_cells);
    let threshold_cells = threshold_cells
        .into_iter()
        .map(CellKey::into_stats)
        .collect();

    let mut cumulative_cells = 0u64;
    let levels = levels
        .into_iter()
        .map(|(level, acc)| {
            cumulative_cells += acc.edges.len() as u64;
            LevelStats {
                level,
                cells: acc.edges.len() as u64,
                cumulative_cells,
                edges_per_cell: summarize_distribution(acc.edges),
                short_edges_per_cell: summarize_distribution(acc.short_edges),
                geometries_per_cell: summarize_distribution(acc.geometries),
            }
        })
        .collect();

    distinct_edge_refs.sort_unstable();
    distinct_edge_refs.dedup();
    let distinct = distinct_edge_refs.len() as u64;
    let duplication_factor = if distinct == 0 {
        0.0
    } else {
        total_edge_refs as f64 / distinct as f64
    };

    SpatialStats {
        options: StatsOptions {
            max_edges_per_cell: options.max_edges_per_cell,
            min_short_edge_fraction: options.min_short_edge_fraction,
        },
        slices: SliceStats {
            cells: hash_slice(cells_bytes),
            edges: hash_slice(edges_bytes),
            doc_ids: hash_slice(doc_ids_bytes),
        },
        total_cells,
        levels,
        edge_references: EdgeReferenceStats {
            total: total_edge_refs,
            distinct,
            duplication_factor,
        },
        threshold: ThresholdStats {
            cells_over_threshold,
            cells_at_threshold,
            worst_cells: threshold_cells,
        },
        heaviest_cells,
    }
}

impl CellKey {
    fn into_stats(self) -> CellStats {
        CellStats {
            token: self.cell_id.to_token(),
            cell_id: self.cell_id.0,
            level: self.level,
            shapes: self.shapes,
            distinct_geometries: self.distinct_geometries,
            edge_refs: self.edge_refs,
            short_edge_refs: self.short_edge_refs,
            threshold: self.threshold,
            threshold_excess: self.threshold_excess,
        }
    }
}

fn select_top_cell(
    cells: &mut Vec<CellKey>,
    candidate: CellKey,
    compare: fn(&CellKey, &CellKey) -> std::cmp::Ordering,
) {
    if cells.len() < TOP_CELL_COUNT {
        cells.push(candidate);
        return;
    }

    let mut worst = 0;
    for i in 1..cells.len() {
        if compare(&cells[i], &cells[worst]).is_gt() {
            worst = i;
        }
    }

    if compare(&candidate, &cells[worst]).is_lt() {
        cells[worst] = candidate;
    }
}

fn compare_heavy_cells(a: &CellKey, b: &CellKey) -> std::cmp::Ordering {
    b.edge_refs
        .cmp(&a.edge_refs)
        .then_with(|| b.short_edge_refs.cmp(&a.short_edge_refs))
        .then_with(|| b.shapes.cmp(&a.shapes))
        .then_with(|| a.cell_id.cmp(&b.cell_id))
}

fn compare_threshold_cells(a: &CellKey, b: &CellKey) -> std::cmp::Ordering {
    b.threshold_excess
        .cmp(&a.threshold_excess)
        .then_with(|| compare_heavy_cells(a, b))
}

fn summarize_distribution(mut values: Vec<usize>) -> DistributionStats {
    if values.is_empty() {
        return DistributionStats {
            count: 0,
            max: 0,
            mean: 0.0,
            p50: 0,
            p90: 0,
            p95: 0,
            p99: 0,
            buckets: Vec::new(),
        };
    }

    values.sort_unstable();
    let count = values.len() as u64;
    let sum: u64 = values.iter().map(|&v| v as u64).sum();
    let max = *values.last().unwrap();
    let buckets = summarize_buckets(&values);

    DistributionStats {
        count,
        max,
        mean: sum as f64 / count as f64,
        p50: percentile(&values, 50),
        p90: percentile(&values, 90),
        p95: percentile(&values, 95),
        p99: percentile(&values, 99),
        buckets,
    }
}

fn percentile(values: &[usize], percentile_value: usize) -> usize {
    let n = values.len();
    // Nearest-rank percentile convention.
    let rank = ((percentile_value as f64 / 100.0) * n as f64).ceil() as usize;
    let rank = rank.saturating_sub(1).min(n - 1);
    values[rank]
}

fn summarize_buckets(values: &[usize]) -> Vec<BucketStats> {
    let mut buckets: BTreeMap<usize, u64> = BTreeMap::new();
    for &value in values {
        *buckets.entry(bucket_upper(value)).or_default() += 1;
    }
    let max_upper = bucket_upper(*values.last().unwrap());
    bucket_ladder(max_upper)
        .into_iter()
        .map(|upper| BucketStats {
            upper,
            count: buckets.get(&upper).copied().unwrap_or(0),
        })
        .collect()
}

fn bucket_upper(value: usize) -> usize {
    match value {
        0 => 0,
        1 => 1,
        2 => 2,
        _ => value.next_power_of_two(),
    }
}

fn bucket_ladder(max_upper: usize) -> Vec<usize> {
    let mut ladder = vec![0];
    if max_upper == 0 {
        return ladder;
    }

    ladder.push(1);
    if max_upper == 1 {
        return ladder;
    }

    ladder.push(2);
    let mut upper = 4;
    while upper <= max_upper {
        ladder.push(upper);
        upper *= 2;
    }
    ladder
}

fn hash_slice(bytes: &[u8]) -> SliceHash {
    let mut hash = 0xcbf29ce484222325u64;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    SliceHash {
        len: bytes.len(),
        hash64: format!("{hash:016x}"),
    }
}
