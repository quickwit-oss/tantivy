//! `ValueSource` reading the `.vortex` component.
//!
//! The Vortex files of a segment are opened once and cached by the provider (the equivalent of
//! what a segment reader would do). Each query then gets its own `VortexValueSource`, which
//! decompresses one Vortex chunk at a time and keeps the last decompressed chunk around.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};

use tantivy::aggregation::{ValueSource, ValueSourceDictionary, ValueSourceProvider};
use tantivy::columnar::{Cardinality, ColumnType, RowId, StrColumn};
use tantivy::index::{SegmentComponent, SegmentId};
use tantivy::{f64_to_u64, i64_to_u64, DocId, SegmentReader};
use vortex::array::arrays::chunked::ChunkedArrayExt;
use vortex::array::arrays::extension::ExtensionArrayExt;
use vortex::array::arrays::{Chunked, ExtensionArray, ListViewArray, PrimitiveArray};
use vortex::array::validity::Validity;
use vortex::array::{ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute};
use vortex::buffer::{Alignment, Buffer, ByteBufferMut};
use vortex::dtype::{NativePType, PType};
use vortex::file::OpenOptionsSessionExt;

use crate::point::PointAccessor;
use crate::format::{read_footer, ColumnEntry, StoredCardinality, StoredType, VORTEX_EXTENSION};
use crate::{vortex_env, vortex_err_to_tantivy};

/// A column of a segment, as compressed Vortex chunks.
pub struct VortexColumnData {
    pub entry: ColumnEntry,
    /// Compressed chunks, in row order.
    pub chunks: Vec<ArrayRef>,
    /// `chunks.len() + 1` row offsets. Chunk `i` holds rows `chunk_starts[i]..chunk_starts[i+1]`.
    pub chunk_starts: Vec<RowId>,
    /// Point accessors, built on first use and shared by all the queries on the segment.
    /// `None` if the encoding tree of the chunk is not supported.
    point_accessors: Vec<OnceLock<Option<PointAccessor>>>,
}

impl VortexColumnData {
    pub fn column_type(&self) -> ColumnType {
        self.entry.stored_type.column_type()
    }

    fn point_accessor(&self, chunk_ord: usize, ctx: &mut ExecutionCtx) -> Option<&PointAccessor> {
        self.point_accessors[chunk_ord]
            .get_or_init(|| PointAccessor::build(&self.chunks[chunk_ord], ctx))
            .as_ref()
    }

    /// Times `get_many` on every `stride`-th row of each chunk. Returns (ns, num_values).
    /// For diagnostics only.
    pub fn time_point_access(&self, stride: usize) -> (u64, usize) {
        let mut ctx = vortex_env().session.create_execution_ctx();
        let mut total_ns = 0u64;
        let mut num_values = 0;
        let mut output = Vec::new();
        for chunk_ord in 0..self.chunks.len() {
            let Some(accessor) = self.point_accessor(chunk_ord, &mut ctx) else {
                continue;
            };
            if self.entry.cardinality == StoredCardinality::Multivalued {
                continue;
            }
            let len = self.chunks[chunk_ord].len();
            let rows: Vec<usize> = (0..len).step_by(stride).collect();
            let start = std::time::Instant::now();
            for batch in rows.chunks(64) {
                output.clear();
                accessor.get_many(batch, &mut output);
                std::hint::black_box(&output);
            }
            total_ns += start.elapsed().as_nanos() as u64;
            num_values += rows.len();
        }
        (total_ns, num_values)
    }

    /// Number of chunks for which a point accessor can be built (builds them all).
    pub fn num_point_accessible_chunks(&self) -> usize {
        let mut ctx = vortex_env().session.create_execution_ctx();
        let mut num_accessible = 0;
        for chunk_ord in 0..self.chunks.len() {
            if self.point_accessor(chunk_ord, &mut ctx).is_some() {
                num_accessible += 1;
            } else if std::env::var("VORTEX_POINT_DEBUG").is_ok() {
                eprintln!(
                    "no point accessor for {} chunk {chunk_ord}: {}",
                    self.entry.name,
                    crate::encoding_tree(&self.chunks[chunk_ord], 4)
                );
            }
        }
        num_accessible
    }
}

/// All the Vortex columns of a segment.
pub struct VortexSegment {
    pub columns: HashMap<String, Arc<VortexColumnData>>,
}

/// Opens the `.vortex` component of a segment.
pub fn open_vortex_segment(reader: &SegmentReader) -> tantivy::Result<VortexSegment> {
    let env = vortex_env();
    let file_slice = reader.open_read(SegmentComponent::Custom(VORTEX_EXTENSION.to_string()))?;
    let bytes = file_slice.read_bytes()?;
    let entries = read_footer(bytes.as_slice())?;
    let mut columns = HashMap::with_capacity(entries.len());
    for entry in entries {
        let file_bytes = &bytes.as_slice()[entry.start as usize..entry.end as usize];
        // One copy, so that the segments of the Vortex file can be read zero-copy: the base
        // pointer is 64 bytes aligned, but the buffer itself is declared as unaligned, because
        // Vortex slices it at arbitrary offsets.
        let buffer = ByteBufferMut::copy_from_aligned(file_bytes, Alignment::new(64))
            .freeze()
            .aligned(Alignment::new(1));
        let vortex_file = env
            .session
            .open_options()
            .open_buffer(buffer)
            .map_err(vortex_err_to_tantivy)?;
        let array_iter = vortex_file
            .scan()
            .map_err(vortex_err_to_tantivy)?
            .into_array_iter(&env.runtime)
            .map_err(vortex_err_to_tantivy)?;
        let mut chunks: Vec<ArrayRef> = Vec::new();
        let mut chunk_starts: Vec<RowId> = vec![0];
        let mut push_chunk = |chunk: ArrayRef| {
            if chunk.is_empty() {
                return;
            }
            let start = *chunk_starts.last().unwrap();
            chunk_starts.push(start + chunk.len() as RowId);
            chunks.push(chunk);
        };
        for array_res in array_iter {
            let array = array_res.map_err(vortex_err_to_tantivy)?;
            if let Some(chunked) = array.as_opt::<Chunked>() {
                for chunk in chunked.iter_chunks() {
                    push_chunk(chunk.clone());
                }
            } else {
                push_chunk(array);
            }
        }
        let column_name = entry.name.clone();
        let point_accessors = (0..chunks.len()).map(|_| OnceLock::new()).collect();
        let column_data = VortexColumnData {
            entry,
            chunks,
            chunk_starts,
            point_accessors,
        };
        columns.insert(column_name, Arc::new(column_data));
    }
    Ok(VortexSegment { columns })
}

/// Caches the opened Vortex segments, shared by all the providers of a registry.
#[derive(Default)]
pub struct VortexSegmentCache {
    segments: Mutex<HashMap<SegmentId, Arc<VortexSegment>>>,
}

impl VortexSegmentCache {
    pub fn get_or_open(&self, reader: &SegmentReader) -> tantivy::Result<Arc<VortexSegment>> {
        let segment_id = reader.segment_id();
        if let Some(segment) = self.segments.lock().unwrap().get(&segment_id) {
            return Ok(segment.clone());
        }
        // Opening outside of the lock: two threads may open the same segment concurrently,
        // which is harmless.
        let segment = Arc::new(open_vortex_segment(reader)?);
        self.segments
            .lock()
            .unwrap()
            .insert(segment_id, segment.clone());
        Ok(segment)
    }
}

/// Exposes the Vortex column `column_name` to aggregations.
pub struct VortexValueSourceProvider {
    column_name: String,
    cache: Arc<VortexSegmentCache>,
}

impl VortexValueSourceProvider {
    pub fn new(column_name: &str, cache: Arc<VortexSegmentCache>) -> Self {
        VortexValueSourceProvider {
            column_name: column_name.to_string(),
            cache,
        }
    }
}

impl ValueSourceProvider for VortexValueSourceProvider {
    fn for_segment(
        &self,
        reader: &SegmentReader,
        allowed_column_types: Option<&[ColumnType]>,
    ) -> tantivy::Result<Option<Box<dyn ValueSource>>> {
        let segment = self.cache.get_or_open(reader)?;
        let Some(column_data) = segment.columns.get(&self.column_name) else {
            return Ok(None);
        };
        if let Some(allowed_column_types) = allowed_column_types {
            if !allowed_column_types.contains(&column_data.column_type()) {
                return Ok(None);
            }
        }
        // Text columns store the tantivy term ords: resolving them requires the sstable
        // dictionary of the fast field.
        let str_column = if column_data.entry.stored_type == StoredType::Str {
            let Some(str_column) = reader.fast_fields().str(&self.column_name)? else {
                return Err(tantivy::TantivyError::InternalError(format!(
                    "missing dictionary for vortex text column {}",
                    self.column_name
                )));
            };
            Some(str_column)
        } else {
            None
        };
        Ok(Some(Box::new(VortexValueSource::new(
            column_data.clone(),
            str_column,
        ))))
    }
}

/// A run of docs is fetched with a Vortex `take` (rather than by decompressing its chunk) if it
/// spans more than `TAKE_SPAN_RATIO` times its number of docs. 0 disables `take`.
///
/// Configurable with the `VORTEX_TAKE_SPAN_RATIO` environment variable.
static TAKE_SPAN_RATIO: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("VORTEX_TAKE_SPAN_RATIO")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_TAKE_SPAN_RATIO)
});

const DEFAULT_TAKE_SPAN_RATIO: usize = 0;

/// A run of docs is read with a [`PointAccessor`] (rather than by decompressing its chunk) if it
/// spans more than `POINT_SPAN_RATIO` times its number of docs. 0 disables point access.
///
/// Configurable with the `VORTEX_POINT_SPAN_RATIO` environment variable.
static POINT_SPAN_RATIO: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("VORTEX_POINT_SPAN_RATIO")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_POINT_SPAN_RATIO)
});

const DEFAULT_POINT_SPAN_RATIO: usize = 8;

/// Runs shorter than this are always read point by point (when point access is enabled).
const MIN_RUN_LEN_FOR_DECODE: usize = 32;

/// Cost of a point access run, in "values": the per-run overhead (dispatch, chunk lookup...).
///
/// Configurable with the `VORTEX_POINT_RUN_COST` environment variable.
static POINT_RUN_COST: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("VORTEX_POINT_RUN_COST")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16)
});

/// Point reads into a chunk stop once their accumulated cost exceeds `chunk_len / divisor`
/// values: the chunk is then decompressed (and kept in the LRU).
///
/// This is the "ski rental" strategy: keep paying for point reads while they are cheaper than a
/// decompression, then pay for the decompression. Whatever the access pattern, the cost stays
/// within a small factor of the best of the two strategies. The divisor approximates how much
/// cheaper a value is when decompressed in bulk than when read alone.
///
/// Configurable with the `VORTEX_PROMOTE_DIVISOR` environment variable.
static PROMOTE_DIVISOR: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("VORTEX_PROMOTE_DIVISOR")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|&divisor| divisor > 0)
        .unwrap_or(4)
});

/// Number of decompressed chunks kept by each source (LRU).
///
/// Sub-aggregations receive their docs in buffered batches (grouped by parent bucket) that can
/// span several chunks: with a single cached chunk, each bucket of the batch would decompress
/// these chunks again.
///
/// Configurable with the `VORTEX_CHUNK_CACHE` environment variable.
static CHUNK_CACHE_CAPACITY: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("VORTEX_CHUNK_CACHE")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|&capacity| capacity > 0)
        .unwrap_or(DEFAULT_CHUNK_CACHE_CAPACITY)
});

const DEFAULT_CHUNK_CACHE_CAPACITY: usize = 8;

/// A decompressed chunk.
#[derive(Default)]
struct DecodedChunk {
    /// Row range of the chunk. Empty when nothing is decoded.
    start: RowId,
    end: RowId,
    chunk_ord: usize,
    /// Canonical (decompressed) values, in their stored type. Mapped to `u64` when loaded.
    /// - `Full` / `Optional`: one value per row.
    /// - `Multivalued`: list elements, addressed through `list_starts` and `list_sizes`.
    values: Option<PrimitiveArray>,
    /// `Optional` only, one entry per row.
    present: Vec<bool>,
    /// `Multivalued` only, one entry per row.
    list_starts: Vec<u32>,
    list_sizes: Vec<u32>,
    /// Logical clock of the last access, for LRU eviction.
    last_used: u64,
}

impl DecodedChunk {
    #[inline]
    fn contains(&self, row: RowId) -> bool {
        row >= self.start && row < self.end
    }
}

pub struct VortexValueSource {
    data: Arc<VortexColumnData>,
    str_column: Option<StrColumn>,
    ctx: ExecutionCtx,
    /// LRU cache of decompressed chunks.
    cache: Vec<DecodedChunk>,
    /// Slot of the most recently used chunk.
    current_slot: usize,
    clock: u64,
    /// Scratch buffer for the rows read by point access.
    rows_scratch: Vec<usize>,
    /// Accumulated cost of the point reads into each chunk (see `PROMOTE_DIVISOR`).
    point_costs: Vec<usize>,
    /// Diagnostics: number of point runs, point values and chunk decompressions.
    num_point_runs: usize,
    num_point_values: usize,
    num_decodes: usize,
}

/// Prints the counters of each source when it is dropped, if `VORTEX_DEBUG_COUNTS` is set.
static DEBUG_COUNTS: LazyLock<bool> =
    LazyLock::new(|| std::env::var("VORTEX_DEBUG_COUNTS").is_ok());

impl Drop for VortexValueSource {
    fn drop(&mut self) {
        if *DEBUG_COUNTS {
            eprintln!(
                "vortex source {}: {} point runs, {} point values, {} chunk decodes",
                self.data.entry.name, self.num_point_runs, self.num_point_values, self.num_decodes
            );
        }
    }
}

impl fmt::Debug for VortexValueSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VortexValueSource")
            .field("column", &self.data.entry.name)
            .finish()
    }
}

impl VortexValueSource {
    pub fn new(data: Arc<VortexColumnData>, str_column: Option<StrColumn>) -> Self {
        let num_chunks = data.chunks.len();
        VortexValueSource {
            data,
            str_column,
            ctx: vortex_env().session.create_execution_ctx(),
            cache: Vec::with_capacity(*CHUNK_CACHE_CAPACITY),
            current_slot: 0,
            clock: 0,
            rows_scratch: Vec::new(),
            point_costs: vec![0; num_chunks],
            num_point_runs: 0,
            num_point_values: 0,
            num_decodes: 0,
        }
    }

    /// Returns the cache slot holding the decompressed chunk containing `row`, if any.
    #[inline]
    fn find_slot(&self, row: RowId) -> Option<usize> {
        if let Some(current) = self.cache.get(self.current_slot) {
            if current.contains(row) {
                return Some(self.current_slot);
            }
        }
        self.cache.iter().position(|decoded| decoded.contains(row))
    }

    /// Returns the slot to decompress a new chunk into: a new one if the cache is not full, the
    /// least recently used one otherwise.
    fn slot_to_evict(&mut self) -> usize {
        if self.cache.len() < *CHUNK_CACHE_CAPACITY {
            self.cache.push(DecodedChunk::default());
            return self.cache.len() - 1;
        }
        let mut lru_slot = 0;
        for (slot, decoded) in self.cache.iter().enumerate() {
            if decoded.last_used < self.cache[lru_slot].last_used {
                lru_slot = slot;
            }
        }
        lru_slot
    }

    /// Returns true if `run` should be read with the point accessor of its chunk rather than by
    /// decompressing the chunk.
    ///
    /// Decompressing a chunk (~80k rows) only pays off for long, dense runs (scans). Short runs
    /// are read point by point even if they are contiguous: selective queries produce clusters
    /// of a few consecutive docs, and a cluster at the start of a chunk must not trigger the
    /// decompression of the whole chunk.
    #[inline]
    fn should_use_point_access(&self, chunk_ord: usize, run: &[DocId]) -> bool {
        let point_span_ratio = *POINT_SPAN_RATIO;
        if point_span_ratio == 0 {
            return false;
        }
        let chunk_len =
            (self.data.chunk_starts[chunk_ord + 1] - self.data.chunk_starts[chunk_ord]) as usize;
        let cost_after_run = self.point_costs[chunk_ord] + run.len() + *POINT_RUN_COST;
        if cost_after_run > chunk_len / *PROMOTE_DIVISOR {
            // Point reads into this chunk have cost as much as a decompression would.
            return false;
        }
        if run.len() < MIN_RUN_LEN_FOR_DECODE {
            return true;
        }
        let span = (run[run.len() - 1] - run[0]) as usize + 1;
        span > run.len() * point_span_ratio
    }

    /// Loads the values of `run` (docs of chunk `chunk_ord`) with the point accessor of the chunk.
    ///
    /// Returns false (and loads nothing) if the chunk has no point accessor.
    #[inline(never)]
    fn point_run(
        &mut self,
        chunk_ord: usize,
        run: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
    ) -> bool {
        let data = &self.data;
        let Some(accessor) = data.point_accessor(chunk_ord, &mut self.ctx) else {
            return false;
        };
        let chunk_start = data.chunk_starts[chunk_ord];
        let stored_type = data.entry.stored_type;
        match data.entry.cardinality {
            StoredCardinality::Full => {
                let rows = &mut self.rows_scratch;
                rows.clear();
                rows.extend(run.iter().map(|&doc| (doc - chunk_start) as usize));
                let start = values.len();
                accessor.get_many(rows, values);
                for value in &mut values[start..] {
                    *value = raw_to_mapped_u64(*value, stored_type);
                }
            }
            StoredCardinality::Optional => {
                let rows = &mut self.rows_scratch;
                rows.clear();
                for &doc in run {
                    let row = (doc - chunk_start) as usize;
                    if accessor.is_valid(row) {
                        rows.push(row);
                        docids.push(doc);
                    }
                }
                let start = values.len();
                accessor.get_many(rows, values);
                for value in &mut values[start..] {
                    *value = raw_to_mapped_u64(*value, stored_type);
                }
            }
            StoredCardinality::Multivalued => {
                for &doc in run {
                    let row = (doc - chunk_start) as usize;
                    accessor.for_each_list_value(row, |raw| {
                        values.push(raw_to_mapped_u64(raw, stored_type));
                        docids.push(doc);
                    });
                }
            }
        }
        true
    }

    /// Returns true if the values of `run` should be fetched with a Vortex `take` rather than by
    /// decompressing the whole chunk.
    ///
    /// `take` pays a fixed overhead per call, but only decodes what it needs: it wins when the
    /// docs are sparse (e.g. a selective query).
    #[inline]
    fn should_take(&self, run: &[DocId]) -> bool {
        let take_span_ratio = *TAKE_SPAN_RATIO;
        if take_span_ratio == 0 || self.data.entry.cardinality == StoredCardinality::Multivalued {
            return false;
        }
        let span = (run[run.len() - 1] - run[0]) as usize + 1;
        span > run.len() * take_span_ratio
    }

    /// Loads the values of `run` (docs of chunk `chunk_ord`) with a Vortex `take`.
    ///
    /// Precondition: the cardinality is not `Multivalued`.
    #[inline(never)]
    fn take_run(
        &mut self,
        chunk_ord: usize,
        run: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
    ) {
        let chunk_start = self.data.chunk_starts[chunk_ord];
        let indices: Vec<u32> = run.iter().map(|&doc| doc - chunk_start).collect();
        let indices_array =
            PrimitiveArray::new(Buffer::<u32>::from(indices), Validity::NonNullable).into_array();
        let taken = self.data.chunks[chunk_ord]
            .take(indices_array)
            .expect("failed to take from vortex chunk");
        let primitive = execute_primitive(taken, self.data.entry.stored_type, &mut self.ctx);
        let mut mapped_values: Vec<u64> = Vec::with_capacity(run.len());
        match primitive.ptype() {
            PType::U8 => map_all::<u8>(&primitive, &mut mapped_values),
            PType::U16 => map_all::<u16>(&primitive, &mut mapped_values),
            PType::U32 => map_all::<u32>(&primitive, &mut mapped_values),
            PType::U64 => map_all::<u64>(&primitive, &mut mapped_values),
            PType::I64 => map_all::<i64>(&primitive, &mut mapped_values),
            PType::F64 => map_all::<f64>(&primitive, &mut mapped_values),
            other => panic!("unexpected vortex primitive type {other:?}"),
        }
        if self.data.entry.cardinality == StoredCardinality::Full {
            values.extend_from_slice(&mapped_values);
            return;
        }
        let mask = primitive
            .as_ref()
            .validity()
            .and_then(|validity| validity.execute_mask(primitive.len(), &mut self.ctx))
            .expect("failed to decode vortex validity");
        for ((&doc, &value), present) in run.iter().zip(mapped_values.iter()).zip(mask.iter()) {
            if present {
                values.push(value);
                docids.push(doc);
            }
        }
    }

    /// Decompresses chunk `chunk_ord` into a cache slot, and returns that slot.
    #[inline(never)]
    fn decode_chunk(&mut self, chunk_ord: usize) -> usize {
        self.num_decodes += 1;
        let slot = self.slot_to_evict();
        let chunk = self.data.chunks[chunk_ord].clone();
        let stored_type = self.data.entry.stored_type;
        let decoded = &mut self.cache[slot];
        decoded.start = self.data.chunk_starts[chunk_ord];
        decoded.end = self.data.chunk_starts[chunk_ord + 1];
        decoded.chunk_ord = chunk_ord;
        match self.data.entry.cardinality {
            StoredCardinality::Full => {
                decoded.values = Some(execute_primitive(chunk, stored_type, &mut self.ctx));
            }
            StoredCardinality::Optional => {
                let primitive = execute_primitive(chunk, stored_type, &mut self.ctx);
                let mask = primitive
                    .as_ref()
                    .validity()
                    .and_then(|validity| validity.execute_mask(primitive.len(), &mut self.ctx))
                    .expect("failed to decode vortex validity");
                decoded.present.clear();
                decoded.present.extend(mask.iter());
                decoded.values = Some(primitive);
            }
            StoredCardinality::Multivalued => {
                let list_view: ListViewArray = chunk
                    .execute(&mut self.ctx)
                    .expect("failed to decode vortex list chunk");
                let parts = list_view.into_data_parts();
                decoded.values = Some(execute_primitive(
                    parts.elements,
                    stored_type,
                    &mut self.ctx,
                ));
                let offsets: PrimitiveArray = parts
                    .offsets
                    .execute(&mut self.ctx)
                    .expect("failed to decode vortex list offsets");
                integers_to_u32(&offsets, &mut decoded.list_starts);
                let sizes: PrimitiveArray = parts
                    .sizes
                    .execute(&mut self.ctx)
                    .expect("failed to decode vortex list sizes");
                integers_to_u32(&sizes, &mut decoded.list_sizes);
            }
        }
        slot
    }
}

/// Maps a raw value of a [`PointAccessor`] into tantivy's monotonic `u64` mapping.
#[inline]
fn raw_to_mapped_u64(raw: u64, stored_type: StoredType) -> u64 {
    match stored_type {
        StoredType::U64 | StoredType::Str => raw,
        StoredType::I64 | StoredType::DateTime => i64_to_u64(raw as i64),
        StoredType::F64 => f64_to_u64(f64::from_bits(raw)),
    }
}

/// Decompresses a chunk into a canonical primitive array.
fn execute_primitive(
    chunk: ArrayRef,
    stored_type: StoredType,
    ctx: &mut ExecutionCtx,
) -> PrimitiveArray {
    if stored_type == StoredType::DateTime {
        let extension: ExtensionArray = chunk
            .execute(ctx)
            .expect("failed to decode vortex temporal chunk");
        return extension
            .storage_array()
            .clone()
            .execute(ctx)
            .expect("failed to decode vortex temporal storage");
    }
    chunk.execute(ctx).expect("failed to decode vortex chunk")
}

/// Copies list offsets or sizes into a `Vec<u32>`.
fn integers_to_u32(primitive: &PrimitiveArray, output: &mut Vec<u32>) {
    output.clear();
    match primitive.ptype() {
        PType::U8 => output.extend(primitive.as_slice::<u8>().iter().map(|&val| val as u32)),
        PType::U16 => output.extend(primitive.as_slice::<u16>().iter().map(|&val| val as u32)),
        PType::U32 => output.extend_from_slice(primitive.as_slice::<u32>()),
        PType::U64 => output.extend(primitive.as_slice::<u64>().iter().map(|&val| val as u32)),
        PType::I32 => output.extend(primitive.as_slice::<i32>().iter().map(|&val| val as u32)),
        PType::I64 => output.extend(primitive.as_slice::<i64>().iter().map(|&val| val as u32)),
        other => panic!("unexpected vortex list offset type {other:?}"),
    }
}

/// Stored value types, with their mapping into tantivy's monotonic `u64` representation.
///
/// Unsigned integers are term ords (or u64 values): they map to themselves.
trait ToMappedU64: NativePType {
    fn to_mapped_u64(self) -> u64;
}

impl ToMappedU64 for u8 {
    #[inline(always)]
    fn to_mapped_u64(self) -> u64 {
        self as u64
    }
}

impl ToMappedU64 for u16 {
    #[inline(always)]
    fn to_mapped_u64(self) -> u64 {
        self as u64
    }
}

impl ToMappedU64 for u32 {
    #[inline(always)]
    fn to_mapped_u64(self) -> u64 {
        self as u64
    }
}

impl ToMappedU64 for u64 {
    #[inline(always)]
    fn to_mapped_u64(self) -> u64 {
        self
    }
}

impl ToMappedU64 for i64 {
    #[inline(always)]
    fn to_mapped_u64(self) -> u64 {
        i64_to_u64(self)
    }
}

impl ToMappedU64 for f64 {
    #[inline(always)]
    fn to_mapped_u64(self) -> u64 {
        f64_to_u64(self)
    }
}

fn map_all<T: ToMappedU64>(primitive: &PrimitiveArray, output: &mut Vec<u64>) {
    output.extend(primitive.as_slice::<T>().iter().map(|&value| value.to_mapped_u64()));
}

/// Loads the values of `run`, a sorted run of docs that all belong to the decoded chunk.
#[inline]
fn load_run<T: ToMappedU64>(
    decoded: &DecodedChunk,
    cardinality: StoredCardinality,
    run: &[DocId],
    values: &mut Vec<u64>,
    docids: &mut Vec<DocId>,
) {
    let typed_values: &[T] = decoded
        .values
        .as_ref()
        .expect("chunk is decoded")
        .as_slice::<T>();
    match cardinality {
        StoredCardinality::Full => {
            let first = (run[0] - decoded.start) as usize;
            let last = (run[run.len() - 1] - decoded.start) as usize;
            if last - first + 1 == run.len() {
                values.extend(
                    typed_values[first..=last]
                        .iter()
                        .map(|&value| value.to_mapped_u64()),
                );
            } else {
                values.extend(run.iter().map(|&doc| {
                    typed_values[(doc - decoded.start) as usize].to_mapped_u64()
                }));
            }
        }
        StoredCardinality::Optional => {
            for &doc in run {
                let row = (doc - decoded.start) as usize;
                if decoded.present[row] {
                    values.push(typed_values[row].to_mapped_u64());
                    docids.push(doc);
                }
            }
        }
        StoredCardinality::Multivalued => {
            for &doc in run {
                let row = (doc - decoded.start) as usize;
                let start = decoded.list_starts[row] as usize;
                let size = decoded.list_sizes[row] as usize;
                for &value in &typed_values[start..start + size] {
                    values.push(value.to_mapped_u64());
                    docids.push(doc);
                }
            }
        }
    }
}

impl ValueSource for VortexValueSource {
    fn column_type(&self) -> ColumnType {
        self.data.column_type()
    }

    fn load_block(
        &mut self,
        docs: &[DocId],
        values: &mut Vec<u64>,
        docids: &mut Vec<DocId>,
        _row_ids: &mut Vec<RowId>,
    ) -> Cardinality {
        values.clear();
        docids.clear();
        let cardinality = self.data.entry.cardinality;
        let mut i = 0;
        while i < docs.len() {
            let first_doc = docs[i];
            let slot_opt = self.find_slot(first_doc);
            let chunk_ord = match slot_opt {
                Some(slot) => self.cache[slot].chunk_ord,
                None => self.data.chunk_starts.partition_point(|&start| start <= first_doc) - 1,
            };
            let chunk_end = self.data.chunk_starts[chunk_ord + 1];
            // `docs` is sorted: the docs of this chunk form a run.
            let run_len = docs[i..].partition_point(|&doc| doc < chunk_end);
            let run = &docs[i..i + run_len];
            i += run_len;
            let slot = match slot_opt {
                Some(slot) => slot,
                None => {
                    if self.should_use_point_access(chunk_ord, run)
                        && self.point_run(chunk_ord, run, values, docids)
                    {
                        self.point_costs[chunk_ord] += run.len() + *POINT_RUN_COST;
                        self.num_point_runs += 1;
                        self.num_point_values += run.len();
                        continue;
                    }
                    if self.should_take(run) {
                        self.take_run(chunk_ord, run, values, docids);
                        continue;
                    }
                    self.decode_chunk(chunk_ord)
                }
            };
            self.clock += 1;
            self.cache[slot].last_used = self.clock;
            self.current_slot = slot;
            let decoded = &self.cache[slot];
            let ptype = decoded.values.as_ref().expect("chunk is decoded").ptype();
            match ptype {
                PType::U8 => load_run::<u8>(decoded, cardinality, run, values, docids),
                PType::U16 => load_run::<u16>(decoded, cardinality, run, values, docids),
                PType::U32 => load_run::<u32>(decoded, cardinality, run, values, docids),
                PType::U64 => load_run::<u64>(decoded, cardinality, run, values, docids),
                PType::I64 => load_run::<i64>(decoded, cardinality, run, values, docids),
                PType::F64 => load_run::<f64>(decoded, cardinality, run, values, docids),
                other => panic!("unexpected vortex primitive type {other:?}"),
            }
        }
        match cardinality {
            StoredCardinality::Full => Cardinality::Full,
            StoredCardinality::Optional => Cardinality::Optional,
            StoredCardinality::Multivalued => Cardinality::Multivalued,
        }
    }

    fn bounds(&self) -> Option<(u64, u64)> {
        if self.data.entry.num_values == 0 {
            return None;
        }
        Some((self.data.entry.min_value, self.data.entry.max_value))
    }

    fn term_dictionary(&self) -> Option<&dyn ValueSourceDictionary> {
        let str_column = self.str_column.as_ref()?;
        Some(str_column.dictionary())
    }
}
