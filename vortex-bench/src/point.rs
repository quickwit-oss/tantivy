//! Point access into compressed Vortex chunks.
//!
//! The generic Vortex compute path (`take`, `scalar_at`) pays a fixed overhead per call
//! (lazy arrays, optimizer, dynamic dispatch, allocations). For selective queries, the
//! `ValueSource` only needs a few values per chunk, so we walk the encoding tree of a chunk once
//! and build a [`PointAccessor`] that reads single values straight from the compressed buffers.
//!
//! Supported encodings: `fastlanes.bitpacked` (global bit width), `fastlanes.for`,
//! `vortex.constant`, `vortex.primitive`, `vortex.slice`, `vortex.sequence`, `vortex.alprd`,
//! `vortex.datetimeparts`, `vortex.sparse`, `vortex.list`, and the temporal extension wrapper.
//! Small sub-arrays of any other encoding (e.g. dictionaries, FoR references) are materialized.
//! Chunks with an unsupported encoding tree get no accessor: the caller then decompresses the
//! whole chunk.
//!
//! Values are read as "raw" `u64`: integers in two's complement (signed types sign-extended),
//! floats as their IEEE bits. The caller maps them into tantivy's monotonic `u64` mapping.

use fastlanes::BitPacking;
use vortex::array::arrays::extension::ExtensionArrayExt;
use vortex::array::arrays::list::ListArraySlotsExt;
use vortex::array::arrays::masked::MaskedArraySlotsExt;
use vortex::array::arrays::slice::SliceArraySlotsExt;
use vortex::array::arrays::{Chunked, Extension, List, Masked, Primitive, PrimitiveArray, Slice};
use vortex::array::patches::Patches;
use vortex::array::{ArrayRef, ExecutionCtx};
use vortex::buffer::Buffer;
use vortex::dtype::{DType, PType};
use vortex::encodings::alp::{ALPRDArrayExt, ALPRDArraySlotsExt, ALPRD};
use vortex::encodings::datetime_parts::{DateTimeParts, DateTimePartsArraySlotsExt};
use vortex::encodings::fastlanes::{
    BitPacked, BitPackedArrayExt, BitWidthsView, FoR, FoRArrayExt, FoRArraySlotsExt,
};
use vortex::encodings::sequence::Sequence;
use vortex::encodings::sparse::{Sparse, SparseExt};
use vortex::encodings::zigzag::{ZigZag, ZigZagArraySlotsExt};
use vortex::extension::datetime::{TimeUnit, Timestamp};
use vortex::scalar::{PValue, Scalar};

/// FastLanes block size.
const FL_CHUNK_SIZE: usize = 1024;

/// Sub-arrays of an unsupported encoding are materialized if they are at most this long.
const MAX_MATERIALIZED_LEN: usize = 4096;

/// Reads the values of one chunk, one row at a time.
pub(crate) struct PointAccessor {
    /// Rows without a value. `None` if every row has a value.
    validity: Option<Vec<u64>>,
    kind: AccessorKind,
}

enum AccessorKind {
    /// One value per row.
    Single(Node),
    /// A list per row: `offsets[row]..offsets[row + 1]` are indices into `elements`.
    List { offsets: Node, elements: Node },
}

impl PointAccessor {
    /// Builds the accessor of `chunk`, or returns `None` if its encoding tree is not supported.
    pub(crate) fn build(chunk: &ArrayRef, ctx: &mut ExecutionCtx) -> Option<PointAccessor> {
        let validity = build_validity(chunk, ctx)?;
        let kind = if matches!(chunk.dtype(), DType::List(..)) {
            build_list(chunk, ctx)?
        } else {
            AccessorKind::Single(Node::build(chunk, ctx)?)
        };
        Some(PointAccessor { validity, kind })
    }

    #[inline]
    pub(crate) fn is_valid(&self, row: usize) -> bool {
        match &self.validity {
            None => true,
            Some(words) => (words[row / 64] >> (row % 64)) & 1 == 1,
        }
    }

    /// Appends the values of `rows` (sorted) to `output`, for single valued chunks.
    #[inline]
    pub(crate) fn get_many(&self, rows: &[usize], output: &mut Vec<u64>) {
        match &self.kind {
            AccessorKind::Single(node) => node.get_many(rows, output),
            AccessorKind::List { .. } => panic!("get_many called on a list accessor"),
        }
    }

    /// Calls `callback` with each value of `row`, for list chunks.
    #[inline]
    pub(crate) fn for_each_list_value(&self, row: usize, mut callback: impl FnMut(u64)) {
        match &self.kind {
            AccessorKind::List { offsets, elements } => {
                let start = offsets.get(row) as usize;
                let end = offsets.get(row + 1) as usize;
                for element in start..end {
                    callback(elements.get(element));
                }
            }
            AccessorKind::Single(node) => callback(node.get(row)),
        }
    }
}

fn build_validity(chunk: &ArrayRef, ctx: &mut ExecutionCtx) -> Option<Option<Vec<u64>>> {
    if !chunk.dtype().is_nullable() {
        return Some(None);
    }
    let mask = chunk
        .validity()
        .ok()?
        .execute_mask(chunk.len(), ctx)
        .ok()?;
    if mask.all_true() {
        return Some(None);
    }
    let mut words = vec![0u64; chunk.len().div_ceil(64)];
    for (row, is_valid) in mask.iter().enumerate() {
        if is_valid {
            words[row / 64] |= 1u64 << (row % 64);
        }
    }
    Some(Some(words))
}

fn build_list(chunk: &ArrayRef, ctx: &mut ExecutionCtx) -> Option<AccessorKind> {
    // Lists are only supported unsliced, as written by the `VortexPlugin`.
    let list = chunk.as_opt::<List>()?;
    let offsets = Node::build(list.offsets(), ctx)?;
    let elements = Node::build(list.elements(), ctx)?;
    Some(AccessorKind::List { offsets, elements })
}

/// Typed buffer of a primitive array.
enum PrimitiveValues {
    U8(Buffer<u8>),
    U16(Buffer<u16>),
    U32(Buffer<u32>),
    U64(Buffer<u64>),
    I8(Buffer<i8>),
    I16(Buffer<i16>),
    I32(Buffer<i32>),
    I64(Buffer<i64>),
    F64(Buffer<f64>),
}

impl PrimitiveValues {
    fn from_array(primitive: &PrimitiveArray) -> Option<PrimitiveValues> {
        Some(match primitive.ptype() {
            PType::U8 => PrimitiveValues::U8(primitive.to_buffer::<u8>()),
            PType::U16 => PrimitiveValues::U16(primitive.to_buffer::<u16>()),
            PType::U32 => PrimitiveValues::U32(primitive.to_buffer::<u32>()),
            PType::U64 => PrimitiveValues::U64(primitive.to_buffer::<u64>()),
            PType::I8 => PrimitiveValues::I8(primitive.to_buffer::<i8>()),
            PType::I16 => PrimitiveValues::I16(primitive.to_buffer::<i16>()),
            PType::I32 => PrimitiveValues::I32(primitive.to_buffer::<i32>()),
            PType::I64 => PrimitiveValues::I64(primitive.to_buffer::<i64>()),
            PType::F64 => PrimitiveValues::F64(primitive.to_buffer::<f64>()),
            _ => return None,
        })
    }

    #[inline]
    fn get(&self, index: usize) -> u64 {
        match self {
            PrimitiveValues::U8(values) => values[index] as u64,
            PrimitiveValues::U16(values) => values[index] as u64,
            PrimitiveValues::U32(values) => values[index] as u64,
            PrimitiveValues::U64(values) => values[index],
            PrimitiveValues::I8(values) => values[index] as i64 as u64,
            PrimitiveValues::I16(values) => values[index] as i64 as u64,
            PrimitiveValues::I32(values) => values[index] as i64 as u64,
            PrimitiveValues::I64(values) => values[index] as u64,
            PrimitiveValues::F64(values) => values[index].to_bits(),
        }
    }

    fn to_vec(&self, len: usize) -> Vec<u64> {
        (0..len).map(|index| self.get(index)).collect()
    }
}

/// Packed FastLanes buffer, typed by the unsigned type of the array.
enum PackedValues {
    U8(Buffer<u8>),
    U16(Buffer<u16>),
    U32(Buffer<u32>),
    U64(Buffer<u64>),
}

/// Patches materialized as sorted `(array index, raw value)` pairs.
struct MaterializedPatches {
    /// Array indices (the patch offset already removed), sorted.
    indices: Vec<u64>,
    values: Vec<u64>,
    /// Bitset of the patched indices, so that unpatched indices (the common case) skip the
    /// binary search.
    patched: Vec<u64>,
}

impl MaterializedPatches {
    fn build(patches: &Patches, ctx: &mut ExecutionCtx) -> Option<MaterializedPatches> {
        let offset = patches.offset() as u64;
        let indices: PrimitiveArray = patches.indices().clone().execute(ctx).ok()?;
        let indices: Vec<u64> = PrimitiveValues::from_array(&indices)?
            .to_vec(indices.len())
            .into_iter()
            .map(|index| index - offset)
            .collect();
        let values: PrimitiveArray = patches.values().clone().execute(ctx).ok()?;
        let values = PrimitiveValues::from_array(&values)?.to_vec(values.len());
        let num_words = indices.last().map(|&last| last as usize / 64 + 1).unwrap_or(0);
        let mut patched = vec![0u64; num_words];
        for &index in &indices {
            patched[index as usize / 64] |= 1u64 << (index % 64);
        }
        Some(MaterializedPatches {
            indices,
            values,
            patched,
        })
    }

    #[inline]
    fn get(&self, index: usize) -> Option<u64> {
        let word = *self.patched.get(index / 64)?;
        if (word >> (index % 64)) & 1 == 0 {
            return None;
        }
        let position = self.indices.binary_search(&(index as u64)).ok()?;
        Some(self.values[position])
    }
}

/// Node of the accessor tree. `get(index)` returns the raw value at `index`.
enum Node {
    Constant(u64),
    Primitive(PrimitiveValues),
    BitPacked {
        packed: PackedValues,
        bit_width: usize,
        /// Position of the first element within the first FastLanes block.
        offset: usize,
        patches: Option<MaterializedPatches>,
        /// Sign extension for signed types narrower than 64 bits (0 for unsigned or 64 bits).
        sign_shift: u32,
    },
    FoR {
        encoded: Box<Node>,
        /// One reference per FastLanes block.
        references: Vec<u64>,
        offset: usize,
    },
    Slice {
        child: Box<Node>,
        start: usize,
    },
    Sequence {
        base: u64,
        multiplier: u64,
    },
    AlpRd {
        left_codes: Box<Node>,
        right_parts: Box<Node>,
        left_dictionary: Vec<u64>,
        right_bit_width: u32,
        left_patches: Option<MaterializedPatches>,
    },
    DateTimeParts {
        days: Box<Node>,
        seconds: Box<Node>,
        subseconds: Box<Node>,
        /// Number of time units per second.
        divisor: i64,
    },
    Sparse {
        patches: MaterializedPatches,
        fill: u64,
    },
    ZigZag(Box<Node>),
}

#[inline]
fn zigzag_decode(encoded: u64) -> u64 {
    // Sign-extended two's complement of the decoded value, for any integer width.
    (encoded >> 1) ^ 0u64.wrapping_sub(encoded & 1)
}

fn pvalue_to_raw(pvalue: PValue) -> Option<u64> {
    Some(match pvalue {
        PValue::U8(value) => value as u64,
        PValue::U16(value) => value as u64,
        PValue::U32(value) => value as u64,
        PValue::U64(value) => value,
        PValue::I8(value) => value as i64 as u64,
        PValue::I16(value) => value as i64 as u64,
        PValue::I32(value) => value as i64 as u64,
        PValue::I64(value) => value as u64,
        PValue::F64(value) => value.to_bits(),
        _ => return None,
    })
}

/// Raw value of a scalar. Null scalars map to 0 (callers check validity separately).
fn scalar_to_raw(scalar: &Scalar) -> Option<u64> {
    if scalar.is_null() {
        return Some(0);
    }
    let pvalue = scalar.as_primitive().pvalue()?;
    pvalue_to_raw(pvalue)
}

fn signed_bit_width(ptype: PType) -> u32 {
    match ptype {
        PType::I8 => 8,
        PType::I16 => 16,
        PType::I32 => 32,
        _ => 0,
    }
}

impl Node {
    fn build(array: &ArrayRef, ctx: &mut ExecutionCtx) -> Option<Node> {
        if let Some(scalar) = array.as_constant() {
            return Some(Node::Constant(scalar_to_raw(&scalar)?));
        }
        if let Some(primitive) = array.as_opt::<Primitive>() {
            let primitive = primitive.into_owned();
            return Some(Node::Primitive(PrimitiveValues::from_array(&primitive)?));
        }
        if let Some(extension) = array.as_opt::<Extension>() {
            return Node::build(extension.storage_array(), ctx);
        }
        if let Some(slice) = array.as_opt::<Slice>() {
            let child = Node::build(slice.child(), ctx)?;
            return Some(Node::Slice {
                child: Box::new(child),
                start: slice.slice_range().start,
            });
        }
        if let Some(bitpacked) = array.as_opt::<BitPacked>() {
            let BitWidthsView::Global(bit_width) = bitpacked.bit_widths() else {
                return None;
            };
            let ptype = array.dtype().as_ptype();
            let packed_handle = bitpacked.packed().as_host_opt()?.clone();
            let packed = match ptype.to_unsigned() {
                PType::U8 => PackedValues::U8(Buffer::from_byte_buffer(packed_handle)),
                PType::U16 => PackedValues::U16(Buffer::from_byte_buffer(packed_handle)),
                PType::U32 => PackedValues::U32(Buffer::from_byte_buffer(packed_handle)),
                PType::U64 => PackedValues::U64(Buffer::from_byte_buffer(packed_handle)),
                _ => return None,
            };
            let patches = match bitpacked.patches() {
                Some(patches) => Some(MaterializedPatches::build(&patches, ctx)?),
                None => None,
            };
            let signed_width = signed_bit_width(ptype);
            return Some(Node::BitPacked {
                packed,
                bit_width: bit_width as usize,
                offset: bitpacked.offset() as usize,
                patches,
                sign_shift: if signed_width == 0 { 0 } else { 64 - signed_width },
            });
        }
        if let Some(for_array) = array.as_opt::<FoR>() {
            let encoded = Node::build(for_array.encoded(), ctx)?;
            let references_array: PrimitiveArray =
                for_array.references().clone().execute(ctx).ok()?;
            let references =
                PrimitiveValues::from_array(&references_array)?.to_vec(references_array.len());
            return Some(Node::FoR {
                encoded: Box::new(encoded),
                references,
                offset: for_array.offset() as usize,
            });
        }
        if let Some(sequence) = array.as_opt::<Sequence>() {
            return Some(Node::Sequence {
                base: pvalue_to_raw(sequence.base())?,
                multiplier: pvalue_to_raw(sequence.multiplier())?,
            });
        }
        if let Some(alp_rd) = array.as_opt::<ALPRD>() {
            let left_codes = Node::build(alp_rd.left_parts(), ctx)?;
            let right_parts = Node::build(alp_rd.right_parts(), ctx)?;
            let left_dictionary = alp_rd
                .left_parts_dictionary()
                .iter()
                .map(|&left| left as u64)
                .collect();
            let left_patches = match alp_rd.left_parts_patches() {
                Some(patches) => Some(MaterializedPatches::build(&patches, ctx)?),
                None => None,
            };
            return Some(Node::AlpRd {
                left_codes: Box::new(left_codes),
                right_parts: Box::new(right_parts),
                left_dictionary,
                right_bit_width: alp_rd.right_bit_width() as u32,
                left_patches,
            });
        }
        if let Some(datetime_parts) = array.as_opt::<DateTimeParts>() {
            let DType::Extension(extension) = array.dtype() else {
                return None;
            };
            let timestamp = extension.metadata_opt::<Timestamp>()?;
            let divisor = match timestamp.unit {
                TimeUnit::Nanoseconds => 1_000_000_000,
                TimeUnit::Microseconds => 1_000_000,
                TimeUnit::Milliseconds => 1_000,
                TimeUnit::Seconds => 1,
                TimeUnit::Days => return None,
            };
            return Some(Node::DateTimeParts {
                days: Box::new(Node::build(datetime_parts.days(), ctx)?),
                seconds: Box::new(Node::build(datetime_parts.seconds(), ctx)?),
                subseconds: Box::new(Node::build(datetime_parts.subseconds(), ctx)?),
                divisor,
            });
        }
        if let Some(masked) = array.as_opt::<Masked>() {
            // Validity is handled at the top of the chunk.
            return Node::build(masked.child(), ctx);
        }
        if let Some(zigzag) = array.as_opt::<ZigZag>() {
            return Some(Node::ZigZag(Box::new(Node::build(zigzag.encoded(), ctx)?)));
        }
        if let Some(sparse) = array.as_opt::<Sparse>() {
            let patches = MaterializedPatches::build(&sparse.patches(), ctx)?;
            let fill = scalar_to_raw(sparse.fill_scalar())?;
            return Some(Node::Sparse { patches, fill });
        }
        if array.is::<Chunked>() {
            return None;
        }
        // Small arrays of any other encoding are materialized.
        if array.len() <= MAX_MATERIALIZED_LEN && matches!(array.dtype(), DType::Primitive(..)) {
            let primitive: PrimitiveArray = array.clone().execute(ctx).ok()?;
            return Some(Node::Primitive(PrimitiveValues::from_array(&primitive)?));
        }
        None
    }

    #[inline]
    fn get(&self, index: usize) -> u64 {
        match self {
            Node::Constant(value) => *value,
            Node::Primitive(values) => values.get(index),
            Node::BitPacked {
                packed,
                bit_width,
                offset,
                patches,
                sign_shift,
            } => {
                if let Some(patches) = patches {
                    if let Some(value) = patches.get(index) {
                        return value;
                    }
                }
                let raw = unpack_single(packed, *bit_width, index + offset);
                if *sign_shift == 0 {
                    raw
                } else {
                    (((raw << sign_shift) as i64) >> sign_shift) as u64
                }
            }
            Node::FoR {
                encoded,
                references,
                offset,
            } => {
                let reference = references[(index + offset) / FL_CHUNK_SIZE];
                encoded.get(index).wrapping_add(reference)
            }
            Node::Slice { child, start } => child.get(index + start),
            Node::Sequence { base, multiplier } => {
                base.wrapping_add(multiplier.wrapping_mul(index as u64))
            }
            Node::AlpRd {
                left_codes,
                right_parts,
                left_dictionary,
                right_bit_width,
                left_patches,
            } => {
                let left = match left_patches.as_ref().and_then(|patches| patches.get(index)) {
                    Some(left) => left,
                    None => left_dictionary[left_codes.get(index) as usize],
                };
                (left << right_bit_width) | right_parts.get(index)
            }
            Node::DateTimeParts {
                days,
                seconds,
                subseconds,
                divisor,
            } => {
                let days = days.get(index) as i64;
                let seconds = seconds.get(index) as i64;
                let subseconds = subseconds.get(index) as i64;
                (days * 86_400 * divisor + seconds * divisor + subseconds) as u64
            }
            Node::Sparse { patches, fill } => patches.get(index).unwrap_or(*fill),
            Node::ZigZag(child) => zigzag_decode(child.get(index)),
        }
    }
}

impl Node {
    /// Appends the values at `indices` (sorted) to `output`.
    ///
    /// Same result as calling `get` on each index, but dispatches once per batch (and once per
    /// FastLanes block for bitpacked arrays) rather than once per value.
    fn get_many(&self, indices: &[usize], output: &mut Vec<u64>) {
        match self {
            Node::Constant(value) => output.extend(std::iter::repeat_n(*value, indices.len())),
            Node::Primitive(values) => output.extend(indices.iter().map(|&index| values.get(index))),
            Node::BitPacked {
                packed,
                bit_width,
                offset,
                patches,
                sign_shift,
            } => {
                let start = output.len();
                unpack_many(packed, *bit_width, *offset, indices, output);
                if *sign_shift != 0 {
                    for value in &mut output[start..] {
                        *value = (((*value << sign_shift) as i64) >> sign_shift) as u64;
                    }
                }
                if let Some(patches) = patches {
                    for (value, &index) in output[start..].iter_mut().zip(indices) {
                        if let Some(patched) = patches.get(index) {
                            *value = patched;
                        }
                    }
                }
            }
            Node::FoR {
                encoded,
                references,
                offset,
            } => {
                let start = output.len();
                encoded.get_many(indices, output);
                for (value, &index) in output[start..].iter_mut().zip(indices) {
                    *value = value.wrapping_add(references[(index + offset) / FL_CHUNK_SIZE]);
                }
            }
            Node::Slice { child, start } => {
                let shifted: Vec<usize> = indices.iter().map(|&index| index + start).collect();
                child.get_many(&shifted, output);
            }
            Node::ZigZag(child) => {
                let start = output.len();
                child.get_many(indices, output);
                for value in &mut output[start..] {
                    *value = zigzag_decode(*value);
                }
            }
            Node::Sequence { .. } | Node::Sparse { .. } => {
                output.extend(indices.iter().map(|&index| self.get(index)));
            }
            Node::AlpRd {
                left_codes,
                right_parts,
                left_dictionary,
                right_bit_width,
                left_patches,
            } => {
                let start = output.len();
                left_codes.get_many(indices, output);
                let mut rights = Vec::with_capacity(indices.len());
                right_parts.get_many(indices, &mut rights);
                for ((value, &right), &index) in
                    output[start..].iter_mut().zip(&rights).zip(indices)
                {
                    let left = match left_patches.as_ref().and_then(|patches| patches.get(index)) {
                        Some(left) => left,
                        None => left_dictionary[*value as usize],
                    };
                    *value = (left << right_bit_width) | right;
                }
            }
            Node::DateTimeParts {
                days,
                seconds,
                subseconds,
                divisor,
            } => {
                let start = output.len();
                days.get_many(indices, output);
                let mut parts = Vec::with_capacity(indices.len());
                seconds.get_many(indices, &mut parts);
                for (value, &seconds) in output[start..].iter_mut().zip(&parts) {
                    *value = ((*value as i64) * 86_400 * divisor + (seconds as i64) * divisor)
                        as u64;
                }
                parts.clear();
                subseconds.get_many(indices, &mut parts);
                for (value, &subseconds) in output[start..].iter_mut().zip(&parts) {
                    *value = ((*value as i64) + (subseconds as i64)) as u64;
                }
            }
        }
    }
}

/// Appends the elements at `indices` (sorted) of a FastLanes bitpacked buffer to `output`.
///
/// Indices are grouped by FastLanes block, so that the bit width is dispatched once per block.
fn unpack_many(
    packed: &PackedValues,
    bit_width: usize,
    offset: usize,
    indices: &[usize],
    output: &mut Vec<u64>,
) {
    if bit_width == 0 {
        output.extend(std::iter::repeat_n(0u64, indices.len()));
        return;
    }
    match packed {
        PackedValues::U8(values) => unpack_many_typed::<u8>(values, bit_width, offset, indices, output),
        PackedValues::U16(values) => {
            unpack_many_typed::<u16>(values, bit_width, offset, indices, output)
        }
        PackedValues::U32(values) => {
            unpack_many_typed::<u32>(values, bit_width, offset, indices, output)
        }
        PackedValues::U64(values) => {
            unpack_many_typed::<u64>(values, bit_width, offset, indices, output)
        }
    }
}

/// Maximum number of values unpacked by one `unchecked_unpack_indices` call.
const UNPACK_BATCH_LEN: usize = 64;

fn unpack_many_typed<T: BitPacking + Copy + Into<u64>>(
    packed: &[T],
    bit_width: usize,
    offset: usize,
    indices: &[usize],
    output: &mut Vec<u64>,
) {
    // A block of 1024 values of `bit_width` bits is `128 * bit_width` bytes.
    let words_per_block = 128 * bit_width / size_of::<T>();
    let mut in_block = [0usize; UNPACK_BATCH_LEN];
    let mut unpacked = [std::mem::MaybeUninit::<T>::uninit(); UNPACK_BATCH_LEN];
    let mut i = 0;
    while i < indices.len() {
        let block = (indices[i] + offset) / FL_CHUNK_SIZE;
        let mut batch_len = 0;
        while i + batch_len < indices.len()
            && batch_len < UNPACK_BATCH_LEN
            && (indices[i + batch_len] + offset) / FL_CHUNK_SIZE == block
        {
            in_block[batch_len] = (indices[i + batch_len] + offset) % FL_CHUNK_SIZE;
            batch_len += 1;
        }
        let block_words = &packed[block * words_per_block..][..words_per_block];
        // SAFETY: `block_words` holds a full FastLanes block of `bit_width` bits values,
        // `bit_width <= 8 * size_of::<T>()` and all the indices are `< 1024`.
        unsafe {
            T::unchecked_unpack_indices(
                bit_width,
                block_words,
                &in_block[..batch_len],
                &mut unpacked[..batch_len],
            );
        }
        output.extend(
            unpacked[..batch_len]
                .iter()
                // SAFETY: the first `batch_len` values were written by `unchecked_unpack_indices`.
                .map(|value| unsafe { value.assume_init() }.into()),
        );
        i += batch_len;
    }
}

/// Reads element `index` of a FastLanes bitpacked buffer.
#[inline]
fn unpack_single(packed: &PackedValues, bit_width: usize, index: usize) -> u64 {
    let block = index / FL_CHUNK_SIZE;
    let index_in_block = index % FL_CHUNK_SIZE;
    // A block of 1024 values of `bit_width` bits is `128 * bit_width` bytes.
    let block_bytes = 128 * bit_width;
    macro_rules! unpack {
        ($values:expr, $ty:ty) => {{
            let words_per_block = block_bytes / size_of::<$ty>();
            let block_words = &$values[block * words_per_block..][..words_per_block];
            // SAFETY: `block_words` holds a full FastLanes block of `bit_width` bits values,
            // `bit_width <= 8 * size_of::<$ty>()` and `index_in_block < 1024`.
            unsafe {
                <$ty as BitPacking>::unchecked_unpack_single(bit_width, block_words, index_in_block)
                    as u64
            }
        }};
    }
    if bit_width == 0 {
        return 0;
    }
    match packed {
        PackedValues::U8(values) => unpack!(values, u8),
        PackedValues::U16(values) => unpack!(values, u16),
        PackedValues::U32(values) => unpack!(values, u32),
        PackedValues::U64(values) => unpack!(values, u64),
    }
}
