//! SVE kernels, the vector-length-agnostic counterparts of `neon.rs`: fused
//! decode-and-filter plus block unpack, gathering packed bytes per lane and
//! shifting into place so values never exist decoded in memory.
//!
//! SVE pays off because `compact` is a single instruction over a whole
//! vector: NEON compacts 4 ids through a lookup table, SVE compacts `vl` of
//! them, where `vl` is 4 lanes at a 128-bit vector length and 16 at 512-bit.
use std::ops::RangeInclusive;

/// Number of 32-bit lanes in an SVE vector. Not a compile-time constant, so it
/// is queried at runtime.
///
/// # Safety
/// Only callable once SVE is known present.
#[target_feature(enable = "sve")]
unsafe fn num_lanes() -> usize {
    let vl: usize;
    unsafe {
        core::arch::asm!(
            "cntw {vl}",
            vl = out(reg) vl,
            options(nostack, nomem, preserves_flags),
        );
    }
    vl
}

/// Values consumed per iteration of the kernel below, given `vl` 32-bit lanes.
///
/// The loop is unrolled by two, so a step covers `2 * vl` values. That matters
/// for more than throughput: a step advances the data pointer by
/// `2 * vl * w / 8` bytes, which is only a whole number of bytes because
/// `2 * vl` is a multiple of 8 for every architectural vector length. A
/// single-vector loop would need a fractional advance at `vl == 4`.
#[inline]
pub(super) fn values_per_step(vl: usize) -> usize {
    2 * vl
}

/// Whether the SVE kernel takes this width.
///
/// The gather pulls 4 bytes per lane into a 32-bit lane and shifts right by up
/// to 7, so it needs `bit_shift + w <= 32`, exactly as the NEON narrow kernel
/// does. Wider values stay on the NEON kernels.
#[inline]
pub(super) fn kernel_applies(bit_width: u8) -> bool {
    (1..=25).contains(&bit_width)
}

/// Runtime SVE detection, cached.
pub(super) fn available() -> bool {
    static SVE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SVE.get_or_init(|| {
        if std::env::var_os("TANTIVY_BITPACKER_NO_SVE").is_some() {
            return false;
        }
        std::arch::is_aarch64_feature_detected!("sve")
    })
}

/// The group-aligned middle of a fused decode-and-filter, when SVE takes it:
/// picks the 32- or 64-bit kernel by width, caps the step count by what `data`
/// holds, and writes the matching ids densely to `out`.
///
/// Returns `(matches_written, values_covered)`, or `None` when SVE is absent
/// or neither kernel takes the width, leaving the middle to NEON. `Some` with
/// `values_covered == 0` still claims the range: SVE and NEON are never mixed
/// inside one call, so a measurement of one is never a blend of the two.
///
/// `out` must have room for `num_values` ids.
pub(super) fn filter_middle(
    w: usize,
    num_values: usize,
    value_range: &std::ops::RangeInclusive<u64>,
    first_id: u32,
    data: &[u8],
    out: *mut u32,
) -> Option<(usize, usize)> {
    let bit_width = w as u8;
    // Values are at most `w` bits, so for the 32-bit kernel a range starting
    // above `u32::MAX` selects nothing -- NEON's dispatch answers that case.
    let start_over_u32 = *value_range.start() > u64::from(u32::MAX);
    if lanes() > 0 && kernel_applies(bit_width) && !start_over_u32 {
        let vl = lanes();
        let per_step = values_per_step(vl);
        let steps = super::cap_by_data(num_values / per_step, 0, data.len(), |steps| {
            bytes_read(w, vl, steps)
        });
        if steps == 0 {
            return Some((0, 0));
        }
        let range32 = (*value_range.start() as u32)
            ..=(*value_range.end()).min(u64::from(u32::MAX)) as u32;
        let written = unsafe { decode_filter(w, data, steps, range32, first_id, out) };
        Some((written, steps * per_step))
    } else if lanes64() > 0 && kernel64_applies(bit_width) {
        let vl_d = lanes64();
        let per_step = values_per_step64(vl_d);
        let steps = super::cap_by_data(num_values / per_step, 0, data.len(), |steps| {
            bytes_read64(w, vl_d, steps)
        });
        if steps == 0 {
            return Some((0, 0));
        }
        let written =
            unsafe { decode_filter64(w, data, steps, value_range.clone(), first_id, out) };
        Some((written, steps * per_step))
    } else {
        None
    }
}

/// The whole-step middle of a 64-bit-lane block decode: decodes as many of
/// `len` values as whole steps cover into `out`, with `min + gcd * v` fused
/// into the kernel when it is not the identity, and returns how many. All or
/// nothing: 0 when a single step does not fit `len` or `data`, and the caller
/// finishes scalar either way -- never NEON, so a measurement of SVE is never
/// a blend of the two.
///
/// `out` must have room for `len` values, and the caller must have checked
/// [`lanes64`] returns nonzero.
pub(super) fn decode_middle(
    w: usize,
    data: &[u8],
    len: usize,
    out: *mut u64,
    min: u64,
    gcd: u64,
) -> usize {
    let vl_d = lanes64();
    let per_step = decode_values_per_step(vl_d);
    let steps = len / per_step;
    if steps == 0 || data.len() < bytes_read64(w, vl_d, steps) {
        return 0;
    }
    if min == 0 && gcd == 1 {
        unsafe { decode_block(w, data, steps, out) };
    } else {
        unsafe { decode_block_affine(w, data, steps, out, min, gcd) };
    }
    steps * per_step
}

/// Per-lane gather offsets and shifts for one unrolled step, laid out as the
/// four vectors the kernel loads once before its loop: `off_a`, `sh_a`,
/// `off_b`, `sh_b`.
///
/// Lane `j` of the first vector reads the 4 bytes at `j * w / 8` and shifts
/// right by `j * w % 8`; the second vector continues at lane `vl + j`. Both
/// stay fixed across iterations because the loop advances the base pointer
/// instead.
fn step_tables(w: usize, vl: usize) -> Vec<u32> {
    let mut v = vec![0u32; 4 * vl];
    for j in 0..vl {
        let bit_a = j * w;
        let bit_b = (vl + j) * w;
        v[j] = (bit_a / 8) as u32;
        v[vl + j] = (bit_a % 8) as u32;
        v[2 * vl + j] = (bit_b / 8) as u32;
        v[3 * vl + j] = (bit_b % 8) as u32;
    }
    v
}

/// Highest byte the kernel reads, relative to the start of `data`, when run for
/// `num_steps` steps. The gather loads 4 bytes at the last lane's offset.
#[inline]
pub(super) fn bytes_read(w: usize, vl: usize, num_steps: usize) -> usize {
    let last_lane_bit = (2 * vl * num_steps - 1) * w;
    last_lane_bit / 8 + 4
}

/// Fused decode and range filter for bit widths 1..=25.
///
/// Decodes `num_steps * 2 * vl` values starting at `first_id`, keeps those
/// inside `value_range`, and writes their row ids densely to `out`. Returns how
/// many were written.
///
/// # Safety
/// SVE must be available. `data` must hold [`bytes_read`] bytes, and `out` must
/// have room for `num_steps * 2 * vl` ids: each half-step stores a full vector
/// before advancing by the match count.
#[target_feature(enable = "sve")]
pub(super) unsafe fn decode_filter(
    w: usize,
    data: &[u8],
    num_steps: usize,
    value_range: RangeInclusive<u32>,
    first_id: u32,
    out: *mut u32,
) -> usize {
    debug_assert!(kernel_applies(w as u8));
    if num_steps == 0 {
        return 0;
    }
    let vl = unsafe { num_lanes() };
    debug_assert!(data.len() >= bytes_read(w, vl, num_steps));

    let range_start = *value_range.start();
    // Same unsigned-subtraction trick the standalone filter used:
    // `val ∈ [lo, hi]` ⟺ `(val - lo) ≤ᵤ (hi - lo)`. Values below `lo` wrap to
    // large u32 and fail the single unsigned compare.
    let range_width = value_range.end().wrapping_sub(range_start);
    let value_mask = (1u32 << w) - 1;
    let tables = step_tables(w, vl);
    // Bytes consumed per step. Whole because `2 * vl` is a multiple of 8.
    let stride = 2 * vl * w / 8;

    let input_ptr = data.as_ptr();
    let mut output_tail = out;

    unsafe {
        core::arch::asm!(
            // --- Setup ---
            "ptrue p0.s",
            // ids_a = [first_id, first_id+1, ...], ids_b = ids_a + vl.
            "index z0.s, {first_id:w}, #1",
            "mov z1.s, {range_width:w}",
            "mov z2.s, {range_start:w}",
            "cntw {vl_gpr}",
            "mov z4.s, {vl_gpr:w}",
            "lsl {scratch}, {vl_gpr}, #1",
            "mov z3.s, {scratch:w}",
            "add z4.s, z0.s, z4.s",
            "mov z11.s, {value_mask:w}",
            // Gather offsets and shifts, four consecutive vectors.
            "ld1w {{z7.s}}, p0/z, [{tables}]",
            "ld1w {{z8.s}}, p0/z, [{tables}, #1, mul vl]",
            "ld1w {{z9.s}}, p0/z, [{tables}, #2, mul vl]",
            "ld1w {{z10.s}}, p0/z, [{tables}, #3, mul vl]",

            // --- Main loop, two vectors per iteration ---
            "0:",
            // Decode: gather 4 bytes per lane, shift into place, mask to width.
            // This replaces the plain vector load of the pre-fusion filter.
            "ld1w {{z5.s}}, p0/z, [{input}, z7.s, uxtw]",
            "ld1w {{z6.s}}, p0/z, [{input}, z9.s, uxtw]",
            "lsr z5.s, p0/m, z5.s, z8.s",
            "lsr z6.s, p0/m, z6.s, z10.s",
            "and z5.d, z5.d, z11.d",
            "and z6.d, z6.d, z11.d",
            "add {input}, {input}, {stride}",
            // Filter: one unsigned compare after shifting by range_start.
            "sub z5.s, z5.s, z2.s",
            "sub z6.s, z6.s, z2.s",
            "cmphs p1.s, p0/z, z1.s, z5.s",
            "cmphs p2.s, p0/z, z1.s, z6.s",
            // Independent cntp inputs, so both counts issue in parallel.
            "cntp {cnt_a}, p0, p1.s",
            "compact z5.s, p1, z0.s",
            "compact z6.s, p2, z4.s",
            "cntp {cnt_b}, p0, p2.s",
            "add z0.s, z0.s, z3.s",
            "add z4.s, z4.s, z3.s",
            // Store compacted ids. Only the first cnt_a / cnt_b slots are live;
            // later iterations overwrite the rest before the caller truncates.
            "str z5, [{out}]",
            "st1w {{z6.s}}, p0, [{out}, {cnt_a}, lsl #2]",
            "add {out}, {out}, {cnt_a}, lsl #2",
            "add {out}, {out}, {cnt_b}, lsl #2",
            "subs {steps}, {steps}, #1",
            "b.ne 0b",

            input       = inout(reg) input_ptr => _,
            out         = inout(reg) output_tail,
            steps       = inout(reg) num_steps => _,
            tables      = in(reg) tables.as_ptr(),
            stride      = in(reg) stride,
            first_id    = in(reg) first_id,
            range_start = in(reg) range_start,
            range_width = in(reg) range_width,
            value_mask  = in(reg) value_mask,
            vl_gpr      = out(reg) _,
            scratch     = out(reg) _,
            cnt_a       = out(reg) _,
            cnt_b       = out(reg) _,
            out("p0") _, out("p1") _, out("p2") _,
            out("v0") _, out("v1") _, out("v2") _, out("v3") _,
            out("v4") _, out("v5") _, out("v6") _, out("v7") _,
            out("v8") _, out("v9") _, out("v10") _, out("v11") _,
            options(nostack),
        );
    }

    unsafe { output_tail.offset_from(out) as usize }
}

/// Lanes per vector, for the driver to size its chunks. Returns 0 when SVE is
/// not available.
pub(super) fn lanes() -> usize {
    if !available() {
        return 0;
    }
    unsafe { num_lanes() }
}

/// Whether the 64-bit-lane SVE kernel takes this width. Picks up exactly where
/// [`kernel_applies`] stops, and covers 64 as well: at `w == 64` the gather
/// offsets fall on 8-byte boundaries and the shifts are all zero, so the
/// sequence degenerates to a plain load.
#[inline]
pub(super) fn kernel64_applies(bit_width: u8) -> bool {
    (26..=56).contains(&bit_width) || bit_width == 64
}

/// Number of 64-bit lanes in an SVE vector.
///
/// # Safety
/// Only callable once SVE is known present.
#[target_feature(enable = "sve")]
unsafe fn num_lanes64() -> usize {
    let vl: usize;
    unsafe {
        core::arch::asm!(
            "cntd {vl}",
            vl = out(reg) vl,
            options(nostack, nomem, preserves_flags),
        );
    }
    vl
}

/// Lanes per 64-bit vector, 0 when SVE is absent.
pub(super) fn lanes64() -> usize {
    if !available() {
        return 0;
    }
    unsafe { num_lanes64() }
}

/// Values consumed per iteration of the 64-bit kernel.
///
/// Unrolled by four rather than two. With `vl_d` as low as 2 at a 128-bit
/// vector length, a step of `2 * vl_d` would be 4 values and the pointer
/// advance `4 * w / 8` would not be a whole number of bytes for odd `w`.
/// `4 * vl_d` is a multiple of 8 at every vector length, so the advance is
/// always exact.
#[inline]
pub(super) fn values_per_step64(vl_d: usize) -> usize {
    4 * vl_d
}

/// Highest byte the 64-bit kernel reads: its gather pulls 8 bytes per lane.
#[inline]
pub(super) fn bytes_read64(w: usize, vl_d: usize, num_steps: usize) -> usize {
    let last_lane_bit = (4 * vl_d * num_steps - 1) * w;
    last_lane_bit / 8 + 8
}

/// Gather offsets and shifts for the four quarters of one step, as eight
/// consecutive `u64` vectors: offsets for quarters 0..4, then shifts.
fn step_tables64(w: usize, vl_d: usize) -> Vec<u64> {
    let mut v = vec![0u64; 8 * vl_d];
    for q in 0..4 {
        for j in 0..vl_d {
            let bit = (q * vl_d + j) * w;
            v[q * vl_d + j] = (bit / 8) as u64;
            v[(4 + q) * vl_d + j] = (bit % 8) as u64;
        }
    }
    v
}

/// Fused decode and range filter for bit widths 26..=56 and 64.
///
/// Compares in 64-bit lanes, but compacts and stores row ids as 32-bit: the ids
/// live in `.d` lanes and `st1w` writes the low word of each, so the `u32`
/// output needs no separate narrowing step.
///
/// # Safety
/// SVE must be available. `data` must hold [`bytes_read64`] bytes, and `out`
/// room for `num_steps * 4 * vl_d` ids.
#[target_feature(enable = "sve")]
pub(super) unsafe fn decode_filter64(
    w: usize,
    data: &[u8],
    num_steps: usize,
    value_range: RangeInclusive<u64>,
    first_id: u32,
    out: *mut u32,
) -> usize {
    debug_assert!(kernel64_applies(w as u8));
    if num_steps == 0 {
        return 0;
    }
    let vl_d = unsafe { num_lanes64() };
    debug_assert!(data.len() >= bytes_read64(w, vl_d, num_steps));

    let range_start = *value_range.start();
    let range_width = value_range.end().wrapping_sub(range_start);
    let value_mask = if w == 64 { !0u64 } else { (1u64 << w) - 1 };
    let tables = step_tables64(w, vl_d);
    let stride = 4 * vl_d * w / 8;
    let first_id = u64::from(first_id);

    let input_ptr = data.as_ptr();
    let mut output_tail = out;

    unsafe {
        core::arch::asm!(
            "ptrue p0.d",
            "index z0.d, {first_id}, #1",
            "cntd {vl_gpr}",
            "mov z7.d, {vl_gpr}",
            "add z1.d, z0.d, z7.d",
            "add z2.d, z1.d, z7.d",
            "add z3.d, z2.d, z7.d",
            "lsl {scratch}, {vl_gpr}, #2",
            "mov z7.d, {scratch}",
            "mov z4.d, {range_start}",
            "mov z5.d, {range_width}",
            "mov z6.d, {value_mask}",
            "ld1d {{z8.d}}, p0/z, [{tables}]",
            "ld1d {{z9.d}}, p0/z, [{tables}, #1, mul vl]",
            "ld1d {{z10.d}}, p0/z, [{tables}, #2, mul vl]",
            "ld1d {{z11.d}}, p0/z, [{tables}, #3, mul vl]",
            "ld1d {{z12.d}}, p0/z, [{tables}, #4, mul vl]",
            "ld1d {{z13.d}}, p0/z, [{tables}, #5, mul vl]",
            "ld1d {{z14.d}}, p0/z, [{tables}, #6, mul vl]",
            "ld1d {{z15.d}}, p0/z, [{tables}, #7, mul vl]",

            "0:",
            // Four quarters per step. Each: gather 8 bytes per lane, shift and
            // mask to the packed width, then the same unsigned range compare
            // and compact the 32-bit filter uses.
            "ld1d {{z16.d}}, p0/z, [{input}, z8.d]",
            "ld1d {{z17.d}}, p0/z, [{input}, z9.d]",
            "ld1d {{z18.d}}, p0/z, [{input}, z10.d]",
            "ld1d {{z19.d}}, p0/z, [{input}, z11.d]",
            "lsr z16.d, p0/m, z16.d, z12.d",
            "lsr z17.d, p0/m, z17.d, z13.d",
            "lsr z18.d, p0/m, z18.d, z14.d",
            "lsr z19.d, p0/m, z19.d, z15.d",
            "and z16.d, z16.d, z6.d",
            "and z17.d, z17.d, z6.d",
            "and z18.d, z18.d, z6.d",
            "and z19.d, z19.d, z6.d",
            "add {input}, {input}, {stride}",
            "sub z16.d, z16.d, z4.d",
            "sub z17.d, z17.d, z4.d",
            "sub z18.d, z18.d, z4.d",
            "sub z19.d, z19.d, z4.d",
            "cmphs p1.d, p0/z, z5.d, z16.d",
            "cmphs p2.d, p0/z, z5.d, z17.d",
            "cmphs p3.d, p0/z, z5.d, z18.d",
            "cmphs p4.d, p0/z, z5.d, z19.d",
            "compact z16.d, p1, z0.d",
            "compact z17.d, p2, z1.d",
            "compact z18.d, p3, z2.d",
            "compact z19.d, p4, z3.d",
            "cntp {cnt}, p0, p1.d",
            // st1w on a .d vector writes the low 32 bits of each lane, which is
            // exactly the u32 row id.
            "st1w {{z16.d}}, p0, [{out}]",
            "add {out}, {out}, {cnt}, lsl #2",
            "cntp {cnt}, p0, p2.d",
            "st1w {{z17.d}}, p0, [{out}]",
            "add {out}, {out}, {cnt}, lsl #2",
            "cntp {cnt}, p0, p3.d",
            "st1w {{z18.d}}, p0, [{out}]",
            "add {out}, {out}, {cnt}, lsl #2",
            "cntp {cnt}, p0, p4.d",
            "st1w {{z19.d}}, p0, [{out}]",
            "add {out}, {out}, {cnt}, lsl #2",
            "add z0.d, z0.d, z7.d",
            "add z1.d, z1.d, z7.d",
            "add z2.d, z2.d, z7.d",
            "add z3.d, z3.d, z7.d",
            "subs {steps}, {steps}, #1",
            "b.ne 0b",

            input       = inout(reg) input_ptr => _,
            out         = inout(reg) output_tail,
            steps       = inout(reg) num_steps => _,
            tables      = in(reg) tables.as_ptr(),
            stride      = in(reg) stride,
            first_id    = in(reg) first_id,
            range_start = in(reg) range_start,
            range_width = in(reg) range_width,
            value_mask  = in(reg) value_mask,
            vl_gpr      = out(reg) _,
            scratch     = out(reg) _,
            cnt         = out(reg) _,
            out("p0") _, out("p1") _, out("p2") _, out("p3") _, out("p4") _,
            out("v0") _, out("v1") _, out("v2") _, out("v3") _,
            out("v4") _, out("v5") _, out("v6") _, out("v7") _,
            out("v8") _, out("v9") _, out("v10") _, out("v11") _,
            out("v12") _, out("v13") _, out("v14") _, out("v15") _,
            out("v16") _, out("v17") _, out("v18") _, out("v19") _,
            options(nostack),
        );
    }

    unsafe { output_tail.offset_from(out) as usize }
}

/// Values consumed per iteration of [`decode_block`].
///
/// Unrolled by four for the same reason [`values_per_step64`] is: a step
/// advances the data pointer by `4 * vl_d * w / 8` bytes, and `4 * vl_d` is a
/// multiple of 8 at every architectural vector length, so the advance is always
/// a whole number of bytes.
#[inline]
pub(super) fn decode_values_per_step(vl_d: usize) -> usize {
    4 * vl_d
}

/// SVE batch unpack for every packed width, the counterpart of
/// [`super::simd::decode_block`] and its wide sibling.
///
/// One kernel covers `1..=56` and `64` because the output lane is `u64`
/// throughout: gathering 8 bytes per lane leaves room for a shift of up to 7
/// plus 56 bits of value, and at `w == 64` the shift is zero and the gather is
/// already aligned.
///
/// # Safety
/// SVE must be available. `data` must hold [`bytes_read64`] bytes for
/// `num_steps`, and `out` room for `num_steps * 4 * vl_d` values.
/// `min + gcd * val` on the four live vectors, in-register.
///
/// SVE is the one target with a real 64x64 vector multiply (`mul` on `.d`
/// lanes), so unlike NEON and AVX2 this needs no 32-bit widening trick and
/// carries no width cap. `z7` holds `gcd`, `z5` holds `min`.
macro_rules! sve_affine_xform {
    () => {
        "
        mul z16.d, p0/m, z16.d, z7.d
        mul z17.d, p0/m, z17.d, z7.d
        mul z18.d, p0/m, z18.d, z7.d
        mul z19.d, p0/m, z19.d, z7.d
        add z16.d, z16.d, z5.d
        add z17.d, z17.d, z5.d
        add z18.d, z18.d, z5.d
        add z19.d, z19.d, z5.d
        "
    };
}

/// `min + val`, for the `gcd == 1` columns that are most of them.
macro_rules! sve_affine_add_xform {
    () => {
        "
        add z16.d, z16.d, z5.d
        add z17.d, z17.d, z5.d
        add z18.d, z18.d, z5.d
        add z19.d, z19.d, z5.d
        "
    };
}

/// The gather-shift-mask half of the loop, shared by the plain and fused
/// kernels. Ends with the four decoded vectors live in `z16..z19`.
macro_rules! sve_decode_head {
    () => {
        "
        ptrue p0.d
        mov z6.d, {value_mask}
        ld1d {{z8.d}}, p0/z, [{tables}]
        ld1d {{z9.d}}, p0/z, [{tables}, #1, mul vl]
        ld1d {{z10.d}}, p0/z, [{tables}, #2, mul vl]
        ld1d {{z11.d}}, p0/z, [{tables}, #3, mul vl]
        ld1d {{z12.d}}, p0/z, [{tables}, #4, mul vl]
        ld1d {{z13.d}}, p0/z, [{tables}, #5, mul vl]
        ld1d {{z14.d}}, p0/z, [{tables}, #6, mul vl]
        ld1d {{z15.d}}, p0/z, [{tables}, #7, mul vl]
        rdvl {out_step}, #4
        "
    };
}

/// The per-step gather, entered once per iteration.
macro_rules! sve_decode_step {
    () => {
        "
        0:
        ld1d {{z16.d}}, p0/z, [{input}, z8.d]
        ld1d {{z17.d}}, p0/z, [{input}, z9.d]
        ld1d {{z18.d}}, p0/z, [{input}, z10.d]
        ld1d {{z19.d}}, p0/z, [{input}, z11.d]
        lsr z16.d, p0/m, z16.d, z12.d
        lsr z17.d, p0/m, z17.d, z13.d
        lsr z18.d, p0/m, z18.d, z14.d
        lsr z19.d, p0/m, z19.d, z15.d
        and z16.d, z16.d, z6.d
        and z17.d, z17.d, z6.d
        and z18.d, z18.d, z6.d
        and z19.d, z19.d, z6.d
        "
    };
}

/// The store and loop tail.
macro_rules! sve_decode_tail {
    () => {
        "
        st1d {{z16.d}}, p0, [{out}]
        st1d {{z17.d}}, p0, [{out}, #1, mul vl]
        st1d {{z18.d}}, p0, [{out}, #2, mul vl]
        st1d {{z19.d}}, p0, [{out}, #3, mul vl]
        add {input}, {input}, {stride}
        add {out}, {out}, {out_step}
        subs {steps}, {steps}, #1
        b.ne 0b
        "
    };
}

#[target_feature(enable = "sve")]
pub(super) unsafe fn decode_block(w: usize, data: &[u8], num_steps: usize, out: *mut u64) {
    debug_assert!((1..=56).contains(&w) || w == 64);
    if num_steps == 0 {
        return;
    }
    let vl_d = unsafe { num_lanes64() };
    debug_assert!(data.len() >= bytes_read64(w, vl_d, num_steps));

    let value_mask = if w == 64 { !0u64 } else { (1u64 << w) - 1 };
    let tables = step_tables64(w, vl_d);
    let stride = 4 * vl_d * w / 8;

    let input_ptr = data.as_ptr();
    let out_ptr = out;

    unsafe {
        core::arch::asm!(
            sve_decode_head!(),
            sve_decode_step!(),
            sve_decode_tail!(),

            input      = inout(reg) input_ptr => _,
            out        = inout(reg) out_ptr => _,
            steps      = inout(reg) num_steps => _,
            tables     = in(reg) tables.as_ptr(),
            stride     = in(reg) stride,
            value_mask = in(reg) value_mask,
            out_step   = out(reg) _,
            out("p0") _,
            out("v6") _, out("v8") _, out("v9") _, out("v10") _, out("v11") _,
            out("v12") _, out("v13") _, out("v14") _, out("v15") _,
            out("v16") _, out("v17") _, out("v18") _, out("v19") _,
            options(nostack),
        );
    }
}

/// [`decode_block`] with `min + gcd * val` applied before the store.
///
/// The transform arrives as data rather than through the `Store` seam because
/// this kernel writes through a raw pointer, not through a sink.
///
/// # Safety
/// As [`decode_block`].
#[target_feature(enable = "sve")]
pub(super) unsafe fn decode_block_affine(
    w: usize,
    data: &[u8],
    num_steps: usize,
    out: *mut u64,
    min: u64,
    gcd: u64,
) {
    debug_assert!((1..=56).contains(&w) || w == 64);
    if num_steps == 0 {
        return;
    }
    let vl_d = unsafe { num_lanes64() };
    debug_assert!(data.len() >= bytes_read64(w, vl_d, num_steps));

    let value_mask = if w == 64 { !0u64 } else { (1u64 << w) - 1 };
    let tables = step_tables64(w, vl_d);
    let stride = 4 * vl_d * w / 8;

    let input_ptr = data.as_ptr();
    let out_ptr = out;

    // `gcd == 1` is most columns, and skipping the multiply keeps that case at
    // one extra instruction per vector.
    if gcd == 1 {
        unsafe {
            core::arch::asm!(
                sve_decode_head!(),
                "mov z5.d, {min}",
                sve_decode_step!(),
                sve_affine_add_xform!(),
                sve_decode_tail!(),

                input      = inout(reg) input_ptr => _,
                out        = inout(reg) out_ptr => _,
                steps      = inout(reg) num_steps => _,
                tables     = in(reg) tables.as_ptr(),
                stride     = in(reg) stride,
                value_mask = in(reg) value_mask,
                min        = in(reg) min,
                out_step   = out(reg) _,
                out("p0") _,
                out("v5") _,
                out("v6") _, out("v8") _, out("v9") _, out("v10") _, out("v11") _,
                out("v12") _, out("v13") _, out("v14") _, out("v15") _,
                out("v16") _, out("v17") _, out("v18") _, out("v19") _,
                options(nostack),
            );
        }
        return;
    }
    unsafe {
        core::arch::asm!(
            sve_decode_head!(),
            "mov z5.d, {min}",
            "mov z7.d, {gcd}",
            sve_decode_step!(),
            sve_affine_xform!(),
            sve_decode_tail!(),

            input      = inout(reg) input_ptr => _,
            out        = inout(reg) out_ptr => _,
            steps      = inout(reg) num_steps => _,
            tables     = in(reg) tables.as_ptr(),
            stride     = in(reg) stride,
            value_mask = in(reg) value_mask,
            min        = in(reg) min,
            gcd        = in(reg) gcd,
            out_step   = out(reg) _,
            out("p0") _,
            out("v5") _, out("v7") _,
            out("v6") _, out("v8") _, out("v9") _, out("v10") _, out("v11") _,
            out("v12") _, out("v13") _, out("v14") _, out("v15") _,
            out("v16") _, out("v17") _, out("v18") _, out("v19") _,
            options(nostack),
        );
    }
}
