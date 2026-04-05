//! BM25 batch scoring kernel.

use crate::buffer::{Bm25DocInput, Bm25DocOutput, Bm25Params, GpuBuffer};
use crate::device::{BufferUsage, GpuContext, GpuPipelineRaw};
use crate::error::GpuResult;
use crate::kernel::GpuKernel;

const BM25_SHADER: &str = include_str!("../shaders/bm25_score.wgsl");
const WORKGROUP_SIZE: u32 = 256;

/// GPU kernel for batch BM25 scoring.
pub struct Bm25Kernel {
    pipeline: GpuPipelineRaw,
    ctx: GpuContext,
    /// Cached fieldnorm lookup table buffer on GPU.
    fieldnorm_buf: GpuBuffer,
}

/// Dispatch params uniform (binding group 1).
#[repr(C)]
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
struct DispatchParams {
    num_docs: u32,
    _pad: [u32; 3],
}

impl GpuKernel for Bm25Kernel {
    type Params = Bm25Params;
    type Result = Vec<Bm25DocOutput>;

    fn compile(ctx: &GpuContext) -> GpuResult<Self> {
        let pipeline = ctx
            .device()
            .create_pipeline("bm25-score", BM25_SHADER, "bm25_score")?;

        // Pre-upload the fieldnorm lookup table (256 entries).
        // This table maps fieldnorm_id (0..255) to actual field length.
        // Matches Tantivy's FieldNormReader::id_to_fieldnorm.
        let fieldnorm_table = build_fieldnorm_table();
        let fieldnorm_buf =
            GpuBuffer::new::<u32>(ctx, "fieldnorm-table", 256, BufferUsage::STORAGE)?;
        fieldnorm_buf.upload(ctx, &fieldnorm_table)?;

        Ok(Self {
            pipeline,
            ctx: ctx.clone(),
            fieldnorm_buf,
        })
    }
}

impl Bm25Kernel {
    /// Score a batch of documents using BM25.
    ///
    /// `inputs` contains (doc_id, term_freq, fieldnorm_id) for each document.
    /// `params` contains the pre-computed IDF weight and BM25 parameters.
    ///
    /// Returns (doc_id, score) pairs.
    pub fn execute(
        &self,
        inputs: &[Bm25DocInput],
        params: &Bm25Params,
    ) -> GpuResult<Vec<Bm25DocOutput>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let num_docs = inputs.len() as u32;
        let num_workgroups = num_docs.div_ceil(WORKGROUP_SIZE);

        // Upload inputs
        let input_buf = GpuBuffer::new::<Bm25DocInput>(
            &self.ctx,
            "bm25-input",
            inputs.len(),
            BufferUsage::STORAGE,
        )?;
        input_buf.upload(&self.ctx, inputs)?;

        // Output
        let output_buf = GpuBuffer::new::<Bm25DocOutput>(
            &self.ctx,
            "bm25-output",
            inputs.len(),
            BufferUsage::STORAGE_READBACK,
        )?;

        // BM25 params uniform
        let params_buf =
            GpuBuffer::new::<Bm25Params>(&self.ctx, "bm25-params", 1, BufferUsage::UNIFORM)?;
        params_buf.upload(&self.ctx, &[*params])?;

        // Bind group 0: input, output, params, fieldnorm_table
        let bind_group_0 = self.ctx.device().create_bind_group(
            &self.pipeline,
            0,
            &[
                input_buf.as_bind_entry(0),
                output_buf.as_bind_entry(1),
                params_buf.as_bind_entry(2),
                self.fieldnorm_buf.as_bind_entry(3),
            ],
        )?;

        // Bind group 1: dispatch params
        let dispatch_params = DispatchParams {
            num_docs,
            _pad: [0; 3],
        };
        let dispatch_buf =
            GpuBuffer::new::<DispatchParams>(&self.ctx, "bm25-dispatch", 1, BufferUsage::UNIFORM)?;
        dispatch_buf.upload(&self.ctx, &[dispatch_params])?;
        let bind_group_1 = self.ctx.device().create_bind_group(
            &self.pipeline,
            1,
            &[dispatch_buf.as_bind_entry(0)],
        )?;

        // Dispatch
        self.ctx.device().dispatch(
            &self.pipeline,
            &[bind_group_0, bind_group_1],
            (num_workgroups, 1, 1),
        )?;

        // Read back
        output_buf.download(&self.ctx)
    }

    /// Score a batch using buffer pool (zero alloc per dispatch).
    pub fn execute_pooled(
        &self,
        inputs: &[Bm25DocInput],
        params: &Bm25Params,
        pool: &crate::buffer::pool::BufferPool,
    ) -> GpuResult<Vec<Bm25DocOutput>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let num_docs = inputs.len() as u32;
        let num_workgroups = num_docs.div_ceil(WORKGROUP_SIZE);

        let input_buf = pool.lease_typed::<Bm25DocInput>(inputs.len(), BufferUsage::STORAGE)?;
        input_buf.upload(inputs)?;

        let output_buf =
            pool.lease_typed::<Bm25DocOutput>(inputs.len(), BufferUsage::STORAGE_READBACK)?;

        let params_buf = pool.lease_typed::<Bm25Params>(1, BufferUsage::UNIFORM)?;
        params_buf.upload(&[*params])?;

        let bind_group_0 = self.ctx.device().create_bind_group(
            &self.pipeline,
            0,
            &[
                input_buf.as_bind_entry(0),
                output_buf.as_bind_entry(1),
                params_buf.as_bind_entry(2),
                self.fieldnorm_buf.as_bind_entry(3),
            ],
        )?;

        let dispatch_params = DispatchParams {
            num_docs,
            _pad: [0; 3],
        };
        let dispatch_buf = pool.lease_typed::<DispatchParams>(1, BufferUsage::UNIFORM)?;
        dispatch_buf.upload(&[dispatch_params])?;
        let bind_group_1 = self.ctx.device().create_bind_group(
            &self.pipeline,
            1,
            &[dispatch_buf.as_bind_entry(0)],
        )?;

        self.ctx.device().dispatch(
            &self.pipeline,
            &[bind_group_0, bind_group_1],
            (num_workgroups, 1, 1),
        )?;

        output_buf.download()
    }
}

/// Build the fieldnorm lookup table matching Tantivy's `FIELD_NORMS_TABLE`.
///
/// This is a non-linear mapping from byte ID (0..255) to actual field length.
/// Exactly matches tantivy/src/fieldnorm/code.rs.
fn build_fieldnorm_table() -> Vec<u32> {
    FIELD_NORMS_TABLE.to_vec()
}

/// Tantivy's field norm lookup table, copied verbatim from
/// `tantivy/src/fieldnorm/code.rs` to ensure GPU scoring matches CPU scoring exactly.
const FIELD_NORMS_TABLE: [u32; 256] = [
    0,
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    31,
    32,
    33,
    34,
    35,
    36,
    37,
    38,
    39,
    40,
    42,
    44,
    46,
    48,
    50,
    52,
    54,
    56,
    60,
    64,
    68,
    72,
    76,
    80,
    84,
    88,
    96,
    104,
    112,
    120,
    128,
    136,
    144,
    152,
    168,
    184,
    200,
    216,
    232,
    248,
    264,
    280,
    312,
    344,
    376,
    408,
    440,
    472,
    504,
    536,
    600,
    664,
    728,
    792,
    856,
    920,
    984,
    1_048,
    1_176,
    1_304,
    1_432,
    1_560,
    1_688,
    1_816,
    1_944,
    2_072,
    2_328,
    2_584,
    2_840,
    3_096,
    3_352,
    3_608,
    3_864,
    4_120,
    4_632,
    5_144,
    5_656,
    6_168,
    6_680,
    7_192,
    7_704,
    8_216,
    9_240,
    10_264,
    11_288,
    12_312,
    13_336,
    14_360,
    15_384,
    16_408,
    18_456,
    20_504,
    22_552,
    24_600,
    26_648,
    28_696,
    30_744,
    32_792,
    36_888,
    40_984,
    45_080,
    49_176,
    53_272,
    57_368,
    61_464,
    65_560,
    73_752,
    81_944,
    90_136,
    98_328,
    106_520,
    114_712,
    122_904,
    131_096,
    147_480,
    163_864,
    180_248,
    196_632,
    213_016,
    229_400,
    245_784,
    262_168,
    294_936,
    327_704,
    360_472,
    393_240,
    426_008,
    458_776,
    491_544,
    524_312,
    589_848,
    655_384,
    720_920,
    786_456,
    851_992,
    917_528,
    983_064,
    1_048_600,
    1_179_672,
    1_310_744,
    1_441_816,
    1_572_888,
    1_703_960,
    1_835_032,
    1_966_104,
    2_097_176,
    2_359_320,
    2_621_464,
    2_883_608,
    3_145_752,
    3_407_896,
    3_670_040,
    3_932_184,
    4_194_328,
    4_718_616,
    5_242_904,
    5_767_192,
    6_291_480,
    6_815_768,
    7_340_056,
    7_864_344,
    8_388_632,
    9_437_208,
    10_485_784,
    11_534_360,
    12_582_936,
    13_631_512,
    14_680_088,
    15_728_664,
    16_777_240,
    18_874_392,
    20_971_544,
    23_068_696,
    25_165_848,
    27_263_000,
    29_360_152,
    31_457_304,
    33_554_456,
    37_748_760,
    41_943_064,
    46_137_368,
    50_331_672,
    54_525_976,
    58_720_280,
    62_914_584,
    67_108_888,
    75_497_496,
    83_886_104,
    92_274_712,
    100_663_320,
    109_051_928,
    117_440_536,
    125_829_144,
    134_217_752,
    150_994_968,
    167_772_184,
    184_549_400,
    201_326_616,
    218_103_832,
    234_881_048,
    251_658_264,
    268_435_480,
    301_989_912,
    335_544_344,
    369_098_776,
    402_653_208,
    436_207_640,
    469_762_072,
    503_316_504,
    536_870_936,
    603_979_800,
    671_088_664,
    738_197_528,
    805_306_392,
    872_415_256,
    939_524_120,
    1_006_632_984,
    1_073_741_848,
    1_207_959_576,
    1_342_177_304,
    1_476_395_032,
    1_610_612_760,
    1_744_830_488,
    1_879_048_216,
    2_013_265_944,
];

/// Look up field norm from ID using the table.
pub fn id_to_fieldnorm(id: u8) -> u32 {
    FIELD_NORMS_TABLE[id as usize]
}
