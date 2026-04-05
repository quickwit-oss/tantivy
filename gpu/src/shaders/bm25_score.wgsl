// BM25 batch scoring kernel for Tantivy GPU.
//
// Scores a block of documents in parallel using BM25 formula:
//   score = weight * tf / (tf + K1 * (1 - B + B * dl / avgdl))
//
// where weight = IDF * (1 + K1), pre-computed on CPU.
//
// Layout:
//   @group(0) @binding(0) — input: array of Bm25DocInput {doc_id, term_freq, fieldnorm_id, _pad}
//   @group(0) @binding(1) — output: array of Bm25DocOutput {doc_id, score}
//   @group(0) @binding(2) — uniform: Bm25Params {weight, k1, b, avg_fieldnorm}
//   @group(0) @binding(3) — fieldnorm lookup table (256 entries, u32 -> actual field length)

struct Bm25Params {
    weight: f32,
    k1: f32,
    b: f32,
    avg_fieldnorm: f32,
}

struct Bm25DocInput {
    doc_id: u32,
    term_freq: u32,
    fieldnorm_id: u32,
    _pad: u32,
}

struct Bm25DocOutput {
    doc_id: u32,
    score: f32,
}

@group(0) @binding(0) var<storage, read> inputs: array<Bm25DocInput>;
@group(0) @binding(1) var<storage, read_write> outputs: array<Bm25DocOutput>;
@group(0) @binding(2) var<uniform> params: Bm25Params;
@group(0) @binding(3) var<storage, read> fieldnorm_table: array<u32>;

struct DispatchParams {
    num_docs: u32,
}
@group(1) @binding(0) var<uniform> dispatch: DispatchParams;

@compute @workgroup_size(256)
fn bm25_score(
    @builtin(global_invocation_id) global_id: vec3<u32>,
) {
    let gid = global_id.x;
    if gid >= dispatch.num_docs {
        return;
    }

    let input = inputs[gid];
    let tf = f32(input.term_freq);

    // Look up actual field length from fieldnorm table
    let fieldnorm_idx = min(input.fieldnorm_id, 255u);
    let dl = f32(fieldnorm_table[fieldnorm_idx]);

    // BM25 formula:
    // norm = K1 * (1 - B + B * dl / avgdl)
    // tf_factor = tf / (tf + norm)
    // score = weight * tf_factor
    let norm = params.k1 * (1.0 - params.b + params.b * dl / params.avg_fieldnorm);
    let tf_factor = tf / (tf + norm);
    let score = params.weight * tf_factor;

    outputs[gid].doc_id = input.doc_id;
    outputs[gid].score = score;
}
