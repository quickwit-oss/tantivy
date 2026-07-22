use binggan::{black_box, BenchRunner};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tantivy::collector::{Collector, Count, TopDocs};
use tantivy::query::{Query, QueryParser};
use tantivy::schema::{Field, Schema, SchemaBuilder, FAST, INDEXED, TEXT};
use tantivy::{Index, Order, ReloadPolicy, Searcher, TantivyDocument};

#[derive(Clone)]
struct BenchIndex {
    #[allow(dead_code)]
    index: Index,
    searcher: Searcher,
    query_parser: QueryParser,
}

/// Value distribution of a numeric range field. Both distributions are low cardinality (few
/// distinct values, so many docs share each) and start at 0, which lets a target selectivity be
/// turned into concrete `[0, high]` bounds.
#[derive(Clone, Copy)]
enum Distribution {
    /// Uniform random in `0..1_000` (~5k docs per value at 5M docs).
    Rand,
    /// A coarse `doc_id / 10_000` trend with deterministic high/low noise. The noise keeps the
    /// timestamp-query shape while ensuring the fast field selects the bitpacked codec.
    Asc,
}

impl Distribution {
    /// Distinct values the field takes; the denominator turning a `[0, high]` range into a
    /// selectivity.
    fn num_values(self) -> u64 {
        match self {
            Distribution::Rand => 1_000,
            Distribution::Asc => 1_000,
        }
    }

    /// Inclusive `[low, high]` bounds for a range matching ~`target` of documents. At least one
    /// value is always kept, so a target below `1 / num_values` floors there rather than emptying.
    fn bounds_for(self, target: f64) -> (u64, u64) {
        let count = (target * self.num_values() as f64).round().max(1.0) as u64;
        (0, count - 1)
    }
}

const ASC_BLOCK_LEN: usize = 10_000;
const ASC_MATCHING_TAIL_LEN: usize = 5_000;
const ASC_NOISE_OFFSET: u64 = 1 << 25;

/// The numeric range fields and the distribution each draws from. Every field has an indexed-only
/// and an indexed+fast variant so range queries can be compared with and without a fast field.
const NUMERIC_FIELDS: [(&str, Distribution); 4] = [
    ("num_rand", Distribution::Rand),
    ("num_asc", Distribution::Asc),
    ("num_rand_fast", Distribution::Rand),
    ("num_asc_fast", Distribution::Asc),
];

// The clustered layout ([`TermLayout::Clustered`]) decides each doc by thresholding a smooth,
// slowly-drifting latent field: "a" where the field is high (see [`TitleField::next_token`]). The
// threshold is set from `p(a)` so the marginal is preserved regardless of how bursty the drift is;
// these knobs shape only the *arrangement* of the "a" docs. Values were picked (and checked by
// simulation) to keep the realized marginal within a few % of `p(a)` while giving moderate,
// realistic clustering.
//
/// Correlation length of the coarsest drift octave, as a fraction of the corpus. Finer octaves
/// refine at 1/4 this length each, so bursts span a range of sizes rather than one fixed scale.
/// Small enough that hundreds of bursts fit in the corpus, which is what keeps the marginal stable.
const CLUSTERED_DRIFT_SCALE: f64 = 0.001;
/// Octaves summed to build the drift. More = more scales of structure; 3 is plenty of texture.
const CLUSTERED_OCTAVES: usize = 3;
/// Fraction of the latent field's (unit) variance carried by the smooth drift; the rest is per-doc
/// noise. Higher = burstier (neighbouring docs agree more); lower = closer to uniform. ~0.3 gives
/// moderate over-dispersion (a few×–20× a uniform sprinkle) — clustered, but not pathologically so.
const CLUSTERED_SIGNAL_FRAC: f64 = 0.3;

/// How the "a"/"b" tokens of a title field are laid out across documents. Both layouts hit the same
/// marginal `p(a)` — they differ only in where the "a" docs land, which is what an intersection
/// actually sees.
#[derive(Clone, Copy, PartialEq)]
enum TermLayout {
    /// Each doc is independently "a" with probability `p_a`: the "a" docs are scattered uniformly.
    Uniform,
    /// The "a" docs arrive in bursts (see [`TitleField::next_token`]). Real attributes rarely
    /// sprinkle evenly — a log level spikes, a topic is ingested together — so their matching
    /// docs cluster. Bursts are statistically denser regions, not solid runs, and a background
    /// floor keeps some "a" docs scattered in between.
    Clustered,
}

impl TermLayout {
    /// Short label for group names, distinguishing the two layouts at the same `p_a`/range.
    fn tag(self) -> &'static str {
        match self {
            TermLayout::Uniform => "uniform",
            TermLayout::Clustered => "clustered",
        }
    }
}

/// Derives a schema-safe title field name from a term probability and layout, e.g. `(0.001,
/// Uniform)` -> `title_0_1pct` and `(0.001, Clustered)` -> `title_clustered_0_1pct`. A title field
/// is populated with token "a" for a `p_a` fraction of documents, so `<name>:a` matches ~`p_a` of
/// them.
fn title_field_name(p_a: f64, layout: TermLayout) -> String {
    let pct = format_pct(p_a).replace('.', "_").replace('%', "pct");
    match layout {
        TermLayout::Uniform => format!("title_{}", pct),
        TermLayout::Clustered => format!("title_clustered_{}", pct),
    }
}

/// One AR(1) octave of the clustered-intensity drift: a unit-variance value that each step relaxes
/// toward 0 by `rho` and is nudged by fresh noise. High `rho` = slow drift (long, coarse bursts);
/// low `rho` = fast wiggle (short detail). Summing octaves with different `rho` yields bursts at a
/// range of sizes.
struct Ar1Octave {
    rho: f64,
    state: f64,
}

/// A title field plus the generator for its "a"/"b" token stream. Produced tokens hit the marginal
/// `p(a)` under either [`TermLayout`]; see [`Self::next_token`].
struct TitleField {
    field: Field,
    layout: TermLayout,
    /// Target marginal probability of "a".
    p_a: f64,
    // --- Clustered layout only (unused for Uniform) ---
    /// Latent-field value a doc must exceed to be "a". Set to `Φ⁻¹(1 - p_a)` for the unit-variance
    /// field below, so `P(exceed) = p_a` and the marginal is `p_a` however bursty the drift is.
    threshold: f64,
    /// Amplitude per octave, so the summed drift carries `CLUSTERED_SIGNAL_FRAC` of the unit
    /// variance.
    octave_weight: f64,
    /// s.d. of the per-doc noise carrying the remaining `1 - CLUSTERED_SIGNAL_FRAC` of the
    /// variance.
    noise_sd: f64,
    octaves: Vec<Ar1Octave>,
}

impl TitleField {
    fn new(p_a: f64, layout: TermLayout, field: Field, num_docs: usize) -> Self {
        // Octave j has correlation length CLUSTERED_DRIFT_SCALE * num_docs / 4^j; rho = e^(-1/len)
        // is the per-doc retention producing that length. Splitting the signal variance
        // equally across octaves (weight = sqrt(signal/octaves)) plus noise variance (1 -
        // signal) makes the latent field unit-variance, so the threshold is a plain
        // standard-normal quantile.
        let octaves = (0..CLUSTERED_OCTAVES)
            .map(|j| {
                let len = (CLUSTERED_DRIFT_SCALE * num_docs as f64 / 4f64.powi(j as i32)).max(1.0);
                Ar1Octave {
                    rho: (-1.0 / len).exp(),
                    state: 0.0,
                }
            })
            .collect();
        TitleField {
            field,
            layout,
            p_a,
            threshold: inverse_normal_cdf(1.0 - p_a),
            octave_weight: (CLUSTERED_SIGNAL_FRAC / CLUSTERED_OCTAVES as f64).sqrt(),
            noise_sd: (1.0 - CLUSTERED_SIGNAL_FRAC).sqrt(),
            octaves,
        }
    }

    /// Draws this document's token. Uniform: independently "a" with probability `p_a`. Clustered:
    /// advance the drift, add per-doc noise to get a unit-variance latent value, and emit "a" when
    /// it clears `threshold`. Because the drift moves slowly, neighbouring docs land on the
    /// same side of the threshold together — that is the clustering; the noise softens the
    /// boundary and scatters a background of "a" docs through the cold stretches (and "b" docs
    /// through the hot ones).
    fn next_token(&mut self, rng: &mut StdRng) -> &'static str {
        match self.layout {
            TermLayout::Uniform => {
                if rng.random_bool(self.p_a) {
                    "a"
                } else {
                    "b"
                }
            }
            TermLayout::Clustered => {
                let mut latent = self.noise_sd * standard_normal(rng);
                for octave in &mut self.octaves {
                    let noise = (1.0 - octave.rho * octave.rho).sqrt() * standard_normal(rng);
                    octave.state = octave.rho * octave.state + noise;
                    latent += self.octave_weight * octave.state;
                }
                if latent > self.threshold {
                    "a"
                } else {
                    "b"
                }
            }
        }
    }
}

/// A standard-normal sample via Box–Muller, so the clustered drift needs no extra dependency.
fn standard_normal(rng: &mut StdRng) -> f64 {
    let u1 = 1.0 - rng.random::<f64>(); // shift to (0,1] so ln is finite
    let u2 = rng.random::<f64>();
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
}

/// Inverse standard-normal CDF (the probit / quantile function) via Acklam's rational
/// approximation, accurate to ~1e-9 over `p ∈ (0, 1)` — enough to place the clustered-layout
/// threshold. Only called once per field, so speed is irrelevant.
fn inverse_normal_cdf(p: f64) -> f64 {
    // Coefficients for the central and tail regions of the approximation.
    const A: [f64; 6] = [
        -3.969683028665376e+01,
        2.209460984245205e+02,
        -2.759285104469687e+02,
        1.383577518672690e+02,
        -3.066479806614716e+01,
        2.506628277459239e+00,
    ];
    const B: [f64; 5] = [
        -5.447609879822406e+01,
        1.615858368580409e+02,
        -1.556989798598866e+02,
        6.680131188771972e+01,
        -1.328068155288572e+01,
    ];
    const C: [f64; 6] = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e+00,
        -2.549732539343734e+00,
        4.374664141464968e+00,
        2.938163982698783e+00,
    ];
    const D: [f64; 4] = [
        7.784695709041462e-03,
        3.224671290700398e-01,
        2.445134137142996e+00,
        3.754408661907416e+00,
    ];
    const P_LOW: f64 = 0.02425;
    if p < P_LOW {
        let q = (-2.0 * p.ln()).sqrt();
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    } else if p <= 1.0 - P_LOW {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    } else {
        let q = (-2.0 * (1.0 - p).ln()).sqrt();
        -(((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    }
}

/// The numeric field handles, in an indexed and an indexed+fast variant of both distributions.
struct NumericFields {
    rand: Field,
    asc: Field,
    rand_fast: Field,
    asc_fast: Field,
}

impl NumericFields {
    fn add(builder: &mut SchemaBuilder) -> Self {
        NumericFields {
            rand: builder.add_u64_field("num_rand", INDEXED),
            asc: builder.add_u64_field("num_asc", INDEXED),
            rand_fast: builder.add_u64_field("num_rand_fast", INDEXED | FAST),
            asc_fast: builder.add_u64_field("num_asc_fast", INDEXED | FAST),
        }
    }

    fn add_to(&self, doc: &mut TantivyDocument, rng: &mut StdRng, doc_id: usize, num_docs: usize) {
        // The fast and non-fast variants must hold identical data to be comparable.
        let rand = rng.random_range(0u64..1_000);
        // A perfectly ascending field selects the blockwise-linear codec. Add a deterministic
        // timestamp-width offset so each block is noisy enough for bitpacking to win, without
        // consuming RNG state or losing the coarse ascending trend. Value 0 matches in both the
        // first block and the final 5k docs, leaving a long empty interval between them.
        let noise = ((doc_id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 63) * ASC_NOISE_OFFSET;
        let asc = if doc_id >= num_docs.saturating_sub(ASC_MATCHING_TAIL_LEN) {
            0
        } else {
            (doc_id / ASC_BLOCK_LEN) as u64 + noise
        };
        doc.add_u64(self.rand, rand);
        doc.add_u64(self.asc, asc);
        doc.add_u64(self.rand_fast, rand);
        doc.add_u64(self.asc_fast, asc);
    }
}

/// Builds one shared index holding a title field for each requested `(p(a), layout)` plus the
/// numeric range fields. Building once lets all scenarios query the same corpus instead of each
/// rebuilding a near-identical index.
fn build_shared_index(num_docs: usize, term_specs: &[(f64, TermLayout)]) -> BenchIndex {
    let mut schema_builder = Schema::builder();
    // One title field per distinct (probability, layout); querying `<name>:a` matches ~p of
    // documents.
    let mut title_fields: Vec<TitleField> = Vec::new();
    for &(p_a, layout) in term_specs {
        if title_fields
            .iter()
            .any(|tf| tf.p_a == p_a && tf.layout == layout)
        {
            continue; // same (probability, layout) -> same field, build it once
        }
        let field = schema_builder.add_text_field(&title_field_name(p_a, layout), TEXT);
        title_fields.push(TitleField::new(p_a, layout, field, num_docs));
    }
    let numeric = NumericFields::add(&mut schema_builder);
    let index = Index::create_in_ram(schema_builder.build());

    // Populate index with stable RNG for reproducibility.
    let mut rng = StdRng::from_seed([7u8; 32]);

    {
        let mut writer = index.writer_with_num_threads(1, 4_000_000_000).unwrap();

        for doc_id in 0..num_docs {
            let mut doc = TantivyDocument::default();
            for title_field in &mut title_fields {
                let token = title_field.next_token(&mut rng);
                doc.add_text(title_field.field, token);
            }
            numeric.add_to(&mut doc, &mut rng, doc_id, num_docs);
            writer.add_document(doc).unwrap();
        }
        writer.commit().unwrap();
    }

    // Prepare reader/searcher once.
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    let searcher = reader.searcher();

    // Queries always qualify their field, so the default fields are irrelevant.
    let query_parser =
        QueryParser::for_index(&index, title_fields.iter().map(|tf| tf.field).collect());

    BenchIndex {
        index,
        searcher,
        query_parser,
    }
}

/// Formats a fraction as a percentage, with enough precision to stay legible for both coarse
/// (`50%`) and highly selective (`0.0001%`) ranges.
fn format_pct(frac: f64) -> String {
    let pct = frac * 100.0;
    if pct >= 1.0 {
        format!("{:.0}%", pct)
    } else if pct >= 0.1 {
        format!("{:.1}%", pct)
    } else {
        format!("{:.4}%", pct)
    }
}

fn main() {
    // Two independent knobs, both surfaced in the group name: `p(a)` (term selectivity, drives the
    // title field) and a target range selectivity (drives the numeric range bounds, computed per
    // field to hit it). Kept as an explicit paired list rather than a full matrix so the combos
    // stay curated — notably the ~1% contiguous window that reproduces the intersection regression.
    let scenarios = [
        (0.0001, 0.1),
        (0.001, 0.1),
        (0.01, 0.1),
        (0.1, 0.1),
        (0.5, 0.1),
        (0.7, 0.1),
    ];

    // Each scenario is run under both term layouts: `uniform` ("a" scattered evenly) and
    // `clustered` ("a" arriving in organic bursts, same marginal `p(a)`). Real corpora look
    // like the latter, and where the matching docs land is exactly what an intersection's
    // block-skipping sees.
    let layouts = [TermLayout::Uniform, TermLayout::Clustered];

    // Build a single shared corpus with a title field for each (probability, layout).
    let mut term_specs: Vec<(f64, TermLayout)> = Vec::new();
    for &layout in &layouts {
        for &(p_a, _) in &scenarios {
            if !term_specs.iter().any(|&(p, l)| p == p_a && l == layout) {
                term_specs.push((p_a, layout));
            }
        }
    }
    // This density keeps the current seek heuristic near its point-lookup/scan boundary, matching
    // the regression shape rather than immediately settling into scan mode like the 50% fields.
    let regression_term_p = 0.18;
    term_specs.push((regression_term_p, TermLayout::Uniform));
    let bench_index = build_shared_index(5_000_000, &term_specs);

    // Precompute each group's name and queries once; every collector runner reuses them. Layouts
    // are interleaved per scenario so a scenario's `uniform` and `clustered` groups sit next to
    // each other in the output for easy comparison.
    let mut prepared: Vec<(String, Vec<Query3>)> = Vec::new();
    for &(p_a, range_sel) in &scenarios {
        for &layout in &layouts {
            let group_name = format!(
                "{} a {}, {} range",
                format_pct(p_a),
                layout.tag(),
                format_pct(range_sel)
            );
            prepared.push((
                group_name,
                build_term_and_range_queries(p_a, range_sel, layout),
            ));
        }
    }

    // Reproduce the real-data regression shape with the existing fields: a dense term drives the
    // intersection, while the ascending fast-field range matches at the head and tail with a long
    // empty interval in between.
    let narrow_range_sel = 2.0 / Distribution::Asc.num_values() as f64;
    prepared.push((
        format!(
            "{} a {}, {} head+tail range with empty middle",
            format_pct(regression_term_p),
            TermLayout::Uniform.tag(),
            format_pct(narrow_range_sel)
        ),
        vec![(
            format!(
                "{}:a AND num_asc_fast:[0 TO 0]",
                title_field_name(regression_term_p, TermLayout::Uniform)
            ),
            "a_AND_num_asc_fast".to_string(),
            "num_asc_fast".to_string(),
        )],
    ));

    // range∩range queries use no term, so they belong in their own groups keyed by range
    // selectivity only (one per distinct value) rather than under a misleading `p(a)` group.
    let mut range_sels: Vec<f64> = Vec::new();
    for &(_, range_sel) in &scenarios {
        if !range_sels.contains(&range_sel) {
            range_sels.push(range_sel);
        }
    }
    for range_sel in range_sels {
        let group_name = format!("{} range, range∩range", format_pct(range_sel));
        prepared.push((group_name, vec![build_range_intersection_query(range_sel)]));
    }

    // A separate runner per collector type: the collector heads the output section (via the runner
    // name) instead of being repeated in every task name.
    run_collector(
        BenchRunner::with_name("count"),
        &bench_index,
        &prepared,
        false,
        |_| Count,
    );
    run_collector(
        BenchRunner::with_name("cnt+top_score"),
        &bench_index,
        &prepared,
        false,
        |_| (Count, TopDocs::with_limit(100).order_by_score()),
    );
    run_collector(
        BenchRunner::with_name("top100_asc"),
        &bench_index,
        &prepared,
        true,
        |field| TopDocs::with_limit(100).order_by_fast_field::<u64>(field.to_string(), Order::Asc),
    );
    run_collector(
        BenchRunner::with_name("top100_desc"),
        &bench_index,
        &prepared,
        true,
        |field| TopDocs::with_limit(100).order_by_fast_field::<u64>(field.to_string(), Order::Desc),
    );
}

/// `(query_str, label, field_name)`: `query_str` is parsed and executed, `label` names the task,
/// `field_name` is the field used to pick fast-field collectors.
type Query3 = (String, String, String);

/// `title:a AND field:[range]` for each numeric field, at the scenario's range selectivity, using
/// the title field of the given layout. The range selectivity and layout are stated in the group
/// name, so labels only carry the field.
fn build_term_and_range_queries(p_a: f64, range_sel: f64, layout: TermLayout) -> Vec<Query3> {
    let title_field = title_field_name(p_a, layout);
    NUMERIC_FIELDS
        .iter()
        .map(|&(field_name, dist)| {
            let (low, high) = dist.bounds_for(range_sel);
            let query_str = format!("{}:a AND {}:[{} TO {}]", title_field, field_name, low, high);
            let label = format!("a_AND_{}", field_name);
            (query_str, label, field_name.to_string())
        })
        .collect()
}

/// Intersects the two fast range fields (`num_rand_fast AND num_asc_fast`) at `range_sel`. No term
/// is involved, hence its own group.
fn build_range_intersection_query(range_sel: f64) -> Query3 {
    let (rand_low, rand_high) = Distribution::Rand.bounds_for(range_sel);
    let (asc_low, asc_high) = Distribution::Asc.bounds_for(range_sel);
    let query_str = format!(
        "num_rand_fast:[{} TO {}] AND num_asc_fast:[{} TO {}]",
        rand_low, rand_high, asc_low, asc_high
    );
    (
        query_str,
        "num_rand_fast_AND_num_asc_fast".to_string(),
        "num_asc_fast".to_string(),
    )
}

/// Runs one collector over every group in its own named runner. `make_collector` builds the
/// collector for a query's field; `only_fast` skips non-fast fields (fast-field ordering needs a
/// fast field).
fn run_collector<C, F>(
    mut runner: BenchRunner,
    bench_index: &BenchIndex,
    prepared: &[(String, Vec<Query3>)],
    only_fast: bool,
    make_collector: F,
) where
    C: Collector + 'static,
    F: Fn(&str) -> C,
{
    for (group_name, queries) in prepared {
        let mut group = runner.new_group();
        group.set_name(group_name);
        for (query_str, label, field_name) in queries {
            if only_fast && !field_name.ends_with("_fast") {
                continue;
            }
            let query = bench_index.query_parser.parse_query(query_str).unwrap();
            let search_task = SearchTask {
                searcher: bench_index.searcher.clone(),
                collector: make_collector(field_name),
                query,
            };
            group.register(label.clone(), move |_| black_box(search_task.run()));
        }
        group.run();
    }
}

struct SearchTask<C: Collector> {
    searcher: Searcher,
    collector: C,
    query: Box<dyn Query>,
}

impl<C: Collector> SearchTask<C> {
    #[inline(never)]
    pub fn run(&self) -> usize {
        let result = self.searcher.search(&self.query, &self.collector).unwrap();
        if let Some(count) = (&result as &dyn std::any::Any).downcast_ref::<usize>() {
            *count
        } else if let Some(top_docs) = (&result as &dyn std::any::Any)
            .downcast_ref::<Vec<(Option<u64>, tantivy::DocAddress)>>()
        {
            top_docs.len()
        } else if let Some(top_docs_with_count) = (&result as &dyn std::any::Any)
            .downcast_ref::<(usize, Vec<(f32, tantivy::DocAddress)>)>()
        {
            top_docs_with_count.0
        } else if let Some(top_docs) =
            (&result as &dyn std::any::Any).downcast_ref::<Vec<(u64, tantivy::DocAddress)>>()
        {
            top_docs.len()
        } else if let Some(doc_set) = (&result as &dyn std::any::Any)
            .downcast_ref::<std::collections::HashSet<tantivy::DocAddress>>()
        {
            doc_set.len()
        } else {
            eprintln!(
                "Unknown collector result type: {:?}",
                std::any::type_name::<C::Fruit>()
            );
            0
        }
    }
}
