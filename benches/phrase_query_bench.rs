// Benchmarks phrase queries over observability-style documents.
//
// What's measured:
// - A log corpus where most docs are short log lines, and a minority are long documents (JVM stack
//   traces, numeric payload dumps) in which a few tokens repeat hundreds of times.
// - Phrase queries mixing a rare token with a token that is very frequent inside the long docs, so
//   that per-document position lists are highly asymmetric.
// - A balanced phrase as a control.
// - Each query with scoring disabled (count, as when sorting by timestamp) and enabled (top 10).
//
// Run with: cargo bench --bench phrase_query_bench

use binggan::{black_box, BenchRunner};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, TEXT};
use tantivy::{doc, Index, ReloadPolicy};

const NUM_DOCS: usize = 100_000;

const PACKAGES: &[&str] = &[
    "org.springframework.web.servlet",
    "org.apache.catalina.core",
    "org.apache.tomcat.util.net",
    "com.zaxxer.hikari.pool",
    "io.netty.channel",
    "java.util.concurrent",
    "com.acme.platform.http",
];
const CLASSES: &[&str] = &[
    "FrameworkServlet",
    "StandardWrapperValve",
    "NioEndpoint",
    "HikariPool",
    "AbstractChannelHandlerContext",
    "ThreadPoolExecutor",
    "RequestHandler",
];
const METHODS: &[&str] = &[
    "service",
    "invoke",
    "run",
    "doFilter",
    "getConnection",
    "handle",
];
const LOG_WORDS: &[&str] = &[
    "request",
    "completed",
    "user",
    "session",
    "started",
    "cache",
    "miss",
    "timeout",
    "retry",
    "connection",
    "ok",
    "status",
    "latency",
    "ms",
    "GET",
    "POST",
    "upstream",
    "pod",
    "ready",
];

fn log_line(rng: &mut StdRng) -> String {
    let num_words = rng.random_range(8..30);
    let words: Vec<&str> = (0..num_words)
        .map(|_| *LOG_WORDS.choose(rng).unwrap())
        .collect();
    format!(
        "INFO {} trace_id={:x}",
        words.join(" "),
        rng.random::<u64>()
    )
}

/// A JVM stack trace with 50-250 frames. In a fraction of them, one frame comes from
/// `InvoiceService.java:42`, so `invoiceservice` appears once while `java` appears once per frame.
fn stack_trace(rng: &mut StdRng) -> String {
    let num_frames = rng.random_range(50..250);
    let culprit_frame = if rng.random_bool(0.2) {
        Some(rng.random_range(0..num_frames))
    } else {
        None
    };
    let mut trace = String::from("ERROR request failed java.lang.IllegalStateException: boom\n");
    for frame in 0..num_frames {
        if Some(frame) == culprit_frame {
            trace.push_str("\tat com.acme.billing.InvoiceService.charge(InvoiceService.java:42)\n");
            continue;
        }
        let package = PACKAGES.choose(rng).unwrap();
        let class = CLASSES.choose(rng).unwrap();
        let method = METHODS.choose(rng).unwrap();
        let line = rng.random_range(1..2000);
        trace.push_str(&format!(
            "\tat {package}.{class}.{method}({class}.java:{line})\n"
        ));
    }
    trace
}

/// A dump of a metrics/JSON payload: hundreds of small integers (mostly `0`), and in a fraction
/// of them a client IP of `10.0.0.1`.
fn payload_dump(rng: &mut StdRng) -> String {
    let num_fields = rng.random_range(100..400);
    let mut payload = String::from("DEBUG response payload {");
    let ip_field = if rng.random_bool(0.2) {
        Some(rng.random_range(0..num_fields))
    } else {
        None
    };
    for field in 0..num_fields {
        if Some(field) == ip_field {
            payload.push_str("\"client_ip\":\"10.0.0.1\",");
            continue;
        }
        let value = if rng.random_bool(0.7) {
            0
        } else {
            rng.random_range(1..5)
        };
        payload.push_str(&format!("\"f{field}\":{value},"));
    }
    payload.push('}');
    payload
}

fn main() {
    let mut schema_builder = Schema::builder();
    let message = schema_builder.add_text_field("message", TEXT);
    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema);

    let mut rng = StdRng::from_seed([42u8; 32]);
    {
        let mut writer = index.writer_with_num_threads(1, 500_000_000).unwrap();
        for _ in 0..NUM_DOCS {
            let text = match rng.random_range(0..100) {
                0..10 => stack_trace(&mut rng),
                10..20 => payload_dump(&mut rng),
                _ => log_line(&mut rng),
            };
            writer.add_document(doc!(message => text)).unwrap();
        }
        writer.commit().unwrap();
    }
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();
    let searcher = reader.searcher();
    let query_parser = QueryParser::for_index(&index, vec![message]);

    let queries = [
        // Rare token followed by a token repeated once per stack frame.
        "\"InvoiceService.java\"",
        "\"InvoiceService.java:42\"",
        // `10` once, followed by `0` repeated hundreds of times in the payload.
        "\"10.0.0.1\"",
        // Phrase prefix: the suffix expansions are always counted.
        "\"InvoiceService.jav\"*",
        // Control: both tokens appear once per frame (balanced position lists).
        "\"at org\"",
    ];

    let mut runner = BenchRunner::new();
    for query_str in queries {
        let query = query_parser.parse_query(query_str).unwrap();
        println!(
            "{query_str}: {} hits",
            searcher.search(&query, &Count).unwrap()
        );
        let mut group = runner.new_group();
        group.set_name(query_str);
        {
            let searcher = searcher.clone();
            let query = query.box_clone();
            group.register("count", move |_| {
                black_box(searcher.search(&query, &Count).unwrap())
            });
        }
        {
            let searcher = searcher.clone();
            group.register("top10", move |_| {
                let collector = TopDocs::with_limit(10).order_by_score();
                black_box(searcher.search(&query, &collector).unwrap());
                1usize
            });
        }
        group.run();
    }
}
