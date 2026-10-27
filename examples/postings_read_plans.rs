//! Compare external-postings read plans from a local, non-inline `.term` file.
//! Usage: postings_read_plans FILE.term > summary.json
//! See experiments/inline-postings/README.md for scope and interpretation.
use std::io;
use std::ops::Range;
use std::path::Path;

use itertools::Itertools;
use serde_json::{json, Value};
use tantivy::directory::footer::Footer;
use tantivy::directory::{CompositeFile, FileSlice};
use tantivy::schema::Field;
use tantivy::termdict::TermDictionary;

const CUTOFFS: [u32; 5] = [0, 8, 32, 64, 128];
const PATTERNS: [&str; 7] = [
    "*a*", "*ab*", "*aba*", "*traaa*", "traaa*", "*player*", "*ms*",
];
const BASELINE_GAP: usize = 512 * 1024;
const MIN_EXTRA_BYTES: usize = 2 * 1024 * 1024;

#[derive(Default)]
struct Matches {
    lists: usize,
    inline_lists: usize,
    ranges: Vec<Range<usize>>,
}

fn field_component(path: &Path) -> Result<FileSlice, Box<dyn std::error::Error>> {
    let bytes = std::fs::read(path)?;
    let (footer, body) = Footer::extract_footer(FileSlice::from(bytes))?;
    footer.is_compatible().map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("incompatible footer: {err:?}"),
        )
    })?;
    assert_eq!(
        crc32fast::hash(&body.read_bytes()?),
        footer.crc,
        "footer CRC"
    );
    CompositeFile::open(&body)?
        .open_read(Field::from_field_id(3))
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "missing physical field 3").into()
        })
}

// Check exact coverage and ordered output. Inputs are ordered and disjoint.
fn summarize(input: &[Range<usize>], plan: &[Range<usize>], useful_bytes: usize) -> Value {
    let mut remaining = input.iter().peekable();
    for read in plan {
        assert!(!read.is_empty());
        assert_eq!(remaining.peek().unwrap().start, read.start);
        let mut end = read.start;
        while let Some(range) = remaining.peek() {
            if range.start >= read.end {
                break;
            }
            assert!(range.start >= end);
            assert!(range.end <= read.end);
            end = range.end;
            remaining.next();
        }
        assert_eq!(end, read.end);
    }
    assert!(remaining.next().is_none());
    let planned_bytes: usize = plan.iter().map(|range| range.len()).sum();
    json!({
        "requests": plan.len(),
        "planned_bytes": planned_bytes,
        "extra_bytes": planned_bytes - useful_bytes,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args().nth(1).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: postings_read_plans FILE.term",
        )
    })?;
    let terms = TermDictionary::open(field_component(Path::new(&path))?)?;
    let mut stream = terms.stream()?;
    let mut matches: [[Matches; PATTERNS.len()]; CUTOFFS.len()] = Default::default();
    let mut removed_bytes = [0usize; CUTOFFS.len()];
    let mut removed_lists = [0usize; CUTOFFS.len()];
    let mut count = 0usize;
    let mut offset = 0usize;
    while stream.advance() {
        let info = stream.value();
        assert!(
            info.short_postings.is_none(),
            "source must have no inline postings"
        );
        assert!(info.doc_freq > 0);
        assert!(!info.postings_range.is_empty());
        assert_eq!(
            info.postings_range.start, offset,
            "non-contiguous source postings"
        );
        let len = info.postings_range.len();
        let key = stream.key();
        let matched = [
            key.contains(&b'a'),
            key.windows(2).any(|s| s == b"ab"),
            key.windows(3).any(|s| s == b"aba"),
            key.windows(5).any(|s| s == b"traaa"),
            key.starts_with(b"traaa"),
            key.windows(6).any(|s| s == b"player"),
            key.windows(2).any(|s| s == b"ms"),
        ];
        for (c, cutoff) in CUTOFFS.iter().enumerate() {
            let inline = info.doc_freq <= *cutoff;
            for (q, yes) in matched.iter().enumerate() {
                if *yes {
                    let query = &mut matches[c][q];
                    query.lists += 1;
                    if inline {
                        query.inline_lists += 1;
                    } else {
                        let start = offset - removed_bytes[c];
                        query.ranges.push(start..start + len);
                    }
                }
            }
            // Compact ALL qualifying lists, including terms matching none of the patterns.
            if inline {
                removed_bytes[c] += len;
                removed_lists[c] += 1;
            }
        }
        offset = info.postings_range.end;
        count += 1;
    }
    assert_eq!(count, terms.num_terms());

    let mut rows = Vec::new();
    for (c, cutoff) in CUTOFFS.iter().enumerate() {
        for (q, pattern) in PATTERNS.iter().enumerate() {
            let query = &matches[c][q];
            let useful_bytes: usize = query.ranges.iter().map(|range| range.len()).sum();
            let merge_up_to = |gap| {
                query
                    .ranges
                    .iter()
                    .cloned()
                    .coalesce(|left, right| {
                        if right.start.saturating_sub(left.end) <= gap {
                            Ok(left.start..left.end.max(right.end))
                        } else {
                            Err((left, right))
                        }
                    })
                    .collect::<Vec<_>>()
            };
            // Historical production policy: merge every gap <=512 KiB.
            let fixed = merge_up_to(BASELINE_GAP);
            let baseline_bytes: usize = fixed.iter().map(|range| range.len()).sum();
            let allowed_extra_bytes = MIN_EXTRA_BYTES.max(baseline_bytes / 4);
            // Spend extra bytes to remove requests from the fixed baseline.
            let mut request_budget = fixed.clone();
            common::merge_ranges_with_byte_budget(&mut request_budget, allowed_extra_bytes);
            let planned_bytes: usize = request_budget.iter().map(|range| range.len()).sum();
            assert!(planned_bytes >= baseline_bytes);
            assert!(planned_bytes <= baseline_bytes + allowed_extra_bytes);
            assert!(request_budget.len() <= fixed.len());
            assert_eq!(query.lists, query.inline_lists + query.ranges.len());
            rows.push(json!({
                "cutoff": cutoff,
                "pattern": pattern,
                "matching_lists": query.lists,
                "inline_lists": query.inline_lists,
                "external_lists": query.ranges.len(),
                "useful_bytes": useful_bytes,
                "allowed_extra_bytes": allowed_extra_bytes,
                "fixed_512kib": summarize(&query.ranges, &fixed, useful_bytes),
                "request_budget": summarize(&query.ranges, &request_budget, useful_bytes),
            }));
        }
    }
    let summary = json!({
        "source": path,
        "physical_field": 3,
        "all_terms": count,
        "original_postings_bytes": offset,
        "cutoffs": CUTOFFS,
        "removed_lists": removed_lists,
        "removed_bytes": removed_bytes,
        "policy": {
            "name": "request_budget",
            "allowed_extra_bytes": "max(2097152, fixed_512kib.planned_bytes / 4)",
            "baseline_gap_bytes": BASELINE_GAP,
            "min_extra_bytes": MIN_EXTRA_BYTES,
            "objective": "Minimize requests, then bytes, by merging the globally cheapest gaps in the fixed-512-KiB plan within the extra-byte allowance. No subsequent per-gap ceiling."
        },
        "validation": "Compatible footer and CRC; no source inline lists; nonempty contiguous source ranges; dictionary count; all input ranges covered exactly; ordered disjoint output; requests no greater than fixed merging; bytes between fixed baseline and baseline plus allowance.",
        "rows": rows,
    });
    serde_json::to_writer_pretty(io::stdout().lock(), &summary)?;
    println!();
    Ok(())
}
