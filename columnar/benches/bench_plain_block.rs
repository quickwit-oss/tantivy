//! Cost of opening an OnPair16 block of a plain column, and of reading values from it.
//!
//! The values are the messages of the loghub `Apache_2k.log` sample, see `data/README.md`.

use binggan::{InputGroup, black_box};
use common::OwnedBytes;
use tantivy_columnar::{PlainBlock, encode_plain_block};

/// Kind byte of an OnPair16 block.
const BLOCK_KIND_ONPAIR16: u8 = 1;

const MESSAGES: &str = include_str!("data/apache_2k_messages.txt");

struct BenchInput {
    block_bytes: OwnedBytes,
    num_values: u32,
    /// The block, already open: reads are measured without the cost of opening it.
    open_block: PlainBlock,
}

fn build_apache_2k() -> BenchInput {
    let mut bytes: Vec<u8> = Vec::new();
    let mut offsets: Vec<u32> = vec![0];
    for message in MESSAGES.lines() {
        bytes.extend_from_slice(message.as_bytes());
        offsets.push(bytes.len() as u32);
    }
    build_input("apache_2k", &bytes, &offsets)
}

/// ~1 MiB of varied log lines, large enough to exercise a full 4,096-token (12-bit) dictionary.
fn build_varied_logs_1m() -> BenchInput {
    let services = [
        "auth-api",
        "billing-worker",
        "search-gateway",
        "ingest-router",
        "metrics-aggregator",
        "trace-collector",
        "cache-proxy",
        "scheduler-core",
    ];
    let paths = [
        "/api/v2/events/query",
        "/api/v1/users/profile",
        "/internal/healthz",
        "/v3/metrics/submit",
        "/v2/logs/stream",
        "/admin/shards/rebalance",
        "/graphql/dashboard",
        "/oauth2/token/refresh",
    ];
    let levels = ["INFO", "WARN", "ERROR", "DEBUG"];
    let mut bytes: Vec<u8> = Vec::with_capacity(1_200_000);
    let mut offsets: Vec<u32> = Vec::with_capacity(12_001);
    offsets.push(0);
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next_rand = || -> u64 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state >> 16
    };
    let lines: Vec<&str> = MESSAGES.lines().collect();
    for i in 0..10_000usize {
        use std::io::Write;
        let r1 = next_rand() as usize;
        let r2 = next_rand() as usize;
        let _ = write!(
            &mut bytes,
            "2026-10-{:02}T{:02}:{:02}:{:02}.{:03}Z {} svc={} host=prod-{:03}-{:02} path={} \
             status={} latency_ms={} req_id={:08x} msg={}",
            1 + (i % 28),
            (r1 / 3600) % 24,
            (r1 / 60) % 60,
            r1 % 60,
            r2 % 1000,
            levels[r1 % levels.len()],
            services[(r1 >> 3) % services.len()],
            r2 % 200,
            (r2 >> 8) % 32,
            paths[(r2 >> 12) % paths.len()],
            [200, 201, 204, 400, 404, 429, 500, 503][r1 % 8],
            r2 % 4500,
            (next_rand() as u32),
            lines[i % lines.len()],
        );
        offsets.push(bytes.len() as u32);
    }
    build_input("varied_logs_1m", &bytes, &offsets)
}

fn build_input(label: &str, bytes: &[u8], offsets: &[u32]) -> BenchInput {
    let num_values = (offsets.len() - 1) as u32;
    let mut block_bytes = Vec::new();
    let mut scratch = Vec::new();
    encode_plain_block(bytes, offsets, &mut block_bytes, &mut scratch).unwrap();
    assert_eq!(
        block_bytes[0], BLOCK_KIND_ONPAIR16,
        "the sample is expected to be compressed with OnPair16"
    );
    let num_tokens = u16::from_le_bytes([block_bytes[1], block_bytes[2]]) as usize + 1;
    let dict_bytes_len = u32::from_le_bytes(block_bytes[3..7].try_into().unwrap());
    let code_offsets_len = u32::from_le_bytes(block_bytes[7..11].try_into().unwrap());
    let num_codes = u32::from_le_bytes(block_bytes[11..15].try_into().unwrap());
    println!(
        "{label}: {num_values} values, {} raw bytes, {} block bytes (num_tokens={num_tokens}, \
         dict_bytes={dict_bytes_len}, code_offsets={code_offsets_len}, num_codes={num_codes})",
        bytes.len(),
        block_bytes.len()
    );
    let block_bytes = OwnedBytes::new(block_bytes);
    let open_block = PlainBlock::open(block_bytes.clone(), num_values);
    BenchInput {
        block_bytes,
        num_values,
        open_block,
    }
}

fn main() {
    let inputs = vec![
        ("apache_2k".to_string(), build_apache_2k()),
        ("varied_logs_1m".to_string(), build_varied_logs_1m()),
    ];
    let mut group = InputGroup::new_with_inputs(inputs);

    group.register("open_onpair16_block", |input| {
        // `OwnedBytes::clone` only clones an `Arc`.
        let block = PlainBlock::open(input.block_bytes.clone(), input.num_values);
        black_box(block);
    });

    // Divide by `num_values` to get the cost of reading one value.
    group.register("read_all_values", |input| {
        let mut value: Vec<u8> = Vec::new();
        let mut total_len = 0;
        for slot in 0..input.num_values {
            value.clear();
            input.open_block.append_value(slot, &mut value);
            total_len += value.len();
        }
        black_box(total_len);
    });

    // A point lookup: open the block, then read one value.
    group.register("open_and_read_one_value", |input| {
        let block = PlainBlock::open(input.block_bytes.clone(), input.num_values);
        let mut value: Vec<u8> = Vec::new();
        block.append_value(input.num_values / 2, &mut value);
        black_box(value);
    });

    group.run();
}
