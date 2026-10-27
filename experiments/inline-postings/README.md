# Inline postings

Inlining stores whole short postings lists, including frequencies and positions,
in their term dictionary block. Quickwit's `max_inline_docs` accepts 0–32 and
defaults to 0 (disabled). The [format documentation](../../src/termdict/sstable_termdict/FORMAT.md)
describes the encoding and compatibility.

Results below cover the `message` field (physical field 3) of split
`KH4Y_01M5MTJQ3SVHDTK2W3XCB5`, segment `4fea6c896fd045aba3cece0808f3e41e`:
10,002,000 documents and 7,671,280 terms. MB means 1,000,000 bytes.
**Cutoffs 64 and 128 are experiments beyond the supported limit of 32.**

## Storage by inline cutoff

These results use per-list document-ID deltas, compressed byte lanes, tagged
singleton positions, and count widths derived from the dictionary's cutoff.
The dictionary includes inline documents, frequencies, and positions. The other
columns contain only the remaining external data.

| Maximum inline documents | Dictionary MB | External postings MB | External positions MB | Total MB | Total reduction |
|---|---:|---:|---:|---:|---:|
| 0 | 51.678 | 253.765 | 211.057 | 516.500 | 0.00% |
| 1 | 71.786 | 222.905 | 198.140 | 492.831 | 4.58% |
| 4 | 72.391 | 222.205 | 197.924 | 492.521 | 4.64% |
| 8 | 74.477 | 219.453 | 197.160 | 491.090 | 4.92% |
| 16 | 91.810 | 196.914 | 191.079 | 479.803 | 7.11% |
| 32 | 124.717 | 154.032 | 179.441 | 458.190 | 11.29% |
| 64 | 138.647 | 134.993 | 174.284 | 447.924 | 13.28% |
| 128 | 143.411 | 128.280 | 172.472 | 444.163 | 14.01% |

- Single-document lists account for most of the saving through cutoff 8.
- Cutoff 8 inlines 6,597,536 lists. Cutoff 32 inlines 7,537,196 lists and saves
  58.31 MB overall, while growing the dictionary from 51.68 to 124.72 MB.
- Moving from 32 to 64 saves another 10.27 MB. Moving from 64 to 128 saves 3.76 MB.

Dictionary sizes are serialized sizes. External sizes model byte-preserving
compaction. Totals exclude the unchanged token-count header, composite metadata,
component footers, and other fields. No complete rewritten split was measured.

## Document-ID deltas: size and decoding cost

The first ID in each list remains absolute. Later IDs become gaps. External
postings and positions are identical between encodings. The following combines
dictionary-size savings with full RAM-resident dictionary-scan timings.

| Cutoff | Absolute dictionary MB | Delta dictionary MB | Bytes saved | Absolute scan median ms [range] | Delta scan median ms [range] | Scan time change |
|---|---:|---:|---:|---:|---:|---:|
| 8 | 74.501 | 74.477 | 24,448 | 758.33 [755.14–778.73] | 767.15 [756.87–781.74] | +1.16% |
| 32 | 129.426 | 124.717 | 4,708,905 | 1,034.70 [1,023.34–1,068.91] | 1,053.27 [1,041.26–1,087.68] | +1.79% |

At cutoff 8, only 9.14% of inline document entries become gaps. At cutoff 32,
68.31% do, making deltas more effective. At cutoffs 1–6, delta dictionaries were
37–8,114 bytes larger than the absolute-ID controls.

Timings are from an Apple M4 Max with 64 GiB RAM, Rust 1.98 release build, and
12 alternating measured scans after warmup. They include decompression,
allocations, validation, positions, and an inline-ID checksum. File I/O and
opening the dictionary are excluded. Shared-workstation variance limits precision.
These scans show a small CPU cost for deltas; live-query latency remains unmeasured.

## External reads and request budget

The planner first merges gaps up to 512 KiB, then fills the cheapest remaining
gaps within an additional allowance of `max(2 MiB, initial_read_bytes / 4)` per
field, segment, and call. This minimizes requests within that allowance. Small
plans can grow by more than 25% because of the 2 MiB minimum.

At cutoff 8, each policy cell below is **requests / external bytes**. Bytes include
coalesced gaps. These plans apply to both absolute and delta encodings because
their external ranges are identical.

| Query | Matching lists | External lists | Fixed 512 KiB | With byte budget |
|---|---:|---:|---:|---:|
| `*a*` | 1,921,269 | 3,870 | 33 / 79,121,102 | 11 / 97,597,707 |
| `*ab*` | 75,774 | 203 | 30 / 1,947,701 | 28 / 3,401,299 |
| `*aba*` | 2,817 | 16 | 5 / 657,307 | 4 / 2,015,570 |
| `*traaa*` | 2 | 0 | 0 / 0 | 0 / 0 |
| `traaa*` | 0 | 0 | 0 / 0 | 0 / 0 |
| `*player*` | 21 | 21 | 11 / 4,699,006 | 10 / 5,943,196 |
| `*ms*` | 50,585 | 27 | 16 / 1,556,712 | 14 / 2,922,609 |

For `*a*`, the byte budget cuts requests from 33 to 11 for 18.48 MB more data
(23.4%). Both `*traaa*` matches are inline. The prefix `traaa*` has no matches.

Sums across these seven independent queries, with equal weight and no cross-query
caching, show the effect of the inline cutoff. Each query gets its own allowance.

| Cutoff | Useful external bytes | Fixed requests / external bytes | Budgeted requests / external bytes |
|---:|---:|---:|---:|
| 0 | 50,089,825 | 378 / 596,950,546 | 185 / 743,579,954 |
| 8 | 40,176,390 | 95 / 87,981,828 | 67 / 111,880,381 |
| 32 | 39,993,334 | 73 / 59,797,495 | 53 / 77,019,020 |
| 64 | 39,959,502 | 54 / 54,016,768 | 37 / 69,984,821 |
| 128 | 39,950,059 | 47 / 53,577,891 | 32 / 67,019,221 |

These are planned external-postings reads. They exclude dictionary reads,
external positions, document retrieval, retries, and storage-specific request
limits. They show cutoff sensitivity rather than a workload forecast. Request
overhead, throughput, concurrency, and caching determine the latency tradeoff.

## Cold dictionary costs

This historical measurement uses the earlier absolute-ID, fixed-four-bit layout
at cutoff 8. Both columns use fixed 512 KiB coalescing, without the extra-byte
budget. Values include dictionary-block reads plus external postings, with the
dictionary index already open.

| Query | No inlining, cold total MB | Cutoff 8, cold total MB |
|---|---:|---:|
| `*a*` | 246.726 | 153.425 |
| `*ab*` | 214.028 | 76.251 |
| `*aba*` | 145.121 | 74.961 |
| `*traaa*` | 51.482 | 74.304 |
| `traaa*` | 0.002940 | 0.004161 |
| `*player*` | 56.366 | 79.003 |
| `*ms*` | 192.122 | 75.860 |

All leading-wildcard queries read the entire dictionary's data blocks: 51.482 MB
without inlining and 74.304 MB at cutoff 8, in one dictionary request. This makes
`*traaa*` and `*player*` more expensive with cold dictionary blocks even though
external postings reads shrink. The unmatched prefix still reads one candidate
block. With dictionary blocks cached, only the external-postings cost remains.

Storage and query read volume favor different cutoffs. Compaction can also move
gaps across the coalescing threshold, making fetched bytes nonmonotonic as the
cutoff increases. Cold dictionary reads for the current delta encoding and larger
cutoffs, indexing throughput, and end-to-end query latency remain unmeasured.
