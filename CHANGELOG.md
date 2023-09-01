Tantivy 0.21
================================
#### Bugfixes
- Fix track fast field memory consumption, which led to higher memory consumption than the budget allowed during indexing [#2148](https://github.com/quickwit-oss/tantivy/issues/2148)[#2147](https://github.com/quickwit-oss/tantivy/issues/2147)(@PSeitz)
- Fix a regression from 0.20 where sort index by date wasn't working anymore [#2124](https://github.com/quickwit-oss/tantivy/issues/2124)(@PSeitz)
- Fix getting the root facet on the `FacetCollector`. [#2086](https://github.com/quickwit-oss/tantivy/issues/2086)(@adamreichold)
- Align numerical type priority order of columnar and query. [#2088](https://github.com/quickwit-oss/tantivy/issues/2088)(@fmassot)
#### Breaking Changes
- Remove support for Brotli and Snappy compression [#2123](https://github.com/quickwit-oss/tantivy/issues/2123)(@adamreichold)
#### Features/Improvements
- Implement lenient query parser [#2129](https://github.com/quickwit-oss/tantivy/pull/2129)(@trinity-1686a)
- order_by_u64_field and order_by_fast_field allow sorting in ascending and descending order [#2111](https://github.com/quickwit-oss/tantivy/issues/2111)(@naveenann)
- Allow dynamic filters in text analyzer builder [#2110](https://github.com/quickwit-oss/tantivy/issues/2110)(@fulmicoton @fmassot)
- **Aggregation**
  - Add missing parameter for term aggregation [#2149](https://github.com/quickwit-oss/tantivy/issues/2149)[#2103](https://github.com/quickwit-oss/tantivy/issues/2103)(@PSeitz)
  - Add missing parameter for percentiles [#2157](https://github.com/quickwit-oss/tantivy/issues/2157)(@PSeitz)
  - Add missing parameter for stats,min,max,count,sum,avg [#2151](https://github.com/quickwit-oss/tantivy/issues/2151)(@PSeitz)
  - Improve aggregation deserialization error message [#2150](https://github.com/quickwit-oss/tantivy/issues/2150)(@PSeitz)
  - Add validation for type Bytes to term_agg [#2077](https://github.com/quickwit-oss/tantivy/issues/2077)(@PSeitz)
  - Alternative mixed field collection [#2135](https://github.com/quickwit-oss/tantivy/issues/2135)(@PSeitz)
- Add missing query_terms impl for TermSetQuery. [#2120](https://github.com/quickwit-oss/tantivy/issues/2120)(@adamreichold)
- Minor improvements to OwnedBytes [#2134](https://github.com/quickwit-oss/tantivy/issues/2134)(@adamreichold)
- Remove allocations in split compound words [#2080](https://github.com/quickwit-oss/tantivy/issues/2080)(@PSeitz)
- Ngram tokenizer now returns an error with invalid arguments [#2102](https://github.com/quickwit-oss/tantivy/issues/2102)(@fmassot)
- Make TextAnalyzerBuilder public [#2097](https://github.com/quickwit-oss/tantivy/issues/2097)(@adamreichold)
- Return an error when tokenizer is not found while indexing [#2093](https://github.com/quickwit-oss/tantivy/issues/2093)(@naveenann)
- Delayed column opening during merge [#2132](https://github.com/quickwit-oss/tantivy/issues/2132)(@PSeitz)

Tantivy 0.20.2
================================
- Align numerical type priority order on the search side.  [#2088](https://github.com/quickwit-oss/tantivy/issues/2088) (@fmassot)
- Fix is_child_of function not considering the root facet. [#2086](https://github.com/quickwit-oss/tantivy/issues/2086) (@adamreichhold)

Tantivy 0.20.1
================================
- Fix building on windows with mmap [#2070](https://github.com/quickwit-oss/tantivy/issues/2070) (@ChillFish8)

Tantivy 0.20
================================
#### Bugfixes
- Fix phrase queries with slop (slop supports now transpositions, algorithm that carries slop so far for num terms > 2) [#2031](https://github.com/quickwit-oss/tantivy/issues/2031)[#2020](https://github.com/quickwit-oss/tantivy/issues/2020)(@PSeitz)
- Handle error for exists on MMapDirectory [#1988](https://github.com/quickwit-oss/tantivy/issues/1988) (@PSeitz)
- Aggregation
  - Fix min doc_count empty merge bug [#2057](https://github.com/quickwit-oss/tantivy/issues/2057) (@PSeitz)
  - Fix: Sort order for term aggregations (sort order on key was inverted) [#1858](https://github.com/quickwit-oss/tantivy/issues/1858) (@PSeitz)

#### Features/Improvements
- Add PhrasePrefixQuery [#1842](https://github.com/quickwit-oss/tantivy/issues/1842) (@trinity-1686a)
- Add `coerce` option for text and numbers types (convert the value instead of returning an error during indexing) [#1904](https://github.com/quickwit-oss/tantivy/issues/1904) (@PSeitz)
- Add regex tokenizer [#1759](https://github.com/quickwit-oss/tantivy/issues/1759)(@mkleen)
- Move tokenizer API to seperate crate. Having a seperate crate with a stable API will allow us to use tokenizers with different tantivy versions. [#1767](https://github.com/quickwit-oss/tantivy/issues/1767) (@PSeitz)
- **Columnar crate**: New fast field handling (@fulmicoton @PSeitz) [#1806](https://github.com/quickwit-oss/tantivy/issues/1806)[#1809](https://github.com/quickwit-oss/tantivy/issues/1809)
  - Support for fast fields with optional values. Previously tantivy supported only single-valued and multi-value fast fields. The encoding of optional fast fields is now very compact.
  - Fast field Support for JSON (schemaless fast fields). Support multiple types on the same column. [#1876](https://github.com/quickwit-oss/tantivy/issues/1876) (@fulmicoton)
  - Unified access for fast fields over different cardinalities.
  - Unified storage for typed and untyped fields.
  - Move fastfield codecs into columnar. [#1782](https://github.com/quickwit-oss/tantivy/issues/1782) (@fulmicoton)
  - Sparse dense index for optional values [#1716](https://github.com/quickwit-oss/tantivy/issues/1716) (@PSeitz)
  - Switch to nanosecond precision in DateTime fastfield [#2016](https://github.com/quickwit-oss/tantivy/issues/2016) (@PSeitz)
- **Aggregation**
  - Add `date_histogram` aggregation (only `fixed_interval` for now) [#1900](https://github.com/quickwit-oss/tantivy/issues/1900) (@PSeitz)
  - Add `percentiles` aggregations [#1984](https://github.com/quickwit-oss/tantivy/issues/1984) (@PSeitz)
  - [**breaking**] Drop JSON support on intermediate agg result (we use postcard as format in `quickwit` to send intermediate results) [#1992](https://github.com/quickwit-oss/tantivy/issues/1992) (@PSeitz)
  - Set memory limit in bytes for aggregations after which they abort (Previously there was only the bucket limit) [#1942](https://github.com/quickwit-oss/tantivy/issues/1942)[#1957](https://github.com/quickwit-oss/tantivy/issues/1957)(@PSeitz)
  - Add support for u64,i64,f64 fields in term aggregation [#1883](https://github.com/quickwit-oss/tantivy/issues/1883) (@PSeitz)
  - Allow histogram bounds to be passed as Rfc3339 [#2076](https://github.com/quickwit-oss/tantivy/issues/2076) (@PSeitz)
  - Add count, min, max, and sum aggregations [#1794](https://github.com/quickwit-oss/tantivy/issues/1794) (@guilload)
  - Switch to Aggregation without serde_untagged => better deserialization errors. [#2003](https://github.com/quickwit-oss/tantivy/issues/2003) (@PSeitz)
  - Switch to ms in histogram for date type (ES compatibility) [#2045](https://github.com/quickwit-oss/tantivy/issues/2045) (@PSeitz)
  - Reduce term aggregation memory consumption [#2013](https://github.com/quickwit-oss/tantivy/issues/2013) (@PSeitz)
  - Reduce agg memory consumption: Replace generic aggregation collector (which has a high memory requirement per instance) in aggregation tree with optimized versions behind a trait.
  - Split term collection count and sub_agg (Faster term agg with less memory consumption for cases without sub-aggs) [#1921](https://github.com/quickwit-oss/tantivy/issues/1921) (@PSeitz)
  - Schemaless aggregations: In combination with stacker tantivy supports now schemaless aggregations via the JSON type.
    - Add aggregation support for JSON type [#1888](https://github.com/quickwit-oss/tantivy/issues/1888) (@PSeitz)
    - Mixed types support on JSON fields in aggs [#1971](https://github.com/quickwit-oss/tantivy/issues/1971) (@PSeitz)
  - Perf: Fetch blocks of vals in aggregation for all cardinality [#1950](https://github.com/quickwit-oss/tantivy/issues/1950) (@PSeitz)
  - Allow histogram bounds to be passed as Rfc3339 [#2076](https://github.com/quickwit-oss/tantivy/issues/2076) (@PSeitz)
- `Searcher` with disabled scoring via `EnableScoring::Disabled` [#1780](https://github.com/quickwit-oss/tantivy/issues/1780) (@shikhar)
- Enable tokenizer on json fields [#2053](https://github.com/quickwit-oss/tantivy/issues/2053) (@PSeitz)
- Enforcing "NOT" and "-" queries consistency in UserInputAst [#1609](https://github.com/quickwit-oss/tantivy/issues/1609) (@bazhenov)
- Faster indexing
  - Refactor tokenization pipeline to use GATs [#1924](https://github.com/quickwit-oss/tantivy/issues/1924) (@trinity-1686a)
  - Faster term hash map [#2058](https://github.com/quickwit-oss/tantivy/issues/2058)[#1940](https://github.com/quickwit-oss/tantivy/issues/1940) (@PSeitz)
  - tokenizer-api: reduce Tokenizer allocation overhead [#2062](https://github.com/quickwit-oss/tantivy/issues/2062) (@PSeitz)
  - Refactor vint [#2010](https://github.com/quickwit-oss/tantivy/issues/2010) (@PSeitz)
- Faster search
  - Work in batches of docs on the SegmentCollector (Only for cases without score for now) [#1937](https://github.com/quickwit-oss/tantivy/issues/1937) (@PSeitz)
  - Faster fast field range queries using SIMD [#1954](https://github.com/quickwit-oss/tantivy/issues/1954) (@fulmicoton)
  - Improve fast field range query performance [#1864](https://github.com/quickwit-oss/tantivy/issues/1864) (@PSeitz)
- Make BM25 scoring more flexible [#1855](https://github.com/quickwit-oss/tantivy/issues/1855) (@alexcole)
- Switch fs2 to fs4 as it is now unmaintained and does not support illumos [#1944](https://github.com/quickwit-oss/tantivy/issues/1944) (@Toasterson)
- Made BooleanWeight and BoostWeight public [#1991](https://github.com/quickwit-oss/tantivy/issues/1991) (@fulmicoton)
- Make index compatible with virtual drives on Windows [#1843](https://github.com/quickwit-oss/tantivy/issues/1843) (@gyk)
- Add stop words for Hungarian language [#2069](https://github.com/quickwit-oss/tantivy/issues/2069) (@tnxbutno)
- Auto downgrade index record option, instead of vint error [#1857](https://github.com/quickwit-oss/tantivy/issues/1857) (@PSeitz)
- Enable range query on fast field for u64 compatible types [#1762](https://github.com/quickwit-oss/tantivy/issues/1762) (@PSeitz) [#1876]
- sstable
  - Isolating sstable and stacker in independant crates. [#1718](https://github.com/quickwit-oss/tantivy/issues/1718) (@fulmicoton)
  - New sstable format [#1943](https://github.com/quickwit-oss/tantivy/issues/1943)[#1953](https://github.com/quickwit-oss/tantivy/issues/1953) (@trinity-1686a)
  - Use DeltaReader directly to implement Dictionnary::ord_to_term [#1928](https://github.com/quickwit-oss/tantivy/issues/1928) (@trinity-1686a)
  - Use DeltaReader directly to implement Dictionnary::term_ord [#1925](https://github.com/quickwit-oss/tantivy/issues/1925) (@trinity-1686a)
- Add seperate tokenizer manager for fast fields [#2019](https://github.com/quickwit-oss/tantivy/issues/2019) (@PSeitz)
- Make construction of LevenshteinAutomatonBuilder for FuzzyTermQuery instances lazy. [#1756](https://github.com/quickwit-oss/tantivy/issues/1756) (@adamreichold)
- Added support for madvise when opening an mmaped Index [#2036](https://github.com/quickwit-oss/tantivy/issues/2036) (@fulmicoton)
- Rename `DatePrecision` to `DateTimePrecision` [#2051](https://github.com/quickwit-oss/tantivy/issues/2051) (@guilload)
- Query Parser
  - Quotation mark can now be used for phrase queries. [#2050](https://github.com/quickwit-oss/tantivy/issues/2050) (@fulmicoton)
  - PhrasePrefixQuery is supported in the query parser via: `field:"phrase ter"*` [#2044](https://github.com/quickwit-oss/tantivy/issues/2044) (@adamreichold)
- Docs
  - Update examples for literate docs [#1880](https://github.com/quickwit-oss/tantivy/issues/1880) (@PSeitz)
  - Add ip field example [#1775](https://github.com/quickwit-oss/tantivy/issues/1775) (@PSeitz)
  - Fix doc store cache documentation [#1821](https://github.com/quickwit-oss/tantivy/issues/1821) (@PSeitz)
  - Fix BooleanQuery document [#1999](https://github.com/quickwit-oss/tantivy/issues/1999) (@RT_Enzyme)
  - Update comments in the faceted search example [#1737](https://github.com/quickwit-oss/tantivy/issues/1737) (@DawChihLiou)


Tantivy 0.19
================================
#### Bugfixes
- Fix missing fieldnorms for u64, i64, f64, bool, bytes and date [#1620](https://github.com/quickwit-oss/tantivy/pull/1620) (@PSeitz)
- Fix interpolation overflow in linear interpolation fastfield codec [#1480](https://github.com/quickwit-oss/tantivy/pull/1480) (@PSeitz @fulmicoton)

#### Features/Improvements
- Add support for `IN` in queryparser , e.g. `field: IN [val1 val2 val3]` [#1683](https://github.com/quickwit-oss/tantivy/pull/1683) (@trinity-1686a)
- Skip score calculation, when no scoring is required [#1646](https://github.com/quickwit-oss/tantivy/pull/1646) (@PSeitz)
- Limit fast fields to u32 (`get_val(u32)`) [#1644](https://github.com/quickwit-oss/tantivy/pull/1644) (@PSeitz)
- The `DateTime` type has been updated to hold timestamps with microseconds precision.
  `DateOptions` and `DatePrecision` have been added to configure Date fields. The precision is used to hint on fast values compression. Otherwise, seconds precision is used everywhere else (i.e terms, indexing) [#1396](https://github.com/quickwit-oss/tantivy/pull/1396) (@evanxg852000)
- Add IP address field type [#1553](https://github.com/quickwit-oss/tantivy/pull/1553) (@PSeitz)
- Add boolean field type [#1382](https://github.com/quickwit-oss/tantivy/pull/1382) (@boraarslan)
- Remove Searcher pool and make `Searcher` cloneable. (@PSeitz)
- Validate settings on create [#1570](https://github.com/quickwit-oss/tantivy/pull/1570) (@PSeitz)
- Detect and apply gcd on fastfield codecs [#1418](https://github.com/quickwit-oss/tantivy/pull/1418) (@PSeitz)
- Doc store
  - use separate thread to compress block store [#1389](https://github.com/quickwit-oss/tantivy/pull/1389) [#1510](https://github.com/quickwit-oss/tantivy/pull/1510) (@PSeitz @fulmicoton)
  - Expose doc store cache size [#1403](https://github.com/quickwit-oss/tantivy/pull/1403) (@PSeitz)
  - Enable compression levels for doc store [#1378](https://github.com/quickwit-oss/tantivy/pull/1378) (@PSeitz)
  - Make block size configurable [#1374](https://github.com/quickwit-oss/tantivy/pull/1374) (@kryesh)
- Make `tantivy::TantivyError` cloneable [#1402](https://github.com/quickwit-oss/tantivy/pull/1402) (@PSeitz)
- Add support for phrase slop in query language [#1393](https://github.com/quickwit-oss/tantivy/pull/1393) (@saroh)
- Aggregation
  - Add aggregation support for date type [#1693](https://github.com/quickwit-oss/tantivy/pull/1693)(@PSeitz)
  - Add support for keyed parameter in range and histgram aggregations [#1424](https://github.com/quickwit-oss/tantivy/pull/1424) (@k-yomo)
  - Add aggregation bucket limit [#1363](https://github.com/quickwit-oss/tantivy/pull/1363) (@PSeitz)
- Faster indexing
  - [#1610](https://github.com/quickwit-oss/tantivy/pull/1610) (@PSeitz)
  - [#1594](https://github.com/quickwit-oss/tantivy/pull/1594) (@PSeitz)
  - [#1582](https://github.com/quickwit-oss/tantivy/pull/1582) (@PSeitz)
  - [#1611](https://github.com/quickwit-oss/tantivy/pull/1611) (@PSeitz)
  - Added a pre-configured stop word filter for various language [#1666](https://github.com/quickwit-oss/tantivy/pull/1666) (@adamreichold)

Tantivy 0.18
================================

- For date values `chrono` has been replaced with `time` (@uklotzde) #1304 :
  - The `time` crate is re-exported as `tantivy::time` instead of `tantivy::chrono`.
  - The type alias `tantivy::DateTime` has been removed.
  - `Value::Date` wraps `time::PrimitiveDateTime` without time zone information.
  - Internally date/time values are stored as seconds since UNIX epoch in UTC.
  - Converting a `time::OffsetDateTime` to `Value::Date` implicitly converts the value into UTC.
    If this is not desired do the time zone conversion yourself and use `time::PrimitiveDateTime`
    directly instead.
- Add [histogram](https://github.com/quickwit-oss/tantivy/pull/1306) aggregation (@PSeitz)
- Add support for fastfield on text fields (@PSeitz)
- Add terms aggregation (@PSeitz)
- Add support for zstd compression (@kryesh)

Tantivy 0.18.1
================================
- Hotfix: positions computation.  #1629 (@fmassot, @fulmicoton, @PSeitz)

Tantivy 0.17
================================

- LogMergePolicy now triggers merges if the ratio of deleted documents reaches a threshold (@shikhar @fulmicoton) [#115](https://github.com/quickwit-oss/tantivy/issues/115)
- Adds a searcher Warmer API (@shikhar @fulmicoton)
- Change to non-strict schema. Ignore fields in data which are not defined in schema. Previously this returned an error. #1211
- Facets are necessarily indexed. Existing index with indexed facets should work out of the box. Index without facets that are marked with index: false should be broken (but they were already broken in a sense). (@fulmicoton) #1195 .
- Bugfix that could in theory impact durability in theory on some filesystems [#1224](https://github.com/quickwit-oss/tantivy/issues/1224)
- Schema now offers not indexing fieldnorms (@lpouget) [#922](https://github.com/quickwit-oss/tantivy/issues/922)
- Reduce the number of fsync calls [#1225](https://github.com/quickwit-oss/tantivy/issues/1225)
- Fix opening bytes index with dynamic codec (@PSeitz) [#1278](https://github.com/quickwit-oss/tantivy/issues/1278)
- Added an aggregation collector for range, average and stats compatible with Elasticsearch. (@PSeitz)
- Added a JSON schema type @fulmicoton [#1251](https://github.com/quickwit-oss/tantivy/issues/1251)
- Added support for slop in phrase queries @halvorboe [#1068](https://github.com/quickwit-oss/tantivy/issues/1068)

Tantivy 0.16.2
================================

- Bugfix in FuzzyTermQuery. (transposition_cost_one was not doing anything)

Tantivy 0.16.1
========================

- Major Bugfix on multivalued fastfield.  #1151
- Demux operation (@PSeitz)

Tantivy 0.16.0
=========================

- Bugfix in the filesum check. (@evanxg852000) #1127
- Bugfix in positions when the index is sorted by a field. (@appaquet) #1125

Tantivy 0.15.3
=========================

- Major bugfix. Deleting documents was broken when the index was sorted by a field. (@appaquet, @fulmicoton) #1101

Tantivy 0.15.2
========================

- Major bugfix. DocStore still panics when a deleted doc is at the beginning of a block. (@appaquet) #1088

Tantivy 0.15.1
=========================

- Major bugfix. DocStore panics when first block is deleted. (@appaquet) #1077

Tantivy 0.15.0
=========================

- API Changes. Using Range instead of (start, end) in the API and internals (`FileSlice`, `OwnedBytes`, `Snippets`, ...)
  This change is breaking but migration is trivial.
- Added an Histogram collector. (@fulmicoton) #994
- Added support for Option<TCollector>.  (@fulmicoton)
- DocAddress is now a struct (@scampi) #987
- Bugfix consistent tie break handling in facet's topk (@hardikpnsp) #357
- Date field support for range queries (@rihardsk) #516
- Added lz4-flex as the default compression scheme in tantivy (@PSeitz) #1009
- Renamed a lot of symbols to avoid all uppercasing on acronyms, as per new clippy recommendation. For instance, RAMDirectory -> RamDirectory. (@fulmicoton)
- Simplified positions index format (@fulmicoton) #1022
- Moved bitpacking to bitpacker subcrate and add BlockedBitpacker, which bitpacks blocks of 128 elements (@PSeitz) #1030
- Added support for more-like-this query in tantivy (@evanxg852000) #1011
- Added support for sorting an index, e.g presorting documents in an index by a timestamp field. This can heavily improve performance for certain scenarios, by utilizing the sorted data (Top-n optimizations)(@PSeitz). #1026
- Add iterator over documents in doc store (@PSeitz). #1044
- Fix log merge policy (@PSeitz). #1043
- Add detection to avoid small doc store blocks on merge (@PSeitz). #1054
- Make doc store compression dynamic (@PSeitz). #1060
- Switch to json for footer version handling (@PSeitz). #1060
- Updated TermMerger implementation to rely on the union feature of the FST (@scampi) #469
- Add boolean marking whether position is required in the query_terms API call (@fulmicoton). #1070

Tantivy 0.14.0
=========================

- Remove dependency to atomicwrites #833 .Implemented by @fulmicoton upon suggestion and research from @asafigan).
- Migrated tantivy error from the now deprecated `failure` crate to `thiserror` #760. (@hirevo)
- API Change. Accessing the typed value off a `Schema::Value` now returns an Option instead of panicking if the type does not match.
- Large API Change in the Directory API. Tantivy used to assume that all files could be somehow memory mapped. After this change, Directory return a `FileSlice` that can be reduced and eventually read into an `OwnedBytes` object. Long and blocking io operation are still required by they do not span over the entire file.
- Added support for Brotli compression in the DocStore. (@ppodolsky)
- Added helper for building intersections and unions in BooleanQuery (@guilload)
- Bugfix in `Query::explain`
- Removed dependency on `notify` #924. Replaced with `FileWatcher` struct that polls meta file every 500ms in background thread. (@halvorboe @guilload)
- Added `FilterCollector`, which wraps another collector and filters docs using a predicate over a fast field (@barrotsteindev)
- Simplified the encoding of the skip reader struct. BlockWAND max tf is now encoded over a single byte. (@fulmicoton)
- `FilterCollector` now supports all Fast Field value types (@barrotsteindev)
- FastField are not all loaded when opening the segment reader. (@fulmicoton)
- Added an API to merge segments, see `tantivy::merge_segments` #1005. (@evanxg852000)

This version breaks compatibility and requires users to reindex everything.

Tantivy 0.13.2
===================

Bugfix. Acquiring a facet reader on a segment that does not contain any
doc with this facet returns `None`. (#896)

Tantivy 0.13.1
===================

Made `Query` and `Collector` `Send + Sync`.
Updated misc dependency versions.

Tantivy 0.13.0
======================

Tantivy 0.13 introduce a change in the index format that will require
you to reindex your index (BlockWAND information are added in the skiplist).
The index size increase is minor as this information is only added for
full blocks.
If you have a massive index for which reindexing is not an option, please contact me
so that we can discuss possible solutions.

- Bugfix in `FuzzyTermQuery` not matching terms by prefix when it should (@Peachball)
- Relaxed constraints on the custom/tweak score functions. At the segment level, they can be mut, and they are not required to be Sync + Send.
- `MMapDirectory::open` does not return a `Result` anymore.
- Change in the DocSet and Scorer API. (@fulmicoton).
A freshly created DocSet point directly to their first doc. A sentinel value called TERMINATED marks the end of a DocSet.
`.advance()` returns the new DocId. `Scorer::skip(target)` has been replaced by `Scorer::seek(target)` and returns the resulting DocId.
As a result, iterating through DocSet now looks as follows

```rust
let mut doc = docset.doc();
while doc != TERMINATED {
   // ...
   doc = docset.advance();
}
```

The change made it possible to greatly simplify a lot of the docset's code.

- Misc internal optimization and introduction of the `Scorer::for_each_pruning` function. (@fulmicoton)
- Added an offset option to the Top(.*)Collectors. (@robyoung)
- Added Block WAND. Performance on TOP-K on term-unions should be greatly increased. (@fulmicoton, and special thanks
to the PISA team for answering all my questions!)

Tantivy 0.12.0
======================

- Removing static dispatch in tokenizers for simplicity. (#762)
- Added backward iteration for `TermDictionary` stream. (@halvorboe)
- Fixed a performance issue when searching for the posting lists of a missing term (@audunhalland)
- Added a configurable maximum number of docs (10M by default) for a segment to be considered for merge (@hntd187, landed by @halvorboe #713)
- Important Bugfix #777, causing tantivy to retain memory mapping. (diagnosed by @poljar)
- Added support for field boosting. (#547, @fulmicoton)

## How to update?

Crates relying on custom tokenizer, or registering tokenizer in the manager will require some
minor changes. Check <https://github.com/quickwit-oss/tantivy/blob/main/examples/custom_tokenizer.rs>
to check for some code sample.

Tantivy 0.11.3
=======================

- Fixed DateTime as a fast field (#735)

Tantivy 0.11.2
=======================

- The future returned by `IndexWriter::merge` does not borrow `self` mutably anymore (#732)
- Exposing a constructor for `WatchHandle` (#731)

Tantivy 0.11.1
=====================

- Bug fix #729

Tantivy 0.11.0
=====================

- Added f64 field. Internally reuse u64 code the same way i64 does (@fdb-hiroshima)
- Various bugfixes in the query parser.
  - Better handling of hyphens in query parser. (#609)
  - Better handling of whitespaces.
- Closes #498 - add support for Elastic-style unbounded range queries for alphanumeric types eg. "title:>hello", "weight:>=70.5", "height:<200" (@petr-tik)
- API change around `Box<BoxableTokenizer>`. See detail in #629
- Avoid rebuilding Regex automaton whenever a regex query is reused. #639 (@brainlock)
- Add footer with some metadata to index files. #605 (@fdb-hiroshima)
- Add a method to check the compatibility of the footer in the index with the running version of tantivy (@petr-tik)
- TopDocs collector: ensure stable sorting on equal score. #671 (@brainlock)
- Added handling of pre-tokenized text fields (#642), which will enable users to
  load tokens created outside tantivy. See usage in examples/pre_tokenized_text. (@kkoziara)
- Fix crash when committing multiple times with deleted documents. #681 (@brainlock)

## How to update?

- The index format is changed. You are required to reindex your data to use tantivy 0.11.
- `Box<dyn BoxableTokenizer>` has been replaced by a `BoxedTokenizer` struct.
- Regex are now compiled when the `RegexQuery` instance is built. As a result, it can now return
an error and handling the `Result` is required.
- `tantivy::version()` now returns a `Version` object. This object implements `ToString()`

Tantivy 0.10.2
=====================

- Closes #656. Solving memory leak.

Tantivy 0.10.1
=====================

- Closes #544.  A few users experienced problems with the directory watching system.
Avoid watching the mmap directory until someone effectively creates a reader that uses
this functionality.

Tantivy 0.10.0
=====================

*Tantivy 0.10.0 index format is compatible with the index format in 0.9.0.*

- Added an API to easily tweak or entirely replace the
 default score. See `TopDocs::tweak_score`and `TopScore::custom_score` (@fulmicoton)
- Added an ASCII folding filter (@drusellers)
- Bugfix in `query.count` in presence of deletes (@fulmicoton)
- Added `.explain(...)` in `Query` and `Weight` to (@fulmicoton)
- Added an efficient way to `delete_all_documents` in `IndexWriter` (@petr-tik).
  All segments are simply removed.

Minor
---------

- Switched to Rust 2018 (@uvd)
- Small simplification of the code.
Calling .freq() or .doc() when .advance() has never been called
on segment postings should panic from now on.
- Tokens exceeding `u16::max_value() - 4` chars are discarded silently instead of panicking.
- Fast fields are now preloaded when the `SegmentReader` is created.
- `IndexMeta` is now public.  (@hntd187)
- `IndexWriter` `add_document`, `delete_term`. `IndexWriter` is `Sync`, making it possible to use it with a `Arc<RwLock<IndexWriter>>`. `add_document` and `delete_term` can
only require a read lock. (@fulmicoton)
- Introducing `Opstamp` as an expressive type alias for `u64`. (@petr-tik)
- Stamper now relies on `AtomicU64` on all platforms (@petr-tik)
- Bugfix - Files get deleted slightly earlier
- Compilation resources improved (@fdb-hiroshima)

## How to update?

Your program should be usable as is.

### Fast fields

Fast fields used to be accessed directly from the `SegmentReader`.
The API changed, you are now required to acquire your fast field reader via the
`segment_reader.fast_fields()`, and use one of the typed method:

- `.u64()`, `.i64()` if your field is single-valued ;
- `.u64s()`, `.i64s()` if your field is multi-valued ;
- `.bytes()` if your field is bytes fast field.

Tantivy 0.9.0
=====================

*0.9.0 index format is not compatible with the
previous index format.*

- MAJOR BUGFIX :
  Some `Mmap` objects were being leaked, and would never get released. (@fulmicoton)
- Removed most unsafe (@fulmicoton)
- Indexer memory footprint improved. (VInt comp, inlining the first block. (@fulmicoton)
- Stemming in other language possible (@pentlander)
- Segments with no docs are deleted earlier (@barrotsteindev)
- Added grouped add and delete operations.
  They are guaranteed to happen together (i.e. they cannot be split by a commit).
  In addition, adds are guaranteed to happen on the same segment. (@elbow-jason)
- Removed `INT_STORED` and `INT_INDEXED`. It is now possible to use `STORED` and `INDEXED`
  for int fields. (@fulmicoton)
- Added DateTime field (@barrotsteindev)
- Added IndexReader. By default, index is reloaded automatically upon new commits (@fulmicoton)
- SIMD linear search within blocks (@fulmicoton)

## How to update ?

tantivy 0.9 brought some API breaking change.
To update from tantivy 0.8, you will need to go through the following steps.

- `schema::INT_INDEXED` and `schema::INT_STORED`  should be replaced by `schema::INDEXED` and `schema::INT_STORED`.
- The index now does not hold the pool of searcher anymore. You are required to create an intermediary object called
`IndexReader` for this.

    ```rust
    // create the reader. You typically need to create 1 reader for the entire
    // lifetime of you program.
    let reader = index.reader()?;

    // Acquire a searcher (previously `index.searcher()`) is now written:
    let searcher = reader.searcher();

    // With the default setting of the reader, you are not required to
    // call `index.load_searchers()` anymore.
    //
    // The IndexReader will pick up that change automatically, regardless
    // of whether the update was done in a different process or not.
    // If this behavior is not wanted, you can create your reader with
    // the `ReloadPolicy::Manual`, and manually decide when to reload the index
    // by calling `reader.reload()?`.

    ```

Tantivy 0.8.2
=====================

Fixing build for x86_64 platforms. (#496)
No need to update from 0.8.1 if tantivy
is building on your platform.

Tantivy 0.8.1
=====================

Hotfix of #476.

Merge was reflecting deletes before commit was passed.
Thanks @barrotsteindev  for reporting the bug.

Tantivy 0.8.0
=====================

*No change in the index format*

- API Breaking change in the collector API. (@jwolfe, @fulmicoton)
- Multithreaded search (@jwolfe, @fulmicoton)

Tantivy 0.7.1
=====================

*No change in the index format*

- Bugfix: NGramTokenizer panics on non ascii chars
- Added a space usage API

Tantivy 0.7
=====================

- Skip data for doc ids and positions (@fulmicoton),
  greatly improving performance
- Tantivy error now rely on the failure crate (@drusellers)
- Added support for `AND`, `OR`, `NOT` syntax in addition to the `+`,`-` syntax
- Added a snippet generator with highlight (@vigneshsarma, @fulmicoton)
- Added a `TopFieldCollector` (@pentlander)

Tantivy 0.6.1
=========================

- Bugfix #324. GC removing was removing file that were still in useful
- Added support for parsing AllQuery and RangeQuery via QueryParser
  - AllQuery: `*`
  - RangeQuery:
    - Inclusive `field:[startIncl to endIncl]`
    - Exclusive `field:{startExcl to endExcl}`
    - Mixed `field:[startIncl to endExcl}` and vice versa
    - Unbounded `field:[start to *]`, `field:[* to end]`

Tantivy 0.6
==========================

Special thanks to @drusellers and @jason-wolfe for their contributions
to this release!

- Removed C code. Tantivy is now pure Rust. (@fulmicoton)
- BM25 (@fulmicoton)
- Approximate field norms encoded over 1 byte. (@fulmicoton)
- Compiles on stable rust (@fulmicoton)
- Add &[u8] fastfield for associating arbitrary bytes to each document (@jason-wolfe) (#270)
  - Completely uncompressed
  - Internally: One u64 fast field for indexes, one fast field for the bytes themselves.
- Add NGram token support (@drusellers)
- Add Stopword Filter support (@drusellers)
- Add a FuzzyTermQuery (@drusellers)
- Add a RegexQuery (@drusellers)
- Various performance improvements (@fulmicoton)_

Tantivy 0.5.2
===========================

- bugfix #274
- bugfix #280
- bugfix #289

Tantivy 0.5.1
==========================

- bugfix #254 : tantivy failed if no documents in a segment contained a specific field.

Tantivy 0.5
==========================

- Faceting
- RangeQuery
- Configurable tokenization pipeline
- Bugfix in PhraseQuery
- Various query optimisation
- Allowing very large indexes
  - 64 bits file address
  - Smarter encoding of the `TermInfo` objects

Tantivy 0.4.3
==========================

- Bugfix race condition when deleting files. (#198)

Tantivy 0.4.2
==========================

- Prevent usage of AVX2 instructions (#201)

Tantivy 0.4.1
==========================

- Bugfix for non-indexed fields. (#199)

Tantivy 0.4.0
==========================

- Raise the limit of number of fields (previously 256 fields) (@fulmicoton)
- Removed u32 fields. They are replaced by u64 and i64 fields (#65) (@fulmicoton)
- Optimized skip in SegmentPostings (#130) (@lnicola)
- Replacing rustc_serialize by serde. Kudos to @KodrAus and @lnicola
- Using error-chain (@KodrAus)
- QueryParser: (@fulmicoton)
  - Explicit error returned when searched for a term that is not indexed
  - Searching for a int term via the query parser was broken `(age:1)`
  - Searching for a non-indexed field returns an explicit Error
  - Phrase query for non-tokenized field are not tokenized by the query parser.
- Faster/Better indexing (@fulmicoton)
  - using murmurhash2
  - faster merging
  - more memory efficient fast field writer (@lnicola )
  - better handling of collisions
  - lesser memory usage
- Added API, most notably to iterate over ranges of terms (@fulmicoton)
- Bugfix that was preventing to unmap segment files, on index drop (@fulmicoton)
- Made the doc! macro public (@fulmicoton)
- Added an alternative implementation of the streaming dictionary (@fulmicoton)

Tantivy 0.3.1
==========================

- Expose a method to trigger files garbage collection

Tantivy 0.3
==========================

Special thanks to @Kodraus @lnicola @Ameobea @manuel-woelker @celaus
for their contribution to this release.

Thanks also to everyone in tantivy gitter chat
for their advise and company :)

<https://gitter.im/tantivy-search/tantivy>

Warning:

Tantivy 0.3 is NOT backward compatible with tantivy 0.2
code and index format.
You should not expect backward compatibility before
tantivy 1.0.

New Features
------------

- Delete. You can now delete documents from an index.
- Support for windows (Thanks to @lnicola)

Various Bugfixes & small improvements
----------------------------------------

- Added CI for Windows (<https://ci.appveyor.com/project/fulmicoton/tantivy>)
Thanks to @KodrAus ! (#108)
- Various dependy version update (Thanks to @Ameobea) #76
- Fixed several race conditions in `Index.wait_merge_threads`
- Fixed #72. Mmap were never released.
- Fixed #80. Fast field used to take an amplitude of 32 bits after a merge. (Ouch!)
- Fixed #92. u32 are now encoded using big endian in the fst
  in order to make there enumeration consistent with
  the natural ordering.
- Building binary targets for tantivy-cli (Thanks to @KodrAus)
- Misc invisible bug fixes, and code cleanup.
- Use
