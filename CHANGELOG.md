Tantivy 0.12.1
=====================
- By default IndexReader are in `Manual` mode.

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
minor changes. Check https://github.com/tantivy-search/tantivy/blob/master/examples/custom_tokenizer.rs
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
 default score. See `TopDocs::tweak_score`and `TopScore::custom_score` (@pmasurel)
- Added an ASCII folding filter (@drusellers)
- Bugfix in `query.count` in presence of deletes (@pmasurel)
- Added `.explain(...)` in `Query` and `Weight` to (@pmasurel)
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
- `IndexWriter` `add_document`, `delete_term`. `IndexWriter` is `Sync`, making it possible to use it with a `
Arc<RwLock<IndexWriter>>`. `add_document` and `delete_term` can 
only require a read lock. (@pmasurel)
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

- Removed C code. Tantivy is now pure Rust. (@pmasurel)
- BM25 (@pmasurel)
- Approximate field norms encoded over 1 byte. (@pmasurel)
- Compiles on stable rust (@pmasurel)
- Add &[u8] fastfield for associating arbitrary bytes to each document (@jason-wolfe) (#270)
    - Completely uncompressed
    - Internally: One u64 fast field for indexes, one fast field for the bytes themselves.
- Add NGram token support (@drusellers)
- Add Stopword Filter support (@drusellers)
- Add a FuzzyTermQuery (@drusellers)
- Add a RegexQuery (@drusellers)
- Various performance improvements (@pmasurel)_


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

https://gitter.im/tantivy-search/tantivy


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

- Added CI for Windows (https://ci.appveyor.com/project/fulmicoton/tantivy)
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




