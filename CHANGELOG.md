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




