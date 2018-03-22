Tantivy 0.5.2
==========================

- Removed C code. Tantivy is now pure Rust.
- BM25
- Approximate field norms encoded over 1 byte.

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




