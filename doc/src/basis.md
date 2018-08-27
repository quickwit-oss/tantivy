# Anatomy of an index

## Straight from disk

By default, tantivy accesses its data using its `MMapDirectory`. 
While this design has some downsides, this greatly simplifies the source code of tantivy, 
and entirely delegates the caching to the OS.

`tantivy` works entirely (or almost) by directly reading the datastructures as they are layed on disk.
As a result, the act of opening an indexing does not involve loading different datastructures 
from the disk into random access memory : starting a process, opening an index, and performing a query 
can typically be done in a matter of milliseconds. 

This is an interesting property for a command line search engine, or for some multi-tenant log search engine.
Spawning a new process for each new query can be a perfectly sensible solution in some use case.

In later chapters, we will discuss tantivy's inverted index data layout.
One key take away is that to achieve great performance, search indexes are extremely compact. 
Of course this is crucial to reduce IO, and ensure that as much of our index can sit in RAM.

Also, whenever possible the data is accessed sequentially. Of course, this is an amazing property when tantivy needs to access
the data from your spinning hard disk, but this is also a great property when working with `SSD` or `RAM`, 
as it makes our read patterns very predictable for the CPU.


## Segments, and the log method

That kind compact layout comes at one cost: it prevents our datastructures from being dynamic.
In fact, a trait called `Directory` is in charge of abstracting all of tantivy's data access
and its API does not even allow editing these file once they are written.

To allow the addition / deletion of documents, and create the illusion that
your index is dynamic (i.e.: adding and deleting documents), tantivy uses a common database trick sometimes
referred to as the *log method*.

Let's forget about deletes for a moment. As you add documents, these documents are processed and stored in 
a dedicated datastructure, in a `RAM` buffer. This datastructure is designed to be dynamic but 
cannot be accessed for search. As you add documents, this buffer will reach its capacity and tantivy will
transparently stop adding document to it and start converting this datastructure to its final
read-only format on disk. Once written, an brand empty buffer is available to resume adding documents. 

The resulting chunk of index obtained after this serialization is called a `Segment`.

> A segment is a self-contained atomic piece of index. It is identified with a UUID, and all of its files
are identified using the naming scheme : `<UUID>.*`.


> A tantivy `Index` is a collection of `Segments`.