/*!
The term dictionary contains all of the terms in
`tantivy index` in a sorted manner.

`fstdict` is the default implementation
of the term dictionary. It is implemented as a wrapper
of the `Fst` crate in order to add a value type.

A finite state transducer itself associates
each term `&[u8]` to a `u64` that is in fact an address
in a buffer. The value is then accessible via
deserializing the value at this address.

Keys (`&[u8]`) in this datastructure are sorted.
*/

mod termdict;
mod streamer;
mod term_info_store;

pub use self::termdict::TermDictionaryImpl;
pub use self::termdict::TermDictionaryBuilderImpl;
pub use self::term_info_store::TermInfoStoreWriter;
pub use self::streamer::TermStreamerImpl;
pub use self::streamer::TermStreamerBuilderImpl;
