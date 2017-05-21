/*!
The term dictionary contains all of the terms in
`tantivy index` in a sorted manner.

It is implemented as a wrapper of the `Fst` crate in order
to add a value type.

A finite state transducer itself associates
each term `&[u8]` to a `u64` that is in fact an address
in a buffer. The value is then accessible via
deserializing the value at this address.

Keys (`&[u8]`) in this datastructure are sorted.
*/

#[cfg(not(feature="streamdict"))]
mod fstdict;
#[cfg(not(feature="streamdict"))]
pub use self::fstdict::*;


#[cfg(feature="streamdict")]
mod streamdict;
#[cfg(feature="streamdict")]
pub use self::termdict::*;
