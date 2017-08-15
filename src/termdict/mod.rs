/*!
The term dictionary is one of the key datastructure of
tantivy. It associates sorted `terms` to their respective
posting list.

The term dictionary makes it possible to iterate through
the keys in a sorted manner.

# Example

```
extern crate tantivy;
use tantivy::termdict::*;
use tantivy::directory::ReadOnlySource;

# fn main() {
#    run().expect("Test failed");
# }
# fn run() -> tantivy::Result<()> {
let mut term_dictionary_builder = TermDictionaryBuilderImpl::new(vec!())?;

// keys have to be insert in order.
term_dictionary_builder.insert("apple", &1u32)?;
term_dictionary_builder.insert("grape", &2u32)?;
term_dictionary_builder.insert("pear", &3u32)?;
let buffer: Vec<u8> = term_dictionary_builder.finish()?;

let source = ReadOnlySource::from(buffer);
let term_dictionary = TermDictionaryImpl::from_source(source)?;

assert_eq!(term_dictionary.get("grape"), Some(2u32));
# Ok(())
# }
```


# Implementations

There is currently two implementations of the term dictionary.

## Default implementation : `fstdict`

The default one relies heavily on the `fst` crate.
It associate each terms `&[u8]` representation to a `u64`
that is in fact an address in a buffer. The value is then accessible
via deserializing the value at this address.


## Stream implementation : `streamdict`

The `fstdict` is a tiny bit slow when streaming all of
the terms.
For some use case (analytics engine), it is preferrable
to use the `streamdict`, that offers better streaming
performance, to the detriment of `lookup` performance.

`streamdict` can be enabled by adding the `streamdict`
feature when compiling `tantivy`.

`streamdict` encodes each term relatively to the precedent
as follows.

- number of bytes that needs to be popped.
- number of bytes that needs to be added.
- sequence of bytes that is to be added
- value.

Because such a structure does not allow for lookups,
it comes with a `fst` that indexes 1 out of `1024`
terms in this structure.

A `lookup` therefore consists in a lookup in the `fst`
followed by a streaming through at most `1024` elements in the
term `stream`.
*/

use schema::{Field, Term};
use common::BinarySerializable;
use directory::ReadOnlySource;


pub use self::merger::TermMerger;


#[cfg(not(feature="streamdict"))]
mod fstdict;
#[cfg(not(feature="streamdict"))]
pub use self::fstdict::{TermDictionaryImpl, TermDictionaryBuilderImpl, TermStreamerImpl,
                        TermStreamerBuilderImpl};


#[cfg(feature="streamdict")]
mod streamdict;
#[cfg(feature="streamdict")]
pub use self::streamdict::{TermDictionaryImpl, TermDictionaryBuilderImpl, TermStreamerImpl,
                           TermStreamerBuilderImpl};

mod merger;
use std::io;


/// Dictionary associating sorted `&[u8]` to values
pub trait TermDictionary<'a, V>
    where V: BinarySerializable + Default + 'a,
          Self: Sized
{
    /// Streamer type associated to the term dictionary
    type Streamer: TermStreamer<V> + 'a;

    /// StreamerBuilder type associated to the term dictionary
    type StreamBuilder: TermStreamerBuilder<V, Streamer = Self::Streamer> + 'a;

    /// Opens a `TermDictionary` given a data source.
    fn from_source(source: ReadOnlySource) -> io::Result<Self>;

    /// Lookups the value corresponding to the key.
    fn get<K: AsRef<[u8]>>(&self, target_key: K) -> Option<V>;

    /// Returns a range builder, to stream all of the terms
    /// within an interval.
    fn range(&'a self) -> Self::StreamBuilder;

    /// A stream of all the sorted terms. [See also `.stream_field()`](#method.stream_field)
    fn stream(&'a self) -> Self::Streamer {
        self.range().into_stream()
    }

    /// A stream of all the sorted terms in the given field.
    fn stream_field(&'a self, field: Field) -> Self::Streamer {
        let start_term = Term::from_field_text(field, "");
        let stop_term = Term::from_field_text(Field(field.0 + 1), "");
        self.range()
            .ge(start_term.as_slice())
            .lt(stop_term.as_slice())
            .into_stream()
    }
}

/// Builder for the new term dictionary.
///
/// Inserting must be done in the order of the `keys`.
pub trait TermDictionaryBuilder<W, V>: Sized
    where W: io::Write,
          V: BinarySerializable + Default
{
    /// Creates a new `TermDictionaryBuilder`
    fn new(write: W) -> io::Result<Self>;

    /// Inserts a `(key, value)` pair in the term dictionary.
    ///
    /// *Keys have to be inserted in order.*
    fn insert<K: AsRef<[u8]>>(&mut self, key: K, value: &V) -> io::Result<()>;

    /// Finalize writing the builder, and returns the underlying
    /// `Write` object.
    fn finish(self) -> io::Result<W>;
}


/// `TermStreamer` acts as a cursor over a range of terms of a segment.
/// Terms are guaranteed to be sorted.
pub trait TermStreamer<V>: Sized {
    /// Advance position the stream on the next item.
    /// Before the first call to `.advance()`, the stream
    /// is an unitialized state.
    fn advance(&mut self) -> bool;

    /// Accesses the current key.
    ///
    /// `.key()` should return the key that was returned
    /// by the `.next()` method.
    ///
    /// If the end of the stream as been reached, and `.next()`
    /// has been called and returned `None`, `.key()` remains
    /// the value of the last key encountered.
    ///
    /// Before any call to `.next()`, `.key()` returns an empty array.
    fn key(&self) -> &[u8];

    /// Accesses the current value.
    ///
    /// Calling `.value()` after the end of the stream will return the
    /// last `.value()` encountered.
    ///
    /// # Panics
    ///
    /// Calling `.value()` before the first call to `.advance()` returns
    /// `V::default()`.
    fn value(&self) -> &V;

    /// Return the next `(key, value)` pair.
    fn next(&mut self) -> Option<(Term<&[u8]>, &V)> {
        if self.advance() {
            Some((Term::wrap(self.key()), self.value()))
        } else {
            None
        }
    }
}


/// `TermStreamerBuilder` is an helper object used to define
/// a range of terms that should be streamed.
pub trait TermStreamerBuilder<V>
    where V: BinarySerializable + Default
{
    /// Associated `TermStreamer` type that this builder is building.
    type Streamer: TermStreamer<V>;

    /// Limit the range to terms greater or equal to the bound
    fn ge<T: AsRef<[u8]>>(self, bound: T) -> Self;

    /// Limit the range to terms strictly greater than the bound
    fn gt<T: AsRef<[u8]>>(self, bound: T) -> Self;

    /// Limit the range to terms lesser or equal to the bound
    fn lt<T: AsRef<[u8]>>(self, bound: T) -> Self;

    /// Limit the range to terms lesser or equal to the bound
    fn le<T: AsRef<[u8]>>(self, bound: T) -> Self;

    /// Creates the stream corresponding to the range
    /// of terms defined using the `TermStreamerBuilder`.
    fn into_stream(self) -> Self::Streamer;
}


#[cfg(test)]
mod tests {
    use super::{TermDictionaryImpl, TermDictionaryBuilderImpl, TermStreamerImpl};
    use directory::{RAMDirectory, Directory, ReadOnlySource};
    use std::path::PathBuf;
    use schema::{Term, SchemaBuilder, Document, TEXT};
    use core::Index;
    use std::str;
    use termdict::TermStreamer;
    use termdict::TermStreamerBuilder;
    use termdict::TermDictionary;
    use termdict::TermDictionaryBuilder;
    const BLOCK_SIZE: usize = 1_500;


    #[test]
    fn test_term_dictionary() {
        let mut directory = RAMDirectory::create();
        let path = PathBuf::from("TermDictionary");
        {
            let write = directory.open_write(&path).unwrap();
            let mut term_dictionary_builder = TermDictionaryBuilderImpl::new(write).unwrap();
            term_dictionary_builder
                .insert("abc".as_bytes(), &34u32)
                .unwrap();
            term_dictionary_builder
                .insert("abcd".as_bytes(), &346u32)
                .unwrap();
            term_dictionary_builder.finish().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        let term_dict: TermDictionaryImpl<u32> = TermDictionaryImpl::from_source(source).unwrap();
        assert_eq!(term_dict.get("abc"), Some(34u32));
        assert_eq!(term_dict.get("abcd"), Some(346u32));
        let mut stream = term_dict.stream();
        {
            {
                let (k, v) = stream.next().unwrap();
                assert_eq!(k.as_ref(), "abc".as_bytes());
                assert_eq!(v, &34u32);
            }
            assert_eq!(stream.key(), "abc".as_bytes());
            assert_eq!(*stream.value(), 34u32);
        }
        {
            {
                let (k, v) = stream.next().unwrap();
                assert_eq!(k.as_slice(), "abcd".as_bytes());
                assert_eq!(v, &346u32);
            }
            assert_eq!(stream.key(), "abcd".as_bytes());
            assert_eq!(*stream.value(), 346u32);
        }
        assert!(!stream.advance());
    }

    #[test]
    fn test_term_iterator() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let index = Index::create_in_ram(schema_builder.build());
        {
            let mut index_writer = index.writer_with_num_threads(1, 40_000_000).unwrap();
            {
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "a b d f");
                    index_writer.add_document(doc);
                }
                index_writer.commit().unwrap();
            }
            {
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "a b c d f");
                    index_writer.add_document(doc);
                }
                index_writer.commit().unwrap();
            }
            {
                {
                    let mut doc = Document::default();
                    doc.add_text(text_field, "e f");
                    index_writer.add_document(doc);
                }
                index_writer.commit().unwrap();
            }
        }
        index.load_searchers().unwrap();
        let searcher = index.searcher();

        let field_searcher = searcher.field(text_field).unwrap();
        let mut term_it = field_searcher.terms();
        let mut term_string = String::new();
        while term_it.advance() {
            let term = Term::from_bytes(term_it.key());
            term_string.push_str(term.text());
        }
        assert_eq!(&*term_string, "abcdef");
    }


    #[test]
    fn test_term_dictionary_stream() {
        let ids: Vec<_> = (0u32..10_000u32)
            .map(|i| (format!("doc{:0>6}", i), i))
            .collect();
        let buffer: Vec<u8> = {
            let mut term_dictionary_builder = TermDictionaryBuilderImpl::new(vec![]).unwrap();
            for &(ref id, ref i) in &ids {
                term_dictionary_builder.insert(id.as_bytes(), i).unwrap();
            }
            term_dictionary_builder.finish().unwrap()
        };
        let source = ReadOnlySource::from(buffer);
        let term_dictionary: TermDictionaryImpl<u32> = TermDictionaryImpl::from_source(source)
            .unwrap();
        {
            let mut streamer = term_dictionary.stream();
            let mut i = 0;
            while let Some((streamer_k, streamer_v)) = streamer.next() {
                let &(ref key, ref v) = &ids[i];
                assert_eq!(streamer_k.as_ref(), key.as_bytes());
                assert_eq!(streamer_v, v);
                i += 1;
            }
        }

        let &(ref key, ref _v) = &ids[2047];
        term_dictionary.get(key.as_bytes());
    }

    #[test]
    fn test_stream_range() {
        let ids: Vec<_> = (0u32..50_000u32)
            .map(|i| (format!("doc{:0>6}", i), i))
            .collect();
        let buffer: Vec<u8> = {
            let mut term_dictionary_builder = TermDictionaryBuilderImpl::new(vec![]).unwrap();
            for &(ref id, ref i) in &ids {
                term_dictionary_builder.insert(id.as_bytes(), i).unwrap();
            }
            term_dictionary_builder.finish().unwrap()
        };

        let source = ReadOnlySource::from(buffer);

        let term_dictionary: TermDictionaryImpl<u32> = TermDictionaryImpl::from_source(source)
            .unwrap();
        {
            for i in (0..20).chain(6000..8_000) {
                let &(ref target_key, _) = &ids[i];
                let mut streamer = term_dictionary
                    .range()
                    .ge(target_key.as_bytes())
                    .into_stream();
                for j in 0..3 {
                    let (streamer_k, streamer_v) = streamer.next().unwrap();
                    let &(ref key, ref v) = &ids[i + j];
                    assert_eq!(str::from_utf8(streamer_k.as_ref()).unwrap(), key);
                    assert_eq!(streamer_v, v);
                }
            }
        }

        {
            for i in (0..20).chain((BLOCK_SIZE - 10..BLOCK_SIZE + 10)) {
                let &(ref target_key, _) = &ids[i];
                let mut streamer = term_dictionary
                    .range()
                    .gt(target_key.as_bytes())
                    .into_stream();
                for j in 0..3 {
                    let (streamer_k, streamer_v) = streamer.next().unwrap();
                    let &(ref key, ref v) = &ids[i + j + 1];
                    assert_eq!(streamer_k.as_ref(), key.as_bytes());
                    assert_eq!(streamer_v, v);
                }
            }
        }

        {
            for i in (0..20).chain((BLOCK_SIZE - 10..BLOCK_SIZE + 10)) {
                for j in 0..3 {
                    let &(ref fst_key, _) = &ids[i];
                    let &(ref last_key, _) = &ids[i + j];
                    let mut streamer = term_dictionary
                        .range()
                        .ge(fst_key.as_bytes())
                        .lt(last_key.as_bytes())
                        .into_stream();
                    for _ in 0..j {
                        assert!(streamer.next().is_some());
                    }
                    assert!(streamer.next().is_none());
                }
            }
        }
    }


    #[test]
    fn test_stream_range_boundaries() {
        let buffer: Vec<u8> = {
            let mut term_dictionary_builder = TermDictionaryBuilderImpl::new(vec![]).unwrap();
            for i in 0u8..10u8 {
                let number_arr = [i; 1];
                term_dictionary_builder.insert(&number_arr, &i).unwrap();
            }
            term_dictionary_builder.finish().unwrap()
        };
        let source = ReadOnlySource::from(buffer);
        let term_dictionary: TermDictionaryImpl<u8> = TermDictionaryImpl::from_source(source)
            .unwrap();

        let value_list = |mut streamer: TermStreamerImpl<u8>| {
            let mut res: Vec<u8> = vec![];
            while let Some((_, &v)) = streamer.next() {
                res.push(v);
            }
            res
        };
        {
            let range = term_dictionary.range().ge([2u8]).into_stream();
            assert_eq!(value_list(range),
                       vec![2u8, 3u8, 4u8, 5u8, 6u8, 7u8, 8u8, 9u8]);
        }
        {
            let range = term_dictionary.range().gt([2u8]).into_stream();
            assert_eq!(value_list(range), vec![3u8, 4u8, 5u8, 6u8, 7u8, 8u8, 9u8]);
        }
        {
            let range = term_dictionary.range().lt([6u8]).into_stream();
            assert_eq!(value_list(range), vec![0u8, 1u8, 2u8, 3u8, 4u8, 5u8]);
        }
        {
            let range = term_dictionary.range().le([6u8]).into_stream();
            assert_eq!(value_list(range), vec![0u8, 1u8, 2u8, 3u8, 4u8, 5u8, 6u8]);
        }
        {
            let range = term_dictionary.range().ge([0u8]).lt([5u8]).into_stream();
            assert_eq!(value_list(range), vec![0u8, 1u8, 2u8, 3u8, 4u8]);
        }
    }

}
