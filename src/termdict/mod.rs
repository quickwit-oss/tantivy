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
pub use self::fstdict::{TermDictionary, TermDictionaryBuilder, TermStreamer, TermStreamerBuilder};

#[cfg(feature="streamdict")]
mod streamdict;
#[cfg(feature="streamdict")]
pub use self::termdict::{TermDictionary, TermDictionaryBuilder, TermStreamer, TermStreamerBuilder};

mod merger;
pub use self::merger::TermMerger;


#[cfg(test)]
mod tests {
    use super::{TermDictionary, TermDictionaryBuilder};
    use directory::{RAMDirectory, Directory, ReadOnlySource};
    use std::path::PathBuf;
    use fst::Streamer;
    use schema::{Term, SchemaBuilder, Document, TEXT};
    use core::Index;
    use std::str;
    const BLOCK_SIZE: usize = 1_500;



    #[test]
    fn test_term_dictionary() {
        let mut directory = RAMDirectory::create();
        let path = PathBuf::from("TermDictionary");
        {
            let write = directory.open_write(&path).unwrap();
            let mut term_dictionary_builder = TermDictionaryBuilder::new(write).unwrap();
            term_dictionary_builder
                .insert("abc".as_bytes(), &34u32)
                .unwrap();
            term_dictionary_builder
                .insert("abcd".as_bytes(), &346u32)
                .unwrap();
            term_dictionary_builder.finish().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        let term_dict: TermDictionary<u32> = TermDictionary::from_source(source).unwrap();
        assert_eq!(term_dict.get("abc"), Some(34u32));
        assert_eq!(term_dict.get("abcd"), Some(346u32));
        let mut stream = term_dict.stream();
        assert_eq!(stream.next().unwrap(), ("abc".as_bytes(), 34u32));
        assert_eq!(stream.key(), "abc".as_bytes());
        assert_eq!(stream.value(), 34u32);
        assert_eq!(stream.next().unwrap(), ("abcd".as_bytes(), 346u32));
        assert_eq!(stream.key(), "abcd".as_bytes());
        assert_eq!(stream.value(), 346u32);
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
        let mut term_it = searcher.terms();
        let mut term_string = String::new();
        while term_it.advance() {
            let term = Term::from_bytes(term_it.key());
            term_string.push_str(term.text());
        }
        assert_eq!(&*term_string, "abcdef");
    }


    #[test]
    fn test_stream_dictionary() {
        let ids: Vec<_> = (0u32..10_000u32)
            .map(|i| (format!("doc{:0>6}", i), i))
            .collect();
        let buffer: Vec<u8> = {
            let mut stream_dictionary_builder = TermDictionaryBuilder::new(vec!()).unwrap();
            for &(ref id, ref i) in &ids {
                stream_dictionary_builder.insert(id.as_bytes(), i).unwrap();
            }
            stream_dictionary_builder.finish().unwrap()
        };
        let source = ReadOnlySource::from(buffer);
        let stream_dictionary: TermDictionary<u32> = TermDictionary::from_source(source).unwrap();
        {
            let mut streamer = stream_dictionary.stream();
            let mut i = 0;
            while let Some((streamer_k, streamer_v)) = streamer.next() {
                let &(ref key, ref v) = &ids[i];
                assert_eq!(streamer_k, key.as_bytes());
                assert_eq!(streamer_v, *v);
                i += 1;
            }
        }
        
        let &(ref key, ref _v) = &ids[2047];
        stream_dictionary.get(key.as_bytes());
    }

    #[test]
    fn test_stream_range() {
        let ids: Vec<_> = (0u32..10_000u32)
            .map(|i| (format!("doc{:0>6}", i), i))
            .collect();
        let buffer: Vec<u8> = {
            let mut stream_dictionary_builder = TermDictionaryBuilder::new(vec!()).unwrap();
            for &(ref id, ref i) in &ids {
                stream_dictionary_builder.insert(id.as_bytes(), i).unwrap();
            }
            stream_dictionary_builder.finish().unwrap()
        };

        println!("a");
        let source = ReadOnlySource::from(buffer);
        
        let stream_dictionary: TermDictionary<u32> = TermDictionary::from_source(source).unwrap();
        {
            for i in (0..20).chain(2_000 - 10..2_000 + 10) {
                println!("i {}", i);
                let &(ref target_key, _) = &ids[i];
                let mut streamer = stream_dictionary
                    .range()
                    .ge(target_key.as_bytes())
                    .into_stream();
                for j in 0..3 {
                    let (streamer_k, streamer_v) = streamer.next().unwrap();
                    let &(ref key, ref v) = &ids[i + j];
                    assert_eq!(str::from_utf8(streamer_k).unwrap(), key);
                    assert_eq!(streamer_v, *v);
                }
            }
        }

        {
            for i in (0..20).chain((BLOCK_SIZE - 10..BLOCK_SIZE + 10)) {
                let &(ref target_key, _) = &ids[i];
                let mut streamer = stream_dictionary
                    .range()
                    .gt(target_key.as_bytes())
                    .into_stream();
                for j in 0..3 {
                    let (streamer_k, streamer_v) = streamer.next().unwrap();
                    let &(ref key, ref v) = &ids[i + j + 1];
                    assert_eq!(streamer_k, key.as_bytes());
                    assert_eq!(streamer_v, *v);
                }
            }
        }

        {
            for i in (0..20).chain((BLOCK_SIZE - 10..BLOCK_SIZE + 10)) {
                println!("i2:{}", i);
                for j in 0..3 {
                    println!("j2:{}", j);
                    let &(ref fst_key, _) = &ids[i];
                    let &(ref last_key, _) = &ids[i + 3];
                    let mut streamer = stream_dictionary
                        .range()
                        .ge(fst_key.as_bytes())
                        .lt(last_key.as_bytes())
                        .into_stream();
                    for _ in 0..j {
                        assert!(streamer.next().is_some());
                    }
                    assert!(streamer.next().is_some());
                }
            }
        }
        
    }

}
