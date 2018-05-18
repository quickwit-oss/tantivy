/*!
The term dictionary main role is to associate the sorted [`Term`s](../struct.Term.html) to
a [`TermInfo`](../postings/struct.TermInfo.html) struct that contains some meta-information
about the term.

Internally, the term dictionary relies on the `fst` crate to store
a sorted mapping that associate each term to its rank in the lexicographical order.
For instance, in a dictionary containing the sorted terms "abba", "bjork", "blur" and "donovan",
the `TermOrdinal` are respectively `0`, `1`, `2`, and `3`.

For `u64`-terms, tantivy explicitely uses a `BigEndian` representation to ensure that the
lexicographical order matches the natural order of integers.

`i64`-terms are transformed to `u64` using a continuous mapping `val ⟶ val - i64::min_value()`
and then treated as a `u64`.

A second datastructure makes it possible to access a [`TermInfo`](../postings/struct.TermInfo.html).
*/

/// Position of the term in the sorted list of terms.
pub type TermOrdinal = u64;

mod merger;
mod streamer;
mod term_info_store;
mod termdict;

pub use self::merger::TermMerger;
pub use self::streamer::{TermStreamer, TermStreamerBuilder};
pub use self::termdict::{TermDictionary, TermDictionaryBuilder};

#[cfg(test)]
mod tests {
    use super::{TermDictionary, TermDictionaryBuilder, TermStreamer};
    use core::Index;
    use directory::{Directory, RAMDirectory, ReadOnlySource};
    use postings::TermInfo;
    use schema::{Document, FieldType, SchemaBuilder, TEXT};
    use std::path::PathBuf;
    use std::str;

    const BLOCK_SIZE: usize = 1_500;

    fn make_term_info(val: u64) -> TermInfo {
        TermInfo {
            doc_freq: val as u32,
            positions_offset: val * 2u64,
            postings_offset: val * 3u64,
            positions_inner_offset: 5u8,
        }
    }

    #[test]
    fn test_term_ordinals() {
        const COUNTRIES: [&'static str; 7] = [
            "San Marino",
            "Serbia",
            "Slovakia",
            "Slovenia",
            "Spain",
            "Sweden",
            "Switzerland",
        ];
        let mut directory = RAMDirectory::create();
        let path = PathBuf::from("TermDictionary");
        {
            let write = directory.open_write(&path).unwrap();
            let field_type = FieldType::Str(TEXT);
            let mut term_dictionary_builder =
                TermDictionaryBuilder::new(write, field_type).unwrap();
            for term in COUNTRIES.iter() {
                term_dictionary_builder
                    .insert(term.as_bytes(), &make_term_info(0u64))
                    .unwrap();
            }
            term_dictionary_builder.finish().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        let term_dict: TermDictionary = TermDictionary::from_source(source);
        for (term_ord, term) in COUNTRIES.iter().enumerate() {
            assert_eq!(term_dict.term_ord(term).unwrap(), term_ord as u64);
            let mut bytes = vec![];
            assert!(term_dict.ord_to_term(term_ord as u64, &mut bytes));
            assert_eq!(bytes, term.as_bytes());
        }
    }

    #[test]
    fn test_term_dictionary_simple() {
        let mut directory = RAMDirectory::create();
        let path = PathBuf::from("TermDictionary");
        {
            let write = directory.open_write(&path).unwrap();
            let field_type = FieldType::Str(TEXT);
            let mut term_dictionary_builder =
                TermDictionaryBuilder::new(write, field_type).unwrap();
            term_dictionary_builder
                .insert("abc".as_bytes(), &make_term_info(34u64))
                .unwrap();
            term_dictionary_builder
                .insert("abcd".as_bytes(), &make_term_info(346u64))
                .unwrap();
            term_dictionary_builder.finish().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        let term_dict: TermDictionary = TermDictionary::from_source(source);
        assert_eq!(term_dict.get("abc").unwrap().doc_freq, 34u32);
        assert_eq!(term_dict.get("abcd").unwrap().doc_freq, 346u32);
        let mut stream = term_dict.stream();
        {
            {
                let (k, v) = stream.next().unwrap();
                assert_eq!(k.as_ref(), "abc".as_bytes());
                assert_eq!(v.doc_freq, 34u32);
            }
            assert_eq!(stream.key(), "abc".as_bytes());
            assert_eq!(stream.value().doc_freq, 34u32);
        }
        {
            {
                let (k, v) = stream.next().unwrap();
                assert_eq!(k, "abcd".as_bytes());
                assert_eq!(v.doc_freq, 346u32);
            }
            assert_eq!(stream.key(), "abcd".as_bytes());
            assert_eq!(stream.value().doc_freq, 346u32);
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

        let field_searcher = searcher.field(text_field);
        let mut term_it = field_searcher.terms();
        let mut term_string = String::new();
        while term_it.advance() {
            //let term = Term::from_bytes(term_it.key());
            term_string.push_str(unsafe { str::from_utf8_unchecked(term_it.key()) }); // ok test
        }
        assert_eq!(&*term_string, "abcdef");
    }

    #[test]
    fn test_term_dictionary_stream() {
        let ids: Vec<_> = (0u32..10_000u32)
            .map(|i| (format!("doc{:0>6}", i), i))
            .collect();
        let field_type = FieldType::Str(TEXT);
        let buffer: Vec<u8> = {
            let mut term_dictionary_builder =
                TermDictionaryBuilder::new(vec![], field_type).unwrap();
            for &(ref id, ref i) in &ids {
                term_dictionary_builder
                    .insert(id.as_bytes(), &make_term_info(*i as u64))
                    .unwrap();
            }
            term_dictionary_builder.finish().unwrap()
        };
        let source = ReadOnlySource::from(buffer);
        let term_dictionary: TermDictionary = TermDictionary::from_source(source);
        {
            let mut streamer = term_dictionary.stream();
            let mut i = 0;
            while let Some((streamer_k, streamer_v)) = streamer.next() {
                let &(ref key, ref v) = &ids[i];
                assert_eq!(streamer_k.as_ref(), key.as_bytes());
                assert_eq!(streamer_v, &make_term_info(*v as u64));
                i += 1;
            }
        }

        let &(ref key, ref _v) = &ids[2047];
        term_dictionary.get(key.as_bytes());
    }

    #[test]
    fn test_stream_high_range_prefix_suffix() {
        let field_type = FieldType::Str(TEXT);
        let buffer: Vec<u8> = {
            let mut term_dictionary_builder =
                TermDictionaryBuilder::new(vec![], field_type).unwrap();
            // term requires more than 16bits
            term_dictionary_builder
                .insert("abcdefghijklmnopqrstuvwxy", &make_term_info(1))
                .unwrap();
            term_dictionary_builder
                .insert("abcdefghijklmnopqrstuvwxyz", &make_term_info(2))
                .unwrap();
            term_dictionary_builder
                .insert("abr", &make_term_info(2))
                .unwrap();
            term_dictionary_builder.finish().unwrap()
        };
        let source = ReadOnlySource::from(buffer);
        let term_dictionary: TermDictionary = TermDictionary::from_source(source);
        let mut kv_stream = term_dictionary.stream();
        assert!(kv_stream.advance());
        assert_eq!(kv_stream.key(), "abcdefghijklmnopqrstuvwxy".as_bytes());
        assert_eq!(kv_stream.value(), &make_term_info(1));
        assert!(kv_stream.advance());
        assert_eq!(kv_stream.key(), "abcdefghijklmnopqrstuvwxyz".as_bytes());
        assert_eq!(kv_stream.value(), &make_term_info(2));
        assert!(kv_stream.advance());
        assert_eq!(kv_stream.key(), "abr".as_bytes());
        assert!(!kv_stream.advance());
    }

    #[test]
    fn test_stream_range() {
        let ids: Vec<_> = (0u32..10_000u32)
            .map(|i| (format!("doc{:0>6}", i), i))
            .collect();
        let field_type = FieldType::Str(TEXT);
        let buffer: Vec<u8> = {
            let mut term_dictionary_builder =
                TermDictionaryBuilder::new(vec![], field_type).unwrap();
            for &(ref id, ref i) in &ids {
                term_dictionary_builder
                    .insert(id.as_bytes(), &make_term_info(*i as u64))
                    .unwrap();
            }
            term_dictionary_builder.finish().unwrap()
        };

        let source = ReadOnlySource::from(buffer);

        let term_dictionary: TermDictionary = TermDictionary::from_source(source);
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
                    assert_eq!(streamer_v.doc_freq, *v);
                    assert_eq!(streamer_v, &make_term_info(*v as u64));
                }
            }
        }

        {
            for i in (0..20).chain(BLOCK_SIZE - 10..BLOCK_SIZE + 10) {
                let &(ref target_key, _) = &ids[i];
                let mut streamer = term_dictionary
                    .range()
                    .gt(target_key.as_bytes())
                    .into_stream();
                for j in 0..3 {
                    let (streamer_k, streamer_v) = streamer.next().unwrap();
                    let &(ref key, ref v) = &ids[i + j + 1];
                    assert_eq!(streamer_k.as_ref(), key.as_bytes());
                    assert_eq!(streamer_v.doc_freq, *v);
                }
            }
        }

        {
            for i in (0..20).chain(BLOCK_SIZE - 10..BLOCK_SIZE + 10) {
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
    fn test_empty_string() {
        let field_type = FieldType::Str(TEXT);
        let buffer: Vec<u8> = {
            let mut term_dictionary_builder =
                TermDictionaryBuilder::new(vec![], field_type).unwrap();
            term_dictionary_builder
                .insert(&[], &make_term_info(1 as u64))
                .unwrap();
            term_dictionary_builder
                .insert(&[1u8], &make_term_info(2 as u64))
                .unwrap();
            term_dictionary_builder.finish().unwrap()
        };
        let source = ReadOnlySource::from(buffer);
        let term_dictionary: TermDictionary = TermDictionary::from_source(source);
        let mut stream = term_dictionary.stream();
        assert!(stream.advance());
        assert!(stream.key().is_empty());
        assert!(stream.advance());
        assert_eq!(stream.key(), &[1u8]);
        assert!(!stream.advance());
    }

    #[test]
    fn test_stream_range_boundaries() {
        let field_type = FieldType::Str(TEXT);
        let buffer: Vec<u8> = {
            let mut term_dictionary_builder =
                TermDictionaryBuilder::new(vec![], field_type).unwrap();
            for i in 0u8..10u8 {
                let number_arr = [i; 1];
                term_dictionary_builder
                    .insert(&number_arr, &make_term_info(i as u64))
                    .unwrap();
            }
            term_dictionary_builder.finish().unwrap()
        };
        let source = ReadOnlySource::from(buffer);
        let term_dictionary: TermDictionary = TermDictionary::from_source(source);

        let value_list = |mut streamer: TermStreamer| {
            let mut res: Vec<u32> = vec![];
            while let Some((_, ref v)) = streamer.next() {
                res.push(v.doc_freq);
            }
            res
        };
        {
            let range = term_dictionary.range().ge([2u8]).into_stream();
            assert_eq!(
                value_list(range),
                vec![2u32, 3u32, 4u32, 5u32, 6u32, 7u32, 8u32, 9u32]
            );
        }
        {
            let range = term_dictionary.range().gt([2u8]).into_stream();
            assert_eq!(
                value_list(range),
                vec![3u32, 4u32, 5u32, 6u32, 7u32, 8u32, 9u32]
            );
        }
        {
            let range = term_dictionary.range().lt([6u8]).into_stream();
            assert_eq!(value_list(range), vec![0u32, 1u32, 2u32, 3u32, 4u32, 5u32]);
        }
        {
            let range = term_dictionary.range().le([6u8]).into_stream();
            assert_eq!(
                value_list(range),
                vec![0u32, 1u32, 2u32, 3u32, 4u32, 5u32, 6u32]
            );
        }
        {
            let range = term_dictionary.range().ge([0u8]).lt([5u8]).into_stream();
            assert_eq!(value_list(range), vec![0u32, 1u32, 2u32, 3u32, 4u32]);
        }
    }

    #[test]
    fn test_automaton_search() {
        use levenshtein_automata::LevenshteinAutomatonBuilder;

        const COUNTRIES: [&'static str; 7] = [
            "San Marino",
            "Serbia",
            "Slovakia",
            "Slovenia",
            "Spain",
            "Sweden",
            "Switzerland",
        ];

        let mut directory = RAMDirectory::create();
        let path = PathBuf::from("TermDictionary");
        {
            let write = directory.open_write(&path).unwrap();
            let field_type = FieldType::Str(TEXT);
            let mut term_dictionary_builder =
                TermDictionaryBuilder::new(write, field_type).unwrap();
            for term in COUNTRIES.iter() {
                term_dictionary_builder
                    .insert(term.as_bytes(), &make_term_info(0u64))
                    .unwrap();
            }
            term_dictionary_builder.finish().unwrap();
        }
        let source = directory.open_read(&path).unwrap();
        let term_dict: TermDictionary = TermDictionary::from_source(source);

        // We can now build an entire dfa.
        let lev_automaton_builder = LevenshteinAutomatonBuilder::new(2, true);
        let automaton = lev_automaton_builder.build_dfa("Spaen");

        let mut range = term_dict.search(automaton).into_stream();

        // get the first finding
        assert!(range.advance());
        assert_eq!("Spain".as_bytes(), range.key());
        assert!(!range.advance());
    }
}
