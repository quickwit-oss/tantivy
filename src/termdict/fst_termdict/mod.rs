//! The term dictionary main role is to associate the sorted [`Term`](crate::Term)s with
//! a [`TermInfo`](crate::postings::TermInfo) struct that contains some meta-information
//! about the term.
//!
//! Internally, the term dictionary relies on the `fst` crate to store
//! a sorted mapping that associates each term to its rank in the lexicographical order.
//! For instance, in a dictionary containing the sorted terms "abba", "bjork", "blur" and "donovan",
//! the `TermOrdinal` are respectively `0`, `1`, `2`, and `3`.
//!
//! For `u64`-terms, tantivy explicitly uses a `BigEndian` representation to ensure that the
//! lexicographical order matches the natural order of integers.
//!
//! `i64`-terms are transformed to `u64` using a continuous mapping `val ⟶ val - i64::MIN`
//! and then treated as a `u64`.
//!
//! `f64`-terms are transformed to `u64` using a mapping that preserve order, and are then treated
//! as `u64`.
//!
//! A second datastructure makes it possible to access a
//! [`TermInfo`](crate::postings::TermInfo).
mod merger;
mod streamer;
mod term_info_store;
mod termdict;

pub use self::merger::TermMerger;
pub use self::streamer::{TermStreamer, TermStreamerBuilder};
pub use self::termdict::{TermDictionary, TermDictionaryBuilder};

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use common::{OwnedBytes, TerminatingWrite};

    use super::{TermDictionary, TermDictionaryBuilder};
    use crate::directory::{Directory, FileSlice, RamDirectory};
    use crate::postings::TermInfo;

    #[test]
    fn test_format_footer() -> crate::Result<()> {
        let directory = RamDirectory::create();
        let path = PathBuf::from("TermDictionary");
        {
            let write = directory.open_write(&path)?;
            let mut term_dictionary_builder = TermDictionaryBuilder::create(write)?;
            term_dictionary_builder.insert(
                b"key",
                &TermInfo {
                    doc_freq: 0,
                    postings_range: 0..1,
                    positions_range: 0..2,
                },
            )?;
            term_dictionary_builder.finish()?.terminate()?;
        }

        let term_file = directory.open_read(&path)?;
        // on change, this should be kept in sync with :/sstable/src/lib.rs test
        assert_eq!(
            term_file.read_bytes()?.as_slice(),
            &[
                2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 157, 194, 231, 1, 0, 0, 0,
                0, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 39, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 1, 2, 5, 56, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0,
                0, 0
            ]
        );
        TermDictionary::open(term_file)?;

        Ok(())
    }

    #[test]
    fn test_error_reading_sstable_with_fst() -> crate::Result<()> {
        let sstable_file = OwnedBytes::new(vec![
            7u8, 0u8, 0u8, 0u8, 16u8, 17u8, 33u8, 18u8, 19u8, 17u8, 20u8, 0u8, 0u8, 0u8, 0u8, 7u8,
            0u8, 0u8, 0u8, 1, 0, 11, 0, 32, 17, 20, 0, 0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0,
            0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0,
        ]);
        let sstable_file = FileSlice::new(Arc::new(sstable_file));

        match TermDictionary::open(sstable_file) {
            Ok(_) => panic!("successfully parsed sstable with fst"),
            Err(e) => assert!(e.to_string().contains("Invalid dictionary type")),
        }

        Ok(())
    }
}
