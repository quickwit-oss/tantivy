use super::MultiValueIntFastFieldReader;
use crate::schema::Facet;
use crate::termdict::TermDictionary;
use crate::termdict::TermOrdinal;
use crate::DocId;
use std::str;

/// The facet reader makes it possible to access the list of
/// facets associated to a given document in a specific
/// segment.
///
/// Rather than manipulating `Facet` object directly, the API
/// exposes those in the form of list of `Facet` ordinal.
///
/// A segment ordinal can then be translated into a facet via
/// `.facet_from_ord(...)`.
///
/// Facet ordinals are defined as their position in the sorted
/// list of facets. This ordinal is segment local and
/// only makes sense for a given segment.
pub struct FacetReader {
    term_ords: MultiValueIntFastFieldReader<u64>,
    term_dict: TermDictionary,
    buffer: Vec<u8>,
}

impl FacetReader {
    /// Creates a new `FacetReader`.
    ///
    /// A facet reader just wraps :
    /// - a `MultiValueIntFastFieldReader` that makes it possible to
    /// access the list of facet ords for a given document.
    /// - a `TermDictionary` that helps associating a facet to
    /// an ordinal and vice versa.
    pub fn new(
        term_ords: MultiValueIntFastFieldReader<u64>,
        term_dict: TermDictionary,
    ) -> FacetReader {
        FacetReader {
            term_ords,
            term_dict,
            buffer: vec![],
        }
    }

    /// Returns the size of the sets of facets in the segment.
    /// This does not take in account the documents that may be marked
    /// as deleted.
    ///
    /// `Facet` ordinals range from `0` to `num_facets() - 1`.
    pub fn num_facets(&self) -> usize {
        self.term_dict.num_terms()
    }

    /// Accessor for the facet term dictionary.
    pub fn facet_dict(&self) -> &TermDictionary {
        &self.term_dict
    }

    /// Given a term ordinal returns the term associated to it.
    pub fn facet_from_ord(
        &mut self,
        facet_ord: TermOrdinal,
        output: &mut Facet,
    ) -> Result<(), str::Utf8Error> {
        let found_term = self
            .term_dict
            .ord_to_term(facet_ord as u64, &mut self.buffer);
        assert!(found_term, "Term ordinal {} no found.", facet_ord);
        let facet_str = str::from_utf8(&self.buffer[..])?;
        output.set_facet_str(facet_str);
        Ok(())
    }

    /// Return the list of facet ordinals associated to a document.
    pub fn facet_ords(&mut self, doc: DocId, output: &mut Vec<u64>) {
        self.term_ords.get_vals(doc, output);
    }
}
