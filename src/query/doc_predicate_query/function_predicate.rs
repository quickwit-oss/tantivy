use super::{DocPredicate, SegmentDocPredicate};
use crate::index::SegmentReader;
use crate::DocId;

/// Blanket [`SegmentDocPredicate`] implementation for any per-document
/// closure.
///
/// Hidden contract: [`SegmentDocPredicate::eval`] takes `&self`, not
/// `&mut self`, so the closure must be callable through an immutable
/// reference (`Fn`, not `FnMut`). A caller that needs per-document mutable
/// state must capture it behind its own synchronization primitive (for
/// example `Arc<Mutex<_>>` or `Arc<AtomicUsize>`) rather than relying on
/// interior mutability provided by this blanket impl.
impl<F> SegmentDocPredicate for F
where F: Fn(DocId) -> bool + Send + Sync + 'static
{
    fn eval(&self, doc_id: DocId) -> bool {
        (self)(doc_id)
    }
}

/// A [`DocPredicate`] built from a plain factory function.
///
/// `FunctionPredicate` wraps a factory closure that is called once per
/// segment (via [`DocPredicate::doc_predicate`]) and returns a per-document
/// closure evaluated once per candidate document. This predicate has no
/// dependency on fast fields or any other segment data structure: the
/// factory receives the [`SegmentReader`] and is free to ignore it entirely,
/// for example to build a predicate over `doc_id` alone.
///
/// # Example
///
/// ```
/// use tantivy::query::doc_predicate_query::FunctionPredicate;
/// use tantivy::query::doc_predicate_query::DocPredicateQuery;
/// use tantivy::index::SegmentReader;
/// use tantivy::DocId;
///
/// // Matches every even doc id, in every segment.
/// let predicate = FunctionPredicate::new(|_segment_reader: &SegmentReader| {
///     Ok(move |doc_id: DocId| doc_id % 2 == 0)
/// });
/// let _query: DocPredicateQuery = predicate.into();
/// ```
pub struct FunctionPredicate<F> {
    factory: F,
}

impl<F> FunctionPredicate<F> {
    /// Creates a new predicate from a per-segment factory function.
    ///
    /// `factory` is called once per segment and must return a per-document
    /// closure implementing `Fn(DocId) -> bool`.
    pub fn new(factory: F) -> Self {
        Self { factory }
    }
}

// `F` is a plain closure or function type and will not generally implement
// `Debug`. `DocPredicate` requires `Debug`, so this manual impl exists purely
// to satisfy that bound; it intentionally does not attempt to expose the
// closure's captured state.
impl<F> std::fmt::Debug for FunctionPredicate<F> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter
            .debug_struct("FunctionPredicate")
            .finish_non_exhaustive()
    }
}

impl<F, SegmentF> DocPredicate for FunctionPredicate<F>
where
    F: Fn(&SegmentReader) -> crate::Result<SegmentF> + Send + Sync + 'static,
    SegmentF: SegmentDocPredicate,
{
    type SegmentDocPredicate = SegmentF;

    fn doc_predicate(&self, segment_reader: &SegmentReader) -> crate::Result<SegmentF> {
        (self.factory)(segment_reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::Count;
    use crate::query::doc_predicate_query::{create_index_for_test, DocPredicateQuery};

    #[test]
    fn test_function_predicate_matches_even_doc_ids() {
        let index = create_index_for_test(4);
        let searcher = index.reader().unwrap().searcher();
        let predicate = FunctionPredicate::new(|_segment_reader: &SegmentReader| {
            Ok(move |doc_id: DocId| doc_id % 2 == 0)
        });
        let query: DocPredicateQuery = predicate.into();
        assert_eq!(searcher.search(&query, &Count).unwrap(), 2);
    }
}
