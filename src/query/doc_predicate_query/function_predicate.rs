use super::{DocPredicate, SegmentDocPredicate};
use crate::index::SegmentReader;
use crate::DocId;

/// Blanket [`SegmentDocPredicate`] implementation for any per-document
/// function.
impl<F> SegmentDocPredicate for F
where F: Fn(DocId) -> bool + Send + Sync + 'static
{
    fn eval(&mut self, doc_id: DocId) -> bool {
        (self)(doc_id)
    }
}

/// A [`DocPredicate`] built from a plain factory function.
///
/// `FunctionPredicate` wraps a factory closure that is called once per
/// segment (via [`DocPredicate::doc_predicate`]) and returns a per-document
/// closure evaluated once per candidate document.
pub struct FunctionPredicate<F> {
    segment_predicate_factory: F,
}

impl<F, SegmentF> From<F> for FunctionPredicate<F>
where
    F: Fn(&SegmentReader) -> crate::Result<SegmentF> + Send + 'static,
    SegmentF: SegmentDocPredicate,
{
    fn from(segment_predicate_factory: F) -> Self {
        Self {
            segment_predicate_factory,
        }
    }
}

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
        (self.segment_predicate_factory)(segment_reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::Count;
    use crate::query::doc_predicate_query::tests::create_index_for_test;
    use crate::query::doc_predicate_query::DocPredicateQuery;

    #[test]
    fn test_function_predicate_matches_even_doc_ids() {
        let index = create_index_for_test(4);
        let searcher = index.reader().unwrap().searcher();
        let predicate = FunctionPredicate::from(|_segment_reader: &SegmentReader| {
            Ok(move |doc_id: DocId| doc_id % 2 == 0)
        });
        let query: DocPredicateQuery = predicate.into();
        assert_eq!(searcher.search(&query, &Count).unwrap(), 2);
    }
}
