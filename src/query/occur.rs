/// Defines whether a term in a query must be present,
/// should be present or must not be present.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Occur {
    /// For a given document to be considered for scoring, 
    /// at least one of the document with the Should or the Must
    /// Occur constraint must be within the document.
    Should,
    /// Document without the term are excluded from the search.
    Must,
    /// Document that contain the term are excluded from the
    /// search.
    MustNot,
}