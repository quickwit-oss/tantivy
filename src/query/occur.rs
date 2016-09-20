/// Defines whether a term in a query must be present,
/// should be present or must not be present.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Occur {
    /// The term should be present in the document.
    /// Document without the term will be considered
    /// in scoring as well.
    Should,
    /// Document without the term are excluded from the search.
    Must,
    /// Document that contain the term are excluded from the
    /// search.
    MustNot,
}