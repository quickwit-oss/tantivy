use std::fmt;
use std::fmt::Write;

/// Defines whether a term in a query must be present,
/// should be present or must not be present.
#[derive(Debug, Clone, Hash, Copy, Eq, PartialEq)]
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

impl Occur {
    /// Returns the one-char prefix symbol for this `Occur`.
    /// - `Should` => '?',
    /// - `Must` => '+'
    /// - `Not` => '-'
    fn to_char(self) -> char {
        match self {
            Occur::Should => '?',
            Occur::Must => '+',
            Occur::MustNot => '-',
        }
    }

    /// Compose two occur values.
    pub fn compose(left: Occur, right: Occur) -> Occur {
        match left {
            Occur::Should => right,
            Occur::Must => {
                if right == Occur::MustNot {
                    Occur::MustNot
                } else {
                    Occur::Must
                }
            }
            Occur::MustNot => {
                if right == Occur::MustNot {
                    Occur::Must
                } else {
                    Occur::MustNot
                }
            }
        }
    }
}

impl fmt::Display for Occur {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_char(self.to_char())
    }
}
