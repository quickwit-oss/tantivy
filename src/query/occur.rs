
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Occur {
    Should,
    Must,
    MustNot,
}