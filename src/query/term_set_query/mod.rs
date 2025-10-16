mod term_set_query;
mod term_set_query_fastfield;

pub use self::term_set_query::{InvertedIndexTermSetQuery, TermSetQuery};
pub use self::term_set_query_fastfield::FastFieldTermSetQuery;
