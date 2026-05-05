mod term_set_gallop;
mod term_set_query;
mod term_set_query_fastfield;
mod term_set_strategy;

pub use self::term_set_query::{InvertedIndexTermSetQuery, TermSetQuery};
pub use self::term_set_query_fastfield::FastFieldTermSetQuery;
pub use self::term_set_strategy::{StrategyTag, TermSetStrategyConfig};
