mod term_set_bitset;
mod term_set_gallop;
mod term_set_query;
mod term_set_strategy;
mod term_set_weight;

pub use self::term_set_query::{InvertedIndexTermSetQuery, TermSetQuery};
pub use self::term_set_strategy::{StrategyTag, TermSetStrategyConfig};
pub use self::term_set_weight::FastFieldTermSetQuery;
