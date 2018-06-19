/*!
Query
*/

mod all_query;
mod automaton_weight;
mod bitset;
mod bm25;
mod boolean_query;
mod exclude;
mod fuzzy_query;
mod intersection;
mod occur;
mod phrase_query;
mod query;
mod query_parser;
mod range_query;
mod regex_query;
mod reqopt_scorer;
mod scorer;
mod term_query;
mod union;
mod weight;

#[cfg(test)]
mod vec_docset;

pub(crate) mod score_combiner;

pub use self::intersection::Intersection;
pub use self::union::Union;

#[cfg(test)]
pub use self::vec_docset::VecDocSet;

pub use self::all_query::{AllQuery, AllScorer, AllWeight};
pub use self::automaton_weight::AutomatonWeight;
pub use self::bitset::BitSetDocSet;
pub use self::boolean_query::BooleanQuery;
pub use self::exclude::Exclude;
pub use self::fuzzy_query::FuzzyTermQuery;
pub use self::intersection::intersect_scorers;
pub use self::occur::Occur;
pub use self::phrase_query::PhraseQuery;
pub use self::query::Query;
pub use self::query_parser::QueryParser;
pub use self::query_parser::QueryParserError;
pub use self::range_query::RangeQuery;
pub use self::regex_query::RegexQuery;
pub use self::reqopt_scorer::RequiredOptionalScorer;
pub use self::scorer::ConstScorer;
pub use self::scorer::EmptyScorer;
pub use self::scorer::Scorer;
pub use self::term_query::TermQuery;
pub use self::weight::Weight;
