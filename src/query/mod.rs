/*! Query Module */

mod all_query;
mod automaton_weight;
mod bitset;
mod bm25;
mod boolean_query;
mod boost_query;
mod empty_query;
mod exclude;
mod explanation;
mod fuzzy_query;
mod intersection;
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
pub(crate) use self::bm25::BM25Weight;
pub use self::intersection::Intersection;
pub use self::union::Union;

#[cfg(test)]
pub use self::vec_docset::VecDocSet;

pub use self::all_query::{AllQuery, AllScorer, AllWeight};
pub use self::automaton_weight::AutomatonWeight;
pub use self::bitset::BitSetDocSet;
pub use self::boolean_query::BooleanQuery;
pub use self::boost_query::BoostQuery;
pub use self::empty_query::{EmptyQuery, EmptyScorer, EmptyWeight};
pub use self::exclude::Exclude;
pub use self::explanation::Explanation;
#[cfg(test)]
pub(crate) use self::fuzzy_query::DFAWrapper;
pub use self::fuzzy_query::FuzzyTermQuery;
pub use self::intersection::intersect_scorers;
pub use self::phrase_query::PhraseQuery;
pub use self::query::{Query, QueryClone};
pub use self::query_parser::QueryParser;
pub use self::query_parser::QueryParserError;
pub use self::range_query::RangeQuery;
pub use self::regex_query::RegexQuery;
pub use self::reqopt_scorer::RequiredOptionalScorer;
pub use self::scorer::ConstScorer;
pub use self::scorer::Scorer;
pub use self::term_query::TermQuery;
pub use self::weight::Weight;
pub use tantivy_query_grammar::Occur;

#[cfg(test)]
mod tests {
    use crate::query::QueryParser;
    use crate::schema::{Schema, TEXT};
    use crate::Index;
    use crate::Term;
    use std::collections::BTreeSet;

    #[test]
    fn test_query_terms() {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let query_parser = QueryParser::for_index(&index, vec![text_field]);
        let term_a = Term::from_field_text(text_field, "a");
        let term_b = Term::from_field_text(text_field, "b");
        {
            let mut terms_set: BTreeSet<Term> = BTreeSet::new();
            query_parser
                .parse_query("a")
                .unwrap()
                .query_terms(&mut terms_set);
            let terms: Vec<&Term> = terms_set.iter().collect();
            assert_eq!(vec![&term_a], terms);
        }
        {
            let mut terms_set: BTreeSet<Term> = BTreeSet::new();
            query_parser
                .parse_query("a b")
                .unwrap()
                .query_terms(&mut terms_set);
            let terms: Vec<&Term> = terms_set.iter().collect();
            assert_eq!(vec![&term_a, &term_b], terms);
        }
        {
            let mut terms_set: BTreeSet<Term> = BTreeSet::new();
            query_parser
                .parse_query("\"a b\"")
                .unwrap()
                .query_terms(&mut terms_set);
            let terms: Vec<&Term> = terms_set.iter().collect();
            assert_eq!(vec![&term_a, &term_b], terms);
        }
        {
            let mut terms_set: BTreeSet<Term> = BTreeSet::new();
            query_parser
                .parse_query("a a a a a")
                .unwrap()
                .query_terms(&mut terms_set);
            let terms: Vec<&Term> = terms_set.iter().collect();
            assert_eq!(vec![&term_a], terms);
        }
        {
            let mut terms_set: BTreeSet<Term> = BTreeSet::new();
            query_parser
                .parse_query("a -b")
                .unwrap()
                .query_terms(&mut terms_set);
            let terms: Vec<&Term> = terms_set.iter().collect();
            assert_eq!(vec![&term_a, &term_b], terms);
        }
    }
}
