use combine::*;
use collector::Collector;
use std::io;
use core::searcher::Searcher;
use common::TimerTree;
use query::{Query, MultiTermQuery};
use schema::Schema;
use schema::{Term, Field};

#[derive(Debug)]
pub enum ParsingError {
    SyntaxError,
    FieldDoesNotExist(String),
}

pub struct QueryParser {
    schema: Schema,
    default_field: Field,
}

pub enum StandardQuery {
    MultiTerm(MultiTermQuery),
}

impl Query for StandardQuery {
    fn search<C: Collector>(&self, searcher: &Searcher, collector: &mut C) -> io::Result<TimerTree> {
        match *self {
            StandardQuery::MultiTerm(ref q) => {
                q.search(searcher, collector)
            }
        }
    }
}


impl QueryParser {
    pub fn new(schema: Schema,
               default_field: Field) -> QueryParser {
        QueryParser {
            schema: schema,
            default_field: default_field,
        }
    }

    // TODO check that the term is str.
    // we only support str field for the moment
    fn transform_literal(&self, literal: Literal) -> Result<Term, ParsingError> {
        match literal {
            Literal::DefaultField(val) => {
                Ok(Term::from_field_text(self.default_field, &val))
            },
            Literal::WithField(field_name, val) => {
                match self.schema.get_field(&field_name) {
                    Some(field) => {
                        Ok(Term::from_field_text(field, &val))
                    }
                    None => {
                        Err(ParsingError::FieldDoesNotExist(field_name))
                    }
                } 
            }
        }
    }

    pub fn parse_query(&self, query: &str) -> Result<StandardQuery, ParsingError> {
        match parser(query_language).parse(query) {
            Ok(literals) => {
                let terms_result: Result<Vec<Term>, ParsingError> = literals.0.into_iter()
                    .map(|literal| self.transform_literal(literal))
                    .collect();
                terms_result
                    .map(MultiTermQuery::new)
                    .map(StandardQuery::MultiTerm)
            }  
            Err(_) => {
                Err(ParsingError::SyntaxError)
            }
        }
    }

}

#[derive(Debug, Eq, PartialEq)]
pub enum Literal {
    WithField(String, String),
    DefaultField(String),
}

pub fn query_language(input: State<&str>) -> ParseResult<Vec<Literal>, &str>
{
    let literal = || {
        let term_val = || {
            let word = many1(letter());
            let phrase =  
                (char('"'), many1(satisfy(|c| c != '"')), char('"'),)
                .map(|(_, s, _)| s);
            phrase.or(word)
        };

        let field = many1(letter());
        let term_query = (field, char(':'), term_val())
            .map(|(field,_, value)| Literal::WithField(field, value));
        let term_default_field = term_val().map(|w| Literal::DefaultField(w));
        try(term_query)
            .or(term_default_field) 
    };
    sep_by(literal(), spaces())
    .parse_state(input)
}


#[cfg(test)]
mod tests {
    
    use combine::*;
    use super::*;

    #[test]
    pub fn test_query_parser() {
        let mut query_parser = parser(query_language);
        assert_eq!(query_parser.parse("abc:toto").unwrap().0,
            vec!(Literal::WithField(String::from("abc"), String::from("toto"))));
        assert_eq!(query_parser.parse("\"some phrase query\"").unwrap().0,
            vec!(Literal::DefaultField(String::from("some phrase query"))));
        assert_eq!(query_parser.parse("field:\"some phrase query\"").unwrap().0,
            vec!(Literal::WithField(String::from("field"), String::from("some phrase query"))));
        assert_eq!(query_parser.parse("field:\"some phrase query\" field:toto a").unwrap().0,
            vec!(
                Literal::WithField(String::from("field"), String::from("some phrase query")),
                Literal::WithField(String::from("field"), String::from("toto")),
                Literal::DefaultField(String::from("a")),
            ));
    }

}