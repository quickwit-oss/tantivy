use Result as tantivy_Error;
use combine::*;
use collector::Collector;
use core::searcher::Searcher;
use common::TimerTree;
use query::{Query, MultiTermQuery};
use schema::{Schema, Term, Field, FieldEntry};
use analyzer::SimpleTokenizer;
use analyzer::StreamingIterator;
use DocAddress;
use query::Explanation;
use query::Occur;

#[derive(Debug)]
pub enum ParsingError {
    SyntaxError,
    FieldDoesNotExist(String),
    ExpectedU32(String, String),
}

pub struct QueryParser {
    schema: Schema,
    default_fields: Vec<Field>,
}

#[derive(Eq, PartialEq, Debug)]
pub enum StandardQuery {
    MultiTerm(MultiTermQuery),
}

impl StandardQuery {
    pub fn num_terms(&self,) -> usize {
        match self {
            &StandardQuery::MultiTerm(ref q) => {
                q.num_terms()
            }
        }
    }
}

impl Query for StandardQuery {
    fn search<C: Collector>(&self, searcher: &Searcher, collector: &mut C) -> tantivy_Error<TimerTree> {
        match *self {
            StandardQuery::MultiTerm(ref q) => {
                q.search(searcher, collector)
            }
        }
    }

    fn explain(
        &self,
        searcher: &Searcher,
        doc_address: &DocAddress) -> tantivy_Error<Explanation> {
        match self {
            &StandardQuery::MultiTerm(ref q) => q.explain(searcher, doc_address)
        }
    }
}


fn compute_terms(field: Field, text: &str) -> Vec<Term> {
    let tokenizer = SimpleTokenizer::new();
    let mut tokens = Vec::new();
    let mut token_it = tokenizer.tokenize(text);
    loop {
        match token_it.next() {
            Some(token_str) => {
                tokens.push(Term::from_field_text(field, token_str));
            }
            None => { break; }
        }
    }
    tokens
}


impl QueryParser {
    pub fn new(schema: Schema,
               default_fields: Vec<Field>) -> QueryParser {
        QueryParser {
            schema: schema,
            default_fields: default_fields,
        }
    }   
    
    
    fn transform_field_and_value(&self, field: Field, val: &str) -> Result<Vec<Term>, ParsingError> {
        let field_entry = self.schema.get_field_entry(field);
        Ok(match field_entry {
            &FieldEntry::Text(_, _) => {
                compute_terms(field, val)
            },
            &FieldEntry::U32(ref field_name, _) => {
                let u32_parsed: u32 = try!(val
                    .parse::<u32>()
                    .map_err(|_| {
                        ParsingError::ExpectedU32(field_name.clone(), String::from(val))
                    })
                );
                vec!(Term::from_field_u32(field, u32_parsed))
            }
        })
    }    
    
    fn transform_literal(&self, literal: Literal) -> Result<Vec<Term>, ParsingError> {
        match literal {
            Literal::DefaultField(val) => {
                let mut terms = Vec::new();
                for &field in &self.default_fields {
                    let extra_terms = try!(self.transform_field_and_value(field, &val));
                    terms.extend_from_slice(&extra_terms);
                }
                Ok(terms)
            },
            Literal::WithField(field_name, val) => {
                match self.schema.get_field(&field_name) {
                    Some(field) => {
                        let terms = try!(self.transform_field_and_value(field, &val));
                        Ok(terms)
                    },
                    None => Err(ParsingError::FieldDoesNotExist(field_name))
                } 
            }
        }
    }

    pub fn parse_query(&self, query: &str) -> Result<StandardQuery, ParsingError> {
        match parser(query_language).parse(query.trim()) {
            Ok(literals) => {
                let mut terms_result: Vec<(Occur, Term)> = Vec::new();
                for (occur, literal) in literals.0 {
                    let literal_terms = try!(self.transform_literal(literal));
                    terms_result
                        .extend(literal_terms
                        .into_iter()
                        .map(|term| (occur, term) ));
                }
                Ok(
                    StandardQuery::MultiTerm(
                        MultiTermQuery::from(terms_result)
                    )
                )
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

// TODO handle as a specific case, having a single MUST_NOT term 


pub fn query_language(input: State<&str>) -> ParseResult<Vec<(Occur, Literal)>, &str>
{
    let literal = || {
        let term_val = || {
            let word = many1(satisfy(|c: char| c.is_alphanumeric()));
            let phrase =
                (char('"'), many1(satisfy(|c| c != '"')), char('"'),)
                .map(|(_, s, _)| s);
            phrase.or(word)
        };
        
        let field = many1(letter());
        let term_query = (field, char(':'), term_val())
            .map(|(field,_, value)| Literal::WithField(field, value));
        let term_default_field = term_val().map(Literal::DefaultField);
        
        let occur = optional(char('-').or(char('+')))
            .map(|opt_c| {
                match opt_c {
                    Some('-') => Occur::MustNot,
                    Some('+') => Occur::Must,
                    _ => Occur::Should, 
                }
            });        
        (occur, try(term_query).or(term_default_field)) 
    };
     
    (sep_by(literal(), spaces()), eof())
    .map(|(first, _)| first)
    .parse_state(input)
}


#[cfg(test)]
mod tests {
    
    use combine::*;
    use schema::*;
    use query::MultiTermQuery;
    use query::Occur;
    use super::*;
    



    #[test]
    pub fn test_query_grammar() {
        let mut query_parser = parser(query_language);
        assert_eq!(query_parser.parse("abc:toto").unwrap().0,
            vec!(
                (Occur::Should, Literal::WithField(String::from("abc"), String::from("toto")))
            )
        );       
        assert_eq!(
            query_parser.parse("\"some phrase query\"").unwrap().0,
            vec!(
                (Occur::Should, Literal::DefaultField(String::from("some phrase query"))),
            )
        );
        assert_eq!(
            query_parser.parse("field:\"some phrase query\"").unwrap().0,
            vec!(
                (Occur::Should, Literal::WithField(String::from("field"), String::from("some phrase query")))
        ));
        assert_eq!(query_parser.parse("field:\"some phrase query\" field:toto a").unwrap().0,
            vec!(
                (Occur::Should, Literal::WithField(String::from("field"), String::from("some phrase query"))),
                (Occur::Should, Literal::WithField(String::from("field"), String::from("toto"))),
                (Occur::Should, Literal::DefaultField(String::from("a"))),
            ));
        assert_eq!(query_parser.parse("field:\"a ! b\"").unwrap().0,
            vec!(
                (Occur::Should, Literal::WithField(String::from("field"), String::from("a ! b"))),
            ));
        assert_eq!(query_parser.parse("field:a9e3").unwrap().0,
            vec!(
                (Occur::Should, Literal::WithField(String::from("field"), String::from("a9e3")),)
            ));
        assert_eq!(query_parser.parse("a9e3").unwrap().0,
            vec!(
                (Occur::Should, Literal::DefaultField(String::from("a9e3"))),
            ));  
        assert_eq!(query_parser.parse("field:タンタイビーって早い").unwrap().0,
            vec!(
                (Occur::Should, Literal::WithField(String::from("field"), String::from("タンタイビーって早い"))),
            ));
    }
    
    #[test]
    pub fn test_query_grammar_with_occur() {
        let mut query_parser = parser(query_language);
        assert_eq!(query_parser.parse("+abc:toto").unwrap().0,
            vec!(
                (Occur::Must, Literal::WithField(String::from("abc"), String::from("toto")))
            )
        );
        assert_eq!(query_parser.parse("+field:\"some phrase query\" -field:toto a").unwrap().0,
            vec!(
                (Occur::Must, Literal::WithField(String::from("field"), String::from("some phrase query"))),
                (Occur::MustNot, Literal::WithField(String::from("field"), String::from("toto"))),
                (Occur::Should, Literal::DefaultField(String::from("a"))),
            ));
    }
            
    #[test]
    pub fn test_invalid_queries() {
        let mut query_parser = parser(query_language);
        println!("{:?}", query_parser.parse("ab!c:"));
        assert!(query_parser.parse("ab!c:").is_err());
        assert!(query_parser.parse("").is_ok());
        assert!(query_parser.parse(":fval").is_err());
        assert!(query_parser.parse("field:").is_err());
        assert!(query_parser.parse(":field").is_err());
        assert!(query_parser.parse("f:@e!e").is_err());
        assert!(query_parser.parse("f:@e!e").is_err());
    }
    
    #[test]
    pub fn test_query_parser() {
        let mut schema = Schema::new();
        let text_field = schema.add_text_field("text", STRING);
        let title_field = schema.add_text_field("title", STRING);
        let author_field = schema.add_text_field("author", STRING);
        let query_parser = QueryParser::new(schema, vec!(text_field, author_field));
        assert!(query_parser.parse_query("a:b").is_err());
        {
            let terms = vec!(Term::from_field_text(title_field, "abctitle"));
            let query = StandardQuery::MultiTerm(MultiTermQuery::from(terms)); 
            assert_eq!(
                query_parser.parse_query("title:abctitle").unwrap(), 
                query
            );
        }
        {
            let terms = vec!(
                Term::from_field_text(text_field, "abctitle"),
                Term::from_field_text(author_field, "abctitle"),
            );
            let query = StandardQuery::MultiTerm(MultiTermQuery::from(terms)); 
            assert_eq!(
                query_parser.parse_query("abctitle").unwrap(), 
                query
            );
        }
        {
            let terms = vec!(Term::from_field_text(title_field, "abctitle"));
            let query = StandardQuery::MultiTerm(MultiTermQuery::from(terms)); 
            assert_eq!(
                query_parser.parse_query("title:abctitle   ").unwrap(), 
                query
            );
            assert_eq!(
                query_parser.parse_query("    title:abctitle").unwrap(), 
                query
            );
        }
    }

}
