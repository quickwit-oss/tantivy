use combine::*;
use query::{Query, MultiTermQuery};
use schema::{Schema, FieldType, Term, Field};
use analyzer::SimpleTokenizer;
use analyzer::StreamingIterator;
use query::Occur;



/// Possible error that may happen when parsing a query.
#[derive(Debug)]
pub enum ParsingError {
    /// Error in the query syntax
    SyntaxError,
    /// `FieldDoesNotExist(field_name: String)`
    /// The query references a field that is not in the schema
    FieldDoesNotExist(String),
    /// `ExpectedU32(field_name: String, field_value: String)`
    /// The query contains a term for a `u32`-field, but the value
    /// is not a u32.
    ExpectedU32(String, String),
}

/// Tantivy's Query parser
///
/// The language covered by the current parser is extremely simple.
///
/// * simple terms: "e.g.: `Barack Obama` are simply analyzed using 
///   tantivy's `StandardTokenizer`, hence becoming `["barack", "obama"]`.
///   The terms are then searched within the default terms of the query parser.
///   
///   e.g. If `body` and `title` are default fields, our example terms are
///   `["title:barack", "body:barack", "title:obama", "body:obama"]`.
///   By default, all tokenized and indexed fields are default fields.
///   
///   Multiple terms are handled as an `OR` : any document containing at least
///   one of the term will go through the scoring.
///
///   This behavior is slower, but is not a bad idea if the user is sorting
///   by relevance : The user typically just scans through the first few
///   documents in order of decreasing relevance and will stop when the documents
///   are not relevant anymore.
///   Making it possible to make this behavior customizable is tracked in
///   [issue #27](https://github.com/fulmicoton/tantivy/issues/27).
///   
/// * negative terms: By prepending a term by a `-`, a term can be excluded
///   from the search. This is useful for disambiguating a query.
///   e.g. `apple -fruit` 
///
/// * must terms: By prepending a term by a `+`, a term can be made required for the search.
///   
pub struct QueryParser {
    schema: Schema,
    default_fields: Vec<Field>,
}



impl QueryParser {
    /// Creates a `QueryParser`
    /// * schema - index Schema
    /// * default_fields - fields used to search if no field is specifically defined
    ///   in the query.
    pub fn new(schema: Schema,
               default_fields: Vec<Field>) -> QueryParser {
        QueryParser {
            schema: schema,
            default_fields: default_fields,
        }
    }   
    
    fn transform_field_and_value(&self, field: Field, val: &str) -> Result<Vec<Term>, ParsingError> {
        let field_entry = self.schema.get_field_entry(field);
        Ok(match *field_entry.field_type() {
            FieldType::Str(_) => {
                compute_terms(field, val)
            },
            FieldType::U32(_) => {
                let u32_parsed: u32 = try!(val
                    .parse::<u32>()
                    .map_err(|_| {
                        ParsingError::ExpectedU32(field_entry.name().clone(), String::from(val))
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

    /// Parse a query
    ///
    /// Note that `parse_query` returns an error if the input
    /// is not a valid query.
    /// 
    /// There is currently no lenient mode for the query parser
    /// which makes it a bad choice for a public/broad user search engine.
    ///
    /// Implementing a lenient mode for this query parser is tracked 
    /// in [Issue 5](https://github.com/fulmicoton/tantivy/issues/5)
    pub fn parse_query(&self, query: &str) -> Result<Box<Query>, ParsingError> {
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
                    box MultiTermQuery::from(terms_result)
                )
            }  
            Err(_) => {
                Err(ParsingError::SyntaxError)
            }
        }
    }
}




fn compute_terms(field: Field, text: &str) -> Vec<Term> {
    let tokenizer = SimpleTokenizer::new();
    let mut tokens = Vec::new();
    let mut token_it = tokenizer.tokenize(text);
    while let Some(token_str) = token_it.next() {
        tokens.push(Term::from_field_text(field, token_str));
    }
    tokens
}


#[derive(Debug, Eq, PartialEq)]
pub enum Literal {
    WithField(String, String),
    DefaultField(String),
}


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
mod test {
    
    use combine::*;
    use schema::*;
    use query::MultiTermQuery;
    use query::Occur;
    use super::*;
    
    #[test]
    pub fn test_query_grammar() {
        let mut grammar_parser = parser(query_language);
        assert_eq!(grammar_parser.parse("abc:toto").unwrap().0,
            vec!(
                (Occur::Should, Literal::WithField(String::from("abc"), String::from("toto")))
            )
        );
        assert_eq!(grammar_parser.parse("+toto").unwrap().0,
            vec!(
                (Occur::Must, Literal::DefaultField(String::from("toto")))
            )
        );            
        assert_eq!(
            grammar_parser.parse("\"some phrase query\"").unwrap().0,
            vec!(
                (Occur::Should, Literal::DefaultField(String::from("some phrase query"))),
            )
        );
        assert_eq!(
            grammar_parser.parse("field:\"some phrase query\"").unwrap().0,
            vec!(
                (Occur::Should, Literal::WithField(String::from("field"), String::from("some phrase query")))
        ));
        assert_eq!(grammar_parser.parse("field:\"some phrase query\" field:toto a").unwrap().0,
            vec!(
                (Occur::Should, Literal::WithField(String::from("field"), String::from("some phrase query"))),
                (Occur::Should, Literal::WithField(String::from("field"), String::from("toto"))),
                (Occur::Should, Literal::DefaultField(String::from("a"))),
            ));
        assert_eq!(grammar_parser.parse("field:\"a ! b\"").unwrap().0,
            vec!(
                (Occur::Should, Literal::WithField(String::from("field"), String::from("a ! b"))),
            ));
        assert_eq!(grammar_parser.parse("field:a9e3").unwrap().0,
            vec!(
                (Occur::Should, Literal::WithField(String::from("field"), String::from("a9e3")),)
            ));
        assert_eq!(grammar_parser.parse("a9e3").unwrap().0,
            vec!(
                (Occur::Should, Literal::DefaultField(String::from("a9e3"))),
            ));  
        assert_eq!(grammar_parser.parse("field:タンタイビーって早い").unwrap().0,
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
    
    // fn extract<T: Query>(query_parser: &QueryParser, q: &str) -> T {
    //     query_parser.parse_query(q).unwrap().as_any().downcast_ref::<T>().unwrap(),
    // }
    
    #[test]
    pub fn test_query_parser() {
        let mut schema_builder = SchemaBuilder::default();
        let text_field = schema_builder.add_text_field("text", STRING);
        let title_field = schema_builder.add_text_field("title", STRING);
        let author_field = schema_builder.add_text_field("author", STRING);
        let query_parser = QueryParser::new(schema_builder.build(), vec!(text_field, author_field));
        assert!(query_parser.parse_query("a:b").is_err());
        {
            let terms = vec!(Term::from_field_text(title_field, "abctitle"));
            let query = MultiTermQuery::from(terms); 
            assert_eq!(
                *query_parser.parse_query("title:abctitle").unwrap().as_any().downcast_ref::<MultiTermQuery>().unwrap(), 
                query
            );
        }
        {
            let terms = vec!(
                Term::from_field_text(text_field, "abctitle"),
                Term::from_field_text(author_field, "abctitle"),
            );
            let query = MultiTermQuery::from(terms); 
            assert_eq!(
                *query_parser.parse_query("abctitle").unwrap().as_any().downcast_ref::<MultiTermQuery>().unwrap(),
                query
            );
        }
        {
            let terms = vec!(Term::from_field_text(title_field, "abctitle"));
            let query = MultiTermQuery::from(terms); 
            assert_eq!(
                *query_parser.parse_query("title:abctitle   ").unwrap().as_any().downcast_ref::<MultiTermQuery>().unwrap(),
                query
            );
            assert_eq!(
                *query_parser.parse_query("    title:abctitle").unwrap().as_any().downcast_ref::<MultiTermQuery>().unwrap(), 
                query
            );
        }
    }

}
