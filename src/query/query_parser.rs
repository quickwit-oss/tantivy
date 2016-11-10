use combine::*;
use combine::char::*;
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
    pub fn parse_query<I>(&self, query: I) -> Result<Box<Query>, ParsingError> where I: Stream<Item = char> {
        panic!("a");
    }
}




#[derive(Debug, Eq, PartialEq)]
pub enum Literal {
    WithField(String, String),
    DefaultField(String),
}


pub fn query_language<I>(input: I) -> ParseResult<Vec<(Occur, Literal)>, I>
    where I: Stream<Item = char>
{
   panic!("a");
}

