use schema::{Schema, Field};
use query::Query;
use query::BooleanQuery;
use super::logical_ast::*;
use super::user_input_ast::*;
use super::query_grammar::parse_to_ast;
use query::Occur;
use query::TermQuery;
use query::PhraseQuery;
use analyzer::SimpleTokenizer;
use analyzer::StreamingIterator;
use schema::Term;



/// Possible error that may happen when parsing a query.
#[derive(Debug, PartialEq, Eq)]
pub enum QueryParserError {
    /// Error in the query syntax
    SyntaxError,
    /// `FieldDoesNotExist(field_name: String)`
    /// The query references a field that is not in the schema
    FieldDoesNotExist(String),
    /// `ExpectedU32(field_name: String, field_value: String)`
    /// The query contains a term for a `u32`-field, but the value
    /// is not a u32.
    ExpectedU32(String, String),
    
    AllButQueryForbidden,
    
    NoDefaultFieldDeclared,
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
    conjunction_by_default: bool,
    analyzer: Box<SimpleTokenizer>,
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
            conjunction_by_default: false,
            analyzer: box SimpleTokenizer,
        }
    }   
    
    pub fn set_conjunction_by_default(&mut self) {
        self.conjunction_by_default = true;
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
    pub fn parse_query(&self, query: &str) -> Result<Box<Query>, QueryParserError> {
        let logical_ast = self.parse_query_to_logical_ast(query)?;
        Ok(convert_to_query(logical_ast))
    }

    pub fn parse_query_to_logical_ast(&self, query: &str) -> Result<LogicalAST, QueryParserError> {
        let (user_input_ast, remaining) = parse_to_ast(query).map_err(|_| QueryParserError::SyntaxError)?;
        self.compute_logical_ast(user_input_ast)
    }
    
    fn resolve_field_name(&self, field_name: &str) -> Result<Field, QueryParserError> {
        self.schema.get_field(field_name)
                   .ok_or_else(|| QueryParserError::FieldDoesNotExist(String::from(field_name)))
    }
    
    pub fn compute_logical_ast(&self, user_input_ast: UserInputAST) -> Result<LogicalAST, QueryParserError> {
        let (occur, ast) = try!(self.compute_logical_ast_with_occur(user_input_ast));
        if occur == Occur::MustNot {
            return Err(QueryParserError::AllButQueryForbidden)
        }
        Ok(ast)        
    }
        
    fn compute_logical_ast_for_leaf(&self, field: Field, phrase: &str) -> Result<Option<LogicalLiteral>, QueryParserError> {
        let mut token_iter = self.analyzer.tokenize(phrase);
        let mut tokens: Vec<Term> = Vec::new();
        loop {
            if let Some(token) = token_iter.next() {
                let text = token.to_string();
                // TODO Handle u32
                let term = Term::from_field_text(field, &text);
                tokens.push(term);
            }
            else {
                break;
            }
        }
        if tokens.is_empty() {
            Ok(None)
        }
        else if tokens.len() == 1 {
            Ok(Some(LogicalLiteral::Term(tokens.into_iter().next().unwrap())))
        }
        else {
            Ok(Some(LogicalLiteral::Phrase(tokens)))
        }
    }
    
    fn default_occur(&self) -> Occur {
        if self.conjunction_by_default {
            Occur::Must
        }
        else {
            Occur::Should
        }
    }
    

    pub fn compute_logical_ast_with_occur(&self, user_input_ast: UserInputAST) -> Result<(Occur, LogicalAST), QueryParserError> {
        match user_input_ast {
            UserInputAST::Clause(sub_queries) => {
                let default_occur = self.default_occur();
                let logical_sub_queries: Vec<(Occur, LogicalAST)> = try!(sub_queries
                    .into_iter()
                    .map(|sub_query| self.compute_logical_ast_with_occur(*sub_query))
                    .map(|res| 
                        res.map(
                            |(occur, sub_ast)| (default_occur.compose(occur), sub_ast)
                        )
                    )
                    .collect());
                Ok((Occur::Should, LogicalAST::Clause(logical_sub_queries)))
            }
            UserInputAST::Not(subquery) => {
                let (occur, logical_sub_queries) = try!(self.compute_logical_ast_with_occur(*subquery));
                Ok((Occur::MustNot.compose(occur), logical_sub_queries))
            },
            UserInputAST::Must(subquery) => {
                let (occur, logical_sub_queries) = try!(self.compute_logical_ast_with_occur(*subquery));
                Ok((Occur::Must.compose(occur), logical_sub_queries))
            },
            UserInputAST::Leaf(literal) => {
                let term_phrases: Vec<(Field, String)> = match literal.field_name {
                    Some(ref field_name) => {
                        let field = try!(self.resolve_field_name(&field_name));
                        vec!((field, literal.phrase.clone()))
                    }
                    None => {
                        if self.default_fields.len() == 0 {
                            return Err(QueryParserError::NoDefaultFieldDeclared)
                        }
                        else if self.default_fields.len() == 1 {
                            vec!((self.default_fields[0], literal.phrase.clone()))
                        }
                        else {
                            self.default_fields
                                .iter()
                                .map(|default_field| (*default_field, literal.phrase.clone()))
                                .collect()
                        }
                    }
                };
                let mut asts: Vec<LogicalAST> = Vec::new();
                for (field, phrase) in term_phrases {
                    if let Some(ast) = try!(self.compute_logical_ast_for_leaf(field, &phrase)) {
                        asts.push(LogicalAST::Leaf(box ast));
                    }
                }
                let result_ast = 
                    if asts.len() == 0 {
                        panic!("not working");
                    }
                    else if asts.len() == 1 {
                        asts[0].clone()
                    }
                    else {
                        LogicalAST::Clause(asts
                            .into_iter()
                            .map(|ast| (Occur::Should, ast))
                            .collect())
                    };
                Ok((Occur::Should, result_ast))
           }
        }
    }
    
}


fn convert_literal_to_query(logical_literal: LogicalLiteral) -> Box<Query> {
    match logical_literal {
        LogicalLiteral::Term(term) => {
            box TermQuery::from(term)
        }
        LogicalLiteral::Phrase(terms) => {
            box PhraseQuery::from(terms)
        }
    }
    
}

fn convert_to_query(logical_ast: LogicalAST) -> Box<Query> {
    match logical_ast {
        LogicalAST::Clause(clause) => {
            let occur_subqueries = clause.into_iter()
                .map(|(occur, subquery)| (occur, convert_to_query(subquery)))
                .collect::<Vec<_>>();
            box BooleanQuery::from(occur_subqueries)
        }
        LogicalAST::Leaf(logical_literal) => {
            convert_literal_to_query(*logical_literal)
        }
    }
}


#[cfg(test)]
mod test {
    use schema::{SchemaBuilder, TEXT};
    use super::QueryParser;
    use super::QueryParserError;
    use super::super::logical_ast::*;
    
    fn parse_query_to_logical_ast(query: &str, default_conjunction: bool) -> Result<LogicalAST, QueryParserError> {
        let mut schema_builder = SchemaBuilder::default();
        let title = schema_builder.add_text_field("title", TEXT);
        let text = schema_builder.add_text_field("text", TEXT);
        let schema = schema_builder.build();
        let default_fields = vec!(title, text);
        let mut query_parser = QueryParser::new(schema, default_fields);
        if default_conjunction {
            query_parser.set_conjunction_by_default();
        }
        query_parser.parse_query_to_logical_ast(query)
    }

    fn test_parse_query_to_logical_ast_helper(query: &str, expected: &str, default_conjunction: bool) {
        let query = parse_query_to_logical_ast(query, default_conjunction).unwrap();
        let query_str = format!("{:?}", query);
        assert_eq!(query_str, expected);
    }

    #[test]
    pub fn test_parse_query_to_ast_disjunction() {
        test_parse_query_to_logical_ast_helper("title:toto", "Term([0, 116, 111, 116, 111])", false);
        test_parse_query_to_logical_ast_helper("+title:toto", "Term([0, 116, 111, 116, 111])", false);
        test_parse_query_to_logical_ast_helper("+title:toto -titi", "(+Term([0, 116, 111, 116, 111]) -(Term([0, 116, 105, 116, 105]) Term([1, 116, 105, 116, 105])))", false);
        assert_eq!(parse_query_to_logical_ast("-title:toto", false).err().unwrap(), QueryParserError::AllButQueryForbidden);
        test_parse_query_to_logical_ast_helper("title:a b", "(Term([0, 97]) (Term([0, 98]) Term([1, 98])))", false);
        test_parse_query_to_logical_ast_helper("title:\"a b\"", "\"[Term([0, 97]), Term([0, 98])]\"", false);
    }

    #[test]
    pub fn test_parse_query_to_ast_conjunction() {
        test_parse_query_to_logical_ast_helper("title:toto", "Term([0, 116, 111, 116, 111])", true);
        test_parse_query_to_logical_ast_helper("+title:toto", "Term([0, 116, 111, 116, 111])", true);
        test_parse_query_to_logical_ast_helper("+title:toto -titi", "(+Term([0, 116, 111, 116, 111]) -(Term([0, 116, 105, 116, 105]) Term([1, 116, 105, 116, 105])))", true);
        assert_eq!(parse_query_to_logical_ast("-title:toto", true).err().unwrap(), QueryParserError::AllButQueryForbidden);
        test_parse_query_to_logical_ast_helper("title:a b", "(+Term([0, 97]) +(Term([0, 98]) Term([1, 98])))", true);
        test_parse_query_to_logical_ast_helper("title:\"a b\"", "\"[Term([0, 97]), Term([0, 98])]\"", true);
    }
}
