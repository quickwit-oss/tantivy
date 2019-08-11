use super::logical_ast::*;
use super::query_grammar::parse_to_ast;
use super::user_input_ast::*;
use crate::core::Index;
use crate::query::occur::compose_occur;
use crate::query::query_parser::logical_ast::LogicalAST;
use crate::query::AllQuery;
use crate::query::BooleanQuery;
use crate::query::EmptyQuery;
use crate::query::Occur;
use crate::query::PhraseQuery;
use crate::query::Query;
use crate::query::RangeQuery;
use crate::query::TermQuery;
use crate::schema::IndexRecordOption;
use crate::schema::{Field, Schema};
use crate::schema::{FieldType, Term};
use crate::tokenizer::TokenizerManager;
use combine::Parser;
use std::borrow::Cow;
use std::num::{ParseFloatError, ParseIntError};
use std::ops::Bound;
use std::str::FromStr;

/// Possible error that may happen when parsing a query.
#[derive(Debug, PartialEq, Eq, Fail)]
pub enum QueryParserError {
    /// Error in the query syntax
    #[fail(display = "Syntax Error")]
    SyntaxError,
    /// `FieldDoesNotExist(field_name: String)`
    /// The query references a field that is not in the schema
    #[fail(display = "File does not exists: '{:?}'", _0)]
    FieldDoesNotExist(String),
    /// The query contains a term for a `u64` or `i64`-field, but the value
    /// is neither.
    #[fail(display = "Expected a valid integer: '{:?}'", _0)]
    ExpectedInt(ParseIntError),
    /// The query contains a term for a `f64`-field, but the value
    /// is not a f64.
    #[fail(display = "Invalid query: Only excluding terms given")]
    ExpectedFloat(ParseFloatError),
    /// It is forbidden queries that are only "excluding". (e.g. -title:pop)
    #[fail(display = "Invalid query: Only excluding terms given")]
    AllButQueryForbidden,
    /// If no default field is declared, running a query without any
    /// field specified is forbbidden.
    #[fail(display = "No default field declared and no field specified in query")]
    NoDefaultFieldDeclared,
    /// The field searched for is not declared
    /// as indexed in the schema.
    #[fail(display = "The field '{:?}' is not declared as indexed", _0)]
    FieldNotIndexed(String),
    /// A phrase query was requested for a field that does not
    /// have any positions indexed.
    #[fail(display = "The field '{:?}' does not have positions indexed", _0)]
    FieldDoesNotHavePositionsIndexed(String),
    /// The tokenizer for the given field is unknown
    /// The two argument strings are the name of the field, the name of the tokenizer
    #[fail(
        display = "The tokenizer '{:?}' for the field '{:?}' is unknown",
        _0, _1
    )]
    UnknownTokenizer(String, String),
    /// The query contains a range query with a phrase as one of the bounds.
    /// Only terms can be used as bounds.
    #[fail(display = "A range query cannot have a phrase as one of the bounds")]
    RangeMustNotHavePhrase,
    /// The format for the date field is not RFC 3339 compliant.
    #[fail(display = "The date field has an invalid format")]
    DateFormatError(chrono::ParseError),
}

impl From<ParseIntError> for QueryParserError {
    fn from(err: ParseIntError) -> QueryParserError {
        QueryParserError::ExpectedInt(err)
    }
}

impl From<ParseFloatError> for QueryParserError {
    fn from(err: ParseFloatError) -> QueryParserError {
        QueryParserError::ExpectedFloat(err)
    }
}

impl From<chrono::ParseError> for QueryParserError {
    fn from(err: chrono::ParseError) -> QueryParserError {
        QueryParserError::DateFormatError(err)
    }
}

/// Recursively remove empty clause from the AST
///
/// Returns `None` iff the `logical_ast` ended up being empty.
fn trim_ast(logical_ast: LogicalAST) -> Option<LogicalAST> {
    match logical_ast {
        LogicalAST::Clause(children) => {
            let trimmed_children = children
                .into_iter()
                .flat_map(|(occur, child)| {
                    trim_ast(child).map(|trimmed_child| (occur, trimmed_child))
                })
                .collect::<Vec<_>>();
            if trimmed_children.is_empty() {
                None
            } else {
                Some(LogicalAST::Clause(trimmed_children))
            }
        }
        _ => Some(logical_ast),
    }
}

/// Tantivy's Query parser
///
/// The language covered by the current parser is extremely simple.
///
/// * simple terms: "e.g.: `Barack Obama` are simply tokenized using
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
///
///   Switching to a default of `AND` can be done by calling `.set_conjunction_by_default()`.
///
///
/// * boolean operators `AND`, `OR`. `AND` takes precedence over `OR`, so that `a AND b OR c` is interpreted
/// as `(a AND b) OR c`.
///
/// * In addition to the boolean operators, the `-`, `+` can help define. These operators
///   are sufficient to express all queries using boolean operators. For instance `x AND y OR z` can
///   be written (`(+x +y) z`). In addition, these operators can help define "required optional"
///   queries. `(+x y)` matches the same document set as simply `x`, but `y` will help refining the score.
///
/// * negative terms: By prepending a term by a `-`, a term can be excluded
///   from the search. This is useful for disambiguating a query.
///   e.g. `apple -fruit`
///
/// * must terms: By prepending a term by a `+`, a term can be made required for the search.
///
///
/// * phrase terms: Quoted terms become phrase searches on fields that have positions indexed.
///   e.g., `title:"Barack Obama"` will only find documents that have "barack" immediately followed
///   by "obama".
///
/// * range terms: Range searches can be done by specifying the start and end bound. These can be
///   inclusive or exclusive. e.g., `title:[a TO c}` will find all documents whose title contains
///   a word lexicographically between `a` and `c` (inclusive lower bound, exclusive upper bound).
///   Inclusive bounds are `[]`, exclusive are `{}`.
///
/// * date values: The query parser supports rfc3339 formatted dates. For example "2002-10-02T15:00:00.05Z"
///
/// *  all docs query: A plain `*` will match all documents in the index.
///
#[derive(Clone)]
pub struct QueryParser {
    schema: Schema,
    default_fields: Vec<Field>,
    conjunction_by_default: bool,
    tokenizer_manager: TokenizerManager,
}

impl QueryParser {
    /// Creates a `QueryParser`, given
    /// * schema - index Schema
    /// * default_fields - fields used to search if no field is specifically defined
    ///   in the query.
    pub fn new(
        schema: Schema,
        default_fields: Vec<Field>,
        tokenizer_manager: TokenizerManager,
    ) -> QueryParser {
        QueryParser {
            schema,
            default_fields,
            tokenizer_manager,
            conjunction_by_default: false,
        }
    }

    /// Creates a `QueryParser`, given
    ///  * an index
    ///  * a set of default - fields used to search if no field is specifically defined
    ///   in the query.
    pub fn for_index(index: &Index, default_fields: Vec<Field>) -> QueryParser {
        QueryParser::new(index.schema(), default_fields, index.tokenizers().clone())
    }

    /// Set the default way to compose queries to a conjunction.
    ///
    /// By default, the query `happy tax payer` is equivalent to the query
    /// `happy OR tax OR payer`. After calling `.set_conjunction_by_default()`
    /// `happy tax payer` will be interpreted by the parser as `happy AND tax AND payer`.
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
    pub fn parse_query(&self, query: &str) -> Result<Box<dyn Query>, QueryParserError> {
        let logical_ast = self.parse_query_to_logical_ast(query)?;
        Ok(convert_to_query(logical_ast))
    }

    /// Parse the user query into an AST.
    fn parse_query_to_logical_ast(&self, query: &str) -> Result<LogicalAST, QueryParserError> {
        let (user_input_ast, _remaining) = parse_to_ast()
            .parse(query)
            .map_err(|_| QueryParserError::SyntaxError)?;
        self.compute_logical_ast(user_input_ast)
    }

    fn resolve_field_name(&self, field_name: &str) -> Result<Field, QueryParserError> {
        self.schema
            .get_field(field_name)
            .ok_or_else(|| QueryParserError::FieldDoesNotExist(String::from(field_name)))
    }

    fn compute_logical_ast(
        &self,
        user_input_ast: UserInputAST,
    ) -> Result<LogicalAST, QueryParserError> {
        let (occur, ast) = self.compute_logical_ast_with_occur(user_input_ast)?;
        if occur == Occur::MustNot {
            return Err(QueryParserError::AllButQueryForbidden);
        }
        Ok(ast)
    }

    fn compute_terms_for_string(
        &self,
        field: Field,
        phrase: &str,
    ) -> Result<Vec<(usize, Term)>, QueryParserError> {
        let field_entry = self.schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        if !field_type.is_indexed() {
            let field_name = field_entry.name().to_string();
            return Err(QueryParserError::FieldNotIndexed(field_name));
        }
        match *field_type {
            FieldType::I64(_) => {
                let val: i64 = i64::from_str(phrase)?;
                let term = Term::from_field_i64(field, val);
                Ok(vec![(0, term)])
            }
            FieldType::F64(_) => {
                let val: f64 = f64::from_str(phrase)?;
                let term = Term::from_field_f64(field, val);
                Ok(vec![(0, term)])
            }
            FieldType::Date(_) => match chrono::DateTime::parse_from_rfc3339(phrase) {
                Ok(x) => Ok(vec![(
                    0,
                    Term::from_field_date(field, &x.with_timezone(&chrono::Utc)),
                )]),
                Err(e) => Err(QueryParserError::DateFormatError(e)),
            },
            FieldType::U64(_) => {
                let val: u64 = u64::from_str(phrase)?;
                let term = Term::from_field_u64(field, val);
                Ok(vec![(0, term)])
            }
            FieldType::Str(ref str_options) => {
                if let Some(option) = str_options.get_indexing_options() {
                    let tokenizer =
                        self.tokenizer_manager
                            .get(option.tokenizer())
                            .ok_or_else(|| {
                                QueryParserError::UnknownTokenizer(
                                    field_entry.name().to_string(),
                                    option.tokenizer().to_string(),
                                )
                            })?;
                    let mut terms: Vec<(usize, Term)> = Vec::new();
                    let mut token_stream = tokenizer.token_stream(phrase);
                    token_stream.process(&mut |token| {
                        let term = Term::from_field_text(field, &token.text);
                        terms.push((token.position, term));
                    });
                    if terms.is_empty() {
                        Ok(vec![])
                    } else if terms.len() == 1 {
                        Ok(terms)
                    } else {
                        let field_entry = self.schema.get_field_entry(field);
                        let field_type = field_entry.field_type();
                        if let Some(index_record_option) = field_type.get_index_record_option() {
                            if index_record_option.has_positions() {
                                Ok(terms)
                            } else {
                                let fieldname = self.schema.get_field_name(field).to_string();
                                Err(QueryParserError::FieldDoesNotHavePositionsIndexed(
                                    fieldname,
                                ))
                            }
                        } else {
                            let fieldname = self.schema.get_field_name(field).to_string();
                            Err(QueryParserError::FieldNotIndexed(fieldname))
                        }
                    }
                } else {
                    // This should have been seen earlier really.
                    Err(QueryParserError::FieldNotIndexed(
                        field_entry.name().to_string(),
                    ))
                }
            }
            FieldType::HierarchicalFacet => Ok(vec![(0, Term::from_field_text(field, phrase))]),
            FieldType::Bytes => {
                let field_name = self.schema.get_field_name(field).to_string();
                Err(QueryParserError::FieldNotIndexed(field_name))
            }
        }
    }

    fn compute_logical_ast_for_leaf(
        &self,
        field: Field,
        phrase: &str,
    ) -> Result<Option<LogicalLiteral>, QueryParserError> {
        let terms = self.compute_terms_for_string(field, phrase)?;
        match &terms[..] {
            [] => Ok(None),
            [(_, term)] => Ok(Some(LogicalLiteral::Term(term.clone()))),
            _ => Ok(Some(LogicalLiteral::Phrase(terms.clone()))),
        }
    }

    fn default_occur(&self) -> Occur {
        if self.conjunction_by_default {
            Occur::Must
        } else {
            Occur::Should
        }
    }

    fn resolve_bound(
        &self,
        field: Field,
        bound: &UserInputBound,
    ) -> Result<Bound<Term>, QueryParserError> {
        if bound.term_str() == "*" {
            return Ok(Bound::Unbounded);
        }
        let terms = self.compute_terms_for_string(field, bound.term_str())?;
        if terms.len() != 1 {
            return Err(QueryParserError::RangeMustNotHavePhrase);
        }
        let (_, term) = terms.into_iter().next().unwrap();
        match *bound {
            UserInputBound::Inclusive(_) => Ok(Bound::Included(term)),
            UserInputBound::Exclusive(_) => Ok(Bound::Excluded(term)),
            UserInputBound::Unbounded => Ok(Bound::Unbounded),
        }
    }

    fn resolved_fields(
        &self,
        given_field: &Option<String>,
    ) -> Result<Cow<'_, [Field]>, QueryParserError> {
        match *given_field {
            None => {
                if self.default_fields.is_empty() {
                    Err(QueryParserError::NoDefaultFieldDeclared)
                } else {
                    Ok(Cow::from(&self.default_fields[..]))
                }
            }
            Some(ref field) => Ok(Cow::from(vec![self.resolve_field_name(&*field)?])),
        }
    }

    fn compute_logical_ast_with_occur(
        &self,
        user_input_ast: UserInputAST,
    ) -> Result<(Occur, LogicalAST), QueryParserError> {
        match user_input_ast {
            UserInputAST::Clause(sub_queries) => {
                let default_occur = self.default_occur();
                let mut logical_sub_queries: Vec<(Occur, LogicalAST)> = Vec::new();
                for sub_query in sub_queries {
                    let (occur, sub_ast) = self.compute_logical_ast_with_occur(sub_query)?;
                    let new_occur = compose_occur(default_occur, occur);
                    logical_sub_queries.push((new_occur, sub_ast));
                }
                Ok((Occur::Should, LogicalAST::Clause(logical_sub_queries)))
            }
            UserInputAST::Unary(left_occur, subquery) => {
                let (right_occur, logical_sub_queries) =
                    self.compute_logical_ast_with_occur(*subquery)?;
                Ok((compose_occur(left_occur, right_occur), logical_sub_queries))
            }
            UserInputAST::Leaf(leaf) => {
                let result_ast = self.compute_logical_ast_from_leaf(*leaf)?;
                Ok((Occur::Should, result_ast))
            }
        }
    }

    fn compute_logical_ast_from_leaf(
        &self,
        leaf: UserInputLeaf,
    ) -> Result<LogicalAST, QueryParserError> {
        match leaf {
            UserInputLeaf::Literal(literal) => {
                let term_phrases: Vec<(Field, String)> = match literal.field_name {
                    Some(ref field_name) => {
                        let field = self.resolve_field_name(field_name)?;
                        vec![(field, literal.phrase.clone())]
                    }
                    None => {
                        if self.default_fields.is_empty() {
                            return Err(QueryParserError::NoDefaultFieldDeclared);
                        } else {
                            self.default_fields
                                .iter()
                                .map(|default_field| (*default_field, literal.phrase.clone()))
                                .collect::<Vec<(Field, String)>>()
                        }
                    }
                };
                let mut asts: Vec<LogicalAST> = Vec::new();
                for (field, phrase) in term_phrases {
                    if let Some(ast) = self.compute_logical_ast_for_leaf(field, &phrase)? {
                        asts.push(LogicalAST::Leaf(Box::new(ast)));
                    }
                }
                let result_ast: LogicalAST = if asts.len() == 1 {
                    asts.into_iter().next().unwrap()
                } else {
                    LogicalAST::Clause(asts.into_iter().map(|ast| (Occur::Should, ast)).collect())
                };
                Ok(result_ast)
            }
            UserInputLeaf::All => Ok(LogicalAST::Leaf(Box::new(LogicalLiteral::All))),
            UserInputLeaf::Range {
                field,
                lower,
                upper,
            } => {
                let fields = self.resolved_fields(&field)?;
                let mut clauses = fields
                    .iter()
                    .map(|&field| {
                        let field_entry = self.schema.get_field_entry(field);
                        let value_type = field_entry.field_type().value_type();
                        Ok(LogicalAST::Leaf(Box::new(LogicalLiteral::Range {
                            field,
                            value_type,
                            lower: self.resolve_bound(field, &lower)?,
                            upper: self.resolve_bound(field, &upper)?,
                        })))
                    })
                    .collect::<Result<Vec<_>, QueryParserError>>()?;
                let result_ast = if clauses.len() == 1 {
                    clauses.pop().unwrap()
                } else {
                    LogicalAST::Clause(
                        clauses
                            .into_iter()
                            .map(|clause| (Occur::Should, clause))
                            .collect(),
                    )
                };
                Ok(result_ast)
            }
        }
    }
}

fn convert_literal_to_query(logical_literal: LogicalLiteral) -> Box<dyn Query> {
    match logical_literal {
        LogicalLiteral::Term(term) => Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs)),
        LogicalLiteral::Phrase(term_with_offsets) => {
            Box::new(PhraseQuery::new_with_offset(term_with_offsets))
        }
        LogicalLiteral::Range {
            field,
            value_type,
            lower,
            upper,
        } => Box::new(RangeQuery::new_term_bounds(
            field, value_type, &lower, &upper,
        )),
        LogicalLiteral::All => Box::new(AllQuery),
    }
}

fn convert_to_query(logical_ast: LogicalAST) -> Box<dyn Query> {
    match trim_ast(logical_ast) {
        Some(LogicalAST::Clause(trimmed_clause)) => {
            let occur_subqueries = trimmed_clause
                .into_iter()
                .map(|(occur, subquery)| (occur, convert_to_query(subquery)))
                .collect::<Vec<_>>();
            assert!(
                !occur_subqueries.is_empty(),
                "Should not be empty after trimming"
            );
            Box::new(BooleanQuery::from(occur_subqueries))
        }
        Some(LogicalAST::Leaf(trimmed_logical_literal)) => {
            convert_literal_to_query(*trimmed_logical_literal)
        }
        None => Box::new(EmptyQuery),
    }
}

#[cfg(test)]
mod test {
    use super::super::logical_ast::*;
    use super::QueryParser;
    use super::QueryParserError;
    use crate::query::Query;
    use crate::schema::Field;
    use crate::schema::{IndexRecordOption, TextFieldIndexing, TextOptions};
    use crate::schema::{Schema, Term, INDEXED, STORED, STRING, TEXT};
    use crate::tokenizer::{
        LowerCaser, SimpleTokenizer, StopWordFilter, Tokenizer, TokenizerManager,
    };
    use crate::Index;
    use matches::assert_matches;

    fn make_query_parser() -> QueryParser {
        let mut schema_builder = Schema::builder();
        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer("en_with_stop_words")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_options = TextOptions::default()
            .set_indexing_options(text_field_indexing)
            .set_stored();
        let title = schema_builder.add_text_field("title", TEXT);
        let text = schema_builder.add_text_field("text", TEXT);
        schema_builder.add_i64_field("signed", INDEXED);
        schema_builder.add_u64_field("unsigned", INDEXED);
        schema_builder.add_text_field("notindexed_text", STORED);
        schema_builder.add_text_field("notindexed_u64", STORED);
        schema_builder.add_text_field("notindexed_i64", STORED);
        schema_builder.add_text_field("nottokenized", STRING);
        schema_builder.add_text_field("with_stop_words", text_options);
        schema_builder.add_date_field("date", INDEXED);
        schema_builder.add_f64_field("float", INDEXED);
        let schema = schema_builder.build();
        let default_fields = vec![title, text];
        let tokenizer_manager = TokenizerManager::default();
        tokenizer_manager.register(
            "en_with_stop_words",
            SimpleTokenizer
                .filter(LowerCaser)
                .filter(StopWordFilter::remove(vec!["the".to_string()])),
        );
        QueryParser::new(schema, default_fields, tokenizer_manager)
    }

    fn parse_query_to_logical_ast(
        query: &str,
        default_conjunction: bool,
    ) -> Result<LogicalAST, QueryParserError> {
        let mut query_parser = make_query_parser();
        if default_conjunction {
            query_parser.set_conjunction_by_default();
        }
        query_parser.parse_query_to_logical_ast(query)
    }

    fn test_parse_query_to_logical_ast_helper(
        query: &str,
        expected: &str,
        default_conjunction: bool,
    ) {
        let query = parse_query_to_logical_ast(query, default_conjunction).unwrap();
        let query_str = format!("{:?}", query);
        assert_eq!(query_str, expected);
    }

    #[test]
    pub fn test_parse_query_simple() {
        let query_parser = make_query_parser();
        assert!(query_parser.parse_query("toto").is_ok());
    }

    #[test]
    pub fn test_parse_nonindexed_field_yields_error() {
        let query_parser = make_query_parser();

        let is_not_indexed_err = |query: &str| {
            let result: Result<Box<dyn Query>, QueryParserError> = query_parser.parse_query(query);
            if let Err(QueryParserError::FieldNotIndexed(field_name)) = result {
                Some(field_name.clone())
            } else {
                None
            }
        };

        assert_eq!(
            is_not_indexed_err("notindexed_text:titi"),
            Some(String::from("notindexed_text"))
        );
        assert_eq!(
            is_not_indexed_err("notindexed_u64:23424"),
            Some(String::from("notindexed_u64"))
        );
        assert_eq!(
            is_not_indexed_err("notindexed_i64:-234324"),
            Some(String::from("notindexed_i64"))
        );
    }

    #[test]
    pub fn test_parse_query_untokenized() {
        test_parse_query_to_logical_ast_helper(
            "nottokenized:\"wordone wordtwo\"",
            "Term(field=7,bytes=[119, 111, 114, 100, 111, 110, \
             101, 32, 119, 111, 114, 100, 116, 119, 111])",
            false,
        );
    }

    #[test]
    pub fn test_parse_query_empty() {
        test_parse_query_to_logical_ast_helper("", "<emptyclause>", false);
        test_parse_query_to_logical_ast_helper(" ", "<emptyclause>", false);
        let query_parser = make_query_parser();
        let query_result = query_parser.parse_query("");
        let query = query_result.unwrap();
        assert_eq!(format!("{:?}", query), "EmptyQuery");
    }

    #[test]
    pub fn test_parse_query_ints() {
        let query_parser = make_query_parser();
        assert!(query_parser.parse_query("signed:2324").is_ok());
        assert!(query_parser.parse_query("signed:\"22\"").is_ok());
        assert!(query_parser.parse_query("signed:\"-2234\"").is_ok());
        assert!(query_parser
            .parse_query("signed:\"-9999999999999\"")
            .is_ok());
        assert!(query_parser.parse_query("signed:\"a\"").is_err());
        assert!(query_parser.parse_query("signed:\"2a\"").is_err());
        assert!(query_parser
            .parse_query("signed:\"18446744073709551615\"")
            .is_err());
        assert!(query_parser.parse_query("unsigned:\"2\"").is_ok());
        assert!(query_parser.parse_query("unsigned:\"-2\"").is_err());
        assert!(query_parser
            .parse_query("unsigned:\"18446744073709551615\"")
            .is_ok());
        assert!(query_parser.parse_query("float:\"3.1\"").is_ok());
        assert!(query_parser.parse_query("float:\"-2.4\"").is_ok());
        assert!(query_parser.parse_query("float:\"2.1.2\"").is_err());
        assert!(query_parser.parse_query("float:\"2.1a\"").is_err());
        assert!(query_parser
            .parse_query("float:\"18446744073709551615.0\"")
            .is_ok());
        test_parse_query_to_logical_ast_helper(
            "unsigned:2324",
            "Term(field=3,bytes=[0, 0, 0, 0, 0, 0, 9, 20])",
            false,
        );

        test_parse_query_to_logical_ast_helper(
            "signed:-2324",
            &format!("{:?}", Term::from_field_i64(Field(2u32), -2324)),
            false,
        );

        test_parse_query_to_logical_ast_helper(
            "float:2.5",
            &format!("{:?}", Term::from_field_f64(Field(10u32), 2.5)),
            false,
        );
    }

    #[test]
    pub fn test_parse_query_to_ast_single_term() {
        test_parse_query_to_logical_ast_helper(
            "title:toto",
            "Term(field=0,bytes=[116, 111, 116, 111])",
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "+title:toto",
            "Term(field=0,bytes=[116, 111, 116, 111])",
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "+title:toto -titi",
            "(+Term(field=0,bytes=[116, 111, 116, 111]) \
             -(Term(field=0,bytes=[116, 105, 116, 105]) \
             Term(field=1,bytes=[116, 105, 116, 105])))",
            false,
        );
        assert_eq!(
            parse_query_to_logical_ast("-title:toto", false)
                .err()
                .unwrap(),
            QueryParserError::AllButQueryForbidden
        );
    }

    #[test]
    pub fn test_parse_query_to_ast_two_terms() {
        test_parse_query_to_logical_ast_helper(
            "title:a b",
            "(Term(field=0,bytes=[97]) (Term(field=0,bytes=[98]) Term(field=1,bytes=[98])))",
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "title:\"a b\"",
            "\"[(0, Term(field=0,bytes=[97])), \
             (1, Term(field=0,bytes=[98]))]\"",
            false,
        );
    }

    #[test]
    pub fn test_parse_query_to_ast_ranges() {
        test_parse_query_to_logical_ast_helper(
            "title:[a TO b]",
            "(Included(Term(field=0,bytes=[97])) TO Included(Term(field=0,bytes=[98])))",
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "[a TO b]",
            "((Included(Term(field=0,bytes=[97])) TO \
             Included(Term(field=0,bytes=[98]))) \
             (Included(Term(field=1,bytes=[97])) TO \
             Included(Term(field=1,bytes=[98]))))",
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "title:{titi TO toto}",
            "(Excluded(Term(field=0,bytes=[116, 105, 116, 105])) TO \
             Excluded(Term(field=0,bytes=[116, 111, 116, 111])))",
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "title:{* TO toto}",
            "(Unbounded TO Excluded(Term(field=0,bytes=[116, 111, 116, 111])))",
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "title:{titi TO *}",
            "(Excluded(Term(field=0,bytes=[116, 105, 116, 105])) TO Unbounded)",
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "signed:{-5 TO 3}",
            "(Excluded(Term(field=2,bytes=[127, 255, 255, 255, 255, 255, 255, 251])) TO \
             Excluded(Term(field=2,bytes=[128, 0, 0, 0, 0, 0, 0, 3])))",
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "float:{-1.5 TO 1.5}",
            "(Excluded(Term(field=10,bytes=[64, 7, 255, 255, 255, 255, 255, 255])) TO \
             Excluded(Term(field=10,bytes=[191, 248, 0, 0, 0, 0, 0, 0])))",
            false,
        );

        test_parse_query_to_logical_ast_helper("*", "*", false);
    }

    #[test]
    pub fn test_query_parser_field_does_not_exist() {
        let query_parser = make_query_parser();
        assert_matches!(
            query_parser.parse_query("boujou:\"18446744073709551615\""),
            Err(QueryParserError::FieldDoesNotExist(_))
        );
    }

    #[test]
    pub fn test_query_parser_field_not_indexed() {
        let query_parser = make_query_parser();
        assert_matches!(
            query_parser.parse_query("notindexed_text:\"18446744073709551615\""),
            Err(QueryParserError::FieldNotIndexed(_))
        );
    }

    #[test]
    pub fn test_unknown_tokenizer() {
        let mut schema_builder = Schema::builder();
        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer("nonexistingtokenizer")
            .set_index_option(IndexRecordOption::Basic);
        let text_options = TextOptions::default().set_indexing_options(text_field_indexing);
        let title = schema_builder.add_text_field("title", text_options);
        let schema = schema_builder.build();
        let default_fields = vec![title];
        let tokenizer_manager = TokenizerManager::default();
        let query_parser = QueryParser::new(schema, default_fields, tokenizer_manager);
        assert_matches!(
            query_parser.parse_query("title:\"happy tax payer\""),
            Err(QueryParserError::UnknownTokenizer(_, _))
        );
    }

    #[test]
    pub fn test_query_parser_no_positions() {
        let mut schema_builder = Schema::builder();
        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer("customtokenizer")
            .set_index_option(IndexRecordOption::Basic);
        let text_options = TextOptions::default().set_indexing_options(text_field_indexing);
        let title = schema_builder.add_text_field("title", text_options);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        index
            .tokenizers()
            .register("customtokenizer", SimpleTokenizer);
        let query_parser = QueryParser::for_index(&index, vec![title]);
        assert_eq!(
            query_parser.parse_query("title:\"happy tax\"").unwrap_err(),
            QueryParserError::FieldDoesNotHavePositionsIndexed("title".to_string())
        );
    }

    #[test]
    pub fn test_query_parser_expected_int() {
        let query_parser = make_query_parser();
        assert_matches!(
            query_parser.parse_query("unsigned:18a"),
            Err(QueryParserError::ExpectedInt(_))
        );
        assert!(query_parser.parse_query("unsigned:\"18\"").is_ok());
        assert_matches!(
            query_parser.parse_query("signed:18b"),
            Err(QueryParserError::ExpectedInt(_))
        );
        assert!(query_parser.parse_query("float:\"1.8\"").is_ok());
        assert_matches!(
            query_parser.parse_query("float:1.8a"),
            Err(QueryParserError::ExpectedFloat(_))
        );
    }

    #[test]
    pub fn test_query_parser_expected_date() {
        let query_parser = make_query_parser();
        assert_matches!(
            query_parser.parse_query("date:18a"),
            Err(QueryParserError::DateFormatError(_))
        );
        assert!(query_parser
            .parse_query("date:\"1985-04-12T23:20:50.52Z\"")
            .is_ok());
    }

    #[test]
    pub fn test_query_parser_not_empty_but_no_tokens() {
        let query_parser = make_query_parser();
        assert!(query_parser.parse_query(" !, ").is_ok());
        assert!(query_parser.parse_query("with_stop_words:the").is_ok());
    }

    #[test]
    pub fn test_parse_query_to_ast_conjunction() {
        test_parse_query_to_logical_ast_helper(
            "title:toto",
            "Term(field=0,bytes=[116, 111, 116, 111])",
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "+title:toto",
            "Term(field=0,bytes=[116, 111, 116, 111])",
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "+title:toto -titi",
            "(+Term(field=0,bytes=[116, 111, 116, 111]) \
             -(Term(field=0,bytes=[116, 105, 116, 105]) \
             Term(field=1,bytes=[116, 105, 116, 105])))",
            true,
        );
        assert_eq!(
            parse_query_to_logical_ast("-title:toto", true)
                .err()
                .unwrap(),
            QueryParserError::AllButQueryForbidden
        );
        test_parse_query_to_logical_ast_helper(
            "title:a b",
            "(+Term(field=0,bytes=[97]) \
             +(Term(field=0,bytes=[98]) \
             Term(field=1,bytes=[98])))",
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "title:\"a b\"",
            "\"[(0, Term(field=0,bytes=[97])), \
             (1, Term(field=0,bytes=[98]))]\"",
            true,
        );
    }

    #[test]
    pub fn test_query_parser_hyphen() {
        test_parse_query_to_logical_ast_helper(
            "title:www-form-encoded",
            "\"[(0, Term(field=0,bytes=[119, 119, 119])), (1, Term(field=0,bytes=[102, 111, 114, 109])), (2, Term(field=0,bytes=[101, 110, 99, 111, 100, 101, 100]))]\"",
            false
        );
    }
}
