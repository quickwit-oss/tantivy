use std::collections::{BTreeSet, HashMap};
use std::num::{ParseFloatError, ParseIntError};
use std::ops::Bound;
use std::str::FromStr;

use tantivy_query_grammar::{UserInputAst, UserInputBound, UserInputLeaf, UserInputLiteral};

use super::logical_ast::*;
use crate::core::Index;
use crate::indexer::JsonTermWriter;
use crate::query::{
    AllQuery, BooleanQuery, BoostQuery, EmptyQuery, Occur, PhraseQuery, Query, RangeQuery,
    TermQuery,
};
use crate::schema::{
    Facet, FacetParseError, Field, FieldType, IndexRecordOption, Schema, Term, Type,
};
use crate::tokenizer::{TextAnalyzer, TokenizerManager};
use crate::Score;

/// Possible error that may happen when parsing a query.
#[derive(Debug, PartialEq, Eq, Error)]
pub enum QueryParserError {
    /// Error in the query syntax
    #[error("Syntax Error")]
    SyntaxError,
    /// This query is unsupported.
    #[error("Unsupported query: {0}")]
    UnsupportedQuery(String),
    /// The query references a field that is not in the schema
    #[error("Field does not exists: '{0:?}'")]
    FieldDoesNotExist(String),
    /// The query contains a term for a `u64` or `i64`-field, but the value
    /// is neither.
    #[error("Expected a valid integer: '{0:?}'")]
    ExpectedInt(#[from] ParseIntError),
    /// The query contains a term for a bytes field, but the value is not valid
    /// base64.
    #[error("Expected base64: '{0:?}'")]
    ExpectedBase64(#[from] base64::DecodeError),
    /// The query contains a term for a `f64`-field, but the value
    /// is not a f64.
    #[error("Invalid query: Only excluding terms given")]
    ExpectedFloat(#[from] ParseFloatError),
    /// It is forbidden queries that are only "excluding". (e.g. -title:pop)
    #[error("Invalid query: Only excluding terms given")]
    AllButQueryForbidden,
    /// If no default field is declared, running a query without any
    /// field specified is forbbidden.
    #[error("No default field declared and no field specified in query")]
    NoDefaultFieldDeclared,
    /// The field searched for is not declared
    /// as indexed in the schema.
    #[error("The field '{0:?}' is not declared as indexed")]
    FieldNotIndexed(String),
    /// A phrase query was requested for a field that does not
    /// have any positions indexed.
    #[error("The field '{0:?}' does not have positions indexed")]
    FieldDoesNotHavePositionsIndexed(String),
    /// The tokenizer for the given field is unknown
    /// The two argument strings are the name of the field, the name of the tokenizer
    #[error("The tokenizer '{0:?}' for the field '{1:?}' is unknown")]
    UnknownTokenizer(String, String),
    /// The query contains a range query with a phrase as one of the bounds.
    /// Only terms can be used as bounds.
    #[error("A range query cannot have a phrase as one of the bounds")]
    RangeMustNotHavePhrase,
    /// The format for the date field is not RFC 3339 compliant.
    #[error("The date field has an invalid format")]
    DateFormatError(#[from] chrono::ParseError),
    /// The format for the facet field is invalid.
    #[error("The facet field is malformed: {0}")]
    FacetFormatError(#[from] FacetParseError),
}

/// Recursively remove empty clause from the AST
///
/// Returns `None` if and only if the `logical_ast` ended up being empty.
fn trim_ast(logical_ast: LogicalAst) -> Option<LogicalAst> {
    match logical_ast {
        LogicalAst::Clause(children) => {
            let trimmed_children = children
                .into_iter()
                .flat_map(|(occur, child)| {
                    trim_ast(child).map(|trimmed_child| (occur, trimmed_child))
                })
                .collect::<Vec<_>>();
            if trimmed_children.is_empty() {
                None
            } else {
                Some(LogicalAst::Clause(trimmed_children))
            }
        }
        _ => Some(logical_ast),
    }
}

/// Tantivy's Query parser
///
/// The language covered by the current parser is extremely simple.
///
/// * simple terms: "e.g.: `Barack Obama` are simply tokenized using tantivy's
///   [`SimpleTokenizer`](../tokenizer/struct.SimpleTokenizer.html), hence becoming `["barack",
///   "obama"]`. The terms are then searched within the default terms of the query parser.
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
/// * boolean operators `AND`, `OR`. `AND` takes precedence over `OR`, so that `a AND b OR c` is
///   interpreted
/// as `(a AND b) OR c`.
///
/// * In addition to the boolean operators, the `-`, `+` can help define. These operators are
///   sufficient to express all queries using boolean operators. For instance `x AND y OR z` can be
///   written (`(+x +y) z`). In addition, these operators can help define "required optional"
///   queries. `(+x y)` matches the same document set as simply `x`, but `y` will help refining the
///   score.
///
/// * negative terms: By prepending a term by a `-`, a term can be excluded from the search. This is
///   useful for disambiguating a query. e.g. `apple -fruit`
///
/// * must terms: By prepending a term by a `+`, a term can be made required for the search.
///
/// * phrase terms: Quoted terms become phrase searches on fields that have positions indexed. e.g.,
///   `title:"Barack Obama"` will only find documents that have "barack" immediately followed by
///   "obama".
///
/// * range terms: Range searches can be done by specifying the start and end bound. These can be
///   inclusive or exclusive. e.g., `title:[a TO c}` will find all documents whose title contains a
///   word lexicographically between `a` and `c` (inclusive lower bound, exclusive upper bound).
///   Inclusive bounds are `[]`, exclusive are `{}`.
///
/// * date values: The query parser supports rfc3339 formatted dates. For example
///   `"2002-10-02T15:00:00.05Z"` or `some_date_field:[2002-10-02T15:00:00Z TO
///   2002-10-02T18:00:00Z}`
///
/// * all docs query: A plain `*` will match all documents in the index.
///
/// Parts of the queries can be boosted by appending `^boostfactor`.
/// For instance, `"SRE"^2.0 OR devops^0.4` will boost documents containing `SRE` instead of
/// devops. Negative boosts are not allowed.
///
/// It is also possible to define a boost for a some specific field, at the query parser level.
/// (See [`set_boost(...)`](#method.set_field_boost) ). Typically you may want to boost a title
/// field.
#[derive(Clone)]
pub struct QueryParser {
    schema: Schema,
    default_fields: Vec<Field>,
    conjunction_by_default: bool,
    tokenizer_manager: TokenizerManager,
    boost: HashMap<Field, Score>,
    field_names: BTreeSet<String>,
}

fn all_negative(ast: &LogicalAst) -> bool {
    match ast {
        LogicalAst::Leaf(_) => false,
        LogicalAst::Boost(ref child_ast, _) => all_negative(&*child_ast),
        LogicalAst::Clause(children) => children
            .iter()
            .all(|(ref occur, child)| (*occur == Occur::MustNot) || all_negative(child)),
    }
}

impl QueryParser {
    /// Creates a `QueryParser`, given
    /// * schema - index Schema
    /// * default_fields - fields used to search if no field is specifically defined in the query.
    pub fn new(
        schema: Schema,
        default_fields: Vec<Field>,
        tokenizer_manager: TokenizerManager,
    ) -> QueryParser {
        let field_names = schema
            .fields()
            .map(|(_, field_entry)| field_entry.name().to_string())
            .collect();
        QueryParser {
            schema,
            default_fields,
            tokenizer_manager,
            conjunction_by_default: false,
            boost: Default::default(),
            field_names,
        }
    }

    // Splits a full_path as written in a query, into a field name and a
    // json path.
    pub(crate) fn split_full_path<'a>(&self, full_path: &'a str) -> (&'a str, &'a str) {
        if full_path.is_empty() {
            return ("", "");
        }
        if self.field_names.contains(full_path) {
            return (full_path, "");
        }
        let mut result = ("", full_path);
        let mut cursor = 0;
        while let Some(pos) = full_path[cursor..].find('.') {
            cursor += pos;
            let prefix = &full_path[..cursor];
            let suffix = &full_path[cursor + 1..];
            if self.field_names.contains(prefix) {
                result = (prefix, suffix);
            }
            cursor += 1;
        }
        result
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

    /// Sets a boost for a specific field.
    ///
    /// The parse query will automatically boost this field.
    ///
    /// If the query defines a query boost through the query language (e.g: `country:France^3.0`),
    /// the two boosts (the one defined in the query, and the one defined in the `QueryParser`)
    /// are multiplied together.
    pub fn set_field_boost(&mut self, field: Field, boost: Score) {
        self.boost.insert(field, boost);
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
    fn parse_query_to_logical_ast(&self, query: &str) -> Result<LogicalAst, QueryParserError> {
        let user_input_ast =
            tantivy_query_grammar::parse_query(query).map_err(|_| QueryParserError::SyntaxError)?;
        self.compute_logical_ast(user_input_ast)
    }

    fn resolve_field_name(&self, field_name: &str) -> Result<Field, QueryParserError> {
        self.schema
            .get_field(field_name)
            .ok_or_else(|| QueryParserError::FieldDoesNotExist(String::from(field_name)))
    }

    fn compute_logical_ast(
        &self,
        user_input_ast: UserInputAst,
    ) -> Result<LogicalAst, QueryParserError> {
        let ast = self.compute_logical_ast_with_occur(user_input_ast)?;
        if let LogicalAst::Clause(children) = &ast {
            if children.is_empty() {
                return Ok(ast);
            }
        }
        if all_negative(&ast) {
            return Err(QueryParserError::AllButQueryForbidden);
        }
        Ok(ast)
    }

    fn compute_boundary_term(
        &self,
        field: Field,
        json_path: &str,
        phrase: &str,
    ) -> Result<Term, QueryParserError> {
        let field_entry = self.schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        if !field_type.is_indexed() {
            return Err(QueryParserError::FieldNotIndexed(
                field_entry.name().to_string(),
            ));
        }
        if !json_path.is_empty() && field_type.value_type() != Type::Json {
            return Err(QueryParserError::UnsupportedQuery(format!(
                "Json path is not supported for field {:?}",
                field_entry.name()
            )));
        }
        match *field_type {
            FieldType::U64(_) => {
                let val: u64 = u64::from_str(phrase)?;
                Ok(Term::from_field_u64(field, val))
            }
            FieldType::I64(_) => {
                let val: i64 = i64::from_str(phrase)?;
                Ok(Term::from_field_i64(field, val))
            }
            FieldType::F64(_) => {
                let val: f64 = f64::from_str(phrase)?;
                Ok(Term::from_field_f64(field, val))
            }
            FieldType::Date(_) => {
                let dt = chrono::DateTime::parse_from_rfc3339(phrase)?;
                Ok(Term::from_field_date(
                    field,
                    &dt.with_timezone(&chrono::Utc),
                ))
            }
            FieldType::Str(ref str_options) => {
                let option = str_options.get_indexing_options().ok_or_else(|| {
                    // This should have been seen earlier really.
                    QueryParserError::FieldNotIndexed(field_entry.name().to_string())
                })?;
                let text_analyzer =
                    self.tokenizer_manager
                        .get(option.tokenizer())
                        .ok_or_else(|| {
                            QueryParserError::UnknownTokenizer(
                                field_entry.name().to_string(),
                                option.tokenizer().to_string(),
                            )
                        })?;
                let mut terms: Vec<Term> = Vec::new();
                let mut token_stream = text_analyzer.token_stream(phrase);
                token_stream.process(&mut |token| {
                    let term = Term::from_field_text(field, &token.text);
                    terms.push(term);
                });
                if terms.len() != 1 {
                    return Err(QueryParserError::UnsupportedQuery(format!(
                        "Range query boundary cannot have multiple tokens: {phrase:?}."
                    )));
                }
                Ok(terms.into_iter().next().unwrap())
            }
            FieldType::JsonObject(_) => {
                // Json range are not supported.
                Err(QueryParserError::UnsupportedQuery(
                    "Range query are not supported on json field.".to_string(),
                ))
            }
            FieldType::Facet(_) => match Facet::from_text(phrase) {
                Ok(facet) => Ok(Term::from_facet(field, &facet)),
                Err(e) => Err(QueryParserError::from(e)),
            },
            FieldType::Bytes(_) => {
                let bytes = base64::decode(phrase).map_err(QueryParserError::ExpectedBase64)?;
                Ok(Term::from_field_bytes(field, &bytes))
            }
        }
    }

    fn compute_logical_ast_for_leaf(
        &self,
        field: Field,
        json_path: &str,
        phrase: &str,
    ) -> Result<Vec<LogicalLiteral>, QueryParserError> {
        let field_entry = self.schema.get_field_entry(field);
        let field_type = field_entry.field_type();
        let field_name = field_entry.name();
        if !field_type.is_indexed() {
            return Err(QueryParserError::FieldNotIndexed(field_name.to_string()));
        }
        match *field_type {
            FieldType::U64(_) => {
                let val: u64 = u64::from_str(phrase)?;
                let i64_term = Term::from_field_u64(field, val);
                Ok(vec![LogicalLiteral::Term(i64_term)])
            }
            FieldType::I64(_) => {
                let val: i64 = i64::from_str(phrase)?;
                let i64_term = Term::from_field_i64(field, val);
                Ok(vec![LogicalLiteral::Term(i64_term)])
            }
            FieldType::F64(_) => {
                let val: f64 = f64::from_str(phrase)?;
                let f64_term = Term::from_field_f64(field, val);
                Ok(vec![LogicalLiteral::Term(f64_term)])
            }
            FieldType::Date(_) => {
                let dt = chrono::DateTime::parse_from_rfc3339(phrase)?;
                let dt_term = Term::from_field_date(field, &dt.with_timezone(&chrono::Utc));
                Ok(vec![LogicalLiteral::Term(dt_term)])
            }
            FieldType::Str(ref str_options) => {
                let option = str_options.get_indexing_options().ok_or_else(|| {
                    // This should have been seen earlier really.
                    QueryParserError::FieldNotIndexed(field_name.to_string())
                })?;
                let text_analyzer =
                    self.tokenizer_manager
                        .get(option.tokenizer())
                        .ok_or_else(|| {
                            QueryParserError::UnknownTokenizer(
                                field_name.to_string(),
                                option.tokenizer().to_string(),
                            )
                        })?;
                let index_record_option = option.index_option();
                Ok(generate_literals_for_str(
                    field_name,
                    field,
                    phrase,
                    &text_analyzer,
                    index_record_option,
                )?
                .into_iter()
                .collect())
            }
            FieldType::JsonObject(ref json_options) => {
                let option = json_options.get_text_indexing_options().ok_or_else(|| {
                    // This should have been seen earlier really.
                    QueryParserError::FieldNotIndexed(field_name.to_string())
                })?;
                let text_analyzer =
                    self.tokenizer_manager
                        .get(option.tokenizer())
                        .ok_or_else(|| {
                            QueryParserError::UnknownTokenizer(
                                field_name.to_string(),
                                option.tokenizer().to_string(),
                            )
                        })?;
                let index_record_option = option.index_option();
                generate_literals_for_json_object(
                    field_name,
                    field,
                    json_path,
                    phrase,
                    &text_analyzer,
                    index_record_option,
                )
            }
            FieldType::Facet(_) => match Facet::from_text(phrase) {
                Ok(facet) => {
                    let facet_term = Term::from_facet(field, &facet);
                    Ok(vec![LogicalLiteral::Term(facet_term)])
                }
                Err(e) => Err(QueryParserError::from(e)),
            },
            FieldType::Bytes(_) => {
                let bytes = base64::decode(phrase).map_err(QueryParserError::ExpectedBase64)?;
                let bytes_term = Term::from_field_bytes(field, &bytes);
                Ok(vec![LogicalLiteral::Term(bytes_term)])
            }
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
        json_path: &str,
        bound: &UserInputBound,
    ) -> Result<Bound<Term>, QueryParserError> {
        if bound.term_str() == "*" {
            return Ok(Bound::Unbounded);
        }
        let term = self.compute_boundary_term(field, json_path, bound.term_str())?;
        match *bound {
            UserInputBound::Inclusive(_) => Ok(Bound::Included(term)),
            UserInputBound::Exclusive(_) => Ok(Bound::Excluded(term)),
            UserInputBound::Unbounded => Ok(Bound::Unbounded),
        }
    }

    fn compute_logical_ast_with_occur(
        &self,
        user_input_ast: UserInputAst,
    ) -> Result<LogicalAst, QueryParserError> {
        match user_input_ast {
            UserInputAst::Clause(sub_queries) => {
                let default_occur = self.default_occur();
                let mut logical_sub_queries: Vec<(Occur, LogicalAst)> = Vec::new();
                for (occur_opt, sub_ast) in sub_queries {
                    let sub_ast = self.compute_logical_ast_with_occur(sub_ast)?;
                    let occur = occur_opt.unwrap_or(default_occur);
                    logical_sub_queries.push((occur, sub_ast));
                }
                Ok(LogicalAst::Clause(logical_sub_queries))
            }
            UserInputAst::Boost(ast, boost) => {
                let ast = self.compute_logical_ast_with_occur(*ast)?;
                Ok(ast.boost(boost as Score))
            }
            UserInputAst::Leaf(leaf) => self.compute_logical_ast_from_leaf(*leaf),
        }
    }

    fn field_boost(&self, field: Field) -> Score {
        self.boost.get(&field).cloned().unwrap_or(1.0)
    }

    fn default_indexed_json_fields(&self) -> impl Iterator<Item = Field> + '_ {
        let schema = self.schema.clone();
        self.default_fields.iter().cloned().filter(move |field| {
            let field_type = schema.get_field_entry(*field).field_type();
            field_type.value_type() == Type::Json && field_type.is_indexed()
        })
    }

    fn compute_path_triplet_for_literal<'a>(
        &self,
        literal: &'a UserInputLiteral,
    ) -> Result<Vec<(Field, &'a str, &'a str)>, QueryParserError> {
        match &literal.field_name {
            Some(ref full_path) => {
                // We need to add terms associated to json default fields.
                let (field_name, path) = self.split_full_path(full_path);
                if let Ok(field) = self.resolve_field_name(field_name) {
                    return Ok(vec![(field, path, literal.phrase.as_str())]);
                }
                let triplets: Vec<(Field, &str, &str)> = self
                    .default_indexed_json_fields()
                    .map(|json_field| (json_field, full_path.as_str(), literal.phrase.as_str()))
                    .collect();
                if triplets.is_empty() {
                    return Err(QueryParserError::FieldDoesNotExist(field_name.to_string()));
                }
                Ok(triplets)
            }
            None => {
                if self.default_fields.is_empty() {
                    return Err(QueryParserError::NoDefaultFieldDeclared);
                }
                Ok(self
                    .default_fields
                    .iter()
                    .map(|default_field| (*default_field, "", literal.phrase.as_str()))
                    .collect::<Vec<(Field, &str, &str)>>())
            }
        }
    }

    fn compute_logical_ast_from_leaf(
        &self,
        leaf: UserInputLeaf,
    ) -> Result<LogicalAst, QueryParserError> {
        match leaf {
            UserInputLeaf::Literal(literal) => {
                let term_phrases: Vec<(Field, &str, &str)> =
                    self.compute_path_triplet_for_literal(&literal)?;
                let mut asts: Vec<LogicalAst> = Vec::new();
                for (field, json_path, phrase) in term_phrases {
                    for ast in self.compute_logical_ast_for_leaf(field, json_path, phrase)? {
                        // Apply some field specific boost defined at the query parser level.
                        let boost = self.field_boost(field);
                        asts.push(LogicalAst::Leaf(Box::new(ast)).boost(boost));
                    }
                }
                let result_ast: LogicalAst = if asts.len() == 1 {
                    asts.into_iter().next().unwrap()
                } else {
                    LogicalAst::Clause(asts.into_iter().map(|ast| (Occur::Should, ast)).collect())
                };
                Ok(result_ast)
            }
            UserInputLeaf::All => Ok(LogicalAst::Leaf(Box::new(LogicalLiteral::All))),
            UserInputLeaf::Range {
                field: full_field_opt,
                lower,
                upper,
            } => {
                let full_path = full_field_opt.ok_or_else(|| {
                    QueryParserError::UnsupportedQuery(
                        "Range query need to target a specific field.".to_string(),
                    )
                })?;
                let (field_name, json_path) = self.split_full_path(&full_path);
                let field = self.resolve_field_name(field_name)?;
                let field_entry = self.schema.get_field_entry(field);
                let value_type = field_entry.field_type().value_type();
                let logical_ast = LogicalAst::Leaf(Box::new(LogicalLiteral::Range {
                    field,
                    value_type,
                    lower: self.resolve_bound(field, json_path, &lower)?,
                    upper: self.resolve_bound(field, json_path, &upper)?,
                }));
                Ok(logical_ast)
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

fn generate_literals_for_str(
    field_name: &str,
    field: Field,
    phrase: &str,
    text_analyzer: &TextAnalyzer,
    index_record_option: IndexRecordOption,
) -> Result<Option<LogicalLiteral>, QueryParserError> {
    let mut terms: Vec<(usize, Term)> = Vec::new();
    let mut token_stream = text_analyzer.token_stream(phrase);
    token_stream.process(&mut |token| {
        let term = Term::from_field_text(field, &token.text);
        terms.push((token.position, term));
    });
    if terms.len() <= 1 {
        let term_literal_opt = terms
            .into_iter()
            .next()
            .map(|(_, term)| LogicalLiteral::Term(term));
        return Ok(term_literal_opt);
    }
    if !index_record_option.has_positions() {
        return Err(QueryParserError::FieldDoesNotHavePositionsIndexed(
            field_name.to_string(),
        ));
    }
    Ok(Some(LogicalLiteral::Phrase(terms)))
}

enum NumValue {
    U64(u64),
    I64(i64),
    F64(f64),
    DateTime(crate::DateTime),
}

fn infer_type_num(phrase: &str) -> Option<NumValue> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(phrase) {
        let dt_utc = dt.with_timezone(&chrono::Utc);
        return Some(NumValue::DateTime(dt_utc));
    }
    if let Ok(u64_val) = str::parse::<u64>(phrase) {
        return Some(NumValue::U64(u64_val));
    }
    if let Ok(i64_val) = str::parse::<i64>(phrase) {
        return Some(NumValue::I64(i64_val));
    }
    if let Ok(f64_val) = str::parse::<f64>(phrase) {
        return Some(NumValue::F64(f64_val));
    }
    None
}

fn generate_literals_for_json_object(
    field_name: &str,
    field: Field,
    json_path: &str,
    phrase: &str,
    text_analyzer: &TextAnalyzer,
    index_record_option: IndexRecordOption,
) -> Result<Vec<LogicalLiteral>, QueryParserError> {
    let mut logical_literals = Vec::new();
    let mut term = Term::new();
    term.set_field(Type::Json, field);
    let mut json_term_writer = JsonTermWriter::wrap(&mut term);
    for segment in json_path.split('.') {
        json_term_writer.push_path_segment(segment);
    }
    if let Some(num_value) = infer_type_num(phrase) {
        match num_value {
            NumValue::U64(u64_val) => {
                json_term_writer.set_fast_value(u64_val);
            }
            NumValue::I64(i64_val) => {
                json_term_writer.set_fast_value(i64_val);
            }
            NumValue::F64(f64_val) => {
                json_term_writer.set_fast_value(f64_val);
            }
            NumValue::DateTime(dt_val) => {
                json_term_writer.set_fast_value(dt_val);
            }
        }
        logical_literals.push(LogicalLiteral::Term(json_term_writer.term().clone()));
    }
    json_term_writer.close_path_and_set_type(Type::Str);
    drop(json_term_writer);
    let term_num_bytes = term.as_slice().len();
    let mut token_stream = text_analyzer.token_stream(phrase);
    let mut terms: Vec<(usize, Term)> = Vec::new();
    token_stream.process(&mut |token| {
        term.truncate(term_num_bytes);
        term.append_bytes(token.text.as_bytes());
        terms.push((token.position, term.clone()));
    });
    if terms.len() <= 1 {
        for (_, term) in terms {
            logical_literals.push(LogicalLiteral::Term(term));
        }
        return Ok(logical_literals);
    }
    if !index_record_option.has_positions() {
        return Err(QueryParserError::FieldDoesNotHavePositionsIndexed(
            field_name.to_string(),
        ));
    }
    logical_literals.push(LogicalLiteral::Phrase(terms));
    Ok(logical_literals)
}

fn convert_to_query(logical_ast: LogicalAst) -> Box<dyn Query> {
    match trim_ast(logical_ast) {
        Some(LogicalAst::Clause(trimmed_clause)) => {
            let occur_subqueries = trimmed_clause
                .into_iter()
                .map(|(occur, subquery)| (occur, convert_to_query(subquery)))
                .collect::<Vec<_>>();
            assert!(
                !occur_subqueries.is_empty(),
                "Should not be empty after trimming"
            );
            Box::new(BooleanQuery::new(occur_subqueries))
        }
        Some(LogicalAst::Leaf(trimmed_logical_literal)) => {
            convert_literal_to_query(*trimmed_logical_literal)
        }
        Some(LogicalAst::Boost(ast, boost)) => {
            let query = convert_to_query(*ast);
            let boosted_query = BoostQuery::new(query, boost);
            Box::new(boosted_query)
        }
        None => Box::new(EmptyQuery),
    }
}

#[cfg(test)]
mod test {
    use matches::assert_matches;

    use super::super::logical_ast::*;
    use super::{QueryParser, QueryParserError};
    use crate::query::Query;
    use crate::schema::{
        FacetOptions, Field, IndexRecordOption, Schema, Term, TextFieldIndexing, TextOptions,
        INDEXED, STORED, STRING, TEXT,
    };
    use crate::tokenizer::{
        LowerCaser, SimpleTokenizer, StopWordFilter, TextAnalyzer, TokenizerManager,
    };
    use crate::Index;

    fn make_schema() -> Schema {
        let mut schema_builder = Schema::builder();
        let text_field_indexing = TextFieldIndexing::default()
            .set_tokenizer("en_with_stop_words")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_options = TextOptions::default()
            .set_indexing_options(text_field_indexing)
            .set_stored();
        schema_builder.add_text_field("title", TEXT);
        schema_builder.add_text_field("text", TEXT);
        schema_builder.add_i64_field("signed", INDEXED);
        schema_builder.add_u64_field("unsigned", INDEXED);
        schema_builder.add_text_field("notindexed_text", STORED);
        schema_builder.add_text_field("notindexed_u64", STORED);
        schema_builder.add_text_field("notindexed_i64", STORED);
        schema_builder.add_text_field("nottokenized", STRING);
        schema_builder.add_text_field("with_stop_words", text_options);
        schema_builder.add_date_field("date", INDEXED);
        schema_builder.add_f64_field("float", INDEXED);
        schema_builder.add_facet_field("facet", FacetOptions::default());
        schema_builder.add_bytes_field("bytes", INDEXED);
        schema_builder.add_bytes_field("bytes_not_indexed", STORED);
        schema_builder.add_json_field("json", TEXT);
        schema_builder.add_json_field("json_not_indexed", STORED);
        schema_builder.build()
    }

    fn make_query_parser_with_default_fields(default_fields: &[&'static str]) -> QueryParser {
        let schema = make_schema();
        let default_fields: Vec<Field> = default_fields
            .iter()
            .flat_map(|field_name| schema.get_field(field_name))
            .collect();
        let tokenizer_manager = TokenizerManager::default();
        tokenizer_manager.register(
            "en_with_stop_words",
            TextAnalyzer::from(SimpleTokenizer)
                .filter(LowerCaser)
                .filter(StopWordFilter::remove(vec!["the".to_string()])),
        );
        QueryParser::new(schema, default_fields, tokenizer_manager)
    }

    fn make_query_parser() -> QueryParser {
        make_query_parser_with_default_fields(&["title", "text"])
    }

    fn parse_query_to_logical_ast(
        query: &str,
        default_conjunction: bool,
    ) -> Result<LogicalAst, QueryParserError> {
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
    pub fn test_parse_query_facet() {
        let query_parser = make_query_parser();
        let query = query_parser.parse_query("facet:/root/branch/leaf").unwrap();
        assert_eq!(
            format!("{:?}", query),
            r#"TermQuery(Term(type=Facet, field=11, "/root/branch/leaf"))"#
        );
    }

    #[test]
    pub fn test_parse_query_with_boost() {
        let mut query_parser = make_query_parser();
        let schema = make_schema();
        let text_field = schema.get_field("text").unwrap();
        query_parser.set_field_boost(text_field, 2.0);
        let query = query_parser.parse_query("text:hello").unwrap();
        assert_eq!(
            format!("{:?}", query),
            r#"Boost(query=TermQuery(Term(type=Str, field=1, "hello")), boost=2)"#
        );
    }

    #[test]
    pub fn test_parse_query_range_with_boost() {
        let query = make_query_parser().parse_query("title:[A TO B]").unwrap();
        assert_eq!(
            format!("{:?}", query),
            "RangeQuery { field: Field(0), value_type: Str, left_bound: Included([97]), \
             right_bound: Included([98]) }"
        );
    }

    #[test]
    pub fn test_parse_query_with_default_boost_and_custom_boost() {
        let mut query_parser = make_query_parser();
        let schema = make_schema();
        let text_field = schema.get_field("text").unwrap();
        query_parser.set_field_boost(text_field, 2.0);
        let query = query_parser.parse_query("text:hello^2").unwrap();
        assert_eq!(
            format!("{:?}", query),
            r#"Boost(query=Boost(query=TermQuery(Term(type=Str, field=1, "hello")), boost=2), boost=2)"#
        );
    }

    #[test]
    pub fn test_parse_nonindexed_field_yields_error() {
        let query_parser = make_query_parser();

        let is_not_indexed_err = |query: &str| {
            let result: Result<Box<dyn Query>, QueryParserError> = query_parser.parse_query(query);
            if let Err(QueryParserError::FieldNotIndexed(field_name)) = result {
                Some(field_name)
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
            r#"Term(type=Str, field=7, "wordone wordtwo")"#,
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
            "Term(type=U64, field=3, 2324)",
            false,
        );

        test_parse_query_to_logical_ast_helper(
            "signed:-2324",
            &format!(
                "{:?}",
                Term::from_field_i64(Field::from_field_id(2u32), -2324)
            ),
            false,
        );

        test_parse_query_to_logical_ast_helper(
            "float:2.5",
            &format!(
                "{:?}",
                Term::from_field_f64(Field::from_field_id(10u32), 2.5)
            ),
            false,
        );
    }

    #[test]
    fn test_parse_bytes() {
        test_parse_query_to_logical_ast_helper(
            "bytes:YnVidQ==",
            "Term(type=Bytes, field=12, [98, 117, 98, 117])",
            false,
        );
    }

    #[test]
    fn test_parse_bytes_not_indexed() {
        let error = parse_query_to_logical_ast("bytes_not_indexed:aaa", false).unwrap_err();
        assert!(matches!(error, QueryParserError::FieldNotIndexed(_)));
    }

    #[test]
    fn test_json_field() {
        test_parse_query_to_logical_ast_helper(
            "json.titi:hello",
            "Term(type=Json, field=14, path=titi, vtype=Str, \"hello\")",
            false,
        );
    }

    #[test]
    fn test_json_field_possibly_a_number() {
        test_parse_query_to_logical_ast_helper(
            "json.titi:5",
            r#"(Term(type=Json, field=14, path=titi, vtype=U64, 5) Term(type=Json, field=14, path=titi, vtype=Str, "5"))"#,
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "json.titi:-5",
            r#"(Term(type=Json, field=14, path=titi, vtype=I64, -5) Term(type=Json, field=14, path=titi, vtype=Str, "5"))"#, //< Yes this is a bit weird after going through the tokenizer we lose the "-".
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "json.titi:-5.2",
            r#"(Term(type=Json, field=14, path=titi, vtype=F64, -5.2) "[(0, Term(type=Json, field=14, path=titi, vtype=Str, "5")), (1, Term(type=Json, field=14, path=titi, vtype=Str, "2"))]")"#,
            true,
        );
    }

    #[test]
    fn test_json_field_possibly_a_date() {
        test_parse_query_to_logical_ast_helper(
            r#"json.date:"2019-10-12T07:20:50.52Z""#,
            r#"(Term(type=Json, field=14, path=date, vtype=Date, 2019-10-12T07:20:50Z) "[(0, Term(type=Json, field=14, path=date, vtype=Str, "2019")), (1, Term(type=Json, field=14, path=date, vtype=Str, "10")), (2, Term(type=Json, field=14, path=date, vtype=Str, "12t07")), (3, Term(type=Json, field=14, path=date, vtype=Str, "20")), (4, Term(type=Json, field=14, path=date, vtype=Str, "50")), (5, Term(type=Json, field=14, path=date, vtype=Str, "52z"))]")"#,
            true,
        );
    }

    #[test]
    fn test_json_field_not_indexed() {
        let error = parse_query_to_logical_ast("json_not_indexed.titi:hello", false).unwrap_err();
        assert!(matches!(error, QueryParserError::FieldNotIndexed(_)));
    }

    fn test_query_to_logical_ast_with_default_json(
        query: &str,
        expected: &str,
        default_conjunction: bool,
    ) {
        let mut query_parser = make_query_parser_with_default_fields(&["json"]);
        if default_conjunction {
            query_parser.set_conjunction_by_default();
        }
        let ast = query_parser.parse_query_to_logical_ast(query).unwrap();
        let ast_str = format!("{ast:?}");
        assert_eq!(ast_str, expected);
    }

    #[test]
    fn test_json_default() {
        test_query_to_logical_ast_with_default_json(
            "titi:4",
            "(Term(type=Json, field=14, path=titi, vtype=U64, 4) Term(type=Json, field=14, \
             path=titi, vtype=Str, \"4\"))",
            false,
        );
    }

    #[test]
    fn test_json_default_with_different_field() {
        for conjunction in [false, true] {
            test_query_to_logical_ast_with_default_json(
                "text:4",
                r#"Term(type=Str, field=1, "4")"#,
                conjunction,
            );
        }
    }

    #[test]
    fn test_json_default_with_same_field() {
        for conjunction in [false, true] {
            test_query_to_logical_ast_with_default_json(
                "json:4",
                r#"(Term(type=Json, field=14, path=, vtype=U64, 4) Term(type=Json, field=14, path=, vtype=Str, "4"))"#,
                conjunction,
            );
        }
    }

    #[test]
    fn test_parse_bytes_phrase() {
        test_parse_query_to_logical_ast_helper(
            "bytes:\"YnVidQ==\"",
            "Term(type=Bytes, field=12, [98, 117, 98, 117])",
            false,
        );
    }

    #[test]
    fn test_parse_bytes_invalid_base64() {
        let base64_err: QueryParserError =
            parse_query_to_logical_ast("bytes:aa", false).unwrap_err();
        assert!(matches!(base64_err, QueryParserError::ExpectedBase64(_)));
    }

    #[test]
    fn test_parse_query_to_ast_ab_c() {
        test_parse_query_to_logical_ast_helper(
            "(+title:a +title:b) title:c",
            r#"((+Term(type=Str, field=0, "a") +Term(type=Str, field=0, "b")) Term(type=Str, field=0, "c"))"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "(+title:a +title:b) title:c",
            r#"(+(+Term(type=Str, field=0, "a") +Term(type=Str, field=0, "b")) +Term(type=Str, field=0, "c"))"#,
            true,
        );
    }

    #[test]
    pub fn test_parse_query_to_ast_single_term() {
        test_parse_query_to_logical_ast_helper(
            "title:toto",
            r#"Term(type=Str, field=0, "toto")"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "+title:toto",
            r#"Term(type=Str, field=0, "toto")"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "+title:toto -titi",
            r#"(+Term(type=Str, field=0, "toto") -(Term(type=Str, field=0, "titi") Term(type=Str, field=1, "titi")))"#,
            false,
        );
    }

    #[test]
    fn test_single_negative_term() {
        assert_matches!(
            parse_query_to_logical_ast("-title:toto", false),
            Err(QueryParserError::AllButQueryForbidden)
        );
    }

    #[test]
    pub fn test_parse_query_to_ast_two_terms() {
        test_parse_query_to_logical_ast_helper(
            "title:a b",
            r#"(Term(type=Str, field=0, "a") (Term(type=Str, field=0, "b") Term(type=Str, field=1, "b")))"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            r#"title:"a b""#,
            r#""[(0, Term(type=Str, field=0, "a")), (1, Term(type=Str, field=0, "b"))]""#,
            false,
        );
    }
    #[test]
    pub fn test_parse_query_all_query() {
        let logical_ast = parse_query_to_logical_ast("*", false).unwrap();
        assert_eq!(format!("{logical_ast:?}"), "*");
    }

    #[test]
    pub fn test_parse_query_range_require_a_target_field() {
        let query_parser_error = parse_query_to_logical_ast("[A TO B]", false).err().unwrap();
        assert_eq!(
            query_parser_error.to_string(),
            "Unsupported query: Range query need to target a specific field."
        );
    }

    #[test]
    pub fn test_parse_query_to_ast_ranges() {
        test_parse_query_to_logical_ast_helper(
            "title:[a TO b]",
            r#"(Included(Term(type=Str, field=0, "a")) TO Included(Term(type=Str, field=0, "b")))"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "title:{titi TO toto}",
            r#"(Excluded(Term(type=Str, field=0, "titi")) TO Excluded(Term(type=Str, field=0, "toto")))"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "title:{* TO toto}",
            r#"(Unbounded TO Excluded(Term(type=Str, field=0, "toto")))"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "title:{titi TO *}",
            r#"(Excluded(Term(type=Str, field=0, "titi")) TO Unbounded)"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "signed:{-5 TO 3}",
            r#"(Excluded(Term(type=I64, field=2, -5)) TO Excluded(Term(type=I64, field=2, 3)))"#,
            false,
        );
        test_parse_query_to_logical_ast_helper(
            "float:{-1.5 TO 1.5}",
            r#"(Excluded(Term(type=F64, field=10, -1.5)) TO Excluded(Term(type=F64, field=10, 1.5)))"#,
            false,
        );
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
    pub fn test_query_parser_expected_facet() {
        let query_parser = make_query_parser();
        match query_parser.parse_query("facet:INVALID") {
            Ok(_) => panic!("should never succeed"),
            Err(e) => assert_eq!(
                "The facet field is malformed: Failed to parse the facet string: 'INVALID'",
                format!("{}", e)
            ),
        }
        assert!(query_parser.parse_query("facet:\"/foo/bar\"").is_ok());
    }

    #[test]
    pub fn test_query_parser_not_empty_but_no_tokens() {
        let query_parser = make_query_parser();
        assert!(query_parser.parse_query(" !, ").is_ok());
        assert!(query_parser.parse_query("with_stop_words:the").is_ok());
    }

    #[test]
    pub fn test_parse_query_single_negative_term_through_error() {
        assert_matches!(
            parse_query_to_logical_ast("-title:toto", true),
            Err(QueryParserError::AllButQueryForbidden)
        );
        assert_matches!(
            parse_query_to_logical_ast("-title:toto", false),
            Err(QueryParserError::AllButQueryForbidden)
        );
    }

    #[test]
    pub fn test_parse_query_to_ast_conjunction() {
        test_parse_query_to_logical_ast_helper(
            "title:toto",
            r#"Term(type=Str, field=0, "toto")"#,
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "+title:toto",
            r#"Term(type=Str, field=0, "toto")"#,
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "+title:toto -titi",
            r#"(+Term(type=Str, field=0, "toto") -(Term(type=Str, field=0, "titi") Term(type=Str, field=1, "titi")))"#,
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "title:a b",
            r#"(+Term(type=Str, field=0, "a") +(Term(type=Str, field=0, "b") Term(type=Str, field=1, "b")))"#,
            true,
        );
        test_parse_query_to_logical_ast_helper(
            "title:\"a b\"",
            r#""[(0, Term(type=Str, field=0, "a")), (1, Term(type=Str, field=0, "b"))]""#,
            true,
        );
    }

    #[test]
    pub fn test_query_parser_hyphen() {
        test_parse_query_to_logical_ast_helper(
            "title:www-form-encoded",
            r#""[(0, Term(type=Str, field=0, "www")), (1, Term(type=Str, field=0, "form")), (2, Term(type=Str, field=0, "encoded"))]""#,
            false,
        );
    }

    #[test]
    fn test_and_default_regardless_of_default_conjunctive() {
        for &default_conjunction in &[false, true] {
            test_parse_query_to_logical_ast_helper(
                "title:a AND title:b",
                r#"(+Term(type=Str, field=0, "a") +Term(type=Str, field=0, "b"))"#,
                default_conjunction,
            );
        }
    }

    #[test]
    fn test_or_default_conjunctive() {
        for &default_conjunction in &[false, true] {
            test_parse_query_to_logical_ast_helper(
                "title:a OR title:b",
                r#"(Term(type=Str, field=0, "a") Term(type=Str, field=0, "b"))"#,
                default_conjunction,
            );
        }
    }

    #[test]
    fn test_split_full_path() {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("second", STRING);
        schema_builder.add_text_field("first", STRING);
        schema_builder.add_text_field("first.toto", STRING);
        schema_builder.add_text_field("third.a.b.c", STRING);
        let schema = schema_builder.build();
        let query_parser = QueryParser::new(schema, Vec::new(), TokenizerManager::default());
        assert_eq!(
            query_parser.split_full_path("first.toto"),
            ("first.toto", "")
        );
        assert_eq!(
            query_parser.split_full_path("first.titi"),
            ("first", "titi")
        );
        assert_eq!(query_parser.split_full_path("third"), ("", "third"));
        assert_eq!(
            query_parser.split_full_path("hello.toto"),
            ("", "hello.toto")
        );
        assert_eq!(query_parser.split_full_path(""), ("", ""));
        assert_eq!(query_parser.split_full_path("firsty"), ("", "firsty"));
    }
}
