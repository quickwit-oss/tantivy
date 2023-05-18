use combine::error::StringStreamError;
use combine::parser::char::{char, digit, space, spaces, string};
use combine::parser::combinator::recognize;
use combine::parser::range::{take_while, take_while1};
use combine::parser::repeat::escaped;
use combine::parser::Parser;
use combine::{
    any, attempt, between, choice, eof, many, many1, one_of, optional, parser, satisfy, sep_by,
    skip_many1, value,
};
use once_cell::sync::Lazy;
use regex::Regex;

use super::user_input_ast::{UserInputAst, UserInputBound, UserInputLeaf, UserInputLiteral};
use crate::user_input_ast::Delimiter;
use crate::Occur;

// Note: '-' char is only forbidden at the beginning of a field name, would be clearer to add it to
// special characters.
const SPECIAL_CHARS: &[char] = &[
    '+', '^', '`', ':', '{', '}', '"', '[', ']', '(', ')', '!', '\\', '*', ' ',
];
const ESCAPED_SPECIAL_CHARS_PATTERN: &str = r#"\\(\+|\^|`|:|\{|\}|"|\[|\]|\(|\)|!|\\|\*|\s)"#;

/// Parses a field_name
/// A field name must have at least one character and be followed by a colon.
/// All characters are allowed including special characters `SPECIAL_CHARS`, but these
/// need to be escaped with a backslash character '\'.
fn field_name<'a>() -> impl Parser<&'a str, Output = String> {
    static ESCAPED_SPECIAL_CHARS_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(ESCAPED_SPECIAL_CHARS_PATTERN).unwrap());

    recognize::<String, _, _>(escaped(
        (
            take_while1(|c| !SPECIAL_CHARS.contains(&c) && c != '-'),
            take_while(|c| !SPECIAL_CHARS.contains(&c)),
        ),
        '\\',
        satisfy(|_| true), /* if the next character is not a special char, the \ will be treated
                            * as the \ character. */
    ))
    .skip(char(':'))
    .map(|s| ESCAPED_SPECIAL_CHARS_RE.replace_all(&s, "$1").to_string())
    .and_then(|s: String| match s.is_empty() {
        true => Err(StringStreamError::UnexpectedParse),
        _ => Ok(s),
    })
}

fn word<'a>() -> impl Parser<&'a str, Output = String> {
    (
        satisfy(|c: char| {
            !c.is_whitespace()
                && !['-', '^', '`', ':', '{', '}', '"', '[', ']', '(', ')'].contains(&c)
        }),
        many(satisfy(|c: char| {
            !c.is_whitespace() && ![':', '^', '{', '}', '"', '[', ']', '(', ')'].contains(&c)
        })),
    )
        .map(|(s1, s2): (char, String)| format!("{s1}{s2}"))
        .and_then(|s: String| match s.as_str() {
            "OR" | "AND " | "NOT" => Err(StringStreamError::UnexpectedParse),
            _ => Ok(s),
        })
}

// word variant that allows more characters, e.g. for range queries that don't allow field
// specifier
fn relaxed_word<'a>() -> impl Parser<&'a str, Output = String> {
    (
        satisfy(|c: char| {
            !c.is_whitespace() && !['`', '{', '}', '"', '[', ']', '(', ')'].contains(&c)
        }),
        many(satisfy(|c: char| {
            !c.is_whitespace() && !['{', '}', '"', '[', ']', '(', ')'].contains(&c)
        })),
    )
        .map(|(s1, s2): (char, String)| format!("{s1}{s2}"))
}

/// Parses a date time according to rfc3339
/// 2015-08-02T18:54:42+02
/// 2021-04-13T19:46:26.266051969+00:00
///
/// NOTE: also accepts 999999-99-99T99:99:99.266051969+99:99
/// We delegate rejecting such invalid dates to the logical AST computation code
/// which invokes `time::OffsetDateTime::parse(..., &Rfc3339)` on the value to actually parse
/// it (instead of merely extracting the datetime value as string as done here).
fn date_time<'a>() -> impl Parser<&'a str, Output = String> {
    let two_digits = || recognize::<String, _, _>((digit(), digit()));

    // Parses a time zone
    // -06:30
    // Z
    let time_zone = {
        let utc = recognize::<String, _, _>(char('Z'));
        let offset = recognize((
            choice([char('-'), char('+')]),
            two_digits(),
            char(':'),
            two_digits(),
        ));

        utc.or(offset)
    };

    // Parses a date
    // 2010-01-30
    let date = {
        recognize::<String, _, _>((
            many1::<String, _, _>(digit()),
            char('-'),
            two_digits(),
            char('-'),
            two_digits(),
        ))
    };

    // Parses a time
    // 12:30:02
    // 19:46:26.266051969
    let time = {
        recognize::<String, _, _>((
            two_digits(),
            char(':'),
            two_digits(),
            char(':'),
            two_digits(),
            optional((char('.'), many1::<String, _, _>(digit()))),
            time_zone,
        ))
    };

    recognize((date, char('T'), time))
}

fn escaped_character<'a>() -> impl Parser<&'a str, Output = char> {
    (char('\\'), any()).map(|(_, x)| x)
}

fn escaped_string<'a>(delimiter: char) -> impl Parser<&'a str, Output = String> {
    (
        char(delimiter),
        many(choice((
            escaped_character(),
            satisfy(move |c: char| c != delimiter),
        ))),
        char(delimiter),
    )
        .map(|(_, s, _)| s)
}

fn term_val<'a>() -> impl Parser<&'a str, Output = (Delimiter, String)> {
    // TODO handle escaping of quotation marks.
    let double_quotes = escaped_string('"').map(|phrase| (Delimiter::DoubleQuotes, phrase));
    let single_quotes = escaped_string('\'').map(|phrase| (Delimiter::SingleQuotes, phrase));
    // let single_quotes =
    //     char('\'').with(many1(satisfy(|c| c != '\''))).skip(char('\'')).map(|text|
    // (Delimiter::SingleQuotes, text));
    let text_no_delimiter = word().map(|text| (Delimiter::None, text));
    negative_number()
        .map(|negative_number_str| (Delimiter::None, negative_number_str))
        .or(double_quotes)
        .or(single_quotes)
        .or(text_no_delimiter)
}

fn term_query<'a>() -> impl Parser<&'a str, Output = UserInputLiteral> {
    (field_name(), term_val(), slop_val()).map(|(field_name, (delimiter, phrase), slop)| {
        UserInputLiteral {
            field_name: Some(field_name),
            phrase,
            delimiter,
            slop,
        }
    })
}

fn slop_val<'a>() -> impl Parser<&'a str, Output = u32> {
    let slop =
        (char('~'), many1(digit())).and_then(|(_, slop): (_, String)| match slop.parse::<u32>() {
            Ok(d) => Ok(d),
            _ => Err(StringStreamError::UnexpectedParse),
        });
    optional(slop).map(|slop| match slop {
        Some(d) => d,
        _ => 0,
    })
}

fn literal<'a>() -> impl Parser<&'a str, Output = UserInputLeaf> {
    let term_default_field =
        (term_val(), slop_val()).map(|((delimiter, phrase), slop)| UserInputLiteral {
            field_name: None,
            phrase,
            delimiter,
            slop,
        });

    attempt(term_query())
        .or(term_default_field)
        .map(UserInputLeaf::from)
}

fn negative_number<'a>() -> impl Parser<&'a str, Output = String> {
    (
        char('-'),
        many1(digit()),
        optional((char('.'), many1(digit()))),
    )
        .map(|(s1, s2, s3): (char, String, Option<(char, String)>)| {
            if let Some(('.', s3)) = s3 {
                format!("{s1}{s2}.{s3}")
            } else {
                format!("{s1}{s2}")
            }
        })
}

fn spaces1<'a>() -> impl Parser<&'a str, Output = ()> {
    skip_many1(space())
}

/// Function that parses a range out of a Stream
/// Supports ranges like:
/// [5 TO 10], {5 TO 10}, [* TO 10], [10 TO *], {10 TO *], >5, <=10
/// [a TO *], [a TO c], [abc TO bcd}
fn range<'a>() -> impl Parser<&'a str, Output = UserInputLeaf> {
    let range_term_val = || {
        attempt(date_time())
            .or(negative_number())
            .or(relaxed_word())
            .or(char('*').with(value("*".to_string())))
    };

    // check for unbounded range in the form of <5, <=10, >5, >=5
    let elastic_unbounded_range = (
        choice([
            attempt(string(">=")),
            attempt(string("<=")),
            attempt(string("<")),
            attempt(string(">")),
        ])
        .skip(spaces()),
        range_term_val(),
    )
        .map(
            |(comparison_sign, bound): (&str, String)| match comparison_sign {
                ">=" => (UserInputBound::Inclusive(bound), UserInputBound::Unbounded),
                "<=" => (UserInputBound::Unbounded, UserInputBound::Inclusive(bound)),
                "<" => (UserInputBound::Unbounded, UserInputBound::Exclusive(bound)),
                ">" => (UserInputBound::Exclusive(bound), UserInputBound::Unbounded),
                // default case
                _ => (UserInputBound::Unbounded, UserInputBound::Unbounded),
            },
        );
    let lower_bound = (one_of("{[".chars()), range_term_val()).map(
        |(boundary_char, lower_bound): (char, String)| {
            if lower_bound == "*" {
                UserInputBound::Unbounded
            } else if boundary_char == '{' {
                UserInputBound::Exclusive(lower_bound)
            } else {
                UserInputBound::Inclusive(lower_bound)
            }
        },
    );
    let upper_bound = (range_term_val(), one_of("}]".chars())).map(
        |(higher_bound, boundary_char): (String, char)| {
            if higher_bound == "*" {
                UserInputBound::Unbounded
            } else if boundary_char == '}' {
                UserInputBound::Exclusive(higher_bound)
            } else {
                UserInputBound::Inclusive(higher_bound)
            }
        },
    );
    // return only lower and upper
    let lower_to_upper = (
        lower_bound.skip((spaces(), string("TO"), spaces())),
        upper_bound,
    );

    (
        optional(field_name()).skip(spaces()),
        // try elastic first, if it matches, the range is unbounded
        attempt(elastic_unbounded_range).or(lower_to_upper),
    )
        .map(|(field, (lower, upper))|
             // Construct the leaf from extracted field (optional)
             // and bounds
             UserInputLeaf::Range {
                 field,
                 lower,
                 upper
    })
}

/// Function that parses a set out of a Stream
/// Supports ranges like: `IN [val1 val2 val3]`
fn set<'a>() -> impl Parser<&'a str, Output = UserInputLeaf> {
    let term_list = between(
        char('['),
        char(']'),
        sep_by(term_val().map(|(_delimiter, text)| text), spaces()),
    );

    let set_content = ((string("IN"), spaces()), term_list).map(|(_, elements)| elements);

    (optional(attempt(field_name().skip(spaces()))), set_content)
        .map(|(field, elements)| UserInputLeaf::Set { field, elements })
}

fn negate(expr: UserInputAst) -> UserInputAst {
    expr.unary(Occur::MustNot)
}

fn leaf<'a>() -> impl Parser<&'a str, Output = UserInputAst> {
    parser(|input| {
        char('(')
            .with(ast())
            .skip(char(')'))
            .or(char('*').map(|_| UserInputAst::from(UserInputLeaf::All)))
            .or(attempt(
                string("NOT").skip(spaces1()).with(leaf()).map(negate),
            ))
            .or(attempt(range().map(UserInputAst::from)))
            .or(attempt(set().map(UserInputAst::from)))
            .or(literal().map(UserInputAst::from))
            .parse_stream(input)
            .into_result()
    })
}

fn occur_symbol<'a>() -> impl Parser<&'a str, Output = Occur> {
    char('-')
        .map(|_| Occur::MustNot)
        .or(char('+').map(|_| Occur::Must))
}

fn occur_leaf<'a>() -> impl Parser<&'a str, Output = (Option<Occur>, UserInputAst)> {
    (optional(occur_symbol()), boosted_leaf())
}

fn positive_float_number<'a>() -> impl Parser<&'a str, Output = f64> {
    (many1(digit()), optional((char('.'), many1(digit())))).map(
        |(int_part, decimal_part_opt): (String, Option<(char, String)>)| {
            let mut float_str = int_part;
            if let Some((chr, decimal_str)) = decimal_part_opt {
                float_str.push(chr);
                float_str.push_str(&decimal_str);
            }
            float_str.parse::<f64>().unwrap()
        },
    )
}

fn boost<'a>() -> impl Parser<&'a str, Output = f64> {
    (char('^'), positive_float_number()).map(|(_, boost)| boost)
}

fn boosted_leaf<'a>() -> impl Parser<&'a str, Output = UserInputAst> {
    (leaf(), optional(boost())).map(|(leaf, boost_opt)| match boost_opt {
        Some(boost) if (boost - 1.0).abs() > f64::EPSILON => {
            UserInputAst::Boost(Box::new(leaf), boost)
        }
        _ => leaf,
    })
}

#[derive(Clone, Copy)]
enum BinaryOperand {
    Or,
    And,
}

fn binary_operand<'a>() -> impl Parser<&'a str, Output = BinaryOperand> {
    string("AND")
        .with(value(BinaryOperand::And))
        .or(string("OR").with(value(BinaryOperand::Or)))
}

fn aggregate_binary_expressions(
    left: UserInputAst,
    others: Vec<(BinaryOperand, UserInputAst)>,
) -> UserInputAst {
    let mut dnf: Vec<Vec<UserInputAst>> = vec![vec![left]];
    for (operator, operand_ast) in others {
        match operator {
            BinaryOperand::And => {
                if let Some(last) = dnf.last_mut() {
                    last.push(operand_ast);
                }
            }
            BinaryOperand::Or => {
                dnf.push(vec![operand_ast]);
            }
        }
    }
    if dnf.len() == 1 {
        UserInputAst::and(dnf.into_iter().next().unwrap()) //< safe
    } else {
        let conjunctions = dnf.into_iter().map(UserInputAst::and).collect();
        UserInputAst::or(conjunctions)
    }
}

fn operand_leaf<'a>() -> impl Parser<&'a str, Output = (BinaryOperand, UserInputAst)> {
    (
        binary_operand().skip(spaces()),
        boosted_leaf().skip(spaces()),
    )
}

pub fn ast<'a>() -> impl Parser<&'a str, Output = UserInputAst> {
    let boolean_expr = (boosted_leaf().skip(spaces()), many1(operand_leaf()))
        .map(|(left, right)| aggregate_binary_expressions(left, right));
    let whitespace_separated_leaves = many1(occur_leaf().skip(spaces().silent())).map(
        |subqueries: Vec<(Option<Occur>, UserInputAst)>| {
            if subqueries.len() == 1 {
                let (occur_opt, ast) = subqueries.into_iter().next().unwrap();
                match occur_opt.unwrap_or(Occur::Should) {
                    Occur::Must | Occur::Should => ast,
                    Occur::MustNot => UserInputAst::Clause(vec![(Some(Occur::MustNot), ast)]),
                }
            } else {
                UserInputAst::Clause(subqueries.into_iter().collect())
            }
        },
    );
    let expr = attempt(boolean_expr).or(whitespace_separated_leaves);
    spaces().with(expr).skip(spaces())
}

pub fn parse_to_ast<'a>() -> impl Parser<&'a str, Output = UserInputAst> {
    spaces()
        .with(optional(ast()).skip(eof()))
        .map(|opt_ast| opt_ast.unwrap_or_else(UserInputAst::empty_query))
        .map(rewrite_ast)
}

/// Removes unnecessary children clauses in AST
///
/// Motivated by [issue #1433](https://github.com/quickwit-oss/tantivy/issues/1433)
fn rewrite_ast(mut input: UserInputAst) -> UserInputAst {
    if let UserInputAst::Clause(terms) = &mut input {
        for term in terms {
            rewrite_ast_clause(term);
        }
    }
    input
}

fn rewrite_ast_clause(input: &mut (Option<Occur>, UserInputAst)) {
    match input {
        (None, UserInputAst::Clause(ref mut clauses)) if clauses.len() == 1 => {
            *input = clauses.pop().unwrap(); // safe because clauses.len() == 1
        }
        _ => {}
    }
}

#[cfg(test)]
mod test {

    type TestParseResult = Result<(), StringStreamError>;

    use combine::parser::Parser;

    use super::*;

    pub fn nearly_equals(a: f64, b: f64) -> bool {
        (a - b).abs() < 0.0005 * (a + b).abs()
    }

    fn assert_nearly_equals(expected: f64, val: f64) {
        assert!(
            nearly_equals(val, expected),
            "Got {val}, expected {expected}."
        );
    }

    #[test]
    fn test_occur_symbol() -> TestParseResult {
        assert_eq!(super::occur_symbol().parse("-")?, (Occur::MustNot, ""));
        assert_eq!(super::occur_symbol().parse("+")?, (Occur::Must, ""));
        Ok(())
    }

    #[test]
    fn test_positive_float_number() {
        fn valid_parse(float_str: &str, expected_val: f64, expected_remaining: &str) {
            let (val, remaining) = positive_float_number().parse(float_str).unwrap();
            assert_eq!(remaining, expected_remaining);
            assert_nearly_equals(val, expected_val);
        }
        fn error_parse(float_str: &str) {
            assert!(positive_float_number().parse(float_str).is_err());
        }
        valid_parse("1.0", 1.0, "");
        valid_parse("1", 1.0, "");
        valid_parse("0.234234 aaa", 0.234234f64, " aaa");
        error_parse(".3332");
        error_parse("1.");
        error_parse("-1.");
    }

    #[test]
    fn test_date_time() {
        let (val, remaining) = date_time()
            .parse("2015-08-02T18:54:42+02:30")
            .expect("cannot parse date");
        assert_eq!(val, "2015-08-02T18:54:42+02:30");
        assert_eq!(remaining, "");
        assert!(date_time().parse("2015-08-02T18:54:42+02").is_err());

        let (val, remaining) = date_time()
            .parse("2021-04-13T19:46:26.266051969+00:00")
            .expect("cannot parse fractional date");
        assert_eq!(val, "2021-04-13T19:46:26.266051969+00:00");
        assert_eq!(remaining, "");
    }

    #[track_caller]
    fn test_parse_query_to_ast_helper(query: &str, expected: &str) {
        let query = parse_to_ast().parse(query).unwrap().0;
        let query_str = format!("{query:?}");
        assert_eq!(query_str, expected);
    }

    fn test_is_parse_err(query: &str) {
        assert!(parse_to_ast().parse(query).is_err());
    }

    #[test]
    fn test_parse_empty_to_ast() {
        test_parse_query_to_ast_helper("", "<emptyclause>");
    }

    #[test]
    fn test_parse_query_to_ast_hyphen() {
        test_parse_query_to_ast_helper("\"www-form-encoded\"", "\"www-form-encoded\"");
        test_parse_query_to_ast_helper("'www-form-encoded'", "'www-form-encoded'");
        test_parse_query_to_ast_helper("www-form-encoded", "www-form-encoded");
        test_parse_query_to_ast_helper("www-form-encoded", "www-form-encoded");
    }

    #[test]
    fn test_parse_query_to_ast_not_op() {
        assert_eq!(
            format!("{:?}", parse_to_ast().parse("NOT")),
            "Err(UnexpectedParse)"
        );
        test_parse_query_to_ast_helper("NOTa", "NOTa");
        test_parse_query_to_ast_helper("NOT a", "(-a)");
    }

    #[test]
    fn test_boosting() {
        assert!(parse_to_ast().parse("a^2^3").is_err());
        assert!(parse_to_ast().parse("a^2^").is_err());
        test_parse_query_to_ast_helper("a^3", "(a)^3");
        test_parse_query_to_ast_helper("a^3 b^2", "(*(a)^3 *(b)^2)");
        test_parse_query_to_ast_helper("a^1", "a");
    }

    #[test]
    fn test_parse_query_to_ast_binary_op() {
        test_parse_query_to_ast_helper("a AND b", "(+a +b)");
        test_parse_query_to_ast_helper("a OR b", "(?a ?b)");
        test_parse_query_to_ast_helper("a OR b AND c", "(?a ?(+b +c))");
        test_parse_query_to_ast_helper("a AND b         AND c", "(+a +b +c)");
        assert_eq!(
            format!("{:?}", parse_to_ast().parse("a OR b aaa")),
            "Err(UnexpectedParse)"
        );
        assert_eq!(
            format!("{:?}", parse_to_ast().parse("a AND b aaa")),
            "Err(UnexpectedParse)"
        );
        assert_eq!(
            format!("{:?}", parse_to_ast().parse("aaa a OR b ")),
            "Err(UnexpectedParse)"
        );
        assert_eq!(
            format!("{:?}", parse_to_ast().parse("aaa ccc a OR b ")),
            "Err(UnexpectedParse)"
        );
    }

    #[test]
    fn test_parse_elastic_query_ranges() {
        test_parse_query_to_ast_helper("title: >a", "\"title\":{\"a\" TO \"*\"}");
        test_parse_query_to_ast_helper("title:>=a", "\"title\":[\"a\" TO \"*\"}");
        test_parse_query_to_ast_helper("title: <a", "\"title\":{\"*\" TO \"a\"}");
        test_parse_query_to_ast_helper("title:<=a", "\"title\":{\"*\" TO \"a\"]");
        test_parse_query_to_ast_helper("title:<=bsd", "\"title\":{\"*\" TO \"bsd\"]");

        test_parse_query_to_ast_helper("weight: >70", "\"weight\":{\"70\" TO \"*\"}");
        test_parse_query_to_ast_helper("weight:>=70", "\"weight\":[\"70\" TO \"*\"}");
        test_parse_query_to_ast_helper("weight: <70", "\"weight\":{\"*\" TO \"70\"}");
        test_parse_query_to_ast_helper("weight:<=70", "\"weight\":{\"*\" TO \"70\"]");
        test_parse_query_to_ast_helper("weight: >60.7", "\"weight\":{\"60.7\" TO \"*\"}");

        test_parse_query_to_ast_helper("weight: <= 70", "\"weight\":{\"*\" TO \"70\"]");

        test_parse_query_to_ast_helper("weight: <= 70.5", "\"weight\":{\"*\" TO \"70.5\"]");
    }

    #[test]
    fn test_occur_leaf() {
        let ((occur, ast), _) = super::occur_leaf().parse("+abc").unwrap();
        assert_eq!(occur, Some(Occur::Must));
        assert_eq!(format!("{ast:?}"), "abc");
    }

    #[test]
    fn test_field_name() {
        assert_eq!(
            super::field_name().parse(".my.field.name:a"),
            Ok((".my.field.name".to_string(), "a"))
        );
        assert_eq!(
            super::field_name().parse(r#"にんじん:a"#),
            Ok(("にんじん".to_string(), "a"))
        );
        assert_eq!(
            super::field_name().parse(r#"my\field:a"#),
            Ok((r#"my\field"#.to_string(), "a"))
        );
        assert!(super::field_name().parse("my field:a").is_err());
        assert_eq!(
            super::field_name().parse("\\(1\\+1\\):2"),
            Ok(("(1+1)".to_string(), "2"))
        );
        assert_eq!(
            super::field_name().parse("my_field_name:a"),
            Ok(("my_field_name".to_string(), "a"))
        );
        assert_eq!(
            super::field_name().parse("myfield.b:hello").unwrap(),
            ("myfield.b".to_string(), "hello")
        );
        assert_eq!(
            super::field_name().parse(r#"myfield\.b:hello"#).unwrap(),
            (r#"myfield\.b"#.to_string(), "hello")
        );
        assert!(super::field_name().parse("my_field_name").is_err());
        assert!(super::field_name().parse(":a").is_err());
        assert!(super::field_name().parse("-my_field:a").is_err());
        assert_eq!(
            super::field_name().parse("_my_field:a"),
            Ok(("_my_field".to_string(), "a"))
        );
        assert_eq!(
            super::field_name().parse("~my~field:a"),
            Ok(("~my~field".to_string(), "a"))
        );
        for special_char in SPECIAL_CHARS.iter() {
            let query = &format!("\\{special_char}my\\{special_char}field:a");
            assert_eq!(
                super::field_name().parse(query),
                Ok((format!("{special_char}my{special_char}field"), "a"))
            );
        }
    }

    #[test]
    fn test_field_name_re() {
        let escaped_special_chars_re = Regex::new(ESCAPED_SPECIAL_CHARS_PATTERN).unwrap();
        for special_char in SPECIAL_CHARS.iter() {
            assert_eq!(
                escaped_special_chars_re.replace_all(&format!("\\{special_char}"), "$1"),
                special_char.to_string()
            );
        }
    }

    #[test]
    fn test_range_parser() {
        // testing the range() parser separately
        let res = range()
            .parse("title: <hello")
            .expect("Cannot parse felxible bound word")
            .0;
        let expected = UserInputLeaf::Range {
            field: Some("title".to_string()),
            lower: UserInputBound::Unbounded,
            upper: UserInputBound::Exclusive("hello".to_string()),
        };
        let res2 = range()
            .parse("title:{* TO hello}")
            .expect("Cannot parse ununbounded to word")
            .0;
        assert_eq!(res, expected);
        assert_eq!(res2, expected);

        let expected_weight = UserInputLeaf::Range {
            field: Some("weight".to_string()),
            lower: UserInputBound::Inclusive("71.2".to_string()),
            upper: UserInputBound::Unbounded,
        };
        let res3 = range()
            .parse("weight: >=71.2")
            .expect("Cannot parse flexible bound float")
            .0;
        let res4 = range()
            .parse("weight:[71.2 TO *}")
            .expect("Cannot parse float to unbounded")
            .0;
        assert_eq!(res3, expected_weight);
        assert_eq!(res4, expected_weight);

        let expected_dates = UserInputLeaf::Range {
            field: Some("date_field".to_string()),
            lower: UserInputBound::Exclusive("2015-08-02T18:54:42Z".to_string()),
            upper: UserInputBound::Inclusive("2021-08-02T18:54:42+02:30".to_string()),
        };
        let res5 = range()
            .parse("date_field:{2015-08-02T18:54:42Z TO 2021-08-02T18:54:42+02:30]")
            .expect("Cannot parse date range")
            .0;
        assert_eq!(res5, expected_dates);

        let expected_flexible_dates = UserInputLeaf::Range {
            field: Some("date_field".to_string()),
            lower: UserInputBound::Unbounded,
            upper: UserInputBound::Inclusive("2021-08-02T18:54:42.12345+02:30".to_string()),
        };

        let res6 = range()
            .parse("date_field: <=2021-08-02T18:54:42.12345+02:30")
            .expect("Cannot parse date range")
            .0;
        assert_eq!(res6, expected_flexible_dates);
        // IP Range Unbounded
        let expected_weight = UserInputLeaf::Range {
            field: Some("ip".to_string()),
            lower: UserInputBound::Inclusive("::1".to_string()),
            upper: UserInputBound::Unbounded,
        };
        let res1 = range()
            .parse("ip: >=::1")
            .expect("Cannot parse ip v6 format")
            .0;
        let res2 = range()
            .parse("ip:[::1 TO *}")
            .expect("Cannot parse ip v6 format")
            .0;
        assert_eq!(res1, expected_weight);
        assert_eq!(res2, expected_weight);

        // IP Range Bounded
        let expected_weight = UserInputLeaf::Range {
            field: Some("ip".to_string()),
            lower: UserInputBound::Inclusive("::0.0.0.50".to_string()),
            upper: UserInputBound::Exclusive("::0.0.0.52".to_string()),
        };
        let res1 = range()
            .parse("ip:[::0.0.0.50 TO ::0.0.0.52}")
            .expect("Cannot parse ip v6 format")
            .0;
        assert_eq!(res1, expected_weight);
    }

    #[test]
    fn test_parse_query_to_triming_spaces() {
        test_parse_query_to_ast_helper("   abc", "abc");
        test_parse_query_to_ast_helper("abc ", "abc");
        test_parse_query_to_ast_helper("(  a OR abc)", "(?a ?abc)");
        test_parse_query_to_ast_helper("(a  OR abc)", "(?a ?abc)");
        test_parse_query_to_ast_helper("(a OR  abc)", "(?a ?abc)");
        test_parse_query_to_ast_helper("a OR abc ", "(?a ?abc)");
        test_parse_query_to_ast_helper("(a OR abc )", "(?a ?abc)");
        test_parse_query_to_ast_helper("(a OR  abc) ", "(?a ?abc)");
    }

    #[test]
    fn test_parse_query_single_term() {
        test_parse_query_to_ast_helper("abc", "abc");
    }

    #[test]
    fn test_parse_query_default_clause() {
        test_parse_query_to_ast_helper("a b", "(*a *b)");
    }

    #[test]
    fn test_parse_query_must_default_clause() {
        test_parse_query_to_ast_helper("+(a b)", "(*a *b)");
    }

    #[test]
    fn test_parse_query_must_single_term() {
        test_parse_query_to_ast_helper("+d", "d");
    }

    #[test]
    fn test_single_term_with_field() {
        test_parse_query_to_ast_helper("abc:toto", "\"abc\":toto");
    }

    #[test]
    fn test_phrase_with_field() {
        test_parse_query_to_ast_helper("abc:\"happy tax payer\"", "\"abc\":\"happy tax payer\"");
        test_parse_query_to_ast_helper("abc:'happy tax payer'", "\"abc\":'happy tax payer'");
    }

    #[test]
    fn test_single_term_with_float() {
        test_parse_query_to_ast_helper("abc:1.1", "\"abc\":1.1");
        test_parse_query_to_ast_helper("a.b.c:1.1", "\"a.b.c\":1.1");
        test_parse_query_to_ast_helper("a\\ b\\ c:1.1", "\"a b c\":1.1");
    }

    #[test]
    fn test_must_clause() {
        test_parse_query_to_ast_helper("(+a +b)", "(+a +b)");
    }

    #[test]
    fn test_parse_test_query_plus_a_b_plus_d() {
        test_parse_query_to_ast_helper("+(a b) +d", "(+(*a *b) +d)");
    }

    #[test]
    fn test_parse_test_query_set() {
        test_parse_query_to_ast_helper("abc: IN [a b c]", r#""abc": IN ["a" "b" "c"]"#);
        test_parse_query_to_ast_helper("abc: IN [1]", r#""abc": IN ["1"]"#);
        test_parse_query_to_ast_helper("abc: IN []", r#""abc": IN []"#);
        test_parse_query_to_ast_helper("IN [1 2]", r#"IN ["1" "2"]"#);
    }

    #[test]
    fn test_parse_test_query_other() {
        test_parse_query_to_ast_helper("(+a +b) d", "(*(+a +b) *d)");
        test_parse_query_to_ast_helper("+abc:toto", "\"abc\":toto");
        test_parse_query_to_ast_helper("+a\\+b\\+c:toto", "\"a+b+c\":toto");
        test_parse_query_to_ast_helper("(+abc:toto -titi)", "(+\"abc\":toto -titi)");
        test_parse_query_to_ast_helper("-abc:toto", "(-\"abc\":toto)");
        test_is_parse_err("--abc:toto");
        test_parse_query_to_ast_helper("abc:a b", "(*\"abc\":a *b)");
        test_parse_query_to_ast_helper("abc:\"a b\"", "\"abc\":\"a b\"");
        test_parse_query_to_ast_helper("foo:[1 TO 5]", "\"foo\":[\"1\" TO \"5\"]");
    }

    #[test]
    fn test_parse_query_with_range() {
        test_parse_query_to_ast_helper("[1 TO 5]", "[\"1\" TO \"5\"]");
        test_parse_query_to_ast_helper("foo:{a TO z}", "\"foo\":{\"a\" TO \"z\"}");
        test_parse_query_to_ast_helper("foo:[1 TO toto}", "\"foo\":[\"1\" TO \"toto\"}");
        test_parse_query_to_ast_helper("foo:[* TO toto}", "\"foo\":{\"*\" TO \"toto\"}");
        test_parse_query_to_ast_helper("foo:[1 TO *}", "\"foo\":[\"1\" TO \"*\"}");
        test_parse_query_to_ast_helper(
            "1.2.foo.bar:[1.1 TO *}",
            "\"1.2.foo.bar\":[\"1.1\" TO \"*\"}",
        );
        test_is_parse_err("abc +    ");
    }

    #[test]
    fn test_slop() {
        assert!(parse_to_ast().parse("\"a b\"~").is_err());
        assert!(parse_to_ast().parse("foo:\"a b\"~").is_err());
        assert!(parse_to_ast().parse("\"a b\"~a").is_err());
        assert!(parse_to_ast().parse("\"a b\"~100000000000000000").is_err());
        test_parse_query_to_ast_helper("\"a b\"^2~4", "(*(\"a b\")^2 *~4)");
        test_parse_query_to_ast_helper("\"~Document\"", "\"~Document\"");
        test_parse_query_to_ast_helper("~Document", "~Document");
        test_parse_query_to_ast_helper("a~2", "a~2");
        test_parse_query_to_ast_helper("\"a b\"~0", "\"a b\"");
        test_parse_query_to_ast_helper("\"a b\"~1", "\"a b\"~1");
        test_parse_query_to_ast_helper("\"a b\"~3", "\"a b\"~3");
        test_parse_query_to_ast_helper("foo:\"a b\"~300", "\"foo\":\"a b\"~300");
        test_parse_query_to_ast_helper("\"a b\"~300^2", "(\"a b\"~300)^2");
    }

    #[test]
    fn test_not_queries_are_consistent() {
        test_parse_query_to_ast_helper("tata -toto", "(*tata -toto)");
        test_parse_query_to_ast_helper("tata NOT toto", "(*tata -toto)");
    }

    #[test]
    fn test_escaping() {
        test_parse_query_to_ast_helper(
            r#"myfield:"hello\"happy\'tax""#,
            r#""myfield":"hello"happy'tax""#,
        );
        test_parse_query_to_ast_helper(
            r#"myfield:'hello\"happy\'tax'"#,
            r#""myfield":'hello"happy'tax'"#,
        );
    }
}
