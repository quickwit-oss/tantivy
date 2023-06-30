use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{
    anychar, char, digit1, none_of, one_of, satisfy, space0, space1, u32,
};
use nom::combinator::{eof, map, map_res, opt, recognize, value, verify};
use nom::error::{Error, ErrorKind};
use nom::multi::{many0, many1, separated_list0, separated_list1};
use nom::sequence::{delimited, preceded, separated_pair, terminated, tuple};
use nom::IResult;

use super::user_input_ast::{UserInputAst, UserInputBound, UserInputLeaf, UserInputLiteral};
use crate::user_input_ast::Delimiter;
use crate::Occur;

// Note: '-' char is only forbidden at the beginning of a field name, would be clearer to add it to
// special characters.
const SPECIAL_CHARS: &[char] = &[
    '+', '^', '`', ':', '{', '}', '"', '[', ']', '(', ')', '!', '\\', '*', ' ',
];

/// consume a field name followed by collon. Return the field name with escape sequence
/// already interpreted
fn field_name(i: &str) -> IResult<&str, String> {
    let simple_char = none_of(SPECIAL_CHARS);
    let first_char = verify(none_of(SPECIAL_CHARS), |c| *c != '-');
    let escape_sequence = || preceded(char('\\'), one_of(SPECIAL_CHARS));

    map(
        terminated(
            tuple((
                alt((first_char, escape_sequence())),
                many0(alt((simple_char, escape_sequence(), char('\\')))),
            )),
            char(':'),
        ),
        |(first_char, next)| {
            std::iter::once(first_char)
                .chain(next.into_iter())
                .collect()
        },
    )(i)
}

/// Consume a word outside of any context.
// TODO should support escape sequences
fn word(i: &str) -> IResult<&str, &str> {
    map_res(
        recognize(tuple((
            satisfy(|c| {
                !c.is_whitespace()
                    && !['-', '^', '`', ':', '{', '}', '"', '[', ']', '(', ')'].contains(&c)
            }),
            many0(satisfy(|c: char| {
                !c.is_whitespace() && ![':', '^', '{', '}', '"', '[', ']', '(', ')'].contains(&c)
            })),
        ))),
        |s| match s {
            "OR" | "AND" | "NOT" | "IN" => Err(Error::new(i, ErrorKind::Tag)),
            _ => Ok(s),
        },
    )(i)
}

/// Consume a word inside a Range context. More values are allowed as they are
/// not ambiguous in this context.
// TODO support escape sequences
fn relaxed_word(i: &str) -> IResult<&str, &str> {
    recognize(tuple((
        satisfy(|c| !c.is_whitespace() && !['`', '{', '}', '"', '[', ']', '(', ')'].contains(&c)),
        many0(satisfy(|c: char| {
            !c.is_whitespace() && !['{', '}', '"', '[', ']', '(', ')'].contains(&c)
        })),
    )))(i)
}

fn negative_number(i: &str) -> IResult<&str, &str> {
    recognize(preceded(
        char('-'),
        tuple((digit1, opt(tuple((char('.'), digit1))))),
    ))(i)
}

fn simple_term(i: &str) -> IResult<&str, (Delimiter, String)> {
    // TODO lenient: if unfinished, assume it end at end of buffer or next `term_name:`.
    // emit message about unfinished delimiter
    let escaped_string = |delimiter| {
        // we need this because none_of can't accept an owned array of char.
        let not_delimiter = verify(anychar, move |parsed| *parsed != delimiter);
        map(
            delimited(
                char(delimiter),
                many0(alt((preceded(char('\\'), anychar), not_delimiter))),
                char(delimiter),
            ),
            |res| res.into_iter().collect::<String>(),
        )
    };

    let negative_number = map(negative_number, |number| {
        (Delimiter::None, number.to_string())
    });
    let double_quotes = map(escaped_string('"'), |phrase| {
        (Delimiter::DoubleQuotes, phrase)
    });
    let simple_quotes = map(escaped_string('\''), |phrase| {
        (Delimiter::SingleQuotes, phrase)
    });
    let text_no_delimiter = map(word, |text| (Delimiter::None, text.to_string()));

    alt((
        negative_number,
        simple_quotes,
        double_quotes,
        text_no_delimiter,
    ))(i)
}

fn term_or_phrase(i: &str) -> IResult<&str, UserInputLeaf> {
    map(
        tuple((simple_term, slop_or_prefix_val)),
        |((delimiter, phrase), (slop, prefix))| {
            UserInputLiteral {
                field_name: None,
                phrase,
                delimiter,
                slop,
                prefix,
            }
            .into()
        },
    )(i)
}

fn term_group(i: &str) -> IResult<&str, UserInputAst> {
    let occur_symbol = alt((
        value(Occur::MustNot, char('-')),
        value(Occur::Must, char('+')),
    ));

    map(
        tuple((
            terminated(field_name, space0),
            delimited(
                tuple((char('('), space0)),
                separated_list0(space1, tuple((opt(occur_symbol), term_or_phrase))),
                char(')'),
            ),
        )),
        |(field_name, terms)| {
            UserInputAst::Clause(
                terms
                    .into_iter()
                    .map(|(occur, leaf)| (occur, leaf.set_field(Some(field_name.clone())).into()))
                    .collect(),
            )
        },
    )(i)
}
// TODO term grouping should go here (ie `my_field:(+a +"bc d")`)

fn literal(i: &str) -> IResult<&str, UserInputAst> {
    alt((
        map(char('*'), |_| UserInputLeaf::All.into()),
        map(
            tuple((opt(field_name), alt((range, set, term_or_phrase)))),
            |(field_name, leaf): (Option<String>, UserInputLeaf)| leaf.set_field(field_name).into(),
        ),
        term_group,
    ))(i)
}

fn slop_or_prefix_val(i: &str) -> IResult<&str, (u32, bool)> {
    map(
        opt(alt((
            value((0, true), char('*')),
            map(preceded(char('~'), u32), |slop| (slop, false)),
        ))),
        |slop_or_prefix_opt| slop_or_prefix_opt.unwrap_or_default(),
    )(i)
}

/// Function that parses a range out of a Stream
/// Supports ranges like:
/// [5 TO 10], {5 TO 10}, [* TO 10], [10 TO *], {10 TO *], >5, <=10
/// [a TO *], [a TO c], [abc TO bcd}
fn range(i: &str) -> IResult<&str, UserInputLeaf> {
    let range_term_val = || {
        map(
            alt((negative_number, relaxed_word, tag("*"))),
            ToString::to_string,
        )
    };

    // check for unbounded range in the form of <5, <=10, >5, >=5
    let elastic_unbounded_range = map(
        tuple((
            preceded(space0, alt((tag(">="), tag("<="), tag("<"), tag(">")))),
            preceded(space0, range_term_val()),
        )),
        |(comparison_sign, bound)| match comparison_sign {
            ">=" => (UserInputBound::Inclusive(bound), UserInputBound::Unbounded),
            "<=" => (UserInputBound::Unbounded, UserInputBound::Inclusive(bound)),
            "<" => (UserInputBound::Unbounded, UserInputBound::Exclusive(bound)),
            ">" => (UserInputBound::Exclusive(bound), UserInputBound::Unbounded),
            // unreachable case
            _ => (UserInputBound::Unbounded, UserInputBound::Unbounded),
        },
    );

    let lower_bound = map(
        separated_pair(one_of("{["), space0, range_term_val()),
        |(boundary_char, lower_bound)| {
            if lower_bound == "*" {
                UserInputBound::Unbounded
            } else if boundary_char == '{' {
                UserInputBound::Exclusive(lower_bound)
            } else {
                UserInputBound::Inclusive(lower_bound)
            }
        },
    );

    let upper_bound = map(
        separated_pair(range_term_val(), space0, one_of("}]")),
        |(upper_bound, boundary_char)| {
            if upper_bound == "*" {
                UserInputBound::Unbounded
            } else if boundary_char == '}' {
                UserInputBound::Exclusive(upper_bound)
            } else {
                UserInputBound::Inclusive(upper_bound)
            }
        },
    );

    let lower_to_upper =
        separated_pair(lower_bound, tuple((space1, tag("TO"), space1)), upper_bound);

    map(
        alt((elastic_unbounded_range, lower_to_upper)),
        |(lower, upper)| UserInputLeaf::Range {
            field: None,
            lower,
            upper,
        },
    )(i)
}

fn set(i: &str) -> IResult<&str, UserInputLeaf> {
    map(
        preceded(
            tuple((space0, tag("IN"), space1)),
            delimited(
                tuple((char('['), space0)),
                separated_list0(space1, map(simple_term, |(_, term)| term)),
                char(']'),
            ),
        ),
        |elements| UserInputLeaf::Set {
            field: None,
            elements,
        },
    )(i)
}

fn negate(expr: UserInputAst) -> UserInputAst {
    expr.unary(Occur::MustNot)
}

fn leaf(i: &str) -> IResult<&str, UserInputAst> {
    alt((
        delimited(char('('), ast, char(')')),
        map(char('*'), |_| UserInputAst::from(UserInputLeaf::All)),
        map(preceded(tuple((tag("NOT"), space1)), leaf), negate),
        literal,
    ))(i)
}

fn occur_leaf(i: &str) -> IResult<&str, (Option<Occur>, UserInputAst)> {
    let occur_symbol = alt((
        value(Occur::MustNot, char('-')),
        value(Occur::Must, char('+')),
    ));

    tuple((opt(occur_symbol), boosted_leaf))(i)
}

fn positive_float_number(i: &str) -> IResult<&str, f64> {
    map(
        recognize(tuple((digit1, opt(tuple((char('.'), digit1)))))),
        // TODO this is actually dangerous if the number is actually not representable as a f64
        // (too big for instance)
        |float_str: &str| float_str.parse::<f64>().unwrap(),
    )(i)
}

fn boost(i: &str) -> IResult<&str, f64> {
    preceded(char('^'), positive_float_number)(i)
}

fn boosted_leaf(i: &str) -> IResult<&str, UserInputAst> {
    map(
        tuple((leaf, opt(boost))),
        |(leaf, boost_opt)| match boost_opt {
            Some(boost) if (boost - 1.0).abs() > f64::EPSILON => {
                UserInputAst::Boost(Box::new(leaf), boost)
            }
            _ => leaf,
        },
    )(i)
}

#[derive(Clone, Copy)]
enum BinaryOperand {
    Or,
    And,
}

fn binary_operand(i: &str) -> IResult<&str, BinaryOperand> {
    alt((
        value(BinaryOperand::And, tag("AND")),
        value(BinaryOperand::Or, tag("OR")),
    ))(i)
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

fn operand_leaf(i: &str) -> IResult<&str, (BinaryOperand, UserInputAst)> {
    tuple((
        terminated(binary_operand, space1),
        terminated(boosted_leaf, space0),
    ))(i)
}

pub fn ast(i: &str) -> IResult<&str, UserInputAst> {
    let boolean_expr = map(
        separated_pair(boosted_leaf, space1, many1(operand_leaf)),
        |(left, right)| aggregate_binary_expressions(left, right),
    );
    let whitespace_separated_leaves = map(separated_list1(space1, occur_leaf), |subqueries| {
        if subqueries.len() == 1 {
            let (occur_opt, ast) = subqueries.into_iter().next().unwrap();
            match occur_opt.unwrap_or(Occur::Should) {
                Occur::Must | Occur::Should => ast,
                Occur::MustNot => UserInputAst::Clause(vec![(Some(Occur::MustNot), ast)]),
            }
        } else {
            UserInputAst::Clause(subqueries.into_iter().collect())
        }
    });

    delimited(
        space0,
        alt((boolean_expr, whitespace_separated_leaves)),
        space0,
    )(i)
}

pub fn parse_to_ast(i: &str) -> IResult<&str, UserInputAst> {
    map(delimited(space0, opt(ast), eof), |opt_ast| {
        rewrite_ast(opt_ast.unwrap_or_else(UserInputAst::empty_query))
    })(i)
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

    // TODO test as part of occur_leaf
    // #[test]
    // fn test_occur_symbol() -> TestParseResult {
    // assert_eq!(super::occur_symbol("-")?, ("", Occur::MustNot));
    // assert_eq!(super::occur_symbol("+")?, ("", Occur::Must));
    // Ok(())
    // }

    #[test]
    fn test_positive_float_number() {
        fn valid_parse(float_str: &str, expected_val: f64, expected_remaining: &str) {
            let (remaining, val) = positive_float_number(float_str).unwrap();
            assert_eq!(remaining, expected_remaining);
            assert_nearly_equals(val, expected_val);
        }
        fn error_parse(float_str: &str) {
            assert!(positive_float_number(float_str).is_err());
        }
        valid_parse("1.0", 1.0, "");
        valid_parse("1", 1.0, "");
        valid_parse("0.234234 aaa", 0.234234f64, " aaa");
        error_parse(".3332");
        // TODO trinity-1686a: I disagree that it should fail, I think it should succeeed,
        // consuming only "1", and leave "." for the next thing (which will likely fail then)
        // error_parse("1.");
        error_parse("-1.");
    }

    #[test]
    fn test_date_time() {
        let (remaining, val) =
            relaxed_word("2015-08-02T18:54:42+02:30").expect("cannot parse date");
        assert_eq!(val, "2015-08-02T18:54:42+02:30");
        assert_eq!(remaining, "");
        // this isn't a valid date, but relaxed_word allows it.
        // assert!(date_time().parse("2015-08-02T18:54:42+02").is_err());

        let (remaining, val) = relaxed_word("2021-04-13T19:46:26.266051969+00:00")
            .expect("cannot parse fractional date");
        assert_eq!(val, "2021-04-13T19:46:26.266051969+00:00");
        assert_eq!(remaining, "");
    }

    #[track_caller]
    fn test_parse_query_to_ast_helper(query: &str, expected: &str) {
        let query = parse_to_ast(query).unwrap().1;
        let query_str = format!("{query:?}");
        assert_eq!(query_str, expected);
    }

    fn test_is_parse_err(query: &str) {
        assert!(parse_to_ast(query).is_err());
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
        assert!(parse_to_ast("NOT").is_err(),);
        test_parse_query_to_ast_helper("NOTa", "NOTa");
        test_parse_query_to_ast_helper("NOT a", "(-a)");
    }

    #[test]
    fn test_boosting() {
        assert!(parse_to_ast("a^2^3").is_err());
        assert!(parse_to_ast("a^2^").is_err());
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
        assert!(parse_to_ast("a OR b aaa").is_err());
        assert!(parse_to_ast("a AND b aaa").is_err());
        assert!(parse_to_ast("aaa a OR b ").is_err());
        assert!(parse_to_ast("aaa ccc a OR b ").is_err());
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
        let (_, (occur, ast)) = super::occur_leaf("+abc").unwrap();
        assert_eq!(occur, Some(Occur::Must));
        assert_eq!(format!("{ast:?}"), "abc");
    }

    #[test]
    fn test_field_name() {
        assert_eq!(
            super::field_name(".my.field.name:a"),
            Ok(("a", ".my.field.name".to_string()))
        );
        assert_eq!(
            super::field_name(r#"にんじん:a"#),
            Ok(("a", "にんじん".to_string()))
        );
        assert_eq!(
            super::field_name(r#"my\field:a"#),
            Ok(("a", r#"my\field"#.to_string()))
        );
        assert_eq!(
            super::field_name(r#"my\\field:a"#),
            Ok(("a", r#"my\field"#.to_string()))
        );
        assert!(super::field_name("my field:a").is_err());
        assert_eq!(
            super::field_name("\\(1\\+1\\):2"),
            Ok(("2", "(1+1)".to_string()))
        );
        assert_eq!(
            super::field_name("my_field_name:a"),
            Ok(("a", "my_field_name".to_string()))
        );
        assert_eq!(
            super::field_name("myfield.b:hello").unwrap(),
            ("hello", "myfield.b".to_string())
        );
        assert_eq!(
            super::field_name(r#"myfield\.b:hello"#).unwrap(),
            ("hello", r#"myfield\.b"#.to_string())
        );
        assert!(super::field_name("my_field_name").is_err());
        assert!(super::field_name(":a").is_err());
        assert!(super::field_name("-my_field:a").is_err());
        assert_eq!(
            super::field_name("_my_field:a"),
            Ok(("a", "_my_field".to_string()))
        );
        assert_eq!(
            super::field_name("~my~field:a"),
            Ok(("a", "~my~field".to_string()))
        );
        for special_char in SPECIAL_CHARS.iter() {
            let query = &format!("\\{special_char}my\\{special_char}field:a");
            assert_eq!(
                super::field_name(query),
                Ok(("a", format!("{special_char}my{special_char}field")))
            );
        }
    }

    #[test]
    fn test_range_parser() {
        // testing the range() parser separately
        let res = literal("title: <hello")
            .expect("Cannot parse felxible bound word")
            .1;
        let expected = UserInputLeaf::Range {
            field: Some("title".to_string()),
            lower: UserInputBound::Unbounded,
            upper: UserInputBound::Exclusive("hello".to_string()),
        }
        .into();
        let res2 = literal("title:{* TO hello}")
            .expect("Cannot parse ununbounded to word")
            .1;
        assert_eq!(res, expected);
        assert_eq!(res2, expected);

        let expected_weight = UserInputLeaf::Range {
            field: Some("weight".to_string()),
            lower: UserInputBound::Inclusive("71.2".to_string()),
            upper: UserInputBound::Unbounded,
        }
        .into();
        let res3 = literal("weight: >=71.2")
            .expect("Cannot parse flexible bound float")
            .1;
        let res4 = literal("weight:[71.2 TO *}")
            .expect("Cannot parse float to unbounded")
            .1;
        assert_eq!(res3, expected_weight);
        assert_eq!(res4, expected_weight);

        let expected_dates = UserInputLeaf::Range {
            field: Some("date_field".to_string()),
            lower: UserInputBound::Exclusive("2015-08-02T18:54:42Z".to_string()),
            upper: UserInputBound::Inclusive("2021-08-02T18:54:42+02:30".to_string()),
        }
        .into();
        let res5 = literal("date_field:{2015-08-02T18:54:42Z TO 2021-08-02T18:54:42+02:30]")
            .expect("Cannot parse date range")
            .1;
        assert_eq!(res5, expected_dates);

        let expected_flexible_dates = UserInputLeaf::Range {
            field: Some("date_field".to_string()),
            lower: UserInputBound::Unbounded,
            upper: UserInputBound::Inclusive("2021-08-02T18:54:42.12345+02:30".to_string()),
        }
        .into();

        let res6 = literal("date_field: <=2021-08-02T18:54:42.12345+02:30")
            .expect("Cannot parse date range")
            .1;
        assert_eq!(res6, expected_flexible_dates);
        // IP Range Unbounded
        let expected_weight = UserInputLeaf::Range {
            field: Some("ip".to_string()),
            lower: UserInputBound::Inclusive("::1".to_string()),
            upper: UserInputBound::Unbounded,
        }
        .into();
        let res1 = literal("ip: >=::1").expect("Cannot parse ip v6 format").1;
        let res2 = literal("ip:[::1 TO *}")
            .expect("Cannot parse ip v6 format")
            .1;
        assert_eq!(res1, expected_weight);
        assert_eq!(res2, expected_weight);

        // IP Range Bounded
        let expected_weight = UserInputLeaf::Range {
            field: Some("ip".to_string()),
            lower: UserInputBound::Inclusive("::0.0.0.50".to_string()),
            upper: UserInputBound::Exclusive("::0.0.0.52".to_string()),
        }
        .into();
        let res1 = literal("ip:[::0.0.0.50 TO ::0.0.0.52}")
            .expect("Cannot parse ip v6 format")
            .1;
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
    fn test_parse_query_term_group() {
        test_parse_query_to_ast_helper(r#"field:(abc)"#, r#"(*"field":abc)"#);
        test_parse_query_to_ast_helper(r#"field:(+a -"b c")"#, r#"(+"field":a -"field":"b c")"#);
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
        assert!(parse_to_ast("\"a b\"~").is_err());
        assert!(parse_to_ast("foo:\"a b\"~").is_err());
        assert!(parse_to_ast("\"a b\"~a").is_err());
        assert!(parse_to_ast("\"a b\"~100000000000000000").is_err());
        test_parse_query_to_ast_helper("\"a b\"^2 ~4", "(*(\"a b\")^2 *~4)");
        test_parse_query_to_ast_helper("\"a b\"~4^2", "(\"a b\"~4)^2");
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
    fn test_phrase_prefix() {
        test_parse_query_to_ast_helper("\"a b\"*", "\"a b\"*");
        test_parse_query_to_ast_helper("\"a\"*", "\"a\"*");
        test_parse_query_to_ast_helper("\"\"*", "\"\"*");
        test_parse_query_to_ast_helper("foo:\"a b\"*", "\"foo\":\"a b\"*");
        test_parse_query_to_ast_helper("foo:\"a\"*", "\"foo\":\"a\"*");
        test_parse_query_to_ast_helper("foo:\"\"*", "\"foo\":\"\"*");
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
