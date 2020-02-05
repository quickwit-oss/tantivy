use super::user_input_ast::*;
use crate::Occur;
use combine::error::StreamError;
use combine::parser::char::{char, digit, letter, space, spaces, string};
use combine::stream::StreamErrorFor;
use combine::{
    attempt, choice, eof, many, many1, one_of, optional, parser, satisfy, skip_many1, value, Stream,
};

parser! {
    fn field[I]()(I) -> String
    where [I: Stream<Token = char>] {
        (
            letter(),
            many(satisfy(|c: char| c.is_alphanumeric() || c == '_')),
        ).skip(char(':')).map(|(s1, s2): (char, String)| format!("{}{}", s1, s2))
    }
}

parser! {
    fn word[I]()(I) -> String
    where [I: Stream<Token = char>] {
        (
            satisfy(|c: char| !c.is_whitespace() && !['-', '^', '`', ':', '{', '}', '"', '[', ']', '(',')'].contains(&c) ),
            many(satisfy(|c: char| !c.is_whitespace() && ![':', '^', '{', '}', '"', '[', ']', '(',')'].contains(&c)))
        )
        .map(|(s1, s2): (char, String)| format!("{}{}", s1, s2))
        .and_then(|s: String|
           match s.as_str() {
             "OR" => Err(StreamErrorFor::<I>::unexpected_static_message("OR")),
             "AND" => Err(StreamErrorFor::<I>::unexpected_static_message("AND")),
             "NOT" => Err(StreamErrorFor::<I>::unexpected_static_message("NOT")),
             _ => Ok(s)
           })
    }
}

parser! {
    fn literal[I]()(I) -> UserInputLeaf
    where [I: Stream<Token = char>]
    {
        let term_val = || {
            let phrase = char('"').with(many1(satisfy(|c| c != '"'))).skip(char('"'));
            phrase.or(word())
        };
        let term_val_with_field = negative_number().or(term_val());
        let term_query =
            (field(), term_val_with_field)
            .map(|(field_name, phrase)| UserInputLiteral {
                field_name: Some(field_name),
                phrase,
            });
        let term_default_field = term_val().map(|phrase| UserInputLiteral {
            field_name: None,
            phrase,
        });
        attempt(term_query)
            .or(term_default_field)
            .map(UserInputLeaf::from)
    }
}

parser! {
    fn negative_number[I]()(I) -> String
    where [I: Stream<Token = char>]
    {
        (char('-'), many1(digit()),
         optional((char('.'), many1(digit()))))
            .map(|(s1, s2, s3): (char, String, Option<(char, String)>)| {
                if let Some(('.', s3)) = s3 {
                    format!("{}{}.{}", s1, s2, s3)
                } else {
                    format!("{}{}", s1, s2)
                }
            })
    }
}

parser! {
    fn spaces1[I]()(I) -> ()
    where [I: Stream<Token = char>] {
        skip_many1(space())
    }
}

parser! {
    /// Function that parses a range out of a Stream
    /// Supports ranges like:
    /// [5 TO 10], {5 TO 10}, [* TO 10], [10 TO *], {10 TO *], >5, <=10
    /// [a TO *], [a TO c], [abc TO bcd}
    fn range[I]()(I) -> UserInputLeaf
    where [I: Stream<Token = char>] {
        let range_term_val = || {
            word().or(negative_number()).or(char('*').with(value("*".to_string())))
        };

        // check for unbounded range in the form of <5, <=10, >5, >=5
        let elastic_unbounded_range = (choice([attempt(string(">=")),
                                               attempt(string("<=")),
                                               attempt(string("<")),
                                               attempt(string(">"))])
                                       .skip(spaces()),
                                       range_term_val()).
            map(|(comparison_sign, bound): (&str, String)|
                match comparison_sign {
                    ">=" => (UserInputBound::Inclusive(bound), UserInputBound::Unbounded),
                    "<=" => (UserInputBound::Unbounded, UserInputBound::Inclusive(bound)),
                    "<" => (UserInputBound::Unbounded, UserInputBound::Exclusive(bound)),
                    ">" => (UserInputBound::Exclusive(bound), UserInputBound::Unbounded),
                    // default case
                    _ => (UserInputBound::Unbounded, UserInputBound::Unbounded)
                });
        let lower_bound = (one_of("{[".chars()), range_term_val())
            .map(|(boundary_char, lower_bound): (char, String)|
                 if lower_bound == "*" {
                     UserInputBound::Unbounded
                 } else if boundary_char == '{' {
                         UserInputBound::Exclusive(lower_bound)
                 } else {
                     UserInputBound::Inclusive(lower_bound)
                 });
        let upper_bound = (range_term_val(), one_of("}]".chars()))
            .map(|(higher_bound, boundary_char): (String, char)|
                 if higher_bound == "*" {
                     UserInputBound::Unbounded
                 } else if boundary_char == '}' {
                     UserInputBound::Exclusive(higher_bound)
                 } else {
                     UserInputBound::Inclusive(higher_bound)
                 });
         // return only lower and upper
        let lower_to_upper = (lower_bound.
                                    skip((spaces(),
                                          string("TO"),
                                          spaces())),
                                    upper_bound);

        (optional(field()).skip(spaces()),
         // try elastic first, if it matches, the range is unbounded
         attempt(elastic_unbounded_range).or(lower_to_upper))
            .map(|(field, (lower, upper))|
                 // Construct the leaf from extracted field (optional)
                 // and bounds
                 UserInputLeaf::Range {
                     field,
                     lower,
                     upper
        })
    }
}

fn negate(expr: UserInputAST) -> UserInputAST {
    expr.unary(Occur::MustNot)
}

fn must(expr: UserInputAST) -> UserInputAST {
    expr.unary(Occur::Must)
}

parser! {
    fn leaf[I]()(I) -> UserInputAST
    where [I: Stream<Token = char>] {
            char('-').with(leaf()).map(negate)
        .or(char('+').with(leaf()).map(must))
        .or(char('(').with(ast()).skip(char(')')))
        .or(char('*').map(|_| UserInputAST::from(UserInputLeaf::All)))
        .or(attempt(string("NOT").skip(spaces1()).with(leaf()).map(negate)))
        .or(attempt(range().map(UserInputAST::from)))
        .or(literal().map(UserInputAST::from))
    }
}

parser! {
    fn positive_float_number[I]()(I) -> f32
    where [I: Stream<Token = char>] {
        (
            many1(digit()),
            optional(
                (char('.'), many1(digit()))
            )
        )
        .map(|(int_part, decimal_part_opt): (String, Option<(char, String)>)| {
            let mut float_str = int_part;
            if let Some((chr, decimal_str)) = decimal_part_opt {
                float_str.push(chr);
                float_str.push_str(&decimal_str);
            }
            float_str.parse::<f32>().unwrap()
        })
    }
}

parser! {
    fn boost[I]()(I) -> f32
     where [I: Stream<Token = char>] {
         (char('^'), positive_float_number())
        .map(|(_, boost)| boost)
    }
}

parser! {
    fn boosted_leaf[I]()(I) -> UserInputAST
     where [I: Stream<Token = char>] {
         (leaf(), optional(boost()))
        .map(|(leaf, boost_opt)|
            match boost_opt {
                Some(boost) if (boost - 1.0).abs() > std::f32::EPSILON =>
                    UserInputAST::Boost(Box::new(leaf), boost),
                _ => leaf
            }
        )
    }
}

#[derive(Clone, Copy)]
enum BinaryOperand {
    Or,
    And,
}

parser! {
    fn binary_operand[I]()(I) -> BinaryOperand
    where [I: Stream<Token = char>]
    {
       string("AND").with(value(BinaryOperand::And))
       .or(string("OR").with(value(BinaryOperand::Or)))
    }
}

fn aggregate_binary_expressions(
    left: UserInputAST,
    others: Vec<(BinaryOperand, UserInputAST)>,
) -> UserInputAST {
    let mut dnf: Vec<Vec<UserInputAST>> = vec![vec![left]];
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
        UserInputAST::and(dnf.into_iter().next().unwrap()) //< safe
    } else {
        let conjunctions = dnf.into_iter().map(UserInputAST::and).collect();
        UserInputAST::or(conjunctions)
    }
}

parser! {
    pub fn ast[I]()(I) -> UserInputAST
    where [I: Stream<Token = char>]
    {
        let operand_leaf = (binary_operand().skip(spaces()), boosted_leaf().skip(spaces()));
        let boolean_expr = (boosted_leaf().skip(spaces().silent()), many1(operand_leaf)).map(
            |(left, right)| aggregate_binary_expressions(left,right));
        let whitespace_separated_leaves = many1(boosted_leaf().skip(spaces().silent()))
        .map(|subqueries: Vec<UserInputAST>|
            if subqueries.len() == 1 {
                subqueries.into_iter().next().unwrap()
            } else {
                UserInputAST::Clause(subqueries.into_iter().collect())
            });
        let expr = attempt(boolean_expr).or(whitespace_separated_leaves);
        spaces().with(expr).skip(spaces())
    }
}

parser! {
    pub fn parse_to_ast[I]()(I) -> UserInputAST
    where [I: Stream<Token = char>]
    {
        spaces().with(optional(ast()).skip(eof())).map(|opt_ast| opt_ast.unwrap_or_else(UserInputAST::empty_query))
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use combine::parser::Parser;

    pub fn nearly_equals(a: f32, b: f32) -> bool {
        (a - b).abs() < 0.0005 * (a + b).abs()
    }

    fn assert_nearly_equals(expected: f32, val: f32) {
        assert!(
            nearly_equals(val, expected),
            "Got {}, expected {}.",
            val,
            expected
        );
    }

    #[test]
    fn test_positive_float_number() {
        fn valid_parse(float_str: &str, expected_val: f32, expected_remaining: &str) {
            let (val, remaining) = positive_float_number().parse(float_str).unwrap();
            assert_eq!(remaining, expected_remaining);
            assert_nearly_equals(val, expected_val);
        }
        fn error_parse(float_str: &str) {
            assert!(positive_float_number().parse(float_str).is_err());
        }
        valid_parse("1.0", 1.0f32, "");
        valid_parse("1", 1.0f32, "");
        valid_parse("0.234234 aaa", 0.234234f32, " aaa");
        error_parse(".3332");
        error_parse("1.");
        error_parse("-1.");
    }

    fn test_parse_query_to_ast_helper(query: &str, expected: &str) {
        let query = parse_to_ast().parse(query).unwrap().0;
        let query_str = format!("{:?}", query);
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
        test_parse_query_to_ast_helper("www-form-encoded", "\"www-form-encoded\"");
        test_parse_query_to_ast_helper("www-form-encoded", "\"www-form-encoded\"");
    }

    #[test]
    fn test_parse_query_to_ast_not_op() {
        assert_eq!(
            format!("{:?}", parse_to_ast().parse("NOT")),
            "Err(UnexpectedParse)"
        );
        test_parse_query_to_ast_helper("NOTa", "\"NOTa\"");
        test_parse_query_to_ast_helper("NOT a", "-(\"a\")");
    }

    #[test]
    fn test_boosting() {
        assert!(parse_to_ast().parse("a^2^3").is_err());
        assert!(parse_to_ast().parse("a^2^").is_err());
        test_parse_query_to_ast_helper("a^3", "(\"a\")^3");
        test_parse_query_to_ast_helper("a^3 b^2", "((\"a\")^3 (\"b\")^2)");
        test_parse_query_to_ast_helper("a^1", "\"a\"");
    }

    #[test]
    fn test_parse_query_to_ast_binary_op() {
        test_parse_query_to_ast_helper("a AND b", "(+(\"a\") +(\"b\"))");
        test_parse_query_to_ast_helper("a OR b", "(?(\"a\") ?(\"b\"))");
        test_parse_query_to_ast_helper("a OR b AND c", "(?(\"a\") ?((+(\"b\") +(\"c\"))))");
        test_parse_query_to_ast_helper("a AND b         AND c", "(+(\"a\") +(\"b\") +(\"c\"))");
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
        test_parse_query_to_ast_helper("title: >a", "title:{\"a\" TO \"*\"}");
        test_parse_query_to_ast_helper("title:>=a", "title:[\"a\" TO \"*\"}");
        test_parse_query_to_ast_helper("title: <a", "title:{\"*\" TO \"a\"}");
        test_parse_query_to_ast_helper("title:<=a", "title:{\"*\" TO \"a\"]");
        test_parse_query_to_ast_helper("title:<=bsd", "title:{\"*\" TO \"bsd\"]");

        test_parse_query_to_ast_helper("weight: >70", "weight:{\"70\" TO \"*\"}");
        test_parse_query_to_ast_helper("weight:>=70", "weight:[\"70\" TO \"*\"}");
        test_parse_query_to_ast_helper("weight: <70", "weight:{\"*\" TO \"70\"}");
        test_parse_query_to_ast_helper("weight:<=70", "weight:{\"*\" TO \"70\"]");
        test_parse_query_to_ast_helper("weight: >60.7", "weight:{\"60.7\" TO \"*\"}");

        test_parse_query_to_ast_helper("weight: <= 70", "weight:{\"*\" TO \"70\"]");

        test_parse_query_to_ast_helper("weight: <= 70.5", "weight:{\"*\" TO \"70.5\"]");
    }

    #[test]
    fn test_range_parser() {
        // testing the range() parser separately
        let res = range().parse("title: <hello").unwrap().0;
        let expected = UserInputLeaf::Range {
            field: Some("title".to_string()),
            lower: UserInputBound::Unbounded,
            upper: UserInputBound::Exclusive("hello".to_string()),
        };
        let res2 = range().parse("title:{* TO hello}").unwrap().0;
        assert_eq!(res, expected);
        assert_eq!(res2, expected);
        let expected_weight = UserInputLeaf::Range {
            field: Some("weight".to_string()),
            lower: UserInputBound::Inclusive("71.2".to_string()),
            upper: UserInputBound::Unbounded,
        };

        let res3 = range().parse("weight: >=71.2").unwrap().0;
        let res4 = range().parse("weight:[71.2 TO *}").unwrap().0;
        assert_eq!(res3, expected_weight);
        assert_eq!(res4, expected_weight);
    }

    #[test]
    fn test_parse_query_to_triming_spaces() {
        test_parse_query_to_ast_helper("   abc", "\"abc\"");
        test_parse_query_to_ast_helper("abc ", "\"abc\"");
        test_parse_query_to_ast_helper("(  a OR abc)", "(?(\"a\") ?(\"abc\"))");
        test_parse_query_to_ast_helper("(a  OR abc)", "(?(\"a\") ?(\"abc\"))");
        test_parse_query_to_ast_helper("(a OR  abc)", "(?(\"a\") ?(\"abc\"))");
        test_parse_query_to_ast_helper("a OR abc ", "(?(\"a\") ?(\"abc\"))");
        test_parse_query_to_ast_helper("(a OR abc )", "(?(\"a\") ?(\"abc\"))");
        test_parse_query_to_ast_helper("(a OR  abc) ", "(?(\"a\") ?(\"abc\"))");
    }

    #[test]
    fn test_parse_query_to_ast() {
        test_parse_query_to_ast_helper("abc", "\"abc\"");
        test_parse_query_to_ast_helper("a b", "(\"a\" \"b\")");
        test_parse_query_to_ast_helper("+(a b)", "+((\"a\" \"b\"))");
        test_parse_query_to_ast_helper("+d", "+(\"d\")");
        test_parse_query_to_ast_helper("+(a b) +d", "(+((\"a\" \"b\")) +(\"d\"))");
        test_parse_query_to_ast_helper("(+a +b) d", "((+(\"a\") +(\"b\")) \"d\")");
        test_parse_query_to_ast_helper("(+a)", "+(\"a\")");
        test_parse_query_to_ast_helper("(+a +b)", "(+(\"a\") +(\"b\"))");
        test_parse_query_to_ast_helper("abc:toto", "abc:\"toto\"");
        test_parse_query_to_ast_helper("abc:1.1", "abc:\"1.1\"");
        test_parse_query_to_ast_helper("+abc:toto", "+(abc:\"toto\")");
        test_parse_query_to_ast_helper("(+abc:toto -titi)", "(+(abc:\"toto\") -(\"titi\"))");
        test_parse_query_to_ast_helper("-abc:toto", "-(abc:\"toto\")");
        test_parse_query_to_ast_helper("abc:a b", "(abc:\"a\" \"b\")");
        test_parse_query_to_ast_helper("abc:\"a b\"", "abc:\"a b\"");
        test_parse_query_to_ast_helper("foo:[1 TO 5]", "foo:[\"1\" TO \"5\"]");
        test_parse_query_to_ast_helper("[1 TO 5]", "[\"1\" TO \"5\"]");
        test_parse_query_to_ast_helper("foo:{a TO z}", "foo:{\"a\" TO \"z\"}");
        test_parse_query_to_ast_helper("foo:[1 TO toto}", "foo:[\"1\" TO \"toto\"}");
        test_parse_query_to_ast_helper("foo:[* TO toto}", "foo:{\"*\" TO \"toto\"}");
        test_parse_query_to_ast_helper("foo:[1 TO *}", "foo:[\"1\" TO \"*\"}");
        test_parse_query_to_ast_helper("foo:[1.1 TO *}", "foo:[\"1.1\" TO \"*\"}");
        test_is_parse_err("abc +    ");
    }
}
