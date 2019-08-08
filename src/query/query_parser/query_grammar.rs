use super::user_input_ast::*;
use crate::query::occur::Occur;
use crate::query::query_parser::user_input_ast::UserInputBound;
use combine::char::*;
use combine::error::StreamError;
use combine::stream::StreamErrorFor;
use combine::*;

parser! {
    fn field[I]()(I) -> String
    where [I: Stream<Item = char>] {
        (
            letter(),
            many(satisfy(|c: char| c.is_alphanumeric() || c == '_')),
        ).skip(char(':')).map(|(s1, s2): (char, String)| format!("{}{}", s1, s2))
    }
}

parser! {
    fn word[I]()(I) -> String
    where [I: Stream<Item = char>] {
        (
            satisfy(|c: char| !c.is_whitespace() && !['-', '`', ':', '{', '}', '"', '[', ']', '(',')'].contains(&c) ),
            many(satisfy(|c: char| !c.is_whitespace() && ![':', '{', '}', '"', '[', ']', '(',')'].contains(&c)))
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
    where [I: Stream<Item = char>]
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
    where [I: Stream<Item = char>]
    {
        (char('-'), many1(satisfy(char::is_numeric)))
            .map(|(s1, s2): (char, String)| format!("{}{}", s1, s2))
    }
}

parser! {
    fn spaces1[I]()(I) -> ()
    where [I: Stream<Item = char>] {
        skip_many1(space())
    }
}

parser! {
    fn range[I]()(I) -> UserInputLeaf
    where [I: Stream<Item = char>] {
        let range_term_val = || {
            word().or(negative_number()).or(char('*').with(value("*".to_string())))
        };
        let lower_bound = (one_of("{[".chars()), range_term_val())
            .map(|(boundary_char, lower_bound): (char, String)|
                if boundary_char == '{' { UserInputBound::Exclusive(lower_bound) }
                else { UserInputBound::Inclusive(lower_bound) });
        let upper_bound = (range_term_val(), one_of("}]".chars()))
            .map(|(higher_bound, boundary_char): (String, char)|
                if boundary_char == '}' { UserInputBound::Exclusive(higher_bound) }
                else { UserInputBound::Inclusive(higher_bound) });
        (
            optional(field()),
            lower_bound
            .skip((spaces(), string("TO"), spaces())),
            upper_bound,
        ).map(|(field, lower, upper)| UserInputLeaf::Range {
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
    where [I: Stream<Item = char>] {
            char('-').with(leaf()).map(negate)
        .or(char('+').with(leaf()).map(must))
        .or(char('(').with(ast()).skip(char(')')))
        .or(char('*').map(|_| UserInputAST::from(UserInputLeaf::All)))
        .or(attempt(string("NOT").skip(spaces1()).with(leaf()).map(negate)))
        .or(attempt(range().map(UserInputAST::from)))
        .or(literal().map(UserInputAST::from))
    }
}

#[derive(Clone, Copy)]
enum BinaryOperand {
    Or,
    And,
}

parser! {
    fn binary_operand[I]()(I) -> BinaryOperand
    where [I: Stream<Item = char>]
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
    where [I: Stream<Item = char>]
    {
        let operand_leaf = (binary_operand().skip(spaces()), leaf().skip(spaces()));
        let boolean_expr = (leaf().skip(spaces().silent()), many1(operand_leaf)).map(
            |(left, right)| aggregate_binary_expressions(left,right));
        let whitespace_separated_leaves = many1(leaf().skip(spaces().silent()))
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
    where [I: Stream<Item = char>]
    {
        spaces().with(optional(ast()).skip(eof())).map(|opt_ast| opt_ast.unwrap_or(UserInputAST::empty_query()))
    }
}

#[cfg(test)]
mod test {

    use super::*;

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
        test_parse_query_to_ast_helper("foo:[* TO toto}", "foo:[\"*\" TO \"toto\"}");
        test_parse_query_to_ast_helper("foo:[1 TO *}", "foo:[\"1\" TO \"*\"}");
        test_parse_query_to_ast_helper("foo:[1.1 TO *}", "foo:[\"1.1\" TO \"*\"}");
        test_is_parse_err("abc +    ");
    }
}
