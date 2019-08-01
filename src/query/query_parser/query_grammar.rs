use super::query_grammar;
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
        ).map(|(s1, s2): (char, String)| format!("{}{}", s1, s2))
    }
}

parser! {
    fn word[I]()(I) -> String
    where [I: Stream<Item = char>] {
        (
            satisfy(|c: char| c.is_alphanumeric()),
            many(satisfy(|c: char| !c.is_whitespace() && ![':', '{', '}', '"', '[', ']', '(',')'].contains(&c)))
        )
        .map(|(s1, s2): (char, String)| format!("{}{}", s1, s2))
        .and_then(|s: String| {
           match s.as_str() {
             "OR" => Err(StreamErrorFor::<I>::unexpected_static_message("OR")),
             "AND" => Err(StreamErrorFor::<I>::unexpected_static_message("AND")),
             "NOT" => Err(StreamErrorFor::<I>::unexpected_static_message("NOT")),
             _ => Ok(s)
           }
        })
    }
}

parser! {
    fn literal[I]()(I) -> UserInputLeaf
    where [I: Stream<Item = char>]
    {
        let term_val = || {
            let phrase = (char('"'), many1(satisfy(|c| c != '"')), char('"')).map(|(_, s, _)| s);
            phrase.or(word())
        };
        let term_val_with_field = negative_number().or(term_val());
        let term_query =
            (field(), char(':'), term_val_with_field).map(|(field_name, _, phrase)| UserInputLiteral {
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
        let term_val = || {
            word().or(negative_number()).or(char('*').map(|_| "*".to_string()))
        };
        let lower_bound = {
            let excl = (char('{'), term_val()).map(|(_, w)| UserInputBound::Exclusive(w));
            let incl = (char('['), term_val()).map(|(_, w)| UserInputBound::Inclusive(w));
            attempt(excl).or(incl)
        };
        let upper_bound = {
            let excl = (term_val(), char('}')).map(|(w, _)| UserInputBound::Exclusive(w));
            let incl = (term_val(), char(']')).map(|(w, _)| UserInputBound::Inclusive(w));
            attempt(excl).or(incl)
        };
        (
            optional((field(), char(':')).map(|x| x.0)),
            lower_bound,
            spaces(),
            string("TO"),
            spaces(),
            upper_bound,
        ).map(|(field, lower, _, _, _, upper)| UserInputLeaf::Range {
                field,
                lower,
                upper
        })
    }
}

parser! {
    fn leaf[I]()(I) -> UserInputAST
    where [I: Stream<Item = char>] {
         (char('-'), leaf()).map(|(_, expr)| expr.unary(Occur::MustNot) )
        .or((char('+'), leaf()).map(|(_, expr)| expr.unary(Occur::Must) ))
        .or((char('('), parse_to_ast(), char(')')).map(|(_, expr, _)| expr))
        .or(char('*').map(|_| UserInputAST::from(UserInputLeaf::All) ))
        .or(attempt(
            (string("NOT"), spaces1(), leaf()).map(|(_, _, expr)| expr.unary(Occur::MustNot))
            )
         )
        .or(attempt(
            range().map(UserInputAST::from)))
        .or(literal().map(|leaf| UserInputAST::Leaf(Box::new(leaf))))
    }
}

enum BinaryOperand {
    Or,
    And,
}

parser! {
    fn binary_operand[I]()(I) -> BinaryOperand
    where [I: Stream<Item = char>] {
        (spaces1(),
         (
            string("AND").map(|_| BinaryOperand::And)
           .or(string("OR").map(|_| BinaryOperand::Or))
         ),
         spaces1()).map(|(_, op,_)| op)
    }
}

enum Element {
    SingleEl(UserInputAST),
    NormalDisjunctive(Vec<Vec<UserInputAST>>),
}

impl Element {
    pub fn into_dnf(self) -> Vec<Vec<UserInputAST>> {
        match self {
            Element::NormalDisjunctive(conjunctions) => conjunctions,
            Element::SingleEl(el) => vec![vec![el]],
        }
    }
}

parser! {
    pub fn parse_to_ast[I]()(I) -> UserInputAST
    where [I: Stream<Item = char>]
    {
        (
            attempt(
                chainl1(
                    leaf().map(Element::SingleEl),
                    binary_operand().map(|op: BinaryOperand|
                        move |left: Element, right: Element| {
                            let mut dnf = left.into_dnf();
                            if let Element::SingleEl(el) = right {
                                match op {
                                    BinaryOperand::And => {
                                        if let Some(last) = dnf.last_mut() {
                                            last.push(el);
                                        }
                                    }
                                    BinaryOperand::Or => {
                                        dnf.push(vec!(el));
                                    }
                                }
                            } else {
                                unreachable!("Please report.")
                            }
                            Element::NormalDisjunctive(dnf)
                        }
                    )
                )
                .map(query_grammar::Element::into_dnf)
                .map(|fnd| {
                    if fnd.len() == 1 {
                        UserInputAST::and(fnd.into_iter().next().unwrap()) //< safe
                    } else {
                        let conjunctions = fnd
                        .into_iter()
                        .map(UserInputAST::and)
                        .collect();
                        UserInputAST::or(conjunctions)
                    }
                })
            )
            .or(
                sep_by(leaf(), spaces())
                .map(|subqueries: Vec<UserInputAST>| {
                    if subqueries.len() == 1 {
                        subqueries.into_iter().next().unwrap()
                    } else {
                        UserInputAST::Clause(subqueries.into_iter().collect())
                    }
                })
            )
        )

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
    fn test_parse_query_to_ast() {
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
        test_is_parse_err("abc +    ");
    }

    #[test]
    fn test_parse_query_to_ast_range() {
        test_parse_query_to_ast_helper("foo:[1 TO 5]", "foo:[\"1\" TO \"5\"]");
        test_parse_query_to_ast_helper("[1 TO 5]", "[\"1\" TO \"5\"]");
        test_parse_query_to_ast_helper("foo:{a TO z}", "foo:{\"a\" TO \"z\"}");
        test_parse_query_to_ast_helper("foo:[1 TO toto}", "foo:[\"1\" TO \"toto\"}");
        test_parse_query_to_ast_helper("foo:[* TO toto}", "foo:[\"*\" TO \"toto\"}");
        test_parse_query_to_ast_helper("foo:[1 TO *}", "foo:[\"1\" TO \"*\"}");
        test_parse_query_to_ast_helper("foo:[1.1 TO *}", "foo:[\"1.1\" TO \"*\"}");
    }
}
