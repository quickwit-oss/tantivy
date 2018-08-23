use super::user_input_ast::*;
use combine::char::*;
use combine::*;
use query::query_parser::user_input_ast::UserInputBound;

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
        many1(satisfy(|c: char| c.is_alphanumeric()))
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
        try(term_query)
            .or(term_default_field)
            .map(UserInputLeaf::from)
    }
}

parser! {
    fn negative_number[I]()(I) -> String
    where [I: Stream<Item = char>]
    {
            (char('-'), many1(satisfy(|c: char| c.is_numeric())))
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
            try(excl).or(incl)
        };
        let upper_bound = {
            let excl = (term_val(), char('}')).map(|(w, _)| UserInputBound::Exclusive(w));
            let incl = (term_val(), char(']')).map(|(w, _)| UserInputBound::Inclusive(w));
            try(excl).or(incl)
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
         (char('-'), leaf()).map(|(_, expr)| UserInputAST::Not(Box::new(expr)))
        .or((char('+'), leaf()).map(|(_, expr)| UserInputAST::Must(Box::new(expr))))
        .or((char('('), parse_to_ast(), char(')')).map(|(_, expr, _)| expr))
        .or(char('*').map(|_| UserInputAST::Leaf(Box::new(UserInputLeaf::All)) ))
        .or(
            try(
                range()
                .map(|leaf| UserInputAST::Leaf(Box::new(leaf)))
            )
        )
        .or(literal().map(|leaf| UserInputAST::Leaf(Box::new(leaf))))
    }
}

enum BinaryOperand {
    Or, And
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
    NormalDisjunctive(Vec<Vec<UserInputAST>>)
}

impl Element {
    pub fn into_dnf(self) -> Vec<Vec<UserInputAST>> {
        match self {
            Element::NormalDisjunctive(conjunctions) =>
                conjunctions,
            Element::SingleEl(el) =>
                vec!(vec!(el)),
        }
    }
}

parser! {
    pub fn parse_to_ast[I]()(I) -> UserInputAST
    where [I: Stream<Item = char>]
    {
        (
            try(
                chainl1(
                    leaf().map(|el| Element::SingleEl(el)),
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
                .map(|el| el.into_dnf())
                .map(|fnd| {
                    let conjunctions = fnd
                        .into_iter()
                        .map(|conjunction| {
                            UserInputAST::Clause(conjunction
                                .into_iter()
                                .map(|ast: UserInputAST| {
                                    UserInputAST::Must(Box::new(ast))
                                })
                                .collect::<Vec<_>>()
                           )
                        })
                        .map(|conjunction_ast| UserInputAST::Should(Box::new(conjunction_ast)))
                        .collect();
                    UserInputAST::Clause(conjunctions)
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
    fn test_parse_query_to_ast() {
        test_parse_query_to_ast_helper("+(a b) +d", "(+((\"a\" \"b\")) +(\"d\"))");
        test_parse_query_to_ast_helper("(+a +b) d", "((+(\"a\") +(\"b\")) \"d\")");
        test_parse_query_to_ast_helper("(+a)", "+(\"a\")");
        test_parse_query_to_ast_helper("(+a +b)", "(+(\"a\") +(\"b\"))");
        test_parse_query_to_ast_helper("abc:toto", "abc:\"toto\"");
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
        test_is_parse_err("abc +    ");
    }
}
