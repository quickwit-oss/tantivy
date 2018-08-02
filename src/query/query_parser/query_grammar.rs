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
    fn literal[I]()(I) -> UserInputAST
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
            .map(UserInputAST::from)
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
    fn range[I]()(I) -> UserInputAST
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
        ).map(|(field, lower, _, _, _, upper)| UserInputAST::Range {
                field,
                lower,
                upper
        })
    }
}

parser! {
    fn leaf[I]()(I) -> UserInputAST
    where [I: Stream<Item = char>] {
         (char('-'), leaf())
        .map(|(_, expr)| UserInputAST::Not(Box::new(expr)))
        .or((char('+'), leaf()).map(|(_, expr)| UserInputAST::Must(Box::new(expr))))
        .or((char('('), parse_to_ast(), char(')')).map(|(_, expr, _)| expr))
        .or(char('*').map(|_| UserInputAST::All))
        .or(try(range()))
        .or(literal())
    }
}

parser! {
    pub fn parse_to_ast[I]()(I) -> UserInputAST
    where [I: Stream<Item = char>]
    {
        sep_by(leaf(), spaces())
        .map(|subqueries: Vec<UserInputAST>| {
            if subqueries.len() == 1 {
                subqueries.into_iter().next().unwrap()
            } else {
                UserInputAST::Clause(subqueries.into_iter().map(Box::new).collect())
            }
        })
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
