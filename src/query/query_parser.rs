use combine::primitives::Stream;
use combine::*;


pub struct QueryParser
{
    // split_ptn: Regex,
}

impl QueryParser {
    pub fn new() -> QueryParser {
        QueryParser {}
    }

    // pub fn parse_query() {

    // }

}

#[derive(Debug, Eq, PartialEq)]
pub enum Literal {
    WithField(String, String),
    DefaultField(String),
}

pub fn query_language<I>(input: State<I>) -> ParseResult<Vec<Literal>, I>
    where I: Stream<Item=char>
{
    let literal = || {
        let term_val = || {
            let word = many1(letter());
            let phrase =  
                (char('"'), many1(satisfy(|c| c != '"')), char('"'),)
                .map(|(_, s, _)| s);
            
            phrase.or(word)
        };

        let field = many1(letter());
        let term_query = (field, char(':'), term_val())
            .map(|(field,_, value)| Literal::WithField(field, value));
        
        let term_default_field = term_val().map(|w| Literal::DefaultField(w));
        try(term_query)
            .or(term_default_field) 
    };
    sep_by(literal(), spaces())
    .parse_state(input)
}


#[cfg(test)]
mod tests {
    
    use combine::*;
    use super::*;

    #[test]
    pub fn test_query_parser() {
        let mut query_parser = parser(query_language);
        assert_eq!(query_parser.parse("abc:toto").unwrap().0,
            vec!(Literal::WithField(String::from("abc"), String::from("toto"))));
        assert_eq!(query_parser.parse("\"some phrase query\"").unwrap().0,
            vec!(Literal::DefaultField(String::from("some phrase query"))));
        assert_eq!(query_parser.parse("field:\"some phrase query\"").unwrap().0,
            vec!(Literal::WithField(String::from("field"), String::from("some phrase query"))));
        assert_eq!(query_parser.parse("field:\"some phrase query\" field:toto a").unwrap().0,
            vec!(
                Literal::WithField(String::from("field"), String::from("some phrase query")),
                Literal::WithField(String::from("field"), String::from("toto")),
                Literal::DefaultField(String::from("a")),
            ));
    }

}