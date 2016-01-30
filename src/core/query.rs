use combine;
use combine::{between, char, letter, spaces, space, many1, parser, sep_by1, Parser, ParseError, ParserExt, combinator};
use combine::primitives::{State, Stream, ParseResult};

#[derive(Debug, PartialEq)]
pub struct Term(pub String, pub String);


#[derive(Debug, PartialEq)]
pub enum BoolQuery {
    AlwaysTrue,
    Conjunction(Vec<Term>),
}



pub fn grammar<I>(input: State<I>) -> ParseResult<Vec<Term>, I>
    where I: Stream<Item=char>
{
    let make_term = || {
        let term_field: combinator::Many1<String, _> = many1(letter());
        let term_value: combinator::Many1<String, _>  = many1(letter());
        (term_field, char(':'), term_value).map(|t| Term(t.0.clone(), t.2.clone()))
    };


    // let term_seqs = (make_term(), space(), parser(grammar::<I>),).map(|t| BoolExpr::AlwaysTrue);
    sep_by1(make_term(), space())
        .parse_state(input)
    // make_term().or(term_seqs).parse_state(input)
    //make_term()

    //
    // let word = many1(letter());
    //
    // //Creates a parser which parses a char and skips any trailing whitespace
    // let lex_char = |c| char(c).skip(spaces());
    //
    // let comma_list = sep_by(parser(expr::<I>), lex_char(','));
    // let array = between(lex_char('['), lex_char(']'), comma_list);
    //
    // //We can use tuples to run several parsers in sequence
    // //The resulting type is a tuple containing each parsers output
    // let pair = (lex_char('('),
    //             parser(expr::<I>),
    //             lex_char(','),
    //             parser(expr::<I>),
    //             lex_char(')'))
    //                .map(|t| Expr::Pair(Box::new(t.1), Box::new(t.3)));
    //
    // word.map(Expr::Id)
    //     .or(array.map(Expr::Array))
    //     .or(pair)
    //     .skip(spaces())
    //     .parse_state(input)
}

pub fn parse_query(query_str: &str) -> Result<(Vec<Term>, &str), ParseError<&str>> {
    parser(grammar).parse(query_str)
}
