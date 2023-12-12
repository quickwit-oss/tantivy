use std::iter::once;

use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::{
    anychar, char, digit1, none_of, one_of, satisfy, space0, space1, u32,
};
use nom::combinator::{eof, map, map_res, opt, peek, recognize, value, verify};
use nom::error::{Error, ErrorKind};
use nom::multi::{many0, many1, separated_list0, separated_list1};
use nom::sequence::{delimited, preceded, separated_pair, terminated, tuple};
use nom::IResult;

use super::user_input_ast::{UserInputAst, UserInputBound, UserInputLeaf, UserInputLiteral};
use crate::infallible::*;
use crate::user_input_ast::Delimiter;
use crate::Occur;

// Note: '-' char is only forbidden at the beginning of a field name, would be clearer to add it to
// special characters.
const SPECIAL_CHARS: &[char] = &[
    '+', '^', '`', ':', '{', '}', '"', '[', ']', '(', ')', '!', '\\', '*', ' ',
];

/// consume a field name followed by colon. Return the field name with escape sequence
/// already interpreted
fn field_name(inp: &str) -> IResult<&str, String> {
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
        |(first_char, next)| once(first_char).chain(next).collect(),
    )(inp)
}

/// Consume a word outside of any context.
// TODO should support escape sequences
fn word(inp: &str) -> IResult<&str, &str> {
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
            "OR" | "AND" | "NOT" | "IN" => Err(Error::new(inp, ErrorKind::Tag)),
            _ => Ok(s),
        },
    )(inp)
}

fn word_infallible(delimiter: &str) -> impl Fn(&str) -> JResult<&str, Option<&str>> + '_ {
    |inp| {
        opt_i_err(
            preceded(
                space0,
                recognize(many1(satisfy(|c| {
                    !c.is_whitespace() && !delimiter.contains(c)
                }))),
            ),
            "expected word",
        )(inp)
    }
}

/// Consume a word inside a Range context. More values are allowed as they are
/// not ambiguous in this context.
fn relaxed_word(inp: &str) -> IResult<&str, &str> {
    recognize(tuple((
        satisfy(|c| !c.is_whitespace() && !['`', '{', '}', '"', '[', ']', '(', ')'].contains(&c)),
        many0(satisfy(|c: char| {
            !c.is_whitespace() && !['{', '}', '"', '[', ']', '(', ')'].contains(&c)
        })),
    )))(inp)
}

fn negative_number(inp: &str) -> IResult<&str, &str> {
    recognize(preceded(
        char('-'),
        tuple((digit1, opt(tuple((char('.'), digit1))))),
    ))(inp)
}

fn simple_term(inp: &str) -> IResult<&str, (Delimiter, String)> {
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
    ))(inp)
}

fn simple_term_infallible(
    delimiter: &str,
) -> impl Fn(&str) -> JResult<&str, Option<(Delimiter, String)>> + '_ {
    |inp| {
        let escaped_string = |delimiter| {
            // we need this because none_of can't accept an owned array of char.
            let not_delimiter = verify(anychar, move |parsed| *parsed != delimiter);
            map(
                delimited_infallible(
                    nothing,
                    opt_i(many0(alt((preceded(char('\\'), anychar), not_delimiter)))),
                    opt_i_err(char(delimiter), format!("missing delimiter \\{delimiter}")),
                ),
                |(res, err)| {
                    // many0 can't fail
                    (res.unwrap().into_iter().collect::<String>(), err)
                },
            )
        };

        let double_quotes = map(escaped_string('"'), |(phrase, errors)| {
            (Some((Delimiter::DoubleQuotes, phrase)), errors)
        });
        let simple_quotes = map(escaped_string('\''), |(phrase, errors)| {
            (Some((Delimiter::SingleQuotes, phrase)), errors)
        });

        alt_infallible(
            (
                (value((), char('"')), double_quotes),
                (value((), char('\'')), simple_quotes),
            ),
            // numbers are parsed with words in this case, as we allow string starting with a -
            map(word_infallible(delimiter), |(text, errors)| {
                (text.map(|text| (Delimiter::None, text.to_string())), errors)
            }),
        )(inp)
    }
}

fn term_or_phrase(inp: &str) -> IResult<&str, UserInputLeaf> {
    map(
        tuple((simple_term, fallible(slop_or_prefix_val))),
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
    )(inp)
}

fn term_or_phrase_infallible(inp: &str) -> JResult<&str, Option<UserInputLeaf>> {
    map(
        // ~* for slop/prefix, ) inside group or ast tree, ^ if boost
        tuple_infallible((simple_term_infallible(")^"), slop_or_prefix_val)),
        |((delimiter_phrase, (slop, prefix)), errors)| {
            let leaf = if let Some((delimiter, phrase)) = delimiter_phrase {
                Some(
                    UserInputLiteral {
                        field_name: None,
                        phrase,
                        delimiter,
                        slop,
                        prefix,
                    }
                    .into(),
                )
            } else if slop != 0 {
                Some(
                    UserInputLiteral {
                        field_name: None,
                        phrase: "".to_string(),
                        delimiter: Delimiter::None,
                        slop,
                        prefix,
                    }
                    .into(),
                )
            } else {
                None
            };
            (leaf, errors)
        },
    )(inp)
}

fn term_group(inp: &str) -> IResult<&str, UserInputAst> {
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
    )(inp)
}

// this is a precondition for term_group_infallible. Without it, term_group_infallible can fail
// with a panic. It does not consume its input.
fn term_group_precond(inp: &str) -> IResult<&str, (), ()> {
    value(
        (),
        peek(tuple((
            field_name,
            space0,
            char('('), // when we are here, we know it can't be anything but a term group
        ))),
    )(inp)
    .map_err(|e| e.map(|_| ()))
}

fn term_group_infallible(inp: &str) -> JResult<&str, UserInputAst> {
    let (mut inp, (field_name, _, _, _)) =
        tuple((field_name, space0, char('('), space0))(inp).expect("precondition failed");

    let mut terms = Vec::new();
    let mut errs = Vec::new();

    let mut first_round = true;
    loop {
        let mut space_error = if first_round {
            first_round = false;
            Vec::new()
        } else {
            let (rest, (_, err)) = space1_infallible(inp)?;
            inp = rest;
            err
        };
        if inp.is_empty() {
            errs.push(LenientErrorInternal {
                pos: inp.len(),
                message: "missing )".to_string(),
            });
            break Ok((inp, (UserInputAst::Clause(terms), errs)));
        }
        if let Some(inp) = inp.strip_prefix(')') {
            break Ok((inp, (UserInputAst::Clause(terms), errs)));
        }
        // only append missing space error if we did not reach the end of group
        errs.append(&mut space_error);

        // here we do the assumption term_or_phrase_infallible always consume something if the
        // first byte is not `)` or ' '. If it did not, we would end up looping.

        let (rest, ((occur, leaf), mut err)) =
            tuple_infallible((occur_symbol, term_or_phrase_infallible))(inp)?;
        errs.append(&mut err);
        if let Some(leaf) = leaf {
            terms.push((occur, leaf.set_field(Some(field_name.clone())).into()));
        }
        inp = rest;
    }
}

fn exists(inp: &str) -> IResult<&str, UserInputLeaf> {
    value(
        UserInputLeaf::Exists {
            field: String::new(),
        },
        tuple((space0, char('*'))),
    )(inp)
}

fn exists_precond(inp: &str) -> IResult<&str, (), ()> {
    value(
        (),
        peek(tuple((
            field_name,
            space0,
            char('*'), // when we are here, we know it can't be anything but a exists
        ))),
    )(inp)
    .map_err(|e| e.map(|_| ()))
}

fn exists_infallible(inp: &str) -> JResult<&str, UserInputAst> {
    let (inp, (field_name, _, _)) =
        tuple((field_name, space0, char('*')))(inp).expect("precondition failed");

    let exists = UserInputLeaf::Exists { field: field_name }.into();
    Ok((inp, (exists, Vec::new())))
}

fn literal(inp: &str) -> IResult<&str, UserInputAst> {
    // * alone is already parsed by our caller, so if `exists` succeed, we can be confident
    // something (a field name) got parsed before
    alt((
        map(
            tuple((opt(field_name), alt((range, set, exists, term_or_phrase)))),
            |(field_name, leaf): (Option<String>, UserInputLeaf)| leaf.set_field(field_name).into(),
        ),
        term_group,
    ))(inp)
}

fn literal_no_group_infallible(inp: &str) -> JResult<&str, Option<UserInputAst>> {
    map(
        tuple_infallible((
            opt_i(field_name),
            space0_infallible,
            alt_infallible(
                (
                    (
                        value((), tuple((tag("IN"), space0, char('[')))),
                        map(set_infallible, |(set, errs)| (Some(set), errs)),
                    ),
                    (
                        value((), peek(one_of("{[><"))),
                        map(range_infallible, |(range, errs)| (Some(range), errs)),
                    ),
                ),
                delimited_infallible(space0_infallible, term_or_phrase_infallible, nothing),
            ),
        )),
        |((field_name, _, leaf), mut errors)| {
            (
                leaf.map(|leaf| {
                    if matches!(&leaf, UserInputLeaf::Literal(literal)
                            if literal.phrase.contains(':') && literal.delimiter == Delimiter::None)
                        && field_name.is_none()
                    {
                        errors.push(LenientErrorInternal {
                            pos: inp.len(),
                            message: "parsed possible invalid field as term".to_string(),
                        });
                    }
                    if matches!(&leaf, UserInputLeaf::Literal(literal)
                            if literal.phrase == "NOT" && literal.delimiter == Delimiter::None)
                        && field_name.is_none()
                    {
                        errors.push(LenientErrorInternal {
                            pos: inp.len(),
                            message: "parsed keyword NOT as term. It should be quoted".to_string(),
                        });
                    }
                    leaf.set_field(field_name).into()
                }),
                errors,
            )
        },
    )(inp)
}

fn literal_infallible(inp: &str) -> JResult<&str, Option<UserInputAst>> {
    alt_infallible(
        (
            (
                term_group_precond,
                map(term_group_infallible, |(group, errs)| (Some(group), errs)),
            ),
            (
                exists_precond,
                map(exists_infallible, |(exists, errs)| (Some(exists), errs)),
            ),
        ),
        literal_no_group_infallible,
    )(inp)
}

fn slop_or_prefix_val(inp: &str) -> JResult<&str, (u32, bool)> {
    map(
        opt_i(alt((
            value((0, true), char('*')),
            map(preceded(char('~'), u32), |slop| (slop, false)),
        ))),
        |(slop_or_prefix_opt, err)| (slop_or_prefix_opt.unwrap_or_default(), err),
    )(inp)
}

/// Function that parses a range out of a Stream
/// Supports ranges like:
/// [5 TO 10], {5 TO 10}, [* TO 10], [10 TO *], {10 TO *], >5, <=10
/// [a TO *], [a TO c], [abc TO bcd}
fn range(inp: &str) -> IResult<&str, UserInputLeaf> {
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
    )(inp)
}

fn range_infallible(inp: &str) -> JResult<&str, UserInputLeaf> {
    let lower_to_upper = map(
        tuple_infallible((
            opt_i(anychar),
            space0_infallible,
            word_infallible("]}"),
            space1_infallible,
            opt_i_err(
                terminated(tag("TO"), alt((value((), space1), value((), eof)))),
                "missing keyword TO",
            ),
            word_infallible("]}"),
            opt_i_err(one_of("]}"), "missing range delimiter"),
        )),
        |((lower_bound_kind, _space0, lower, _space1, to, upper, upper_bound_kind), errs)| {
            let lower_bound = match (lower_bound_kind, lower) {
                (_, Some("*")) => UserInputBound::Unbounded,
                (_, None) => UserInputBound::Unbounded,
                // if it is some, TO was actually the bound (i.e. [TO TO something])
                (_, Some("TO")) if to.is_none() => UserInputBound::Unbounded,
                (Some('['), Some(bound)) => UserInputBound::Inclusive(bound.to_string()),
                (Some('{'), Some(bound)) => UserInputBound::Exclusive(bound.to_string()),
                _ => unreachable!("precondition failed, range did not start with [ or {{"),
            };
            let upper_bound = match (upper_bound_kind, upper) {
                (_, Some("*")) => UserInputBound::Unbounded,
                (_, None) => UserInputBound::Unbounded,
                (Some(']'), Some(bound)) => UserInputBound::Inclusive(bound.to_string()),
                (Some('}'), Some(bound)) => UserInputBound::Exclusive(bound.to_string()),
                // the end is missing, assume this is an inclusive bound
                (_, Some(bound)) => UserInputBound::Inclusive(bound.to_string()),
            };
            ((lower_bound, upper_bound), errs)
        },
    );

    map(
        alt_infallible(
            (
                (
                    value((), tag(">=")),
                    map(word_infallible(""), |(bound, err)| {
                        (
                            (
                                bound
                                    .map(|bound| UserInputBound::Inclusive(bound.to_string()))
                                    .unwrap_or(UserInputBound::Unbounded),
                                UserInputBound::Unbounded,
                            ),
                            err,
                        )
                    }),
                ),
                (
                    value((), tag("<=")),
                    map(word_infallible(""), |(bound, err)| {
                        (
                            (
                                UserInputBound::Unbounded,
                                bound
                                    .map(|bound| UserInputBound::Inclusive(bound.to_string()))
                                    .unwrap_or(UserInputBound::Unbounded),
                            ),
                            err,
                        )
                    }),
                ),
                (
                    value((), tag(">")),
                    map(word_infallible(""), |(bound, err)| {
                        (
                            (
                                bound
                                    .map(|bound| UserInputBound::Exclusive(bound.to_string()))
                                    .unwrap_or(UserInputBound::Unbounded),
                                UserInputBound::Unbounded,
                            ),
                            err,
                        )
                    }),
                ),
                (
                    value((), tag("<")),
                    map(word_infallible(""), |(bound, err)| {
                        (
                            (
                                UserInputBound::Unbounded,
                                bound
                                    .map(|bound| UserInputBound::Exclusive(bound.to_string()))
                                    .unwrap_or(UserInputBound::Unbounded),
                            ),
                            err,
                        )
                    }),
                ),
            ),
            lower_to_upper,
        ),
        |((lower, upper), errors)| {
            (
                UserInputLeaf::Range {
                    field: None,
                    lower,
                    upper,
                },
                errors,
            )
        },
    )(inp)
}

fn set(inp: &str) -> IResult<&str, UserInputLeaf> {
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
    )(inp)
}

fn set_infallible(mut inp: &str) -> JResult<&str, UserInputLeaf> {
    // `IN [` has already been parsed when we enter, we only need to parse simple terms until we
    // find a `]`
    let mut elements = Vec::new();
    let mut errs = Vec::new();
    let mut first_round = true;
    loop {
        let mut space_error = if first_round {
            first_round = false;
            Vec::new()
        } else {
            let (rest, (_, err)) = space1_infallible(inp)?;
            inp = rest;
            err
        };
        if inp.is_empty() {
            // TODO push error about missing ]
            //
            errs.push(LenientErrorInternal {
                pos: inp.len(),
                message: "missing ]".to_string(),
            });
            let res = UserInputLeaf::Set {
                field: None,
                elements,
            };
            return Ok((inp, (res, errs)));
        }
        if let Some(inp) = inp.strip_prefix(']') {
            let res = UserInputLeaf::Set {
                field: None,
                elements,
            };
            return Ok((inp, (res, errs)));
        }
        errs.append(&mut space_error);
        // TODO
        // here we do the assumption term_or_phrase_infallible always consume something if the
        // first byte is not `)` or ' '. If it did not, we would end up looping.

        let (rest, (delim_term, mut err)) = simple_term_infallible("]")(inp)?;
        errs.append(&mut err);
        if let Some((_, term)) = delim_term {
            elements.push(term);
        }
        inp = rest;
    }
}

fn negate(expr: UserInputAst) -> UserInputAst {
    expr.unary(Occur::MustNot)
}

fn leaf(inp: &str) -> IResult<&str, UserInputAst> {
    alt((
        delimited(char('('), ast, char(')')),
        map(char('*'), |_| UserInputAst::from(UserInputLeaf::All)),
        map(preceded(tuple((tag("NOT"), space1)), leaf), negate),
        literal,
    ))(inp)
}

fn leaf_infallible(inp: &str) -> JResult<&str, Option<UserInputAst>> {
    alt_infallible(
        (
            (
                value((), char('(')),
                map(
                    delimited_infallible(
                        nothing,
                        ast_infallible,
                        opt_i_err(char(')'), "expected ')'"),
                    ),
                    |(ast, errs)| (Some(ast), errs),
                ),
            ),
            (
                value((), char('*')),
                map(nothing, |_| {
                    (Some(UserInputAst::from(UserInputLeaf::All)), Vec::new())
                }),
            ),
            (
                value((), tag("NOT ")),
                delimited_infallible(
                    space0_infallible,
                    map(leaf_infallible, |(res, err)| (res.map(negate), err)),
                    nothing,
                ),
            ),
        ),
        literal_infallible,
    )(inp)
}

fn positive_float_number(inp: &str) -> IResult<&str, f64> {
    map(
        recognize(tuple((digit1, opt(tuple((char('.'), digit1)))))),
        // TODO this is actually dangerous if the number is actually not representable as a f64
        // (too big for instance)
        |float_str: &str| float_str.parse::<f64>().unwrap(),
    )(inp)
}

fn boost(inp: &str) -> JResult<&str, Option<f64>> {
    opt_i(preceded(char('^'), positive_float_number))(inp)
}

fn boosted_leaf(inp: &str) -> IResult<&str, UserInputAst> {
    map(
        tuple((leaf, fallible(boost))),
        |(leaf, boost_opt)| match boost_opt {
            Some(boost) if (boost - 1.0).abs() > f64::EPSILON => {
                UserInputAst::Boost(Box::new(leaf), boost)
            }
            _ => leaf,
        },
    )(inp)
}

fn boosted_leaf_infallible(inp: &str) -> JResult<&str, Option<UserInputAst>> {
    map(
        tuple_infallible((leaf_infallible, boost)),
        |((leaf, boost_opt), error)| match boost_opt {
            Some(boost) if (boost - 1.0).abs() > f64::EPSILON => (
                leaf.map(|leaf| UserInputAst::Boost(Box::new(leaf), boost)),
                error,
            ),
            _ => (leaf, error),
        },
    )(inp)
}

fn occur_symbol(inp: &str) -> JResult<&str, Option<Occur>> {
    opt_i(alt((
        value(Occur::MustNot, char('-')),
        value(Occur::Must, char('+')),
    )))(inp)
}

fn occur_leaf(inp: &str) -> IResult<&str, (Option<Occur>, UserInputAst)> {
    tuple((fallible(occur_symbol), boosted_leaf))(inp)
}

#[allow(clippy::type_complexity)]
fn operand_occur_leaf_infallible(
    inp: &str,
) -> JResult<&str, (Option<BinaryOperand>, Option<Occur>, Option<UserInputAst>)> {
    // TODO maybe this should support multiple chained AND/OR, and "fuse" them?
    tuple_infallible((
        delimited_infallible(nothing, opt_i(binary_operand), space0_infallible),
        occur_symbol,
        boosted_leaf_infallible,
    ))(inp)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BinaryOperand {
    Or,
    And,
}

fn binary_operand(inp: &str) -> IResult<&str, BinaryOperand> {
    alt((
        value(BinaryOperand::And, tag("AND ")),
        value(BinaryOperand::Or, tag("OR ")),
    ))(inp)
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

fn aggregate_infallible_expressions(
    input_leafs: Vec<(Option<BinaryOperand>, Option<Occur>, Option<UserInputAst>)>,
) -> (UserInputAst, ErrorList) {
    let mut err = Vec::new();
    let mut leafs: Vec<(_, _, UserInputAst)> = input_leafs
        .into_iter()
        .filter_map(|(operand, occur, ast)| ast.map(|ast| (operand, occur, ast)))
        .collect();
    if leafs.is_empty() {
        return (UserInputAst::empty_query(), err);
    }

    let use_operand = leafs.iter().any(|(operand, _, _)| operand.is_some());
    let all_operand = leafs
        .iter()
        .skip(1)
        .all(|(operand, _, _)| operand.is_some());
    let early_operand = leafs
        .iter()
        .take(1)
        .all(|(operand, _, _)| operand.is_some());
    let use_occur = leafs.iter().any(|(_, occur, _)| occur.is_some());

    if use_operand && use_occur {
        err.push(LenientErrorInternal {
            pos: 0,
            message: "Use of mixed occur and boolean operator".to_string(),
        });
    }

    if use_operand && !all_operand {
        err.push(LenientErrorInternal {
            pos: 0,
            message: "Missing boolean operator".to_string(),
        });
    }

    if early_operand {
        err.push(LenientErrorInternal {
            pos: 0,
            message: "Found unexpeted boolean operator before term".to_string(),
        });
    }

    let mut clauses: Vec<Vec<(Option<Occur>, UserInputAst)>> = vec![];
    for ((prev_operator, occur, ast), (next_operator, _, _)) in
        leafs.iter().zip(leafs.iter().skip(1))
    {
        match prev_operator {
            Some(BinaryOperand::And) => {
                if let Some(last) = clauses.last_mut() {
                    last.push((occur.or(Some(Occur::Must)), ast.clone()));
                } else {
                    let last = vec![(occur.or(Some(Occur::Must)), ast.clone())];
                    clauses.push(last);
                }
            }
            Some(BinaryOperand::Or) => {
                let default_op = match next_operator {
                    Some(BinaryOperand::And) => Some(Occur::Must),
                    _ => Some(Occur::Should),
                };
                clauses.push(vec![(occur.or(default_op), ast.clone())]);
            }
            None => {
                let default_op = match next_operator {
                    Some(BinaryOperand::And) => Some(Occur::Must),
                    Some(BinaryOperand::Or) => Some(Occur::Should),
                    None => None,
                };
                clauses.push(vec![(occur.or(default_op), ast.clone())])
            }
        }
    }

    // leaf isn't empty, so we can unwrap
    let (last_operator, last_occur, last_ast) = leafs.pop().unwrap();
    match last_operator {
        Some(BinaryOperand::And) => {
            if let Some(last) = clauses.last_mut() {
                last.push((last_occur.or(Some(Occur::Must)), last_ast));
            } else {
                let last = vec![(last_occur.or(Some(Occur::Must)), last_ast)];
                clauses.push(last);
            }
        }
        Some(BinaryOperand::Or) => {
            clauses.push(vec![(last_occur.or(Some(Occur::Should)), last_ast)]);
        }
        None => clauses.push(vec![(last_occur, last_ast)]),
    }

    if clauses.len() == 1 {
        let mut clause = clauses.pop().unwrap();
        if clause.len() == 1 && clause[0].0 != Some(Occur::MustNot) {
            (clause.pop().unwrap().1, err)
        } else {
            (UserInputAst::Clause(clause), err)
        }
    } else {
        let mut final_clauses: Vec<(Option<Occur>, UserInputAst)> = Vec::new();
        for mut sub_clauses in clauses {
            if sub_clauses.len() == 1 {
                final_clauses.push(sub_clauses.pop().unwrap());
            } else {
                final_clauses.push((Some(Occur::Should), UserInputAst::Clause(sub_clauses)));
            }
        }

        (UserInputAst::Clause(final_clauses), err)
    }
}

fn operand_leaf(inp: &str) -> IResult<&str, (BinaryOperand, UserInputAst)> {
    tuple((
        terminated(binary_operand, space0),
        terminated(boosted_leaf, space0),
    ))(inp)
}

fn ast(inp: &str) -> IResult<&str, UserInputAst> {
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
    )(inp)
}

fn ast_infallible(inp: &str) -> JResult<&str, UserInputAst> {
    // ast() parse either `term AND term OR term` or `+term term -term`
    // both are locally ambiguous, and as we allow error, it's hard to permit backtracking.
    // Instead, we allow a mix of both syntaxes, trying to make sense of what a user meant.
    // For instance `term OR -term` is interpreted as `*term -term`, but `term AND -term`
    // is interpreted as `+term -term`. We also allow `AND term` to make things easier for us,
    // even if it's not very sensical.

    let expression = map(
        separated_list_infallible(space1_infallible, operand_occur_leaf_infallible),
        |(leaf, mut err)| {
            let (res, mut err2) = aggregate_infallible_expressions(leaf);
            err.append(&mut err2);
            (res, err)
        },
    );

    delimited_infallible(space0_infallible, expression, space0_infallible)(inp)
}

pub fn parse_to_ast(inp: &str) -> IResult<&str, UserInputAst> {
    map(delimited(space0, opt(ast), eof), |opt_ast| {
        rewrite_ast(opt_ast.unwrap_or_else(UserInputAst::empty_query))
    })(inp)
}

pub fn parse_to_ast_lenient(query_str: &str) -> (UserInputAst, Vec<LenientError>) {
    if query_str.trim().is_empty() {
        return (UserInputAst::Clause(Vec::new()), Vec::new());
    }
    let (left, (res, mut errors)) = ast_infallible(query_str).unwrap();
    if !left.trim().is_empty() {
        errors.push(LenientErrorInternal {
            pos: left.len(),
            message: "unparsed end of query".to_string(),
        })
    }

    // convert end-based index to start-based index.
    let errors = errors
        .into_iter()
        .map(|internal_error| LenientError::from_internal(internal_error, query_str.len()))
        .collect();

    (rewrite_ast(res), errors)
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
        let query_strict = parse_to_ast(query).unwrap().1;
        let query_strict_str = format!("{query_strict:?}");
        assert_eq!(query_strict_str, expected, "strict parser failed");

        let (query_lenient, errs) = parse_to_ast_lenient(query);
        let query_lenient_str = format!("{query_lenient:?}");
        assert_eq!(query_lenient_str, expected, "lenient parser failed");
        assert!(
            errs.is_empty(),
            "lenient parser returned errors on valid query: {errs:?}"
        );
    }

    #[track_caller]
    fn test_is_parse_err(query: &str, lenient_expected: &str) {
        assert!(
            parse_to_ast(query).is_err(),
            "strict parser succeeded where an error was expected."
        );

        let (query_lenient, errs) = parse_to_ast_lenient(query);
        let query_lenient_str = format!("{query_lenient:?}");
        assert_eq!(query_lenient_str, lenient_expected, "lenient parser failed");
        assert!(!errs.is_empty());
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
        test_parse_query_to_ast_helper("mr james bo?d", "(*mr *james *bo?d)");
        test_parse_query_to_ast_helper("mr james bo*", "(*mr *james *bo*)");
        test_parse_query_to_ast_helper("mr james b*d", "(*mr *james *b*d)");
    }

    #[test]
    fn test_parse_query_lenient_unfinished_quote() {
        test_is_parse_err("\"www-form-encoded", "\"www-form-encoded\"");
        // TODO strict parser default to parsing a normal term, and parse "'www-forme-encoded" (note
        // the initial \')
        // test_is_parse_err("'www-form-encoded", "'www-form-encoded'");
    }

    #[test]
    fn test_parse_query_to_ast_not_op() {
        test_is_parse_err("NOT", "NOT");
        test_parse_query_to_ast_helper("NOTa", "NOTa");
        test_parse_query_to_ast_helper("NOT a", "(-a)");
    }

    #[test]
    fn test_boosting() {
        test_is_parse_err("a^2^3", "(a)^2");
        test_is_parse_err("a^2^", "(a)^2");
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
        test_is_parse_err("a OR b aaa", "(?a ?b *aaa)");
        test_is_parse_err("a AND b aaa", "(?(+a +b) *aaa)");
        test_is_parse_err("aaa a OR b ", "(*aaa ?a ?b)");
        test_is_parse_err("aaa ccc a OR b ", "(*aaa *ccc ?a ?b)");
        test_is_parse_err("aaa a AND b ", "(*aaa ?(+a +b))");
        test_is_parse_err("aaa ccc a AND b ", "(*aaa *ccc ?(+a +b))");
    }

    #[test]
    fn test_parse_mixed_bool_occur() {
        test_is_parse_err("a OR b +aaa", "(?a ?b +aaa)");
        test_is_parse_err("a AND b -aaa", "(?(+a +b) -aaa)");
        test_is_parse_err("+a OR +b aaa", "(+a +b *aaa)");
        test_is_parse_err("-a AND -b aaa", "(?(-a -b) *aaa)");
        test_is_parse_err("-aaa +ccc -a OR b ", "(-aaa +ccc -a ?b)");
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
            .expect("Cannot parse flexible bound word")
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
    fn test_range_parser_lenient() {
        let literal = |query| literal_infallible(query).unwrap().1 .0.unwrap();

        // same tests as non-lenient
        let res = literal("title: <hello");
        let expected = UserInputLeaf::Range {
            field: Some("title".to_string()),
            lower: UserInputBound::Unbounded,
            upper: UserInputBound::Exclusive("hello".to_string()),
        }
        .into();
        let res2 = literal("title:{* TO hello}");
        assert_eq!(res, expected);
        assert_eq!(res2, expected);

        let expected_weight = UserInputLeaf::Range {
            field: Some("weight".to_string()),
            lower: UserInputBound::Inclusive("71.2".to_string()),
            upper: UserInputBound::Unbounded,
        }
        .into();
        let res3 = literal("weight: >=71.2");
        let res4 = literal("weight:[71.2 TO *}");
        assert_eq!(res3, expected_weight);
        assert_eq!(res4, expected_weight);

        let expected_dates = UserInputLeaf::Range {
            field: Some("date_field".to_string()),
            lower: UserInputBound::Exclusive("2015-08-02T18:54:42Z".to_string()),
            upper: UserInputBound::Inclusive("2021-08-02T18:54:42+02:30".to_string()),
        }
        .into();
        let res5 = literal("date_field:{2015-08-02T18:54:42Z TO 2021-08-02T18:54:42+02:30]");
        assert_eq!(res5, expected_dates);

        let expected_flexible_dates = UserInputLeaf::Range {
            field: Some("date_field".to_string()),
            lower: UserInputBound::Unbounded,
            upper: UserInputBound::Inclusive("2021-08-02T18:54:42.12345+02:30".to_string()),
        }
        .into();

        let res6 = literal("date_field: <=2021-08-02T18:54:42.12345+02:30");
        assert_eq!(res6, expected_flexible_dates);
        // IP Range Unbounded
        let expected_weight = UserInputLeaf::Range {
            field: Some("ip".to_string()),
            lower: UserInputBound::Inclusive("::1".to_string()),
            upper: UserInputBound::Unbounded,
        }
        .into();
        let res1 = literal("ip: >=::1");
        let res2 = literal("ip:[::1 TO *}");
        assert_eq!(res1, expected_weight);
        assert_eq!(res2, expected_weight);

        // IP Range Bounded
        let expected_weight = UserInputLeaf::Range {
            field: Some("ip".to_string()),
            lower: UserInputBound::Inclusive("::0.0.0.50".to_string()),
            upper: UserInputBound::Exclusive("::0.0.0.52".to_string()),
        }
        .into();
        let res1 = literal("ip:[::0.0.0.50 TO ::0.0.0.52}");
        assert_eq!(res1, expected_weight);

        // additional tests
        let expected_weight = UserInputLeaf::Range {
            field: Some("ip".to_string()),
            lower: UserInputBound::Inclusive("::0.0.0.50".to_string()),
            upper: UserInputBound::Inclusive("::0.0.0.52".to_string()),
        }
        .into();
        let res1 = literal("ip:[::0.0.0.50 TO ::0.0.0.52");
        let res2 = literal("ip:[::0.0.0.50 ::0.0.0.52");
        let res3 = literal("ip:[::0.0.0.50 ::0.0.0.52 AND ...");
        assert_eq!(res1, expected_weight);
        assert_eq!(res2, expected_weight);
        assert_eq!(res3, expected_weight);

        let expected_weight = UserInputLeaf::Range {
            field: Some("ip".to_string()),
            lower: UserInputBound::Inclusive("::0.0.0.50".to_string()),
            upper: UserInputBound::Unbounded,
        }
        .into();
        let res1 = literal("ip:[::0.0.0.50 TO ");
        let res2 = literal("ip:[::0.0.0.50 TO");
        let res3 = literal("ip:[::0.0.0.50");
        assert_eq!(res1, expected_weight);
        assert_eq!(res2, expected_weight);
        assert_eq!(res3, expected_weight);

        let expected_weight = UserInputLeaf::Range {
            field: Some("ip".to_string()),
            lower: UserInputBound::Unbounded,
            upper: UserInputBound::Unbounded,
        }
        .into();
        let res1 = literal("ip:[ ");
        let res2 = literal("ip:{ ");
        let res3 = literal("ip:[");
        assert_eq!(res1, expected_weight);
        assert_eq!(res2, expected_weight);
        assert_eq!(res3, expected_weight);
        // we don't test ip: as that is not a valid range request as per percondition
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
        test_is_parse_err("(a OR  abc ", "(?a ?abc)");
    }

    #[test]
    fn test_parse_query_term_group() {
        test_parse_query_to_ast_helper(r#"field:(abc)"#, r#"(*"field":abc)"#);
        test_parse_query_to_ast_helper(r#"field:(+a -"b c")"#, r#"(+"field":a -"field":"b c")"#);

        test_is_parse_err(r#"field:(+a -"b c""#, r#"(+"field":a -"field":"b c")"#);
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
        test_is_parse_err("IN [1 2", r#"IN ["1" "2"]"#);

        // TODO maybe support these too?
        // test_is_parse_err("IN (1 2", r#"IN ["1" "2"]"#);
        // test_is_parse_err("IN {1 2", r#"IN ["1" "2"]"#);
    }

    #[test]
    fn test_parse_test_query_other() {
        test_parse_query_to_ast_helper("(+a +b) d", "(*(+a +b) *d)");
        test_parse_query_to_ast_helper("+abc:toto", "\"abc\":toto");
        test_parse_query_to_ast_helper("+a\\+b\\+c:toto", "\"a+b+c\":toto");
        test_parse_query_to_ast_helper("(+abc:toto -titi)", "(+\"abc\":toto -titi)");
        test_parse_query_to_ast_helper("-abc:toto", "(-\"abc\":toto)");
        // TODO not entirely sure about this one (it's seen as a NOT '-abc:toto')
        test_is_parse_err("--abc:toto", "(--abc:toto)");
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
        test_is_parse_err("abc +    ", "abc");
    }

    #[test]
    fn test_slop() {
        test_is_parse_err("\"a b\"~", "(*\"a b\" *~)");
        test_is_parse_err("foo:\"a b\"~", "(*\"foo\":\"a b\" *~)");
        test_is_parse_err("\"a b\"~a", "(*\"a b\" *~a)");
        test_is_parse_err(
            "\"a b\"~100000000000000000",
            "(*\"a b\" *~100000000000000000)",
        );
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
    fn test_exist_query() {
        test_parse_query_to_ast_helper("a:*", "\"a\":*");
        test_parse_query_to_ast_helper("a: *", "\"a\":*");
        // an exist followed by default term being b
        test_is_parse_err("a:*b", "(*\"a\":* *b)");

        // this is a term query (not a phrase prefix)
        test_parse_query_to_ast_helper("a:b*", "\"a\":b*");
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
