use std::fmt;
use std::fmt::{Debug, Formatter};

use serde::Serialize;

use crate::Occur;

#[derive(PartialEq, Clone, Serialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum UserInputLeaf {
    Literal(UserInputLiteral),
    All,
    Range {
        field: Option<String>,
        lower: UserInputBound,
        upper: UserInputBound,
    },
    Set {
        field: Option<String>,
        elements: Vec<String>,
    },
    Exists {
        field: String,
    },
}

impl UserInputLeaf {
    pub(crate) fn set_field(self, field: Option<String>) -> Self {
        match self {
            Self::Literal(mut literal) => {
                literal.field_name = field;
                Self::Literal(literal)
            }
            Self::All => Self::All,
            Self::Range {
                field: _,
                lower,
                upper,
            } => Self::Range {
                field,
                lower,
                upper,
            },
            Self::Set { field: _, elements } => Self::Set { field, elements },
            Self::Exists { field: _ } => Self::Exists {
                field: field.expect("Exist query without a field isn't allowed"),
            },
        }
    }

    pub(crate) fn set_default_field(&mut self, default_field: String) {
        match self {
            Self::Literal(literal) if literal.field_name.is_none() => {
                literal.field_name = Some(default_field)
            }
            Self::All => {
                *self = Self::Exists {
                    field: default_field,
                }
            }
            Self::Range { field, .. } if field.is_none() => *field = Some(default_field),
            Self::Set { field, .. } if field.is_none() => *field = Some(default_field),
            _ => (), // field was already set, do nothing
        }
    }
}

impl Debug for UserInputLeaf {
    fn fmt(&self, formatter: &mut Formatter) -> Result<(), fmt::Error> {
        match self {
            Self::Literal(literal) => literal.fmt(formatter),
            Self::Range {
                field,
                lower,
                upper,
            } => {
                if let Some(field) = field {
                    // TODO properly escape field (in case of \")
                    write!(formatter, "\"{field}\":")?;
                }
                lower.display_lower(formatter)?;
                write!(formatter, " TO ")?;
                upper.display_upper(formatter)?;
                Ok(())
            }
            Self::Set { field, elements } => {
                if let Some(field) = field {
                    // TODO properly escape field (in case of \")
                    write!(formatter, "\"{field}\": ")?;
                }
                write!(formatter, "IN [")?;
                for (i, text) in elements.iter().enumerate() {
                    if i != 0 {
                        write!(formatter, " ")?;
                    }
                    // TODO properly escape element
                    write!(formatter, "\"{text}\"")?;
                }
                write!(formatter, "]")
            }
            Self::All => write!(formatter, "*"),
            Self::Exists { field } => {
                write!(formatter, "$exists(\"{field}\")")
            }
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Delimiter {
    SingleQuotes,
    DoubleQuotes,
    None,
}

#[derive(PartialEq, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct UserInputLiteral {
    pub field_name: Option<String>,
    pub phrase: String,
    pub delimiter: Delimiter,
    pub slop: u32,
    pub prefix: bool,
}

impl fmt::Debug for UserInputLiteral {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if let Some(ref field) = self.field_name {
            // TODO properly escape field (in case of \")
            write!(formatter, "\"{field}\":")?;
        }
        match self.delimiter {
            Delimiter::SingleQuotes => {
                // TODO properly escape element (in case of \')
                write!(formatter, "'{}'", self.phrase)?;
            }
            Delimiter::DoubleQuotes => {
                // TODO properly escape element (in case of \")
                write!(formatter, "\"{}\"", self.phrase)?;
            }
            Delimiter::None => {
                // TODO properly escape element
                write!(formatter, "{}", self.phrase)?;
            }
        }
        if self.slop > 0 {
            write!(formatter, "~{}", self.slop)?;
        } else if self.prefix {
            write!(formatter, "*")?;
        }
        Ok(())
    }
}

#[derive(PartialEq, Debug, Clone, Serialize)]
#[serde(tag = "type", content = "value")]
#[serde(rename_all = "snake_case")]
pub enum UserInputBound {
    Inclusive(String),
    Exclusive(String),
    Unbounded,
}

impl UserInputBound {
    fn display_lower(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            // TODO properly escape word if required
            Self::Inclusive(ref word) => write!(formatter, "[\"{word}\""),
            Self::Exclusive(ref word) => write!(formatter, "{{\"{word}\""),
            Self::Unbounded => write!(formatter, "{{\"*\""),
        }
    }

    fn display_upper(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            // TODO properly escape word if required
            Self::Inclusive(ref word) => write!(formatter, "\"{word}\"]"),
            Self::Exclusive(ref word) => write!(formatter, "\"{word}\"}}"),
            Self::Unbounded => write!(formatter, "\"*\"}}"),
        }
    }

    pub fn term_str(&self) -> &str {
        match *self {
            Self::Inclusive(ref contents) | Self::Exclusive(ref contents) => contents,
            Self::Unbounded => "*",
        }
    }
}

#[derive(PartialEq, Clone, Serialize)]
#[serde(into = "UserInputAstSerde")]
pub enum UserInputAst {
    Clause(Vec<(Option<Occur>, UserInputAst)>),
    Boost(Box<UserInputAst>, f64),
    Leaf(Box<UserInputLeaf>),
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum UserInputAstSerde {
    Bool {
        clauses: Vec<(Option<Occur>, UserInputAst)>,
    },
    Boost {
        underlying: Box<UserInputAst>,
        boost: f64,
    },
    #[serde(untagged)]
    Leaf(Box<UserInputLeaf>),
}

impl From<UserInputAst> for UserInputAstSerde {
    fn from(ast: UserInputAst) -> Self {
        match ast {
            UserInputAst::Clause(clause) => Self::Bool { clauses: clause },
            UserInputAst::Boost(underlying, boost) => Self::Boost { underlying, boost },
            UserInputAst::Leaf(leaf) => Self::Leaf(leaf),
        }
    }
}

impl UserInputAst {
    #[must_use]
    pub fn unary(self, occur: Occur) -> Self {
        Self::Clause(vec![(Some(occur), self)])
    }

    fn compose(occur: Occur, asts: Vec<Self>) -> Self {
        assert_ne!(occur, Occur::MustNot);
        assert!(!asts.is_empty());
        if asts.len() == 1 {
            asts.into_iter().next().unwrap() //< safe
        } else {
            Self::Clause(
                asts.into_iter()
                    .map(|ast: Self| (Some(occur), ast))
                    .collect::<Vec<_>>(),
            )
        }
    }

    pub fn empty_query() -> Self {
        Self::Clause(Vec::default())
    }

    pub fn and(asts: Vec<Self>) -> Self {
        Self::compose(Occur::Must, asts)
    }

    pub fn or(asts: Vec<Self>) -> Self {
        Self::compose(Occur::Should, asts)
    }

    pub(crate) fn set_default_field(&mut self, field: String) {
        match self {
            Self::Clause(clauses) => clauses
                .iter_mut()
                .for_each(|(_, ast)| ast.set_default_field(field.clone())),
            Self::Leaf(leaf) => leaf.set_default_field(field),
            Self::Boost(ast, _) => ast.set_default_field(field),
        }
    }
}

impl From<UserInputLiteral> for UserInputLeaf {
    fn from(literal: UserInputLiteral) -> Self {
        Self::Literal(literal)
    }
}

impl From<UserInputLeaf> for UserInputAst {
    fn from(leaf: UserInputLeaf) -> Self {
        Self::Leaf(Box::new(leaf))
    }
}

fn print_occur_ast(
    occur_opt: Option<Occur>,
    ast: &UserInputAst,
    formatter: &mut fmt::Formatter,
) -> fmt::Result {
    if let Some(occur) = occur_opt {
        write!(formatter, "{occur}{ast:?}")?;
    } else {
        write!(formatter, "*{ast:?}")?;
    }
    Ok(())
}

impl fmt::Debug for UserInputAst {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Self::Clause(ref subqueries) => {
                if subqueries.is_empty() {
                    // TODO this will break ast reserialization, is writing "( )" enough?
                    write!(formatter, "<emptyclause>")?;
                } else {
                    write!(formatter, "(")?;
                    print_occur_ast(subqueries[0].0, &subqueries[0].1, formatter)?;
                    for subquery in &subqueries[1..] {
                        write!(formatter, " ")?;
                        print_occur_ast(subquery.0, &subquery.1, formatter)?;
                    }
                    write!(formatter, ")")?;
                }
                Ok(())
            }
            Self::Leaf(ref subquery) => write!(formatter, "{subquery:?}"),
            Self::Boost(ref leaf, boost) => write!(formatter, "({leaf:?})^{boost}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_leaf_serialization() {
        let ast = UserInputAst::Leaf(Box::new(UserInputLeaf::All));
        let json = serde_json::to_string(&ast).unwrap();
        assert_eq!(json, r#"{"type":"all"}"#);
    }

    #[test]
    fn test_literal_leaf_serialization() {
        let literal = UserInputLiteral {
            field_name: Some("title".to_string()),
            phrase: "hello".to_string(),
            delimiter: Delimiter::None,
            slop: 0,
            prefix: false,
        };
        let ast = UserInputAst::Leaf(Box::new(UserInputLeaf::Literal(literal)));
        let json = serde_json::to_string(&ast).unwrap();
        assert_eq!(
            json,
            r#"{"type":"literal","field_name":"title","phrase":"hello","delimiter":"none","slop":0,"prefix":false}"#
        );
    }

    #[test]
    fn test_range_leaf_serialization() {
        let range = UserInputLeaf::Range {
            field: Some("price".to_string()),
            lower: UserInputBound::Inclusive("10".to_string()),
            upper: UserInputBound::Exclusive("100".to_string()),
        };
        let ast = UserInputAst::Leaf(Box::new(range));
        let json = serde_json::to_string(&ast).unwrap();
        assert_eq!(
            json,
            r#"{"type":"range","field":"price","lower":{"type":"inclusive","value":"10"},"upper":{"type":"exclusive","value":"100"}}"#
        );
    }

    #[test]
    fn test_range_leaf_unbounded_serialization() {
        let range = UserInputLeaf::Range {
            field: Some("price".to_string()),
            lower: UserInputBound::Inclusive("10".to_string()),
            upper: UserInputBound::Unbounded,
        };
        let ast = UserInputAst::Leaf(Box::new(range));
        let json = serde_json::to_string(&ast).unwrap();
        assert_eq!(
            json,
            r#"{"type":"range","field":"price","lower":{"type":"inclusive","value":"10"},"upper":{"type":"unbounded"}}"#
        );
    }

    #[test]
    fn test_boost_serialization() {
        let inner_ast = UserInputAst::Leaf(Box::new(UserInputLeaf::All));
        let boost_ast = UserInputAst::Boost(Box::new(inner_ast), 2.5);
        let json = serde_json::to_string(&boost_ast).unwrap();
        assert_eq!(
            json,
            r#"{"type":"boost","underlying":{"type":"all"},"boost":2.5}"#
        );
    }

    #[test]
    fn test_boost_serialization2() {
        let boost_ast = UserInputAst::Boost(
            Box::new(UserInputAst::Clause(vec![
                (
                    Some(Occur::Must),
                    UserInputAst::Leaf(Box::new(UserInputLeaf::All)),
                ),
                (
                    Some(Occur::Should),
                    UserInputAst::Leaf(Box::new(UserInputLeaf::Literal(UserInputLiteral {
                        field_name: Some("title".to_string()),
                        phrase: "hello".to_string(),
                        delimiter: Delimiter::None,
                        slop: 0,
                        prefix: false,
                    }))),
                ),
            ])),
            2.5,
        );
        let json = serde_json::to_string(&boost_ast).unwrap();
        assert_eq!(
            json,
            r#"{"type":"boost","underlying":{"type":"bool","clauses":[["must",{"type":"all"}],["should",{"type":"literal","field_name":"title","phrase":"hello","delimiter":"none","slop":0,"prefix":false}]]},"boost":2.5}"#
        );
    }

    #[test]
    fn test_clause_serialization() {
        let clause = UserInputAst::Clause(vec![
            (
                Some(Occur::Must),
                UserInputAst::Leaf(Box::new(UserInputLeaf::All)),
            ),
            (
                Some(Occur::Should),
                UserInputAst::Leaf(Box::new(UserInputLeaf::Literal(UserInputLiteral {
                    field_name: Some("title".to_string()),
                    phrase: "hello".to_string(),
                    delimiter: Delimiter::None,
                    slop: 0,
                    prefix: false,
                }))),
            ),
        ]);
        let json = serde_json::to_string(&clause).unwrap();
        assert_eq!(
            json,
            r#"{"type":"bool","clauses":[["must",{"type":"all"}],["should",{"type":"literal","field_name":"title","phrase":"hello","delimiter":"none","slop":0,"prefix":false}]]}"#
        );
    }
}
