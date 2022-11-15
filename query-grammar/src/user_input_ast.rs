use std::fmt;
use std::fmt::{Debug, Formatter};

use crate::Occur;

#[derive(PartialEq)]
pub enum UserInputLeaf {
    Literal(UserInputLiteral),
    All,
    Range {
        field: Option<String>,
        lower: UserInputBound,
        upper: UserInputBound,
    },
    Set {
        field: String,
        elements: Vec<String>,
    },
}

impl Debug for UserInputLeaf {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            UserInputLeaf::Literal(literal) => literal.fmt(formatter),
            UserInputLeaf::Range {
                ref field,
                ref lower,
                ref upper,
            } => {
                if let Some(ref field) = field {
                    write!(formatter, "\"{}\":", field)?;
                }
                lower.display_lower(formatter)?;
                write!(formatter, " TO ")?;
                upper.display_upper(formatter)?;
                Ok(())
            }
            UserInputLeaf::Set { field, elements } => {
                write!(formatter, "\"{}\": IN [", field)?;
                for (i, element) in elements.iter().enumerate() {
                    if i == 0 {
                        write!(formatter, "\"{}\"", element)?;
                    } else {
                        write!(formatter, ", \"{}\"", element)?;
                    }
                }
                write!(formatter, "]")
            }
            UserInputLeaf::All => write!(formatter, "*"),
        }
    }
}

#[derive(PartialEq)]
pub struct UserInputLiteral {
    pub field_name: Option<String>,
    pub phrase: String,
    pub slop: u32,
}

impl fmt::Debug for UserInputLiteral {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if let Some(ref field) = self.field_name {
            write!(formatter, "\"{}\":", field)?;
        }
        write!(formatter, "\"{}\"", self.phrase)?;
        if self.slop > 0 {
            write!(formatter, "~{}", self.slop)?;
        }
        Ok(())
    }
}

#[derive(PartialEq)]
pub enum UserInputBound {
    Inclusive(String),
    Exclusive(String),
    Unbounded,
}

impl UserInputBound {
    fn display_lower(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            UserInputBound::Inclusive(ref word) => write!(formatter, "[\"{}\"", word),
            UserInputBound::Exclusive(ref word) => write!(formatter, "{{\"{}\"", word),
            UserInputBound::Unbounded => write!(formatter, "{{\"*\""),
        }
    }

    fn display_upper(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            UserInputBound::Inclusive(ref word) => write!(formatter, "\"{}\"]", word),
            UserInputBound::Exclusive(ref word) => write!(formatter, "\"{}\"}}", word),
            UserInputBound::Unbounded => write!(formatter, "\"*\"}}"),
        }
    }

    pub fn term_str(&self) -> &str {
        match *self {
            UserInputBound::Inclusive(ref contents) => contents,
            UserInputBound::Exclusive(ref contents) => contents,
            UserInputBound::Unbounded => "*",
        }
    }
}

pub enum UserInputAst {
    Clause(Vec<(Option<Occur>, UserInputAst)>),
    Leaf(Box<UserInputLeaf>),
    Boost(Box<UserInputAst>, f64),
}

impl UserInputAst {
    #[must_use]
    pub fn unary(self, occur: Occur) -> UserInputAst {
        UserInputAst::Clause(vec![(Some(occur), self)])
    }

    fn compose(occur: Occur, asts: Vec<UserInputAst>) -> UserInputAst {
        assert_ne!(occur, Occur::MustNot);
        assert!(!asts.is_empty());
        if asts.len() == 1 {
            asts.into_iter().next().unwrap() //< safe
        } else {
            UserInputAst::Clause(
                asts.into_iter()
                    .map(|ast: UserInputAst| (Some(occur), ast))
                    .collect::<Vec<_>>(),
            )
        }
    }

    pub fn empty_query() -> UserInputAst {
        UserInputAst::Clause(Vec::default())
    }

    pub fn and(asts: Vec<UserInputAst>) -> UserInputAst {
        UserInputAst::compose(Occur::Must, asts)
    }

    pub fn or(asts: Vec<UserInputAst>) -> UserInputAst {
        UserInputAst::compose(Occur::Should, asts)
    }
}

impl From<UserInputLiteral> for UserInputLeaf {
    fn from(literal: UserInputLiteral) -> UserInputLeaf {
        UserInputLeaf::Literal(literal)
    }
}

impl From<UserInputLeaf> for UserInputAst {
    fn from(leaf: UserInputLeaf) -> UserInputAst {
        UserInputAst::Leaf(Box::new(leaf))
    }
}

fn print_occur_ast(
    occur_opt: Option<Occur>,
    ast: &UserInputAst,
    formatter: &mut fmt::Formatter,
) -> fmt::Result {
    if let Some(occur) = occur_opt {
        write!(formatter, "{}{:?}", occur, ast)?;
    } else {
        write!(formatter, "*{:?}", ast)?;
    }
    Ok(())
}

impl fmt::Debug for UserInputAst {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            UserInputAst::Clause(ref subqueries) => {
                if subqueries.is_empty() {
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
            UserInputAst::Leaf(ref subquery) => write!(formatter, "{:?}", subquery),
            UserInputAst::Boost(ref leaf, boost) => write!(formatter, "({:?})^{}", leaf, boost),
        }
    }
}
