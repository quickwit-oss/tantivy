use std::fmt;
use std::fmt::{Debug, Formatter};

pub enum UserInputLeaf {
    Literal(UserInputLiteral),
    All,
    Range {
        field: Option<String>,
        lower: UserInputBound,
        upper: UserInputBound,
    },
}

impl Debug for UserInputLeaf {
    fn fmt(&self, formatter: &mut Formatter) -> Result<(), fmt::Error> {
        match self {
            UserInputLeaf::Literal(literal) => {
                literal.fmt(formatter)
            }
            UserInputLeaf::Range {
                ref field,
                ref lower,
                ref upper,
            } => {
                if let &Some(ref field) = field {
                    write!(formatter, "{}:", field)?;
                }
                lower.display_lower(formatter)?;
                write!(formatter, " TO ")?;
                upper.display_upper(formatter)?;
                Ok(())
            }
            UserInputLeaf::All => write!(formatter, "*"),
        }
    }
}

pub struct UserInputLiteral {
    pub field_name: Option<String>,
    pub phrase: String,
}

impl fmt::Debug for UserInputLiteral {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self.field_name {
            Some(ref field_name) => write!(formatter, "{}:\"{}\"", field_name, self.phrase),
            None => write!(formatter, "\"{}\"", self.phrase),
        }
    }
}

pub enum UserInputBound {
    Inclusive(String),
    Exclusive(String),
}

impl UserInputBound {
    fn display_lower(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            UserInputBound::Inclusive(ref word) => write!(formatter, "[\"{}\"", word),
            UserInputBound::Exclusive(ref word) => write!(formatter, "{{\"{}\"", word),
        }
    }

    fn display_upper(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            UserInputBound::Inclusive(ref word) => write!(formatter, "\"{}\"]", word),
            UserInputBound::Exclusive(ref word) => write!(formatter, "\"{}\"}}", word),
        }
    }

    pub fn term_str(&self) -> &str {
        match *self {
            UserInputBound::Inclusive(ref contents) => contents,
            UserInputBound::Exclusive(ref contents) => contents,
        }
    }
}

pub enum UserInputAST {
    Clause(Vec<Box<UserInputAST>>),
    Not(Box<UserInputAST>),
    Must(Box<UserInputAST>),
    Leaf(Box<UserInputLeaf>),
}

impl From<UserInputLiteral> for UserInputLeaf {
    fn from(literal: UserInputLiteral) -> UserInputLeaf {
        UserInputLeaf::Literal(literal)
    }
}

impl From<UserInputLeaf> for UserInputAST {
    fn from(leaf: UserInputLeaf) -> UserInputAST {
        UserInputAST::Leaf(Box::new(leaf))
    }
}

impl fmt::Debug for UserInputAST {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            UserInputAST::Must(ref subquery) => write!(formatter, "+({:?})", subquery),
            UserInputAST::Clause(ref subqueries) => {
                if subqueries.is_empty() {
                    write!(formatter, "<emptyclause>")?;
                } else {
                    write!(formatter, "(")?;
                    write!(formatter, "{:?}", &subqueries[0])?;
                    for subquery in &subqueries[1..] {
                        write!(formatter, " {:?}", subquery)?;
                    }
                    write!(formatter, ")")?;
                }
                Ok(())
            }
            UserInputAST::Not(ref subquery) => write!(formatter, "-({:?})", subquery),
            UserInputAST::Leaf(ref subquery) => write!(formatter, "{:?}", subquery),
        }
    }
}
