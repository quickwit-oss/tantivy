use std::fmt;

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
    Range { field: Option<String>, lower: UserInputBound, upper: UserInputBound },
    All,
    Leaf(Box<UserInputLiteral>),
}

impl From<UserInputLiteral> for UserInputAST {
    fn from(literal: UserInputLiteral) -> UserInputAST {
        UserInputAST::Leaf(Box::new(literal))
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
            UserInputAST::Range { ref field, ref lower, ref upper } => {
                if let &Some(ref field) = field {
                    write!(formatter, "{}:", field)?;
                }
                lower.display_lower(formatter)?;
                write!(formatter, " TO ")?;
                upper.display_upper(formatter)?;
                Ok(())
            },
            UserInputAST::All => write!(formatter, "*"),
            UserInputAST::Leaf(ref subquery) => write!(formatter, "{:?}", subquery),
        }
    }
}
