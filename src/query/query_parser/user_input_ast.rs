use std::fmt;

pub struct UserInputLiteral {
    pub field_name: Option<String>,
    pub phrase: String, 
}

impl fmt::Debug for UserInputLiteral {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self.field_name {
            Some(ref field_name) => {
                write!(formatter, "{}:\"{}\"", field_name, self.phrase)
            }
            None => {
                write!(formatter, "\"{}\"", self.phrase)
            }
        }
    }
}

pub enum UserInputAST {
    Clause(Vec<Box<UserInputAST>>),
    Not(Box<UserInputAST>),
    Must(Box<UserInputAST>),
    Leaf(Box<UserInputLiteral>)
    
}

impl From<UserInputLiteral> for UserInputAST {
    fn from(literal: UserInputLiteral) -> UserInputAST {
        UserInputAST::Leaf(box literal)
    }
 }

impl fmt::Debug for UserInputAST {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            UserInputAST::Must(ref subquery) => {
                write!(formatter, "+({:?})", subquery)
            },
            UserInputAST::Clause(ref subqueries) => {
                if subqueries.is_empty() {
                    try!(write!(formatter, "<emptyclause>"));
                }
                else {
                    try!(write!(formatter, "{:?}", &subqueries[0]));
                    for subquery in &subqueries[1..] {
                        try!(write!(formatter, " {:?}", subquery));
                    }
                }
                Ok(())
                
            },
            UserInputAST::Not(ref subquery) => {
                write!(formatter, "-({:?})", subquery)
            }
            UserInputAST::Leaf(ref subquery) => {
                write!(formatter, "{:?}", subquery)
            }
        }
    }
}
