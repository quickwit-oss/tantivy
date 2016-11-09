use std::fmt;

pub struct QueryAST {
    root: QueryNode,
}

impl From<QueryNode> for QueryAST {
    fn from(root: QueryNode) -> QueryAST {
        QueryAST {
            root: root
        }
    }
}

impl fmt::Debug for QueryAST {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(formatter, "{:?}", self.root)
    }
}

pub struct QueryLiteral {
    pub field_name: Option<String>,
    pub phrase: String, 
}

impl fmt::Debug for QueryLiteral {
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

pub enum QueryNode {
    Clause(Vec<Box<QueryNode>>),
    Not(Box<QueryNode>),
    Must(Box<QueryNode>),
    Leaf(Box<QueryLiteral>)
    
}

impl From<QueryLiteral> for QueryNode {
    fn from(literal: QueryLiteral) -> QueryNode {
        QueryNode::Leaf(box literal)
    }
 }

impl fmt::Debug for QueryNode {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            QueryNode::Must(ref subquery) => {
                write!(formatter, "+({:?})", subquery)
            },
            QueryNode::Clause(ref subqueries) => {
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
            QueryNode::Not(ref subquery) => {
                write!(formatter, "-({:?})", subquery)
            }
            QueryNode::Leaf(ref subquery) => {
                write!(formatter, "{:?}", subquery)
            }
        }
    }
}
