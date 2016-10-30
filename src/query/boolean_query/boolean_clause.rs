use query::Occur;
use query::Query;

#[derive(Debug)]
pub struct BooleanClause {
    pub query: Box<Query>,
    pub occur: Occur,
}


impl BooleanClause {
    pub fn new(query: Box<Query>, occur: Occur) -> BooleanClause {
        BooleanClause {
            query: query,
            occur: occur
        }
    }    
}