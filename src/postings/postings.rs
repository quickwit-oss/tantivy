use DocId;

#[derive(PartialEq, Eq, Debug)]
pub enum SkipResult {
    Reached,
    OverStep,
    End,
}

pub trait Postings {
    
    
    // goes to the next element.
    // next needs to be called a first time to point to the correct element.
    fn next(&mut self,) -> bool;
    
    fn doc(&self,) -> DocId;
    
    // after skipping position
    // the iterator in such a way that doc() will return a
    // value greater or equal to target.
    fn skip_next(&mut self, target: DocId) -> SkipResult;
}

