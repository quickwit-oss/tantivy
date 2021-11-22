use crate::space_usage::PerFieldSpaceUsage;

pub struct VectorReader {
    
}

impl VectorReader {
    pub fn new() -> VectorReader {
        trace!("New vector reader created! I need the path where I can find my vectors.");
        VectorReader{}
    }

    pub fn space_usage(&self) -> PerFieldSpaceUsage {
        todo!();
    }
}