#[derive(RustcDecodable, Debug)]
pub enum Explanation {
    NotImplementedYet,
    Explanation(String),
}

impl Explanation {
    pub fn to_string(&self,) -> Option<String> {
        match self {
            &Explanation::Explanation(ref expl) => Some(expl.clone()),
            &Explanation::NotImplementedYet => None
        }
    }
}