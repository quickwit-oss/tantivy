use std::collections::HashMap;

#[derive(Clone)]
pub struct Explanation {
    msg: String,
    val: f32,
    children: HashMap<String, Explanation>,
}

impl Explanation {
    pub fn new<T: ToString>(msg: T, val: f32) -> Explanation {
        Explanation {
            msg: msg.to_string(),
            val,
            children: HashMap::default(),
        }
    }

    pub fn set_child<T: ToString>(&mut self, key: T, child_explanation: Explanation) {
        self.children.insert(key.to_string(), child_explanation);
    }
}
