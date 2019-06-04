use {DocId, TantivyError};

pub fn does_not_match(doc: DocId) -> TantivyError {
    TantivyError::InvalidArgument(format!("Document #({}) does not match", doc))
}

#[derive(Clone, Serialize)]
pub struct Explanation {
    value: f32,
    description: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    details: Vec<Explanation>,
}

impl Explanation {
    pub fn new<T: ToString>(description: T, value: f32) -> Explanation {
        Explanation {
            value,
            description: description.to_string(),
            details: vec![],
        }
    }

    pub fn const_value(value: f32) -> Explanation {
        Explanation {
            value,
            description: "".to_string(),
            details: vec![],
        }
    }

    pub fn val(&self) -> f32 {
        self.value
    }

    pub fn add_const<T: ToString>(&mut self, name: T, value: f32) {
        self.details.push(Explanation::new(name, value));
    }

    pub fn add_detail(&mut self, child_explanation: Explanation) {
        self.details.push(child_explanation);
    }

    pub fn to_string(&self) -> String {
        serde_json::to_string_pretty(self).unwrap()
    }
}
