use core::global::*;

#[derive(Clone,Debug,PartialEq,PartialOrd,Eq)]
pub struct FieldValue {
    pub field: Field,
    pub text: String,
}


#[derive(Clone,Debug,PartialEq,PartialOrd,Eq,Hash)]
pub struct Term<'a> {
    pub field: Field,
	pub text: &'a str,
}


pub struct Document {
    fields: Vec<FieldValue>,
}


impl Document {

    pub fn new() -> Document {
        Document {
            fields: Vec::new()
        }
    }

    pub fn set(&mut self, field: Field, text: &str) {
        self.add(FieldValue {
            field: field,
            text: String::from(text)
        });
    }

    pub fn add(&mut self, field_value: FieldValue) {
        self.fields.push(field_value);
    }

}

impl IntoIterator for Document {
    type Item = FieldValue;
    type IntoIter = ::std::vec::IntoIter<FieldValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.into_iter()
    }

}
