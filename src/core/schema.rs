use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

#[derive(Clone,Debug,PartialEq,PartialOrd,Eq)]
pub struct Field(&'static str);


#[derive(Clone,Debug,PartialEq,PartialOrd,Eq)]
pub struct FieldValue {
    pub field: Field,
    pub text: String,
}


#[derive(Clone,Debug,PartialEq,PartialOrd,Eq)]
pub struct Term<'a> {
    pub field: &'a Field,
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

    pub fn set(&mut self, field: &Field, text: &String) {
        self.add(FieldValue {
            field: (*field).clone(),
            text: (*text).clone()
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
