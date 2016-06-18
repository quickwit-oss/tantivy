use std::slice;

use super::*;

///
/// Document are really just a list of field values.
///
///  # Examples
///
/// ```
/// use tantivy::schema::Schema;
/// use tantivy::schema::TEXT;
///
/// let mut schema = Schema::new();
/// schema.add_text_field("body", &TEXT);
/// let field_text = schema.text_field("body");
/// ```
///
#[derive(Debug)]
pub struct Document {
    pub field_values: Vec<FieldValue>,
}

impl Document {

    pub fn new() -> Document {
        Document {
            field_values: Vec::new(),
        }
    }

    pub fn from(field_values: Vec<FieldValue>) -> Document {
        Document {
            field_values: field_values,
        }
    }

    pub fn len(&self,) -> usize {
        self.field_values.len()
    }

    pub fn add_text(&mut self, field: &Field, text: &str) {
        self.add(FieldValue::Text(field.clone(), String::from(text)));
    }

    pub fn add_u32(&mut self, field: &Field, value: u32) {
        self.add(FieldValue::U32(field.clone(), value));
    }

    pub fn add(&mut self, field_value: FieldValue) {
        self.field_values.push(field_value);
    }

    // pub fn text_fields<'a>(&'a self,) -> slice::Iter<'a, TextFieldValue> {
    //     self.text_field_values.iter()
    // }

    // pub fn u32_fields<'a>(&'a self,) -> slice::Iter<'a, U32FieldValue> {
    //     self.u32_field_values.iter()
    // }

    // pub fn get_u32(&self, field: &U32Field) -> Option<u32> {
    //     self.u32_field_values
    //         .iter()
    //         .filter(|field_value| field_value.field == *field)
    //         .map(|field_value| &field_value.value)
    //         .cloned()
    //         .next()
    // }

    // pub fn get_texts<'a>(&'a self, field: &TextField) -> Vec<&'a String> {
    //     self.text_field_values
    //         .iter()
    //         .filter(|field_value| field_value.field == *field)
    //         .map(|field_value| &field_value.text)
    //         .collect()
    // }
    
        
    pub fn get_fields(&self) -> &Vec<FieldValue> {
        &self.field_values
    }
    
    pub fn get_all<'a>(&'a self, field: &Field) -> Vec<&'a FieldValue> {
        self.field_values
            .iter()
            .filter(|field_value| *field_value.field() == *field)
            // .map(|field_value| &field_value.text)
            .collect()
    }

    pub fn get_first<'a>(&'a self, field: &Field) -> Option<&'a FieldValue> {
        self.field_values
            .iter()
            .filter(|field_value| *field_value.field() == *field)
            .next()
    }
}
