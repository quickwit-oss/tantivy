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
    pub text_field_values: Vec<TextFieldValue>,
    pub u32_field_values: Vec<U32FieldValue>,
}

impl Document {

    pub fn new() -> Document {
        Document {
            text_field_values: Vec::new(),
            u32_field_values: Vec::new(),
        }
    }

    pub fn from(text_field_values: Vec<TextFieldValue>,
                u32_field_values: Vec<U32FieldValue>) -> Document {
        Document {
            text_field_values: text_field_values,
            u32_field_values: u32_field_values
        }
    }

    pub fn len(&self,) -> usize {
        self.text_field_values.len()
    }

    pub fn set(&mut self, field: &TextField, text: &str) {
        self.add(TextFieldValue {
            field: field.clone(),
            text: String::from(text)
        });
    }

    pub fn set_u32(&mut self, field: &U32Field, value: u32) {
        self.u32_field_values.push(U32FieldValue {
            field: field.clone(),
            value: value
        });
    }

    pub fn add(&mut self, field_value: TextFieldValue) {
        self.text_field_values.push(field_value);
    }


    pub fn text_fields<'a>(&'a self,) -> slice::Iter<'a, TextFieldValue> {
        self.text_field_values.iter()
    }

    pub fn u32_fields<'a>(&'a self,) -> slice::Iter<'a, U32FieldValue> {
        self.u32_field_values.iter()
    }

    pub fn get_u32(&self, field: &U32Field) -> Option<u32> {
        self.u32_field_values
            .iter()
            .filter(|field_value| field_value.field == *field)
            .map(|field_value| &field_value.value)
            .cloned()
            .next()
    }

    pub fn get_texts<'a>(&'a self, field: &TextField) -> Vec<&'a String> {
        self.text_field_values
            .iter()
            .filter(|field_value| field_value.field == *field)
            .map(|field_value| &field_value.text)
            .collect()
    }

    pub fn get_first_text<'a>(&'a self, field: &TextField) -> Option<&'a String> {
        self.text_field_values
            .iter()
            .filter(|field_value| field_value.field == *field)
            .map(|field_value| &field_value.text)
            .next()
    }
}
