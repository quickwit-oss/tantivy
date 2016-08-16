use super::*;
use itertools::Itertools;    

/// Tantivy's Document is the object that can
/// be indexed and then searched for.  
/// 
/// Documents are really fundamentally a collection of unordered couple `(field, value)`.
/// In this list, one field may appear more than once.
/// 
/// 


/// Documents are really just a list of couple `(field, value)`.
/// In this list, one field may appear more than once.
#[derive(Debug)]
pub struct Document {
    field_values: Vec<FieldValue>,
}

impl PartialEq for Document {
    fn eq(&self, other: &Document) -> bool {
        // super slow, but only here for tests
        let mut self_field_values = self.field_values.clone();
        let mut other_field_values = other.field_values.clone();
        self_field_values.sort();
        other_field_values.sort();
        self_field_values.eq(&other_field_values)
    }
}

impl Eq for Document {}

impl Document {

    pub fn new() -> Document {
        Document {
            field_values: Vec::new(),
        }
    }
    
    /// Returns the number of `(field, value)` pairs.
    pub fn len(&self,) -> usize {
        self.field_values.len()
    }

    /// Add a text field.
    pub fn add_text(&mut self, field: Field, text: &str) {
        self.add(FieldValue {
            field: field,
            value: Value::Str(String::from(text)),
        });
    }

    /// Add a u32 field
    pub fn add_u32(&mut self, field: Field, value: u32) {
        self.add(FieldValue {
            field: field,
            value: Value::U32(value),
        });
    }

    pub fn add(&mut self, field_value: FieldValue) {
        self.field_values.push(field_value);
    }

    pub fn get_fields(&self) -> &Vec<FieldValue> {
        &self.field_values
    }
    
    pub fn get_sorted_fields(&self) -> Vec<(Field, Vec<&FieldValue>)> {
         let mut field_values:  Vec<&FieldValue> = self.get_fields().iter().collect();
         field_values.sort_by_key(|field_value| field_value.field());
         let sorted_fields: Vec<(Field, Vec<&FieldValue>)> = field_values
            .into_iter()
            .group_by(|field_value| field_value.field())
            .into_iter()
            .map(|(key, group)| {
                (key, group.into_iter().collect())
            })
            .collect();
         sorted_fields
     }
    
    pub fn get_all<'a>(&'a self, field: Field) -> Vec<&'a Value> {
        self.field_values
            .iter()
            .filter(|field_value| field_value.field() == field)
            .map(|field_value| field_value.value())
            .collect()
    }

    pub fn get_first<'a>(&'a self, field: Field) -> Option<&'a Value> {
        self.field_values
            .iter()
            .filter(|field_value| field_value.field() == field)
            .map(|field_value| field_value.value())
            .next()
    }
}

impl From<Vec<FieldValue>> for Document {
    fn from(field_values: Vec<FieldValue>) -> Document {
        Document {
            field_values: field_values
        }
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use schema::Schema;
    use schema::TEXT;
    
    #[test]
    fn test_doc() {
        let mut schema = Schema::new();
        let text_field = schema.add_text_field("title", TEXT);
        let mut doc = Document::new();
        doc.add_text(text_field, "My title");
        assert_eq!(doc.get_fields().len(), 1);
    }
    
}