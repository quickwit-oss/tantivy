use super::*;
use itertools::Itertools;    

///
/// Document are really just a list of field values.
///
#[derive(Debug)]
pub struct Document {
    field_values: Vec<FieldValue>,
}

impl Document {

    pub fn new() -> Document {
        Document {
            field_values: Vec::new(),
        }
    }
    
    pub fn len(&self,) -> usize {
        self.field_values.len()
    }

    pub fn add_text(&mut self, field: Field, text: &str) {
        self.add(FieldValue::Text(field.clone(), String::from(text)));
    }

    pub fn add_u32(&mut self, field: Field, value: u32) {
        self.add(FieldValue::U32(field.clone(), value));
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
    
    pub fn get_all<'a>(&'a self, field: Field) -> Vec<&'a FieldValue> {
        self.field_values
            .iter()
            .filter(|field_value| field_value.field() == field)
            .collect()
    }

    pub fn get_first<'a>(&'a self, field: Field) -> Option<&'a FieldValue> {
        self.field_values
            .iter()
            .filter(|field_value| field_value.field() == field)
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