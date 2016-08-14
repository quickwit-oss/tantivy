use std::collections::HashMap;

use rustc_serialize::Decodable;
use rustc_serialize::Encodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encoder;
use rustc_serialize::json;
use rustc_serialize::json::Json;
use super::*;


/// Tantivy has a very strict schema.
/// You need to specify in advance, whether a field is indexed or not,
/// stored or not, and RAM-based or not.
///
/// This is done by creating a schema object, and
/// setting up the fields one by one.
/// It is for the moment impossible to remove fields.
///
/// # Examples
///
/// ```
/// use tantivy::schema::*;
///
///   let mut schema = Schema::new();
///   let id_field = schema.add_text_field("id", STRING);
///   let title_field = schema.add_text_field("title", TEXT);
///   let body_field = schema.add_text_field("body", TEXT);
///
/// ```
#[derive(Clone, Debug)]
pub struct Schema {
    fields: Vec<FieldEntry>,
    fields_map: HashMap<String, Field>,  // transient
}

impl Decodable for Schema {
    fn decode<D: Decoder>(d: &mut D) -> Result  <Self, D::Error> {
        let mut schema = Schema::new();
        try!(d.read_seq(|d, num_fields| {
            for _ in 0..num_fields {
                let field_entry = try!(FieldEntry::decode(d));
                schema.add_field(field_entry);
            }
            Ok(())
        }));
        Ok(schema)
    }
}

impl Encodable for Schema {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        try!(s.emit_seq(self.fields.len(),
            |mut e| {
                for (ord, field) in self.fields.iter().enumerate() {
                    try!(e.emit_seq_elt(ord, |e| field.encode(e)));
                }
                Ok(())
            }));
        Ok(())
    }
}

impl Schema {

    /// Creates a new, empty schema.
    pub fn new() -> Schema {
        Schema {
            fields: Vec::new(),
            fields_map: HashMap::new(),
        }
    }
    
    pub fn get_field_entry(&self, field: Field) -> &FieldEntry {
        &self.fields[field.0 as usize]
    }
    
    pub fn fields(&self,) -> &Vec<FieldEntry> {
        &self.fields
    }
    
    /// Returns the field options associated with a given name.
    ///
    /// # Panics
    /// Panics if the field name does not exist.
    /// It is meant as an helper for user who created
    /// and control the content of their schema.
    ///
    /// If panicking is not an option for you,
    /// you may use `get(&self, field_name: &str)`.
    pub fn get_field(&self, field_name: &str) -> Option<Field> {
        self.fields_map.get(field_name).map(|field| field.clone())
    }

    /// Creates a new field.
    /// Return the associated field handle.
    pub fn add_u32_field(
            &mut self,
            field_name_str: &str, 
            field_options: U32Options) -> Field {
        // TODO case if field already exists
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::new_u32(field_name, field_options);
        self.add_field(field_entry)
    }
    
    pub fn add_text_field(
            &mut self,
            field_name_str: &str, 
            field_options: TextOptions) -> Field {
        // TODO case if field already exists
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::new_text(field_name, field_options);
        self.add_field(field_entry)
    }

    fn add_field(&mut self, field_entry: FieldEntry) -> Field {       
        let field = Field(self.fields.len() as u8);
        // TODO case if field already exists
        let field_name = field_entry.name().clone();
        self.fields.push(field_entry);
        self.fields_map.insert(field_name, field.clone());
        field
    }
    
    /// Build a document object from a json-object. 
    pub fn parse_document(&self, doc_json: &str) -> Result<Document, DocParsingError> {
        let json_node = try!(Json::from_str(doc_json));
        let some_json_obj = json_node.as_object();
        if !some_json_obj.is_some() {
            let doc_json_sample: String;
            if doc_json.len() < 20 {
                doc_json_sample = String::from(doc_json);
            }
            else {
                doc_json_sample = format!("{:?}...", &doc_json[0..20]);
            }
            return Err(DocParsingError::NotJSONObject(doc_json_sample))
        }
        let json_obj = some_json_obj.unwrap();
        let mut doc = Document::new();
        for (field_name, field_value) in json_obj.iter() {
            match self.get_field(field_name) {
                Some(field) => {
                    let field_entry = self.get_field_entry(field);
                    match field_value {
                        &Json::String(ref field_text) => {
                            match field_entry.field_type() {
                                &FieldType::Text(_) => {
                                    doc.add_text(field, field_text);
                                }
                                &FieldType::U32(_) => {
                                    return Err(DocParsingError::MappingError(field_entry.name().clone(), format!("Expected a u32 int, got {:?}", field_value)));
                                }
                            }
                        }
                        &Json::U64(ref field_val_u64) => {
                            match field_entry.field_type() {
                                &FieldType::U32(_) => {
                                    if *field_val_u64 > (u32::max_value() as u64) {
                                        return Err(DocParsingError::OverflowError(field_name.clone()));
                                    }
                                    doc.add_u32(field, *field_val_u64 as u32);
                                }
                                _ => {
                                    return Err(DocParsingError::MappingError(field_name.clone(), format!("Expected a string, got {:?}", field_value)));
                                }
                            }
                        },
                        _ => {
                            return Err(DocParsingError::MappingError(field_name.clone(), String::from("Value is neither u32, nor text.")));
                        }
                    }
                }
                None => {
                    return Err(DocParsingError::NoSuchFieldInSchema(field_name.clone()))
                }
            }
        }
        Ok(doc)    
    }

}






#[derive(Debug)]
pub enum DocParsingError {
    NotJSON(json::ParserError),
    NotJSONObject(String),
    MappingError(String, String),
    OverflowError(String),
    NoSuchFieldInSchema(String),
}

impl From<json::ParserError> for DocParsingError {
    fn from(err: json::ParserError) -> DocParsingError {
        DocParsingError::NotJSON(err)
    }
}



#[cfg(test)]
mod tests {
    
    use schema::*;
    use rustc_serialize::json;
        
    #[test]
    pub fn test_schema_serialization() {
        let mut schema = Schema::new();
        let count_options = U32Options::new().set_stored().set_fast(); 
        schema.add_text_field("title", TEXT);
        schema.add_text_field("author", STRING);
        schema.add_u32_field("count", count_options);
        let schema_json: String = format!("{}", json::as_pretty_json(&schema));
        println!("{}", schema_json);
        let expected = r#"[
  {
    "name": "title",
    "type": "text",
    "options": {
      "indexing": "position",
      "stored": false
    }
  },
  {
    "name": "author",
    "type": "text",
    "options": {
      "indexing": "untokenized",
      "stored": false
    }
  },
  {
    "name": "count",
    "type": "u32",
    "options": {
      "indexed": false,
      "fast": true,
      "stored": true
    }
  }
]"#;
        assert_eq!(schema_json, expected);        
        
    }
    
    
    
    #[test]
    pub fn test_parse_document() {
        let mut schema = Schema::new();
        let count_options = U32Options::new().set_stored().set_fast(); 
        let title_field = schema.add_text_field("title", TEXT);
        let author_field = schema.add_text_field("author", STRING);
        let count_field = schema.add_u32_field("count", count_options);
        {
            let doc = schema.parse_document("{}").unwrap();
            assert!(doc.get_fields().is_empty());
        }
        {
            let doc = schema.parse_document(r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": 4
            }"#).unwrap();
            assert_eq!(doc.get_first(title_field).unwrap().text(), "my title");
            assert_eq!(doc.get_first(author_field).unwrap().text(), "fulmicoton");
            assert_eq!(doc.get_first(count_field).unwrap().u32_value(), 4);
        }
        {
            let json_err = schema.parse_document(r#"{
                "title": "my title",
                "author": "fulmicoton"
                "count": 4
            }"#);
            match json_err {
                Err(DocParsingError::NotJSON(__)) => {
                    assert!(true);
                }
                _ => {
                    assert!(false);
                }
            }
        }
        {
            let json_err = schema.parse_document(r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": 4,
                "jambon": "bayonne" 
            }"#);
            match json_err {
                Err(DocParsingError::NoSuchFieldInSchema(field_name)) => {
                    assert_eq!(field_name, "jambon");
                }
                _ => {
                    assert!(false);
                }
            }
        }
        {
            let json_err = schema.parse_document(r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": "5",
                "jambon": "bayonne" 
            }"#);
            match json_err {
                Err(DocParsingError::MappingError(_, _)) => {
                    assert!(true);
                }
                _ => {
                    assert!(false);
                }
            }
        }
        {
            let json_err = schema.parse_document(r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": -5
            }"#);
            println!("{:?}", json_err);
            match json_err {
                Err(DocParsingError::MappingError(_, _)) => {
                    assert!(true);
                }
                _ => {
                    assert!(false);
                }
            }
        }
        {
            let json_err = schema.parse_document(r#"{
                "title": "my title",
                "author": "fulmicoton",
                "count": 5000000000
            }"#);
            println!("{:?}", json_err);
            match json_err {
                Err(DocParsingError::OverflowError(_)) => {
                    assert!(true);
                }
                _ => {
                    assert!(false);
                }
            }
        }
    }
}