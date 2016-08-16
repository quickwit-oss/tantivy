use schema::TextOptions;
use schema::U32Options;

use rustc_serialize::Decodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encodable;
use rustc_serialize::Encoder;
use rustc_serialize::json::Json;
use schema::Value;

#[derive(Clone, Debug, RustcDecodable, RustcEncodable)]
pub enum FieldType {
    Str(TextOptions),
    U32(U32Options),
}

impl FieldType {
     pub fn value_from_json(&self, json: &Json) -> Result<Value, ValueParsingError> {
        match json {
            &Json::String(ref field_text) => {
                match self {
                    &FieldType::Str(_) => {
                        Ok(Value::Str(field_text.clone()))
                    }
                    &FieldType::U32(_) => {
                        Err(ValueParsingError::TypeError(format!("Expected a u32 int, got {:?}", json)))
                    }
                }
            }
            &Json::U64(ref field_val_u64) => {
                match self {
                    &FieldType::U32(_) => {
                        if *field_val_u64 > (u32::max_value() as u64) {
                            Err(ValueParsingError::OverflowError(format!("Expected u32, but value {:?} overflows.", field_val_u64)))
                        }
                        else {
                            Ok(Value::U32(*field_val_u64 as u32))
                        }
                    }
                    _ => {
                        Err(ValueParsingError::TypeError(format!("Expected a string, got {:?}", json)))
                    }
                }
            },
            _ => {
                Err(ValueParsingError::TypeError(format!("Expected a string or a u32, got {:?}", json)))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct FieldEntry {
    name: String,
    field_type: FieldType,
}

#[derive(Debug)]
pub enum ValueParsingError {
    OverflowError(String),
    TypeError(String),
}

impl FieldEntry {
    
    pub fn new_text(field_name: String, field_type: TextOptions) -> FieldEntry {
        FieldEntry {
            name: field_name,
            field_type: FieldType::Str(field_type),
        }
    }
    
    pub fn new_u32(field_name: String, field_type: U32Options) -> FieldEntry {
        FieldEntry {
            name: field_name,
            field_type: FieldType::U32(field_type),
        }
    }

    pub fn name(&self,) -> &String {
        &self.name
    }
    
    pub fn field_type(&self,) -> &FieldType {
        &self.field_type
    }
    
    pub fn is_indexed(&self,) -> bool {
        match self.field_type {
            FieldType::Str(ref options) => options.get_indexing_options().is_indexed(),
            _ => false, // TODO handle u32 indexed
        }
    }
    
    pub fn is_u32_fast(&self,) -> bool {
        match self.field_type {
            FieldType::U32(ref options) => options.is_fast(),
            _ => false,
        }
    }
    
    pub fn is_stored(&self,) -> bool {
        match self.field_type {
            FieldType::U32(ref options) => {
                options.is_stored()
            }
            FieldType::Str(ref options) => {
                options.is_stored()
            }
        }
    }
}



impl Encodable for FieldEntry {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        s.emit_struct("field_entry", 3, |s| {
            try!(s.emit_struct_field("name", 0, |s| {
                self.name.encode(s)
            }));
            match self.field_type {
                FieldType::Str(ref options) => {
                    try!(s.emit_struct_field("type", 1, |s| {
                        s.emit_str("text")
                    }));
                    try!(s.emit_struct_field("options", 2, |s| {
                        options.encode(s)
                    }));
                }
                FieldType::U32(ref options) => {
                    try!(s.emit_struct_field("type", 1, |s| {
                        s.emit_str("u32")
                    }));
                    try!(s.emit_struct_field("options", 2, |s| {
                        options.encode(s)
                    }));
                }
            }
            
            Ok(())
        })
    }
}

impl Decodable for FieldEntry {
    fn decode<D: Decoder>(d: &mut D) -> Result<Self, D::Error> {
        d.read_struct("field_entry", 3, |d| {
            let name = try!(d.read_struct_field("name", 0, |d| {
                d.read_str()
            }));
            let field_type: String = try!(d.read_struct_field("type", 1, |d| {
                d.read_str()
            }));
            d.read_struct_field("options", 2, |d| {
                match field_type.as_ref() {
                    "u32" => {
                        let u32_options = try!(U32Options::decode(d));
                        Ok(FieldEntry::new_u32(name, u32_options))
                    }
                    "text" => {
                        let text_options = try!(TextOptions::decode(d));
                        Ok(FieldEntry::new_text(name, text_options))
                    }
                    _ => {
                        Err(d.error(&format!("Field type {:?} unknown", field_type)))
                    }
                }
            })
        })
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use schema::TEXT;
    use rustc_serialize::json;
    
    #[test]
    fn test_json_serialization() {
        let field_value = FieldEntry::new_text(String::from("title"), TEXT);
        assert_eq!(format!("{}", json::as_pretty_json(&field_value)), r#"{
  "name": "title",
  "type": "text",
  "options": {
    "indexing": "position",
    "stored": false
  }
}"#);
    }
}