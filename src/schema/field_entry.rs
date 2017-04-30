use schema::TextOptions;
use schema::U32Options;

use std::fmt;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::de::{self, Visitor, SeqAccess, MapAccess};
use schema::FieldType;

/// A `FieldEntry` represents a field and its configuration.
/// `Schema` are a collection of `FieldEntry`
/// 
/// It consists of 
/// - a field name 
/// - a field type, itself wrapping up options describing 
/// how the field should be indexed.
#[derive(Clone, Debug)]
pub struct FieldEntry {
    name: String,
    field_type: FieldType,
}

impl FieldEntry {
    
    /// Creates a new u32 field entry in the schema, given
    /// a name, and some options.
    pub fn new_text(field_name: String, field_type: TextOptions) -> FieldEntry {
        FieldEntry {
            name: field_name,
            field_type: FieldType::Str(field_type),
        }
    }
    
    /// Creates a new u32 field entry in the schema, given
    /// a name, and some options.
    pub fn new_u32(field_name: String, field_type: U32Options) -> FieldEntry {
        FieldEntry {
            name: field_name,
            field_type: FieldType::U32(field_type),
        }
    }
    
    /// Returns the name of the field
    pub fn name(&self,) -> &String {
        &self.name
    }
        
    /// Returns the field type
    pub fn field_type(&self,) -> &FieldType {
        &self.field_type
    }
    
    /// Returns true iff the field is indexed
    pub fn is_indexed(&self,) -> bool {
        match self.field_type {
            FieldType::Str(ref options) => options.get_indexing_options().is_indexed(),
            FieldType::U32(ref options) => options.is_indexed(),
        }
    }
    
    /// Returns true iff the field is a u32 fast field
    pub fn is_u32_fast(&self,) -> bool {
        match self.field_type {
            FieldType::U32(ref options) => options.is_fast(),
            _ => false,
        }
    }
    
    /// Returns true iff the field is stored
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

impl Serialize for FieldEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let mut s = serializer.serialize_struct("field_entry", 3)?;
        s.serialize_field("name", &self.name)?;

        match self.field_type {
            FieldType::Str(ref options) => {
                s.serialize_field("type", "text")?;
                s.serialize_field("options", options)?;
            },
            FieldType::U32(ref options) => {
                s.serialize_field("type", "u32")?;
                s.serialize_field("options", options)?;
            }
        }
        
        s.end()
    }
}

impl<'de> Deserialize<'de> for FieldEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field { Name, Type, Options };

        enum Options { Text(TextOptions), U32(U32Options) };

        const FIELDS: &'static [&'static str] = &["name", "type", "options"];

        struct FieldEntryVisitor;

        impl<'de> Visitor<'de> for FieldEntryVisitor {
            type Value = FieldEntry;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct FieldEntry")
            }

            fn visit_map<V>(self, mut map: V) -> Result<FieldEntry, V::Error>
                where V: MapAccess<'de>
            {
                let mut name = None;
                let mut ty = None;
                let mut field_type = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Name => {
                            if name.is_some() {
                                return Err(de::Error::duplicate_field("name"));
                            }
                            name = Some(map.next_value()?);
                        }
                        Field::Type => {
                            if ty.is_some() {
                                return Err(de::Error::duplicate_field("type"));
                            }
                            ty = Some(map.next_value()?);
                        }
                        Field::Options => {
                            match ty {
                                None => return Err(de::Error::custom("The `type` field must be specified before `options`")),
                                Some(ty) => {
                                    match ty {
                                        "text" => field_type = Some(FieldType::Str(map.next_value()?)),
                                        "u32" => field_type = Some(FieldType::U32(map.next_value()?))
                                    }
                                }
                            }
                        }
                    }
                }

                let name = name.ok_or_else(|| de::Error::missing_field("name"))?;
                let ty = ty.ok_or_else(|| de::Error::missing_field("ty"))?;
                let field_type = field_type.ok_or_else(|| de::Error::missing_field("options"))?;

                Ok(FieldEntry {
                    name: name,
                    field_type: field_type,
                })
            }
        }

        deserializer.deserialize_struct("field_entry", FIELDS, FieldEntryVisitor)
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
