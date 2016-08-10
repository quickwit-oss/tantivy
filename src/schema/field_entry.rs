use schema::TextOptions;
use schema::U32Options;


#[derive(Clone, Debug, RustcDecodable, RustcEncodable)]
pub enum FieldEntry {
    Text(String, TextOptions),
    U32(String, U32Options),
}

impl FieldEntry {
        
    pub fn get_field_name(&self,) -> &str {
        match self {
            &FieldEntry::Text(ref field_name, _) => {
                field_name
            },
            &FieldEntry::U32(ref field_name, _) => {
                field_name
            }
        }
    }
    
    pub fn is_indexed(&self,) -> bool {
        match self {
            &FieldEntry::Text(_, ref options) => options.get_indexing_options().is_indexed(),
            _ => false, // TODO handle u32 indexed
        }
    }
    
    pub fn is_u32_fast(&self,) -> bool {
        match self {
            &FieldEntry::U32(_, ref options) => options.is_fast(),
            _ => false,
        }
    }
    
    pub fn is_stored(&self,) -> bool {
        match self {
            &FieldEntry::U32(_, ref options) => {
                // TODO handle stored u32
                options.is_stored()
            }
            &FieldEntry::Text(_, ref options) => {
                options.is_stored()
            }
        }
    }
}

// TODO implement a nicer JSON format 

#[cfg(test)]
mod tests {

    use super::*;
    use schema::TEXT;
    use rustc_serialize::json;
    
    #[test]
    fn test_json_serialization() {
        let field_value = FieldEntry::Text(String::from("title"), TEXT);
        assert_eq!(format!("{}", json::as_json(&field_value)),
             "{\"variant\":\"Text\",\"fields\":[\"title\",{\"indexing_options\":\"TokenizedWithFreqAndPosition\",\"stored\":false,\"fast\":false}]}"    );
    }
}