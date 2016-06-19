use rustc_serialize::Encodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encoder;
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
    
    pub fn is_u32_fast(&self,) -> bool {
        match self {
            &FieldEntry::U32(_, ref options) => {
                options.is_fast()
            }
            _ => {
                false
            }
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