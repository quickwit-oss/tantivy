use rustc_serialize::Decodable;
use rustc_serialize::Encodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encoder;
use schema::Field;
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
} 