use std::collections::HashMap;

use rustc_serialize::Decodable;
use rustc_serialize::Encodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encoder;
use std::borrow::Borrow;
use super::*;

// #[derive(Clone, Debug, RustcDecodable, RustcEncodable)]
// pub struct TextFieldEntry {
//     name: String,
//     option: TextOptions,
// }


// #[derive(Clone, Debug, RustcDecodable, RustcEncodable)]
// pub struct U32FieldEntry {
//     pub name: String,
//     pub option: U32Options,
// }



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
/// fn create_schema() -> Schema {
///   let mut schema = Schema::new();
///   let str_fieldtype = TextOptions::new();
///   let id_field = schema.add_text_field("id", &STRING);
///   let url_field = schema.add_text_field("url", &STRING);
///   let body_field = schema.add_text_field("body", &TEXT);
///   let id_field = schema.add_text_field("id", &STRING);
///   let url_field = schema.add_text_field("url", &STRING);
///   let title_field = schema.add_text_field("title", &TEXT);
///   let body_field = schema.add_text_field("body", &TEXT);
///   schema
/// }
///
/// let schema = create_schema();
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
                // let field_options: &TextOptions = &field_entry.option;
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

    
    pub fn field_entry(&self, field: Field) -> &FieldEntry {
        &self.fields[field.0 as usize]
    }
    
    pub fn fields(&self,) -> &Vec<FieldEntry> {
        &self.fields
    }
    
    // pub fn get_u32_fields(&self,) -> &Vec<U32FieldEntry> {
    //     &self.u32_fields
    // }

    /// Given a name, returns the field handle, as well as its associated TextOptions
    // pub fn get_text_field(&self, field_name: &str) -> Option<(TextField, TextOptions)> {
    //     self.text_fields_map
    //         .get(field_name)
    //         .map(|&TextField(field_id)| {
    //             let field_options = self.text_fields[field_id as usize].option.clone();
    //             (TextField(field_id), field_options)
    //         })
    // }

    // pub fn get_u32_field(&self, field_name: &str) -> Option<(U32Field, U32Options)> {
    //     self.u32_fields_map
    //     .get(field_name)
    //     .map(|&U32Field(field_id)| {
    //         let u32_field_options = self.u32_fields[field_id as usize].option.clone();
    //         (U32Field(field_id), u32_field_options)
    //     })
    // }
    
    
    
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

    // pub fn u32_field(&self, fieldname: &str) -> U32Field {
    //     self.u32_fields_map.get(fieldname).map(|field| field.clone()).unwrap()
    // }

    /// Returns the field options associated to a field handle.
    // pub fn text_field_options(&self, field: &TextField) -> TextOptions {
    //     let TextField(field_id) = *field;
    //     self.text_fields[field_id as usize].option.clone()
    // }


    // pub fn u32_field_options(&self, field: &U32Field) -> U32Options {
    //     let U32Field(field_id) = *field;
    //     self.u32_fields[field_id as usize].option.clone()
    // }

    // /// Creates a new field.
    // /// Return the associated field handle.
    // pub fn add_text_field<RefTextOptions: Borrow<TextOptions>>(&mut self, field_name_str: &str, field_options: RefTextOptions) -> TextField {
    //     let field = TextField(self.text_fields.len() as u8);
    //     // TODO case if field already exists
    //     let field_name = String::from(field_name_str);
    //     self.text_fields.push(TextFieldEntry {
    //         name: field_name.clone(),
    //         option: field_options.borrow().clone(),
    //     });
    //     self.text_fields_map.insert(field_name, field.clone());
    //     field
    // }

    /// Creates a new field.
    /// Return the associated field handle.
    pub fn add_u32_field(
            &mut self,
            field_name_str: &str, 
            field_options: U32Options) -> Field {
        // TODO case if field already exists
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::U32(field_name, field_options);
        self.add_field(field_entry)
    }
    
    pub fn add_text_field(
            &mut self,
            field_name_str: &str, 
            field_options: TextOptions) -> Field {
        // TODO case if field already exists
        let field_name = String::from(field_name_str);
        let field_entry = FieldEntry::Text(field_name, field_options);
        self.add_field(field_entry)
    }

    fn add_field(&mut self, field_entry: FieldEntry) -> Field {       
        let field = Field(self.fields.len() as u8);
        // TODO case if field already exists
        let field_name = String::from(field_entry.get_field_name());
        self.fields.push(field_entry);
        self.fields_map.insert(field_name, field.clone());
        field
    }
    
    
    
}
