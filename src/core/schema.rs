use std::io::Write;
use std::collections::HashMap;
use std::slice;
use std::fmt;
use std::io;
use std::io::Read;
use std::str;
use core::serialize::BinarySerializable;
use rustc_serialize::Decodable;
use rustc_serialize::Encodable;
use rustc_serialize::Decoder;
use rustc_serialize::Encoder;
use std::ops::BitOr;
use std::borrow::Borrow;

/// u32 identifying a document within a segment.
/// Document gets their doc id assigned incrementally,
/// as they are added in the segment.
pub type DocId = u32;


#[derive(Clone,Debug,PartialEq,Eq, RustcDecodable, RustcEncodable)]
pub struct FieldOptions {
    tokenized_indexed: bool,
    stored: bool,
    fast: bool,
}

/// The field will be tokenized and indexed
pub const TEXT: FieldOptions = FieldOptions {
    tokenized_indexed: true,
    stored: false,
    fast: false
};

/// A stored fields of a document can be retrieved given its DocId.
/// Stored field are stored together and LZ4 compressed.
/// Reading the stored fields of a document is relatively slow.
/// (100 microsecs)
pub const STORED: FieldOptions = FieldOptions {
    tokenized_indexed: false,
    stored: true,
    fast: false
};

/// Fast field are used for field you need to access many times during
/// collection. (e.g: for sort, aggregates).
pub const FAST: FieldOptions = FieldOptions {
    tokenized_indexed: false,
    stored: false,
    fast: true
};


impl BitOr for FieldOptions {

    type Output = FieldOptions;

    fn bitor(self, other: FieldOptions) -> FieldOptions {
        let mut res = FieldOptions::new();
        res.tokenized_indexed = self.tokenized_indexed || other.tokenized_indexed;
        res.stored = self.stored || other.stored;
        res.fast = self.fast || other.fast;
        res
    }
}

/// Field handle
#[derive(Clone,Debug,PartialEq,PartialOrd,Eq,Hash)]
pub struct Field(u8);

impl FieldOptions {
    pub fn is_tokenized_indexed(&self,) -> bool {
        self.tokenized_indexed
    }

    pub fn is_stored(&self,) -> bool {
        self.stored
    }

    pub fn is_fast(&self,) -> bool {
        self.fast
    }

    pub fn set_stored(mut self,) -> FieldOptions {
        self.stored = true;
        self
    }

    pub fn set_fast(mut self,) -> FieldOptions {
        self.fast = true;
        self
    }

    pub fn set_tokenized_indexed(mut self,) -> FieldOptions {
        self.tokenized_indexed = true;
        self
    }

    pub fn new() -> FieldOptions {
        FieldOptions {
            fast: false,
            tokenized_indexed: false,
            stored: false,
        }
    }
}

#[derive(Clone,Debug,PartialEq,PartialOrd,Eq)]
pub struct FieldValue {
    pub field: Field,
    pub text: String,
}

impl BinarySerializable for Field {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        let Field(field_id) = *self;
        field_id.serialize(writer)
    }

    fn deserialize(reader: &mut Read) -> io::Result<Field> {
        u8::deserialize(reader).map(Field)
    }
}

impl BinarySerializable for FieldValue {
    fn serialize(&self, writer: &mut Write) -> io::Result<usize> {
        Ok(
            try!(self.field.serialize(writer)) +
            try!(self.text.serialize(writer))
        )
    }
    fn deserialize(reader: &mut Read) -> io::Result<Self> {
        let field = try!(Field::deserialize(reader));
        let text = try!(String::deserialize(reader));
        Ok(FieldValue {
            field: field,
            text: text,
        })
    }
}



#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Hash)]
pub struct Term {
    data: Vec<u8>,
}

#[derive(Clone, Debug, RustcDecodable, RustcEncodable)]
struct FieldEntry {
    name: String,
    option: FieldOptions,
}

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
/// use tantivy::schema::{Schema, FieldOptions};
///
/// fn create_schema() -> Schema {
///   let mut schema = Schema::new();
///   let str_fieldtype = FieldOptions::new();
///   let text_fieldtype = FieldOptions::new().set_tokenized_indexed();
///   let id_field = schema.add_field("id", &str_fieldtype);
///   let url_field = schema.add_field("url", &str_fieldtype);
///   let body_field = schema.add_field("body", &text_fieldtype);
///   let id_field = schema.add_field("id", &str_fieldtype);
///   let url_field = schema.add_field("url", &str_fieldtype);
///   let title_field = schema.add_field("title", &text_fieldtype);
///   let body_field = schema.add_field("body", &text_fieldtype);
///   schema
/// }
///
/// let schema = create_schema();
#[derive(Clone, Debug)]
pub struct Schema {
    fields: Vec<FieldEntry>,
    fields_map: HashMap<String, Field>,  // transient
    field_options: Vec<FieldOptions>,    // transient
}

impl Decodable for Schema {
    fn decode<D: Decoder>(d: &mut D) -> Result  <Self, D::Error> {
        let mut schema = Schema::new();
        try!(d.read_seq(|d, num_fields| {
            for _ in 0..num_fields {
                let field_entry = try!(FieldEntry::decode(d));
                let field_options: &FieldOptions = &field_entry.option;
                schema.add_field(&field_entry.name, field_options);
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
            field_options: Vec::new(),
        }
    }

    /// Given a name, returns the field handle, as well as its associated FieldOptions
    pub fn get(&self, field_name: &str) -> Option<(Field, FieldOptions)> {
        self.fields_map
            .get(field_name)
            .map(|&Field(field_id)| {
                let field_options = self.field_options[field_id as usize].clone();
                (Field(field_id), field_options)
            })
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
    pub fn field(&self, fieldname: &str) -> Field {
        self.fields_map.get(&String::from(fieldname)).map(|field| field.clone()).unwrap()
    }

    /// Returns the field options associated to a field handle.
    pub fn field_options(&self, field: &Field) -> FieldOptions {
        let Field(field_id) = *field;
        self.field_options[field_id as usize].clone()
    }


    /// Creates a new field.
    /// Return the associated field handle.
    pub fn add_field<RefFieldOptions: Borrow<FieldOptions>>(&mut self, field_name_str: &str, field_options: RefFieldOptions) -> Field {
        let field = Field(self.fields.len() as u8);
        // TODO case if field already exists
        let field_name = String::from(field_name_str);
        self.fields.push(FieldEntry {
            name: field_name.clone(),
            option: field_options.borrow().clone(),
        });
        self.fields_map.insert(field_name, field.clone());
        self.field_options.push(field_options.borrow().clone());
        field
    }

}

impl Term {

    // TODO avoid all these copies in Term.
    // when the term is in the dictionary, there
    // shouldn't be any copy
    pub fn field(&self,) -> Field {
        Field(self.data[0])
    }

    pub fn text(&self,) -> &str {
        str::from_utf8(&self.data[1..]).unwrap()
    }

    pub fn from_field_text(field: &Field, text: &str) -> Term {
        let mut buffer = Vec::with_capacity(1 + text.len());
        let Field(field_idx) = *field;
        buffer.clear();
        buffer.push(field_idx);
        buffer.extend(text.as_bytes());
        Term {
            data: buffer,
        }
    }

    pub fn from(data: &[u8]) -> Term {
        Term {
            data: Vec::from(data),
        }
    }

    pub fn as_slice(&self,)->&[u8] {
        &self.data
    }
}

impl fmt::Debug for Term {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Term({}: {})", self.data[0], self.text())
    }
}

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
/// schema.add_field("body", &TEXT);
/// let field_text = schema.field("body");
/// ```
///
#[derive(Debug)]
pub struct Document {
    field_values: Vec<FieldValue>,
}


impl Document {

    pub fn new() -> Document {
        Document {
            field_values: Vec::new()
        }
    }

    pub fn from(field_values: Vec<FieldValue>) -> Document {
        Document {
            field_values: field_values
        }
    }

    pub fn len(&self,) -> usize {
        self.field_values.len()
    }

    pub fn set(&mut self, field: &Field, text: &str) {
        self.add(FieldValue {
            field: field.clone(),
            text: String::from(text)
        });
    }

    pub fn add(&mut self, field_value: FieldValue) {
        self.field_values.push(field_value);
    }

    pub fn fields<'a>(&'a self,) -> slice::Iter<'a, FieldValue> {
        self.field_values.iter()
    }

    pub fn get<'a>(&'a self, field: &Field) -> Vec<&'a String> {
        self.field_values
            .iter()
            .filter(|field_value| field_value.field == *field)
            .map(|field_value| &field_value.text)
            .collect()
    }

    pub fn get_one<'a>(&'a self, field: &Field) -> Option<&'a String> {
        self.field_values
            .iter()
            .filter(|field_value| field_value.field == *field)
            .map(|field_value| &field_value.text)
            .next()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_options() {
        {
            let field_options = STORED | FAST;
            assert!(field_options.is_stored());
            assert!(field_options.is_fast());
            assert!(!field_options.is_tokenized_indexed());
        }
        {
            let field_options = STORED | TEXT;
            assert!(field_options.is_stored());
            assert!(!field_options.is_fast());
            assert!(field_options.is_tokenized_indexed());
        }
    }

    #[test]
    fn test_schema() {
        {
            let mut schema = Schema::new();
            schema.add_field("body", &TEXT);
            let field = schema.field("body");
            assert!(schema.field_options(&field).is_tokenized_indexed());
        }
    }
}
