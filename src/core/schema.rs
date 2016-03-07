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


pub type DocId = u32;

#[derive(Clone,Debug,PartialEq,Eq, RustcDecodable, RustcEncodable)]
pub struct FieldOptions {
    // untokenized_indexed: bool,
    tokenized_indexed: bool,
    stored: bool,
    doc_value: bool,
}


#[derive(Clone,Debug,PartialEq,PartialOrd,Eq,Hash)]
pub struct Field(u8);

impl FieldOptions {
    pub fn is_tokenized_indexed(&self,) -> bool {
        self.tokenized_indexed
    }

    pub fn is_stored(&self,) -> bool {
        self.stored
    }

    pub fn set_stored(mut self,) -> FieldOptions {
        self.stored = true;
        self
    }

    pub fn set_docvalue(mut self,) -> FieldOptions {
        self.doc_value = true;
        self
    }

    pub fn set_tokenized_indexed(mut self,) -> FieldOptions {
        self.tokenized_indexed = true;
        self
    }

    pub fn new() -> FieldOptions {
        FieldOptions {
            doc_value: false,
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
                schema.add_field(&field_entry.name, &field_entry.option);
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
    pub fn new() -> Schema {
        Schema {
            fields: Vec::new(),
            fields_map: HashMap::new(),
            field_options: Vec::new(),
        }
    }

    pub fn find_field_name(&self, field_name: &str) -> Option<(Field, FieldOptions)> {
        self.fields_map
            .get(field_name)
            .map(|&Field(field_id)| {
                let field_options = self.field_options[field_id as usize].clone();
                (Field(field_id), field_options)
            })
    }

    pub fn field(&self, fieldname: &str) -> Option<Field> {
        self.fields_map.get(&String::from(fieldname)).map(|field| field.clone())
    }

    pub fn field_options(&self, field: &Field) -> FieldOptions {
        let Field(field_id) = *field;
        self.field_options[field_id as usize].clone()
    }

    pub fn add_field(&mut self, field_name_str: &str, field_options: &FieldOptions) -> Field {
        let field = Field(self.fields.len() as u8);
        // TODO case if field already exists
        let field_name = String::from(field_name_str);
        self.fields.push(FieldEntry {
            name: field_name.clone(),
            option: field_options.clone(),
        });
        self.fields_map.insert(field_name, field.clone());
        self.field_options.push(field_options.clone());
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
