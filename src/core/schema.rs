use core::error;
use std::io::Write;
use std::collections::HashMap;
use std::str;
use std::slice;
use std::fmt;
use std::io::Read;
use core::serialize::BinarySerializable;

pub type DocId = u32;

#[derive(Clone,Debug,PartialEq,Eq)]
pub struct FieldOptions {
    // untokenized_indexed: bool,
    tokenized_indexed: bool,
    stored: bool,
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

    pub fn set_tokenized_indexed(mut self,) -> FieldOptions {
        self.tokenized_indexed = true;
        self
    }

    pub fn new() -> FieldOptions {
        FieldOptions {
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
    fn serialize(&self, writer: &mut Write) -> error::Result<usize> {
        let Field(field_id) = *self;
        field_id.serialize(writer)
    }

    fn deserialize(reader: &mut Read) -> error::Result<Field> {
        u8::deserialize(reader).map(Field)
    }
}


impl BinarySerializable for FieldValue {
    fn serialize(&self, writer: &mut Write) -> error::Result<usize> {
        Ok(
            try!(self.field.serialize(writer)) +
            try!(self.text.serialize(writer))
        )
    }
    fn deserialize(reader: &mut Read) -> error::Result<Self> {
        let field = try!(Field::deserialize(reader));
        let text = try!(String::deserialize(reader));
        Ok(FieldValue {
            field: field,
            text: text,
        })
    }
}



#[derive(Clone,PartialEq,PartialOrd,Ord,Eq,Hash)]
pub struct Term {
    data: Vec<u8>,
}

#[derive(Clone,Debug)]
pub struct Schema {
    fields: HashMap<String, Field>,
    field_options: Vec<FieldOptions>,
}

impl Schema {
    pub fn new() -> Schema {
        Schema {
            fields: HashMap::new(),
            field_options: Vec::new(),
        }
    }

    pub fn find_field_name(&self, field_name: &str) -> Option<(Field, FieldOptions)> {
        self.fields
            .get(field_name)
            .map(|&Field(field_id)| {
                let field_options = self.field_options[field_id as usize].clone();
                (Field(field_id), field_options)
            })
    }

    pub fn get_field(&self, field: &Field) -> FieldOptions {
        let Field(field_id) = *field;
        self.field_options[field_id as usize].clone()
    }

    pub fn add_field(&mut self, field_name: &str, field_options: &FieldOptions) -> Field {
        let next_field = Field(self.fields.len() as u8);
        let field = self.fields
                    .entry(String::from(field_name))
                    .or_insert(next_field.clone())
                    .clone();
        if field == next_field {
            self.field_options.push(field_options.clone());
        }
        else {
            let Field(field_id) = field;
            self.field_options[field_id as usize] = field_options.clone();
        }
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
