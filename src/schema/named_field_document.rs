use std::collections::BTreeMap;
use schema::Value;
use rustc_serialize::Encodable;
use rustc_serialize::Encoder;

pub struct NamedFieldDocument(pub BTreeMap<String, Vec<Value>>);


impl Encodable for NamedFieldDocument {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        s.emit_struct("named_field_document", self.0.len(), |s| {
            for (i, (name, vals)) in self.0.iter().enumerate() {
                try!(s.emit_struct_field(name, i, |s| {
                    for (j, val) in vals.iter().enumerate() {
                        try!(s.emit_seq(vals.len(), |s| {
                            s.emit_seq_elt(j, |s| {
                                match *val {
                                    Value::Str(ref text) => {
                                        s.emit_str(text)
                                    },
                                    Value::U32(ref val) => {
                                        s.emit_u32(*val)
                                    }
                                }
                            })
                        }));
                    }
                    Ok(())
                    
                }));
            }
            // try!(s.emit_struct_field("name", 0, |s| {
            //     self.name.encode(s)
            // }));
            // match self.field_type {
            //     FieldType::Text(ref options) => {
            //         try!(s.emit_struct_field("type", 1, |s| {
            //             s.emit_str("text")
            //         }));
            //         try!(s.emit_struct_field("options", 2, |s| {
            //             options.encode(s)
            //         }));
            //     }
            //     FieldType::U32(ref options) => {
            //         try!(s.emit_struct_field("type", 1, |s| {
            //             s.emit_str("u32")
            //         }));
            //         try!(s.emit_struct_field("options", 2, |s| {
            //             options.encode(s)
            //         }));
            //     }
            // }
            
            Ok(())
        })
    }
}
