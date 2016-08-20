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
            Ok(())
        })
    }
}
