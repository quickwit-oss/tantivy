use std::collections::BTreeMap;
use schema::Value;



/// Internal representation of a document used for JSON 
/// serialization.
/// 
/// A `NamedFieldDocument` is a simple representation of a document
/// as a `BTreeMap<String, Vec<Value>>`.
///
#[derive(Serialize)]
pub struct NamedFieldDocument(pub BTreeMap<String, Vec<Value>>);

// TODO: Remove
/*
impl Encodable for NamedFieldDocument {
    fn encode<S: Encoder>(&self, s: &mut S) -> Result<(), S::Error> {
        s.emit_struct("named_field_document", self.0.len(), |s| {
            for (i, (name, vals)) in self.0.iter().enumerate() {
                s.emit_struct_field(name, i, |s| {
                    for (j, val) in vals.iter().enumerate() {
                        s.emit_seq(vals.len(), |s| {
                            s.emit_seq_elt(j, |s| {
                                match *val {
                                    Value::Str(ref text) => {
                                        s.emit_str(text)
                                    },
                                    Value::U64(ref val) => {
                                        s.emit_u64(*val)
                                    }
                                    Value::I64(ref val) => {
                                        s.emit_i64(*val)
                                    }
                                }
                            })
                        })?;
                    }
                    Ok(())
                    
                })?;
            }
            Ok(())
        })
    }
}
*/
