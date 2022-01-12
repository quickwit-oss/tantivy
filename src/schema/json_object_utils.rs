use serde_json::Number;

use crate::schema::Value;

pub fn infer_type_from_string(string: &str) -> Value {
    // TODO can we avoid the copy?
    Value::Str(string.to_string())
}

pub fn infer_type_from_number(number: &Number) -> Value {
    if let Some(num_val) = number.as_u64() {
        return Value::U64(num_val);
    }
    if let Some(num_val) = number.as_i64() {
        return Value::I64(num_val);
    }
    if let Some(num_val) = number.as_f64() {
        return Value::F64(num_val);
    }
    // This should never happen as long as we
    // don't use the serde_json feature = "arbitrary_precision".
    Value::Str(number.to_string())
}

// TODO add unit tests
