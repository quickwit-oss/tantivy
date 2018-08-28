use super::FastFieldFilterWeight;
use query::Query;
use query::Weight;
use Result;
use Searcher;
use schema::Field;
use super::RangeU64;
use std::collections::Bound;
use common::i64_to_u64;
use schema::Schema;
use schema::FieldEntry;
use TantivyError;
use schema::Type;

#[derive(Debug, Copy, Clone)]
enum TypeInt {
    U64, I64
}

impl TypeInt {
    fn value_type(self) -> Type {
        match self {
            TypeInt::I64 => Type::I64,
            TypeInt::U64 => Type::U64
        }
    }
}

//< TODO i64 range Debug string will not look good in the
// current implementation. Defer conversion to the scorer, or
// back convert values for Debug.
#[derive(Debug, Clone)]
pub struct FastFieldFilterQuery {
    field: Field,
    range: RangeU64,
    int_type: TypeInt, //< just here to check the schema at runtime, as we call `.weight`
}

fn convert_bound_to_u64(bound: Bound<i64>) -> Bound<u64> {
    match bound {
        Bound::Included(val) =>
            Bound::Excluded(i64_to_u64(val)),
        Bound::Excluded(val) =>
            Bound::Excluded(i64_to_u64(val)),
        Bound::Unbounded => Bound::Unbounded
    }
}

impl FastFieldFilterQuery {

    pub fn new_u64(field: Field, low: Bound<u64>, high: Bound<u64>) -> FastFieldFilterQuery {
        FastFieldFilterQuery {
            field: field,
            range: RangeU64 { low, high },
            int_type: TypeInt::U64
        }
    }

    pub fn new_i64(field: Field, low: Bound<i64>, high: Bound<i64>) -> FastFieldFilterQuery {
        FastFieldFilterQuery {
            field: field,
            range: RangeU64 {
                low: convert_bound_to_u64(low),
                high: convert_bound_to_u64(high)
            },
            int_type: TypeInt::I64
        }
    }


    fn validate_schema(&self, schema: &Schema) -> Result<()> {
        let field_entry: &FieldEntry = schema.get_field_entry(self.field);
        if !field_entry.is_int_fast() {
            return Err(TantivyError::SchemaError(format!(
                "Field {:?} is not an int fast field",
                field_entry.name()
            )));
        }
        let expected_value_type = self.int_type.value_type();
        if field_entry.field_type().value_type() != self.int_type.value_type() {
            return Err(TantivyError::SchemaError(format!(
                "Field {:?} is not a {:?}",
                field_entry.name(),
                expected_value_type
            )));
        }
        Ok(())
    }
}

impl Query for FastFieldFilterQuery {
    fn weight(&self, searcher: &Searcher, _scoring_enabled: bool) -> Result<Box<Weight>> {
        self.validate_schema(searcher.schema())?;
        Ok(Box::new(FastFieldFilterWeight::new(self.field, self.range.clone())))
    }
}
