use common::CompositeFile;
use fastfield::MultiValueIntFastFieldReader;
use fastfield::{FastFieldNotAvailableError, FastFieldReader};
use schema::{Cardinality, Field, FieldType, Schema};
use std::collections::HashMap;
use Result;

#[derive(Default)]
pub(crate) struct FastFieldReaders {
    fast_field_i64: HashMap<Field, FastFieldReader<i64>>,
    fast_field_u64: HashMap<Field, FastFieldReader<u64>>,
    fast_field_i64s: HashMap<Field, MultiValueIntFastFieldReader<i64>>,
    fast_field_u64s: HashMap<Field, MultiValueIntFastFieldReader<u64>>,
}

enum FastType {
    I64,
    U64,
}

fn type_and_cardinality(field_type: &FieldType) -> Option<(FastType, Cardinality)> {
    match field_type {
        FieldType::U64(options) => options
            .get_fastfield_cardinality()
            .map(|cardinality| (FastType::U64, cardinality)),
        FieldType::I64(options) => options
            .get_fastfield_cardinality()
            .map(|cardinality| (FastType::I64, cardinality)),
        _ => None,
    }
}

impl FastFieldReaders {
    pub(crate) fn load_all(
        schema: &Schema,
        fast_field_composite: &CompositeFile,
    ) -> Result<FastFieldReaders> {
        let mut fast_field_readers: FastFieldReaders = Default::default();
        for (field_id, field_entry) in schema.fields().iter().enumerate() {
            let field = Field(field_id as u32);
            if let Some((fast_type, cardinality)) = type_and_cardinality(field_entry.field_type()) {
                match cardinality {
                    Cardinality::SingleValue => {
                        if let Some(fast_field_data) = fast_field_composite.open_read(field) {
                            match fast_type {
                                FastType::U64 => {
                                    let fast_field_reader = FastFieldReader::open(fast_field_data);
                                    fast_field_readers
                                        .fast_field_u64
                                        .insert(field, fast_field_reader);
                                }
                                FastType::I64 => {
                                    let fast_field_reader = FastFieldReader::open(fast_field_data);
                                    fast_field_readers
                                        .fast_field_i64
                                        .insert(field, fast_field_reader);
                                }
                            }
                        } else {
                            return Err(From::from(FastFieldNotAvailableError::new(field_entry)));
                        }
                    }
                    Cardinality::MultiValues => {
                        let idx_opt = fast_field_composite.open_read_with_idx(field, 0);
                        let data_opt = fast_field_composite.open_read_with_idx(field, 1);
                        if let (Some(fast_field_idx), Some(fast_field_data)) = (idx_opt, data_opt) {
                            let idx_reader = FastFieldReader::open(fast_field_idx);
                            match fast_type {
                                FastType::I64 => {
                                    let vals_reader = FastFieldReader::open(fast_field_data);
                                    let multivalued_int_fast_field =
                                        MultiValueIntFastFieldReader::open(idx_reader, vals_reader);
                                    fast_field_readers
                                        .fast_field_i64s
                                        .insert(field, multivalued_int_fast_field);
                                }
                                FastType::U64 => {
                                    let vals_reader = FastFieldReader::open(fast_field_data);
                                    let multivalued_int_fast_field =
                                        MultiValueIntFastFieldReader::open(idx_reader, vals_reader);
                                    fast_field_readers
                                        .fast_field_u64s
                                        .insert(field, multivalued_int_fast_field);
                                }
                            }
                        } else {
                            return Err(From::from(FastFieldNotAvailableError::new(field_entry)));
                        }
                    }
                }
            }
        }
        Ok(fast_field_readers)
    }
}
