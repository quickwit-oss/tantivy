use crate::fastfield::BytesFastFieldReader;
use crate::fastfield::MultiValueIntFastFieldReader;
use crate::fastfield::{FastFieldNotAvailableError, FastFieldReader};
use crate::schema::{Cardinality, Field, FieldType, Schema};
use crate::space_usage::PerFieldSpaceUsage;
use crate::CompositeFile;
use crate::Result;
use std::collections::HashMap;

/// Provides access to all of the FastFieldReader.
///
/// Internally, `FastFieldReaders` have preloaded fast field readers,
/// and just wraps several `HashMap`.
pub struct FastFieldReaders {
    fast_field_i64: HashMap<Field, FastFieldReader<i64>>,
    fast_field_u64: HashMap<Field, FastFieldReader<u64>>,
    fast_field_f64: HashMap<Field, FastFieldReader<f64>>,
    fast_field_i64s: HashMap<Field, MultiValueIntFastFieldReader<i64>>,
    fast_field_u64s: HashMap<Field, MultiValueIntFastFieldReader<u64>>,
    fast_field_f64s: HashMap<Field, MultiValueIntFastFieldReader<f64>>,
    fast_bytes: HashMap<Field, BytesFastFieldReader>,
    fast_fields_composite: CompositeFile,
}

enum FastType {
    I64,
    U64,
    F64,
}

fn type_and_cardinality(field_type: &FieldType) -> Option<(FastType, Cardinality)> {
    match field_type {
        FieldType::U64(options) => options
            .get_fastfield_cardinality()
            .map(|cardinality| (FastType::U64, cardinality)),
        FieldType::I64(options) => options
            .get_fastfield_cardinality()
            .map(|cardinality| (FastType::I64, cardinality)),
        FieldType::F64(options) => options
            .get_fastfield_cardinality()
            .map(|cardinality| (FastType::F64, cardinality)),
        FieldType::HierarchicalFacet => Some((FastType::U64, Cardinality::MultiValues)),
        _ => None,
    }
}

impl FastFieldReaders {
    pub(crate) fn load_all(
        schema: &Schema,
        fast_fields_composite: &CompositeFile,
    ) -> Result<FastFieldReaders> {
        let mut fast_field_readers = FastFieldReaders {
            fast_field_i64: Default::default(),
            fast_field_u64: Default::default(),
            fast_field_f64: Default::default(),
            fast_field_i64s: Default::default(),
            fast_field_u64s: Default::default(),
            fast_field_f64s: Default::default(),
            fast_bytes: Default::default(),
            fast_fields_composite: fast_fields_composite.clone(),
        };
        for (field_id, field_entry) in schema.fields().iter().enumerate() {
            let field = Field(field_id as u32);
            let field_type = field_entry.field_type();
            if field_type == &FieldType::Bytes {
                let idx_reader = fast_fields_composite
                    .open_read_with_idx(field, 0)
                    .ok_or_else(|| FastFieldNotAvailableError::new(field_entry))
                    .map(FastFieldReader::open)?;
                let data = fast_fields_composite
                    .open_read_with_idx(field, 1)
                    .ok_or_else(|| FastFieldNotAvailableError::new(field_entry))?;
                fast_field_readers
                    .fast_bytes
                    .insert(field, BytesFastFieldReader::open(idx_reader, data));
            } else if let Some((fast_type, cardinality)) = type_and_cardinality(field_type) {
                match cardinality {
                    Cardinality::SingleValue => {
                        if let Some(fast_field_data) = fast_fields_composite.open_read(field) {
                            match fast_type {
                                FastType::U64 => {
                                    let fast_field_reader = FastFieldReader::open(fast_field_data);
                                    fast_field_readers
                                        .fast_field_u64
                                        .insert(field, fast_field_reader);
                                }
                                FastType::I64 => {
                                    fast_field_readers.fast_field_i64.insert(
                                        field,
                                        FastFieldReader::open(fast_field_data.clone()),
                                    );
                                }
                                FastType::F64 => {
                                    fast_field_readers.fast_field_f64.insert(
                                        field,
                                        FastFieldReader::open(fast_field_data.clone()),
                                    );
                                }
                            }
                        } else {
                            return Err(From::from(FastFieldNotAvailableError::new(field_entry)));
                        }
                    }
                    Cardinality::MultiValues => {
                        let idx_opt = fast_fields_composite.open_read_with_idx(field, 0);
                        let data_opt = fast_fields_composite.open_read_with_idx(field, 1);
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
                                FastType::F64 => {
                                    let vals_reader = FastFieldReader::open(fast_field_data);
                                    let multivalued_int_fast_field =
                                        MultiValueIntFastFieldReader::open(idx_reader, vals_reader);
                                    fast_field_readers
                                        .fast_field_f64s
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

    pub(crate) fn space_usage(&self) -> PerFieldSpaceUsage {
        self.fast_fields_composite.space_usage()
    }

    /// Returns the `u64` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a u64 fast field, this method returns `None`.
    pub fn u64(&self, field: Field) -> Option<FastFieldReader<u64>> {
        self.fast_field_u64.get(&field).cloned()
    }

    /// If the field is a u64-fast field return the associated reader.
    /// If the field is a i64-fast field, return the associated u64 reader. Values are
    /// mapped from i64 to u64 using a (well the, it is unique) monotonic mapping.    ///
    ///
    ///TODO should it also be lenient with f64?
    ///
    /// This method is useful when merging segment reader.
    pub(crate) fn u64_lenient(&self, field: Field) -> Option<FastFieldReader<u64>> {
        if let Some(u64_ff_reader) = self.u64(field) {
            return Some(u64_ff_reader);
        }
        if let Some(i64_ff_reader) = self.i64(field) {
            return Some(i64_ff_reader.into_u64_reader());
        }
        None
    }

    /// Returns the `i64` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a i64 fast field, this method returns `None`.
    pub fn i64(&self, field: Field) -> Option<FastFieldReader<i64>> {
        self.fast_field_i64.get(&field).cloned()
    }

    /// Returns the `f64` fast field reader reader associated to `field`.
    ///
    /// If `field` is not a f64 fast field, this method returns `None`.
    pub fn f64(&self, field: Field) -> Option<FastFieldReader<f64>> {
        self.fast_field_f64.get(&field).cloned()
    }

    /// Returns a `u64s` multi-valued fast field reader reader associated to `field`.
    ///
    /// If `field` is not a u64 multi-valued fast field, this method returns `None`.
    pub fn u64s(&self, field: Field) -> Option<MultiValueIntFastFieldReader<u64>> {
        self.fast_field_u64s.get(&field).cloned()
    }

    /// If the field is a u64s-fast field return the associated reader.
    /// If the field is a i64s-fast field, return the associated u64s reader. Values are
    /// mapped from i64 to u64 using a (well the, it is unique) monotonic mapping.
    ///
    /// This method is useful when merging segment reader.
    pub(crate) fn u64s_lenient(&self, field: Field) -> Option<MultiValueIntFastFieldReader<u64>> {
        if let Some(u64s_ff_reader) = self.u64s(field) {
            return Some(u64s_ff_reader);
        }
        if let Some(i64s_ff_reader) = self.i64s(field) {
            return Some(i64s_ff_reader.into_u64s_reader());
        }
        None
    }

    /// Returns a `i64s` multi-valued fast field reader reader associated to `field`.
    ///
    /// If `field` is not a i64 multi-valued fast field, this method returns `None`.
    pub fn i64s(&self, field: Field) -> Option<MultiValueIntFastFieldReader<i64>> {
        self.fast_field_i64s.get(&field).cloned()
    }

    /// Returns a `f64s` multi-valued fast field reader reader associated to `field`.
    ///
    /// If `field` is not a f64 multi-valued fast field, this method returns `None`.
    pub fn f64s(&self, field: Field) -> Option<MultiValueIntFastFieldReader<f64>> {
        self.fast_field_f64s.get(&field).cloned()
    }

    /// Returns the `bytes` fast field reader associated to `field`.
    ///
    /// If `field` is not a bytes fast field, returns `None`.
    pub fn bytes(&self, field: Field) -> Option<BytesFastFieldReader> {
        self.fast_bytes.get(&field).cloned()
    }
}
