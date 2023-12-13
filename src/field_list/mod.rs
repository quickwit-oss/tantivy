//! The list of fields that are stored in a `tantivy` `Index`.

use std::collections::HashSet;
use std::io::{self, ErrorKind, Read};

use columnar::ColumnType;
use common::TinySet;
use fnv::FnvHashMap;

use crate::indexer::path_to_unordered_id::OrderedPathId;
use crate::json_utils::json_path_sep_to_dot;
use crate::postings::IndexingContext;
use crate::schema::{Field, Schema, Type};
use crate::{merge_field_meta_data, FieldMetadata, Term};

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub(crate) struct FieldConfig {
    pub typ: Type,
    pub indexed: bool,
    pub stored: bool,
    pub fast: bool,
}

impl FieldConfig {
    fn serialize(&self) -> [u8; 2] {
        let typ = self.typ.to_code();
        let flags = (self.indexed as u8) << 2 | (self.stored as u8) << 1 | (self.fast as u8);
        [typ, flags]
    }
    fn deserialize_from(data: [u8; 2]) -> io::Result<FieldConfig> {
        let typ = Type::from_code(data[0]).ok_or_else(|| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("could not deserialize type {}", data[0]),
            )
        })?;

        let data = data[1];
        let indexed = (data & 0b100) != 0;
        let stored = (data & 0b010) != 0;
        let fast = (data & 0b001) != 0;

        Ok(FieldConfig {
            typ,
            indexed,
            stored,
            fast,
        })
    }
}
/// Serializes the split fields.
pub(crate) fn serialize_segment_fields(
    ctx: IndexingContext,
    wrt: &mut dyn io::Write,
    schema: &Schema,
    unordered_id_to_ordered_id: &[(OrderedPathId, TinySet)],
    mut columns: Vec<(String, Type)>,
) -> crate::Result<()> {
    let mut field_list_set: HashSet<(Field, OrderedPathId, TinySet)> = HashSet::default();
    let mut encoded_fields = Vec::new();
    let mut map_to_canonical = FnvHashMap::default();

    // Replace unordered ids by ordered ids to be able to sort
    let ordered_id_to_path = ctx.path_to_unordered_id.ordered_id_to_path();

    for (key, _addr) in ctx.term_index.iter() {
        let field = Term::wrap(key).field();
        let field_entry = schema.get_field_entry(field);
        if field_entry.field_type().value_type() == Type::Json {
            let byte_range_unordered_id = 5..5 + 4;
            let unordered_id =
                u32::from_be_bytes(key[byte_range_unordered_id.clone()].try_into().unwrap());
            let (path_id, typ_code_bitvec) = unordered_id_to_ordered_id[unordered_id as usize];
            if !field_list_set.contains(&(field, path_id, typ_code_bitvec)) {
                field_list_set.insert((field, path_id, typ_code_bitvec));
                let mut build_path = |field_name: &str, mut json_path: String| {
                    // In this case we need to map the potential fast field to the field name
                    // accepted by the query parser.
                    let create_canonical =
                        !field_entry.is_expand_dots_enabled() && json_path.contains('.');
                    if create_canonical {
                        // Without expand dots enabled dots need to be escaped.
                        let escaped_json_path = json_path.replace('.', "\\.");
                        let full_path = format!("{}.{}", field_name, escaped_json_path);
                        let full_path_unescaped = format!("{}.{}", field_name, &json_path);
                        map_to_canonical.insert(full_path_unescaped, full_path.to_string());
                        full_path
                    } else {
                        // With expand dots enabled, we can use '.' instead of '\u{1}'.
                        json_path_sep_to_dot(&mut json_path);
                        format!("{}.{}", field_name, json_path)
                    }
                };

                let path = build_path(
                    field_entry.name(),
                    ordered_id_to_path[path_id.path_id() as usize].to_string(), /* String::from_utf8(key[5..].to_vec()).unwrap(), */
                );
                encoded_fields.push((path, typ_code_bitvec));
            }
        }
    }

    let mut indexed_fields: Vec<FieldMetadata> = Vec::new();
    for (_field, field_entry) in schema.fields() {
        let field_name = field_entry.name().to_string();
        let is_indexed = field_entry.is_indexed();

        let is_json = field_entry.field_type().value_type() == Type::Json;
        if is_indexed && !is_json {
            indexed_fields.push(FieldMetadata {
                indexed: true,
                stored: false,
                field_name: field_name.to_string(),
                fast: false,
                typ: field_entry.field_type().value_type(),
            });
        }
    }
    for (field_name, field_type_set) in encoded_fields {
        for field_type in field_type_set {
            let column_type = ColumnType::try_from_code(field_type as u8).unwrap();
            indexed_fields.push(FieldMetadata {
                indexed: true,
                stored: false,
                field_name: field_name.to_string(),
                fast: false,
                typ: Type::from(column_type),
            });
        }
    }
    let mut fast_fields: Vec<FieldMetadata> = columns
        .iter_mut()
        .map(|(field_name, typ)| {
            json_path_sep_to_dot(field_name);
            // map to canonical path, to avoid similar but different entries.
            // Eventually we should just accept '.' seperated for all cases.
            let field_name = map_to_canonical
                .get(field_name)
                .unwrap_or(field_name)
                .to_string();
            FieldMetadata {
                indexed: false,
                stored: false,
                field_name,
                fast: true,
                typ: *typ,
            }
        })
        .collect();
    // Since the type is encoded differently in the fast field and in the inverted index,
    // the order of the fields is not guaranteed to be the same. Therefore, we sort the fields.
    // If we are sure that the order is the same, we can remove this sort.
    indexed_fields.sort_unstable();
    fast_fields.sort_unstable();
    let merged = merge_field_meta_data(vec![indexed_fields, fast_fields], schema);
    let out = serialize_split_fields(&merged);
    wrt.write_all(&out)?;

    Ok(())
}

/// Serializes the Split fields.
///
/// `fields_metadata` has to be sorted.
pub fn serialize_split_fields(fields_metadata: &[FieldMetadata]) -> Vec<u8> {
    // ensure that fields_metadata is strictly sorted.
    debug_assert!(fields_metadata.windows(2).all(|w| w[0] < w[1]));
    let mut payload = Vec::new();
    // Write Num Fields
    let length = fields_metadata.len() as u32;
    payload.extend_from_slice(&length.to_le_bytes());

    for field_metadata in fields_metadata {
        write_field(field_metadata, &mut payload);
    }
    let compression_level = 3;
    let payload_compressed = zstd::stream::encode_all(&mut &payload[..], compression_level)
        .expect("zstd encoding failed");
    let mut out = Vec::new();
    // Write Header -- Format Version
    let format_version = 1u8;
    out.push(format_version);
    // Write Payload
    out.extend_from_slice(&payload_compressed);
    out
}

fn write_field(field_metadata: &FieldMetadata, out: &mut Vec<u8>) {
    let field_config = FieldConfig {
        typ: field_metadata.typ,
        indexed: field_metadata.indexed,
        stored: field_metadata.stored,
        fast: field_metadata.fast,
    };

    // Write Config 2 bytes
    out.extend_from_slice(&field_config.serialize());
    let str_length = field_metadata.field_name.len() as u16;
    // Write String length 2 bytes
    out.extend_from_slice(&str_length.to_le_bytes());
    out.extend_from_slice(field_metadata.field_name.as_bytes());
}

/// Reads a fixed number of bytes into an array and returns the array.
fn read_exact_array<R: Read, const N: usize>(reader: &mut R) -> io::Result<[u8; N]> {
    let mut buffer = [0u8; N];
    reader.read_exact(&mut buffer)?;
    Ok(buffer)
}

/// Reads the Split fields from a zstd compressed stream of bytes
pub fn read_split_fields<R: Read>(
    mut reader: R,
) -> io::Result<impl Iterator<Item = io::Result<FieldMetadata>>> {
    let format_version = read_exact_array::<_, 1>(&mut reader)?[0];
    assert_eq!(format_version, 1);
    let reader = zstd::Decoder::new(reader)?;
    read_split_fields_from_zstd(reader)
}

fn read_field<R: Read>(reader: &mut R) -> io::Result<FieldMetadata> {
    // Read FieldConfig (2 bytes)
    let config_bytes = read_exact_array::<_, 2>(reader)?;
    let field_config = FieldConfig::deserialize_from(config_bytes)?; // Assuming this returns a Result

    // Read field name length and the field name
    let name_len = u16::from_le_bytes(read_exact_array::<_, 2>(reader)?) as usize;

    let mut data = vec![0; name_len];
    reader.read_exact(&mut data)?;

    let field_name = String::from_utf8(data).map_err(|err| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!(
                "Encountered invalid utf8 when deserializing field name: {}",
                err
            ),
        )
    })?;
    Ok(FieldMetadata {
        field_name,
        typ: field_config.typ,
        indexed: field_config.indexed,
        stored: field_config.stored,
        fast: field_config.fast,
    })
}

/// Reads the Split fields from a stream of bytes
fn read_split_fields_from_zstd<R: Read>(
    mut reader: R,
) -> io::Result<impl Iterator<Item = io::Result<FieldMetadata>>> {
    let mut num_fields = u32::from_le_bytes(read_exact_array::<_, 4>(&mut reader)?);

    Ok(std::iter::from_fn(move || {
        if num_fields == 0 {
            return None;
        }
        num_fields -= 1;

        Some(read_field(&mut reader))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_config_deser_test() {
        let field_config = FieldConfig {
            typ: Type::Str,
            indexed: true,
            stored: false,
            fast: true,
        };
        let serialized = field_config.serialize();
        let deserialized = FieldConfig::deserialize_from(serialized).unwrap();
        assert_eq!(field_config, deserialized);
    }
    #[test]
    fn write_read_field_test() {
        for typ in Type::iter_values() {
            let field_metadata = FieldMetadata {
                field_name: "test".to_string(),
                typ,
                indexed: true,
                stored: true,
                fast: true,
            };
            let mut out = Vec::new();
            write_field(&field_metadata, &mut out);
            let deserialized = read_field(&mut &out[..]).unwrap();
            assert_eq!(field_metadata, deserialized);
        }
        let field_metadata = FieldMetadata {
            field_name: "test".to_string(),
            typ: Type::Str,
            indexed: false,
            stored: true,
            fast: true,
        };
        let mut out = Vec::new();
        write_field(&field_metadata, &mut out);
        let deserialized = read_field(&mut &out[..]).unwrap();
        assert_eq!(field_metadata, deserialized);

        let field_metadata = FieldMetadata {
            field_name: "test".to_string(),
            typ: Type::Str,
            indexed: false,
            stored: false,
            fast: true,
        };
        let mut out = Vec::new();
        write_field(&field_metadata, &mut out);
        let deserialized = read_field(&mut &out[..]).unwrap();
        assert_eq!(field_metadata, deserialized);

        let field_metadata = FieldMetadata {
            field_name: "test".to_string(),
            typ: Type::Str,
            indexed: true,
            stored: false,
            fast: false,
        };
        let mut out = Vec::new();
        write_field(&field_metadata, &mut out);
        let deserialized = read_field(&mut &out[..]).unwrap();
        assert_eq!(field_metadata, deserialized);
    }
    #[test]
    fn write_split_fields_test() {
        let fields_metadata = vec![
            FieldMetadata {
                field_name: "test".to_string(),
                typ: Type::Str,
                indexed: true,
                stored: true,
                fast: true,
            },
            FieldMetadata {
                field_name: "test2".to_string(),
                typ: Type::Str,
                indexed: true,
                stored: false,
                fast: false,
            },
            FieldMetadata {
                field_name: "test3".to_string(),
                typ: Type::U64,
                indexed: true,
                stored: false,
                fast: true,
            },
        ];

        let out = serialize_split_fields(&fields_metadata);

        let deserialized: Vec<FieldMetadata> = read_split_fields(&mut &out[..])
            .unwrap()
            .map(|el| el.unwrap())
            .collect();

        assert_eq!(fields_metadata, deserialized);
    }
}
