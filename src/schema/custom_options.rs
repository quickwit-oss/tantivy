use serde::{Deserialize, Serialize};

/// Configuration for a plugin-defined field type.
///
/// Custom field types are opaque to tantivy's built-in components: they are never indexed,
/// stored, or turned into fast fields by the built-ins. A [`SegmentPlugin`] consumes the
/// field's values by matching on [`type_name`](Self::type_name) in the schema and writes its
/// own segment files.
///
/// `type_name` identifies the custom type; `params` carries opaque,
/// type-specific configuration that the consuming plugin interprets.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct CustomOptions {
    type_name: String,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    params: serde_json::Value,
}

impl CustomOptions {
    /// Creates a new `CustomOptions` for the given custom type name and parameters.
    pub fn new<T: Into<String>>(type_name: T, params: serde_json::Value) -> CustomOptions {
        CustomOptions {
            type_name: type_name.into(),
            params,
        }
    }

    /// The name identifying this custom type. Plugins claim a type by matching on it.
    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    /// Opaque, type-specific configuration interpreted by the consuming plugin.
    pub fn params(&self) -> &serde_json::Value {
        &self.params
    }
}
