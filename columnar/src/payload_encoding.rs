use serde::{Deserialize, Serialize};

/// Encoding used to store string and byte column payloads.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PayloadEncoding {
    /// Store values in a dictionary and address them by ordinal.
    #[default]
    Dictionary,
    /// Store values directly without assigning them dictionary ordinals.
    Plain,
}
