use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::schema::Value;

/// Internal representation of a document used for JSON
/// serialization.
///
/// A `NamedFieldDocument` is a simple representation of a document
/// as a `BTreeMap<String, Vec<Value>>`.
#[derive(Debug, Deserialize, Serialize)]
#[serde(bound(deserialize = "'static: 'de, 'de: 'static"))]
pub struct NamedFieldDocument(pub BTreeMap<String, Vec<Value<'static>>>);
