use crate::schema::Value;
use serde::Serialize;
use std::collections::BTreeMap;

/// Internal representation of a document used for JSON
/// serialization.
///
/// A `NamedFieldDocument` is a simple representation of a document
/// as a `BTreeMap<String, Vec<Value>>`.
///
#[derive(Serialize)]
pub struct NamedFieldDocument(pub BTreeMap<String, Vec<Value>>);
