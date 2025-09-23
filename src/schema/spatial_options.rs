use std::ops::BitOr;

use serde::{Deserialize, Serialize};

use crate::schema::flags::StoredFlag;

/// Define how a spatial field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct SpatialOptions {
    #[serde(default)]
    stored: bool,
}

/// The field will be untokenized and indexed.
pub const SPATIAL: SpatialOptions = SpatialOptions { stored: false };

impl SpatialOptions {
    /// Returns true if the geometry is to be stored.
    #[inline]
    pub fn is_stored(&self) -> bool {
        self.stored
    }
}

impl<T: Into<SpatialOptions>> BitOr<T> for SpatialOptions {
    type Output = SpatialOptions;

    fn bitor(self, other: T) -> SpatialOptions {
        let other = other.into();
        SpatialOptions {
            stored: self.stored | other.stored,
        }
    }
}

impl From<StoredFlag> for SpatialOptions {
    fn from(_: StoredFlag) -> SpatialOptions {
        SpatialOptions { stored: true }
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::*;

    #[test]
    fn test_field_options() {
        let field_options = STORED | SPATIAL;
        assert!(field_options.is_stored());
    }
}
