use std::ops::BitOr;

use serde::{Deserialize, Serialize};

use crate::schema::flags::StoredFlag;
use crate::spatial::geometry_set::RingOrientation;

/// Define how a spatial field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct SpatialOptions {
    #[serde(default)]
    stored: bool,
    /// Whether polygon rings follow the right-hand rule of RFC 7946, rather than bounding the
    /// smaller region. See [`RingOrientation`].
    #[serde(default)]
    oriented: bool,
}

/// The field will be untokenized and indexed.
pub const SPATIAL: SpatialOptions = SpatialOptions {
    stored: false,
    oriented: false,
};

impl SpatialOptions {
    /// Returns true if the geometry is to be stored.
    #[inline]
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns true if polygon rings follow the right-hand rule of RFC 7946.
    #[inline]
    pub fn is_oriented(&self) -> bool {
        self.oriented
    }

    /// Have polygon rings follow the right-hand rule of RFC 7946: a polygon lies to the left of
    /// its exterior ring and to the right of its holes, so that it may be larger than a
    /// hemisphere. By default a ring bounds the smaller of the two regions it separates,
    /// whichever way it runs. Query polygons on the field are read the same way.
    #[must_use]
    pub fn set_oriented(mut self) -> SpatialOptions {
        self.oriented = true;
        self
    }

    /// How the field's polygon rings pick the region they bound.
    pub fn ring_orientation(&self) -> RingOrientation {
        if self.oriented {
            RingOrientation::RightHand
        } else {
            RingOrientation::Smaller
        }
    }
}

impl<T: Into<SpatialOptions>> BitOr<T> for SpatialOptions {
    type Output = SpatialOptions;

    fn bitor(self, other: T) -> SpatialOptions {
        let other = other.into();
        SpatialOptions {
            stored: self.stored | other.stored,
            oriented: self.oriented | other.oriented,
        }
    }
}

impl From<StoredFlag> for SpatialOptions {
    fn from(_: StoredFlag) -> SpatialOptions {
        SpatialOptions {
            stored: true,
            oriented: false,
        }
    }
}

// #[cfg(test)]
// mod tests {
// use crate::schema::*;
//
// #[test]
// fn test_field_options() {
// let field_options = STORED | SPATIAL;
// assert!(field_options.is_stored());
// let mut schema_builder = Schema::builder();
// schema_builder.add_spatial_index("where", SPATIAL | STORED);
// }
// }
