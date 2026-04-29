use std::borrow::Cow;
use std::ops::BitOr;

use serde::{Deserialize, Serialize};

use crate::schema::flags::StoredFlag;

const DEFAULT_SPATIAL_INDEX_NAME: &str = "sphere";

/// Name of a registered spatial index implementation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpatialIndexName(Cow<'static, str>);

impl Default for SpatialIndexName {
    fn default() -> Self {
        SpatialIndexName::from_static(DEFAULT_SPATIAL_INDEX_NAME)
    }
}

impl SpatialIndexName {
    /// Create from a static string.
    pub const fn from_static(name: &'static str) -> Self {
        SpatialIndexName(Cow::Borrowed(name))
    }

    /// Create from a runtime string.
    pub fn from_name(name: &str) -> Self {
        SpatialIndexName(Cow::Owned(name.to_string()))
    }

    /// The name as a string slice.
    pub fn name(&self) -> &str {
        &self.0
    }
}

/// Define how a spatial field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct SpatialOptions {
    #[serde(default)]
    stored: bool,
    #[serde(default)]
    spatial_index: SpatialIndexName,
}

/// The field will be indexed on the unit sphere.
pub const SPHERE: SpatialOptions = SpatialOptions {
    stored: false,
    spatial_index: SpatialIndexName(Cow::Borrowed("sphere")),
};

/// The field will be indexed on the Euclidean plane.
pub const PLANE: SpatialOptions = SpatialOptions {
    stored: false,
    spatial_index: SpatialIndexName(Cow::Borrowed("plane")),
};

impl SpatialOptions {
    /// Returns true if the geometry is to be stored.
    #[inline]
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns the name of the spatial index implementation.
    pub fn spatial_index_name(&self) -> &str {
        self.spatial_index.name()
    }

    /// Set the spatial index name.
    pub fn set_spatial_index(mut self, name: &str) -> Self {
        self.spatial_index = SpatialIndexName::from_name(name);
        self
    }
}

impl<T: Into<SpatialOptions>> BitOr<T> for SpatialOptions {
    type Output = SpatialOptions;

    fn bitor(self, other: T) -> SpatialOptions {
        let other = other.into();
        SpatialOptions {
            stored: self.stored | other.stored,
            spatial_index: if other.spatial_index.name() != DEFAULT_SPATIAL_INDEX_NAME {
                other.spatial_index
            } else {
                self.spatial_index
            },
        }
    }
}

impl From<StoredFlag> for SpatialOptions {
    fn from(_: StoredFlag) -> SpatialOptions {
        SpatialOptions {
            stored: true,
            spatial_index: SpatialIndexName::default(),
        }
    }
}
