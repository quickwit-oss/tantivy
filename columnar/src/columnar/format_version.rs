use core::fmt;
use std::fmt::{Display, Formatter};

use crate::InvalidData;

pub const VERSION_FOOTER_NUM_BYTES: usize = MAGIC_BYTES.len() + std::mem::size_of::<u32>();

/// We end the file by these 4 bytes just to somewhat identify that
/// this is indeed a columnar file.
const MAGIC_BYTES: [u8; 4] = [2, 113, 119, 66];

#[cfg(test)]
pub fn footer() -> [u8; VERSION_FOOTER_NUM_BYTES] {
    footer_for_version(CURRENT_VERSION)
}

pub fn footer_for_version(version: Version) -> [u8; VERSION_FOOTER_NUM_BYTES] {
    let mut footer_bytes = [0u8; VERSION_FOOTER_NUM_BYTES];
    footer_bytes[0..4].copy_from_slice(&version.to_bytes());
    footer_bytes[4..8].copy_from_slice(&MAGIC_BYTES[..]);
    footer_bytes
}

pub fn parse_footer(footer_bytes: [u8; VERSION_FOOTER_NUM_BYTES]) -> Result<Version, InvalidData> {
    if footer_bytes[4..8] != MAGIC_BYTES {
        return Err(InvalidData);
    }
    Version::try_from_bytes(footer_bytes[0..4].try_into().unwrap())
}

/// Version written for columnars without plain-encoded columns.
pub const CURRENT_VERSION: Version = Version::V2;

/// Version written for columnars with at least one plain-encoded column.
///
/// Readers that do not know `V3` fail cleanly when opening such a columnar.
pub const PLAIN_CODEC_VERSION: Version = Version::V3;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u32)]
pub enum Version {
    V1 = 1u32,
    V2 = 2u32,
    /// Same as `V2`, with plain-encoded `Str` / `Bytes` columns allowed.
    V3 = 3u32,
}

impl Display for Version {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Version::V1 => write!(f, "v1"),
            Version::V2 => write!(f, "v2"),
            Version::V3 => write!(f, "v3"),
        }
    }
}

impl Version {
    /// Returns true if columns of this columnar may use the plain codec.
    pub fn allows_plain_codec(self) -> bool {
        match self {
            Version::V1 | Version::V2 => false,
            Version::V3 => true,
        }
    }

    fn to_bytes(self) -> [u8; 4] {
        (self as u32).to_le_bytes()
    }

    fn try_from_bytes(bytes: [u8; 4]) -> Result<Version, InvalidData> {
        let code = u32::from_le_bytes(bytes);
        match code {
            1u32 => Ok(Version::V1),
            2u32 => Ok(Version::V2),
            3u32 => Ok(Version::V3),
            _ => Err(InvalidData),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_footer_deserialization() {
        let parsed_version: Version = parse_footer(footer()).unwrap();
        assert_eq!(Version::V2, parsed_version);
    }

    #[test]
    fn test_footer_for_version_deserialization() {
        for version in [Version::V1, Version::V2, Version::V3] {
            assert_eq!(parse_footer(footer_for_version(version)).unwrap(), version);
        }
    }

    #[test]
    fn test_version_serialization() {
        let version_to_tests: Vec<u32> = [0, 1 << 8, 1 << 16, 1 << 24]
            .iter()
            .copied()
            .flat_map(|offset| (0..255).map(move |el| el + offset))
            .collect();
        let mut valid_versions: HashSet<u32> = HashSet::default();
        for &i in &version_to_tests {
            let version_res = Version::try_from_bytes(i.to_le_bytes());
            if let Ok(version) = version_res {
                assert_eq!(version.to_bytes(), i.to_le_bytes());
                valid_versions.insert(i);
            }
        }
        assert_eq!(valid_versions.len(), 3);
    }
}
