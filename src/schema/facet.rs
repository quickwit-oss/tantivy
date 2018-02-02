use std::fmt::{self, Display, Debug, Formatter};
use std::str;
use std::io::{self, Read, Write};
use regex::Regex;
use std::borrow::Borrow;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Cow;
use common::BinarySerializable;


const SLASH_BYTE: u8 = '/' as u8;
const ESCAPE_BYTE: u8 = '\\' as u8;

/// BYTE used as a level separation in the binary
/// representation of facets.
pub const FACET_SEP_BYTE: u8 = 0u8;

/// A Facet represent a point in a given hierarchy.
///
/// They are typically represented similarly to a filepath.
/// For instance, an e-commerce website could
/// have a `Facet` for `/electronics/tv_and_video/led_tv`.
///
/// A document can be associated to any number of facets.
/// The hierarchy implicitely imply that a document
/// belonging to a facet also belongs to the ancestor of
/// its facet. In the example above, `/electronics/tv_and_video/`
/// and `/electronics`.
#[derive(Clone, Eq, Hash, PartialEq, Ord, PartialOrd)]
pub struct Facet(Vec<u8>);

impl Facet {

    /// Returns a new instance of the "root facet"
    /// Equivalent to `/`.
    pub fn root() -> Facet {
        Facet(vec![])
    }

    /// Returns true iff the facet is the root facet `/`.
    pub fn is_root(&self) -> bool {
        self.encoded_bytes().is_empty()
    }

    /// Returns a binary representation of the facet.
    ///
    /// In this representation, `0u8` is used as a separator
    /// and the string parts of the facet are unescaped.
    /// (The first `/` is not encoded at all).
    ///
    /// This representation has the benefit of making it possible to
    /// express "being a child of a given facet" as a range over
    /// the term ordinals.
    pub fn encoded_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Creates a `Facet` from its binary representation.
    pub(crate) fn from_encoded(encoded_bytes: Vec<u8>) -> Facet {
        Facet(encoded_bytes)
    }

    /// Parse a text representation of a facet.
    ///
    /// It is conceptually, if one of the steps of this path
    /// contains a `/` or a `\`, it should be escaped
    /// using an anti-slash `/`.
    pub fn from_text<'a, T>(path: &'a T) -> Facet
        where T: ?Sized + AsRef<str> {
        From::from(path)
    }

    /// Returns a `Facet` from an iterator over the different
    /// steps of the facet path.
    ///
    /// The steps are expected to be unescaped.
    pub fn from_path<Path>(path: Path) -> Facet
        where
            Path: IntoIterator,
            Path::Item: ToString {
        let mut facet_bytes: Vec<u8> = Vec::with_capacity(100);
        let mut step_it = path.into_iter();
        if let Some(step) = step_it.next() {
            facet_bytes.extend_from_slice(step.to_string().as_bytes());
        }
        for step in step_it {
            facet_bytes.push(FACET_SEP_BYTE);
            facet_bytes.extend_from_slice(step.to_string().as_bytes());
        }
        Facet(facet_bytes)
    }

    /// Accessor for the inner buffer of the `Facet`.
    pub(crate) fn inner_buffer_mut(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }


    /// Returns `true` iff other is a subfacet of `self`.
    pub fn is_prefix_of(&self, other: &Facet) -> bool {
        let self_bytes: &[u8] = self.encoded_bytes();
        let other_bytes: &[u8] = other.encoded_bytes();
        if self_bytes.len() < other_bytes.len() {
            if other_bytes.starts_with(self_bytes) {
                return other_bytes[self_bytes.len()] == 0u8;
            }
        }
        false
    }
}

impl Borrow<[u8]> for Facet {
    fn borrow(&self) -> &[u8] {
        self.encoded_bytes()
    }
}

impl<'a, T: ?Sized + AsRef<str>> From<&'a T> for Facet {

    fn from(path_asref: &'a T) -> Facet {
        #[derive(Copy, Clone)]
        enum State {
            Escaped,
            Idle,
        }
        let path: &str = path_asref.as_ref();
        let mut facet_encoded = Vec::new();
        let mut state = State::Idle;
        let path_bytes = path.as_bytes();
        for &c in &path_bytes[1..] {
            match (state, c) {
                (State::Idle, ESCAPE_BYTE) => {
                    state = State::Escaped
                }
                (State::Idle, SLASH_BYTE) => {
                    facet_encoded.push(FACET_SEP_BYTE);
                }
                (State::Escaped, any_char) => {
                    state = State::Idle;
                    facet_encoded.push(any_char);
                }
                (State::Idle, other_char) => {
                    facet_encoded.push(other_char);
                }
            }
        }
        Facet(facet_encoded)
    }
}

impl BinarySerializable for Facet {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        <Vec<u8> as BinarySerializable>::serialize(&self.0, writer)
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let bytes = <Vec<u8> as BinarySerializable>::deserialize(reader)?;
        Ok(Facet(bytes))
    }
}

impl Display for Facet {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        for step in self.0.split(|&b| b == FACET_SEP_BYTE) {
            write!(f, "/")?;
            let step_str = unsafe { str::from_utf8_unchecked(step) };
            write!(f, "{}", escape_slashes(step_str))?;
        }
        Ok(())
    }
}

fn escape_slashes(s: &str) -> Cow<str> {
    lazy_static! {
        static ref SLASH_PTN: Regex = Regex::new(r"[\\/]").unwrap();
    }
    SLASH_PTN.replace_all(s, "\\/")
}

impl Serialize for Facet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Facet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where
        D: Deserializer<'de> {
        <&'de str as Deserialize<'de>>::deserialize(deserializer)
            .map(Facet::from)
    }
}

impl Debug for Facet {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Facet({})", self)?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {

    use super::Facet;

    #[test]
    fn test_root() {
        assert_eq!(Facet::root(), Facet::from("/"));
        assert_eq!(format!("{}", Facet::root()), "/");
        assert!(Facet::root().is_root());
    }

    #[test]
    fn test_facet_display() {
        {
            let v = ["first", "second", "third"];
            let facet = Facet::from_path(v.iter());
            assert_eq!(format!("{}", facet), "/first/second/third");
        }
        {
            let v = ["first", "sec/ond", "third"];
            let facet = Facet::from_path(v.iter());
            assert_eq!(format!("{}", facet), "/first/sec\\/ond/third");
        }
    }


    #[test]
    fn test_facet_debug() {
        let v = ["first", "second", "third"];
        let facet = Facet::from_path(v.iter());
        assert_eq!(format!("{:?}", facet), "Facet(/first/second/third)");
    }

}