use std::io::{self, Read, Write};

use crate::BinarySerializable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum DictionaryKind {
    Fst = 1,
    SSTable = 2,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DictionaryFooter {
    pub kind: DictionaryKind,
    pub version: u32,
}

impl DictionaryFooter {
    pub fn verify_equal(&self, other: &DictionaryFooter) -> io::Result<()> {
        if self.kind != other.kind {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Invalid dictionary type, expected {:?}, found {:?}",
                    self.kind, other.kind
                ),
            ));
        }
        if self.version != other.version {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Unsuported dictionary version, expected {}, found {}",
                    self.version, other.version
                ),
            ));
        }
        Ok(())
    }
}

impl BinarySerializable for DictionaryFooter {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        self.version.serialize(writer)?;
        (self.kind as u32).serialize(writer)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let version = u32::deserialize(reader)?;
        let kind = u32::deserialize(reader)?;
        let kind = match kind {
            1 => DictionaryKind::Fst,
            2 => DictionaryKind::SSTable,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("invalid dictionary kind: {kind}"),
                ))
            }
        };

        Ok(DictionaryFooter { kind, version })
    }
}
