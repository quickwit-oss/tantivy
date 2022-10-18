use std::net::{IpAddr, Ipv6Addr};
use std::ops::BitOr;

use serde::{Deserialize, Serialize};

use super::flags::{FastFlag, IndexedFlag, SchemaFlagList, StoredFlag};
use super::Cardinality;

/// Trait to convert into an Ipv6Addr.
pub trait IntoIpv6Addr {
    /// Consumes the object and returns an Ipv6Addr.
    fn into_ipv6_addr(self) -> Ipv6Addr;
}

impl IntoIpv6Addr for IpAddr {
    fn into_ipv6_addr(self) -> Ipv6Addr {
        match self {
            IpAddr::V4(addr) => addr.to_ipv6_mapped(),
            IpAddr::V6(addr) => addr,
        }
    }
}

/// Define how an ip field should be handled by tantivy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IpAddrOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    fast: Option<Cardinality>,
    stored: bool,
    indexed: bool,
    fieldnorms: bool,
}

impl IpAddrOptions {
    /// Returns true iff the value is a fast field.
    pub fn is_fast(&self) -> bool {
        self.fast.is_some()
    }

    /// Returns `true` if the ip address should be stored in the doc store.
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns true iff the value is indexed and therefore searchable.
    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    /// Returns true if and only if the value is normed.
    pub fn fieldnorms(&self) -> bool {
        self.fieldnorms
    }

    /// Returns the cardinality of the fastfield.
    ///
    /// If the field has not been declared as a fastfield, then
    /// the method returns None.
    pub fn get_fastfield_cardinality(&self) -> Option<Cardinality> {
        self.fast
    }

    /// Set the field as normed.
    ///
    /// Setting an integer as normed will generate
    /// the fieldnorm data for it.
    #[must_use]
    pub fn set_fieldnorms(mut self) -> Self {
        self.fieldnorms = true;
        self
    }

    /// Sets the field as stored
    #[must_use]
    pub fn set_stored(mut self) -> Self {
        self.stored = true;
        self
    }

    /// Set the field as indexed.
    ///
    /// Setting an ip address as indexed will generate
    /// a posting list for each value taken by the ip address.
    /// Ips are normalized to IpV6.
    ///
    /// This is required for the field to be searchable.
    #[must_use]
    pub fn set_indexed(mut self) -> Self {
        self.indexed = true;
        self
    }

    /// Set the field as a fast field.
    ///
    /// Fast fields are designed for random access.
    /// Access time are similar to a random lookup in an array.
    /// If more than one value is associated with a fast field, only the last one is
    /// kept.
    #[must_use]
    pub fn set_fast(mut self, cardinality: Cardinality) -> Self {
        self.fast = Some(cardinality);
        self
    }
}

impl From<()> for IpAddrOptions {
    fn from(_: ()) -> IpAddrOptions {
        IpAddrOptions::default()
    }
}

impl From<FastFlag> for IpAddrOptions {
    fn from(_: FastFlag) -> Self {
        IpAddrOptions {
            fieldnorms: false,
            indexed: false,
            stored: false,
            fast: Some(Cardinality::SingleValue),
        }
    }
}

impl From<StoredFlag> for IpAddrOptions {
    fn from(_: StoredFlag) -> Self {
        IpAddrOptions {
            fieldnorms: false,
            indexed: false,
            stored: true,
            fast: None,
        }
    }
}

impl From<IndexedFlag> for IpAddrOptions {
    fn from(_: IndexedFlag) -> Self {
        IpAddrOptions {
            fieldnorms: true,
            indexed: true,
            stored: false,
            fast: None,
        }
    }
}

impl<T: Into<IpAddrOptions>> BitOr<T> for IpAddrOptions {
    type Output = IpAddrOptions;

    fn bitor(self, other: T) -> IpAddrOptions {
        let other = other.into();
        IpAddrOptions {
            fieldnorms: self.fieldnorms | other.fieldnorms,
            indexed: self.indexed | other.indexed,
            stored: self.stored | other.stored,
            fast: self.fast.or(other.fast),
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for IpAddrOptions
where
    Head: Clone,
    Tail: Clone,
    Self: BitOr<Output = Self> + From<Head> + From<Tail>,
{
    fn from(head_tail: SchemaFlagList<Head, Tail>) -> Self {
        Self::from(head_tail.head) | Self::from(head_tail.tail)
    }
}
