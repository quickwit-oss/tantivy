use std::net::{IpAddr, Ipv6Addr};

pub trait MonotonicallyMappableToU128: 'static + PartialOrd + Copy + Send + Sync {
    /// Converts a value to u128.
    ///
    /// Internally all fast field values are encoded as u64.
    fn to_u128(self) -> u128;

    /// Converts a value from u128
    ///
    /// Internally all fast field values are encoded as u64.
    /// **Note: To be used for converting encoded Term, Posting values.**
    fn from_u128(val: u128) -> Self;
}

impl MonotonicallyMappableToU128 for u128 {
    fn to_u128(self) -> u128 {
        self
    }

    fn from_u128(val: u128) -> Self {
        val
    }
}

impl MonotonicallyMappableToU128 for IpAddr {
    fn to_u128(self) -> u128 {
        ip_to_u128(self)
    }

    fn from_u128(val: u128) -> Self {
        IpAddr::from(val.to_be_bytes())
    }
}

fn ip_to_u128(ip_addr: IpAddr) -> u128 {
    let ip_addr_v6: Ipv6Addr = match ip_addr {
        IpAddr::V4(v4) => v4.to_ipv6_mapped(),
        IpAddr::V6(v6) => v6,
    };
    u128::from_be_bytes(ip_addr_v6.octets())
}
