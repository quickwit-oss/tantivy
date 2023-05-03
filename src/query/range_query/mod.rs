use std::ops::Bound;

use crate::schema::Type;

mod fast_field_range_query;
mod range_query;
mod range_query_ip_fastfield;
mod range_query_u64_fastfield;

pub use self::range_query::{RangeQuery, RangeWeight};
pub use self::range_query_ip_fastfield::IPFastFieldRangeWeight;
pub use self::range_query_u64_fastfield::FastFieldRangeWeight;

// TODO is this correct?
pub(crate) fn is_type_valid_for_fastfield_range_query(typ: Type) -> bool {
    match typ {
        Type::U64 | Type::I64 | Type::F64 | Type::Bool | Type::Date => true,
        Type::IpAddr => true,
        Type::Str | Type::Facet | Type::Bytes | Type::Json => false,
    }
}

fn map_bound<TFrom, TTo>(bound: &Bound<TFrom>, transform: impl Fn(&TFrom) -> TTo) -> Bound<TTo> {
    use self::Bound::*;
    match bound {
        Excluded(ref from_val) => Excluded(transform(from_val)),
        Included(ref from_val) => Included(transform(from_val)),
        Unbounded => Unbounded,
    }
}

fn map_bound_res<TFrom, TTo, Err>(
    bound: &Bound<TFrom>,
    transform: impl Fn(&TFrom) -> Result<TTo, Err>,
) -> Result<Bound<TTo>, Err> {
    use self::Bound::*;
    Ok(match bound {
        Excluded(ref from_val) => Excluded(transform(from_val)?),
        Included(ref from_val) => Included(transform(from_val)?),
        Unbounded => Unbounded,
    })
}
