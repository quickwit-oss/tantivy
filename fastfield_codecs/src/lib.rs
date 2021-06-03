pub mod bitpacked;
pub mod linearinterpol;

#[cfg(test)]
mod tests {
    use super::*;
    pub fn stats_from_vec(data: &[u64]) -> FastFieldStats {
        let min_value = data.iter().cloned().min().unwrap_or(0);
        let max_value = data.iter().cloned().max().unwrap_or(0);
        FastFieldStats {
            min_value,
            max_value,
            num_vals: data.len() as u64,
        }
    }
}

/// FastFieldDataAccess is the trait to access fast field data during serialization and estimation.
pub trait FastFieldDataAccess: Clone {
    /// Return the value associated to the given document.
    ///
    /// Whenever possible use the Iterator passed to the fastfield creation instead, for performance reasons.
    ///
    /// # Panics
    ///
    /// May panic if `doc` is greater than the segment
    fn get(&self, doc: u32) -> u64;
}

/// The FastFieldSerializerEstimate trait is required on all variants
/// of fast field compressions, to decide which one to choose.
pub trait FastFieldSerializerEstimate {
    /// returns an estimate of the compression ratio.
    fn estimate(
        fastfield_accessor: &impl FastFieldDataAccess,
        stats: FastFieldStats,
    ) -> (f32, &'static str);
}

/// `CodecId` is required by each Codec.
///
/// It needs to provide a unique name and id, which is
/// used for debugging and de/serialization.
pub trait CodecId {
    const NAME: &'static str;
    const ID: u8;
}

#[derive(Debug, Clone)]
pub struct FastFieldStats {
    pub min_value: u64,
    pub max_value: u64,
    pub num_vals: u64,
}

impl<'a> FastFieldDataAccess for &'a [u64] {
    fn get(&self, doc: u32) -> u64 {
        self[doc as usize]
    }
}

impl FastFieldDataAccess for Vec<u64> {
    fn get(&self, doc: u32) -> u64 {
        self[doc as usize]
    }
}
