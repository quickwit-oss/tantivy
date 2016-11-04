use query::Occur;


/// An OccurFilter represents a filter over a bitset of
// at most 64 elements.
///
/// It wraps some simple bitmask to compute the filter
/// rapidly. 
#[derive(Clone, Copy)]
pub struct OccurFilter {
    and_mask: u64,
    result: u64,    
}

impl OccurFilter {

    /// Returns true if the bitset is matching the occur list.
    pub fn accept(&self, ord_set: u64) -> bool {
        (self.and_mask & ord_set) == self.result
    }
    
    /// Builds an `OccurFilter` from a list of `Occur`. 
    pub fn new(occurs: &[Occur]) -> OccurFilter {
        let mut and_mask = 0u64;
        let mut result = 0u64;
        for (i, occur) in occurs.iter().enumerate() {
            let shift = 1 << i;
            match *occur {
                Occur::Must => {
                    and_mask |= shift;
                    result |= shift;
                },
                Occur::MustNot => {
                    and_mask |= shift;
                },
                Occur::Should => {},
            }
        }
        OccurFilter {
            and_mask: and_mask,
            result: result
        }
    }
}
