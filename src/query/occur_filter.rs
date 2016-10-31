use query::Occur;

#[derive(Clone)]
pub struct OccurFilter {
    and_mask: u64,
    result: u64,    
}

impl OccurFilter {
    pub fn accept(&self, ord_set: u64) -> bool {
        (self.and_mask & ord_set) == self.result
    }
    
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
