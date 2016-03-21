use std::num::Wrapping;



// ported from  libdivide.h by ridiculous_fish


const LIBDIVIDE_32_SHIFT_MASK: u8 = 0x1F;
const LIBDIVIDE_ADD_MARKER: u8 = 0x40;
const LIBDIVIDE_U32_SHIFT_PATH: u8 = 0x80;

pub fn count_leading_zeros(mut val: u32) -> u8 {
    if val == 0 {
        return 32;
    }
    let mut result = 0u8;
    while (val & (1u32 << 31)) == 0 {
        val <<= 1;
        result += 1;
    }
    return result;
}

pub fn count_trailing_zeros(mut val: u32) -> u8 {
    let mut result = 0u8;
    val = (val ^ (val - 1)) >> 1;
    while val != 0 {
        val >>= 1;
        result += 1;
    }
    result
}

#[derive(Debug)]
pub struct DividerU32 {
    magic: u32,
    more: u8,
}

fn divide_64_div_32_to_32(n: u64, d: u32) -> (u32, u32) {
    let d64: u64 = d as u64;
    let q: u64 = n / d64;
    let r: u32 = (Wrapping(n) - (Wrapping(q) * Wrapping(d64))).0 as u32;
    (q as u32, r)
}

impl DividerU32 {
    pub fn divide_by(d: u32) -> DividerU32 {
        if (d & (d - 1)) == 0 {
            DividerU32 {
                magic: 0,
                more: count_trailing_zeros(d) | LIBDIVIDE_U32_SHIFT_PATH,
            }
        }
        else {
            let floor_log_2_d: u8 = 31 - count_leading_zeros(d);
            let more: u8;
            let (mut proposed_m, rem) = divide_64_div_32_to_32((1u64 << floor_log_2_d) << 32, d);
            assert!(rem > 0 && rem < d);
            let e = d - rem;
            if e < (1u32 << floor_log_2_d) {
                more = floor_log_2_d;
            }
            else {
                proposed_m = proposed_m << 1;
                let twice_rem: u32 = rem * 2;
                if twice_rem >= d || twice_rem < rem {
                    proposed_m += 1;
                }
                more = floor_log_2_d | LIBDIVIDE_ADD_MARKER;
            }
            DividerU32 {
                magic: 1 + proposed_m,
                more: more,
            }
        }
    }

    pub fn divide(&self, n: u32) -> u32 {
        if self.more & LIBDIVIDE_U32_SHIFT_PATH != 0 {
            n >> (self.more & LIBDIVIDE_32_SHIFT_MASK)
        }
        else {
            let q_shifted = (self.magic as u64) * (n as u64);
            let q = (q_shifted >> 32) as u32;
            if self.more & LIBDIVIDE_ADD_MARKER != 0 {
                let t = ((n - q) >> 1) + q;
                t >> (self.more & LIBDIVIDE_32_SHIFT_MASK)
            }
            else {
                q >> self.more
            }
        }
    }
}



#[cfg(test)]
mod tests {
    use super::DividerU32;

    #[test]
    fn test_libdivide() {
        for d in 1..32 {
            let divider = DividerU32::divide_by(d);
            for i in 0..100_000 {
                assert_eq!(divider.divide(i), i / d);
            }
        }
    }
}
