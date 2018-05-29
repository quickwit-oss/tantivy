use std::ptr;
const SEED: u32 = 3_242_157_231u32;
const M: u32 = 0x5bd1_e995;

#[inline(always)]
pub fn murmurhash2(key: &[u8]) -> u32 {
    let mut key_ptr: *const u32 = key.as_ptr() as *const u32;
    let len = key.len() as u32;
    let mut h: u32 = SEED ^ len;

    let num_blocks = len >> 2;
    for _ in 0..num_blocks {
        let mut k: u32 = unsafe { ptr::read_unaligned(key_ptr) }; // ok because of num_blocks definition
        k = k.wrapping_mul(M);
        k ^= k >> 24;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
        key_ptr = key_ptr.wrapping_offset(1);
    }

    // Handle the last few bytes of the input array
    let remaining: &[u8] = &key[key.len() & !3..];
    match remaining.len() {
        3 => {
            h ^= u32::from(remaining[2]) << 16;
            h ^= u32::from(remaining[1]) << 8;
            h ^= u32::from(remaining[0]);
            h = h.wrapping_mul(M);
        }
        2 => {
            h ^= u32::from(remaining[1]) << 8;
            h ^= u32::from(remaining[0]);
            h = h.wrapping_mul(M);
        }
        1 => {
            h ^= u32::from(remaining[0]);
            h = h.wrapping_mul(M);
        }
        _ => {}
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^ (h >> 15)
}