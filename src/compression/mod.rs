mod intersection;
pub use self::intersection::intersection;

mod s4bp128;
pub use self::s4bp128::{S4BP128Encoder, S4BP128Decoder};

mod block128;
pub use self::block128::{Block128Encoder, Block128Decoder};

mod vints;
pub use self::vints::{SortedVIntsEncoder, SortedVIntsDecoder};



#[cfg(test)]
pub mod tests {

    use rand::Rng;
    use rand::SeedableRng;
    use rand::XorShiftRng;

    fn generate_array_with_seed(n: usize, ratio: f32, seed_val: u32) -> Vec<u32> {
        let seed: &[u32; 4] = &[1, 2, 3, seed_val];
        let mut rng: XorShiftRng = XorShiftRng::from_seed(*seed);
        (0..u32::max_value())
            .filter(|_| rng.next_f32()< ratio)
            .take(n)
            .collect()
    }

    pub fn generate_array(n: usize, ratio: f32) -> Vec<u32> {
        generate_array_with_seed(n, ratio, 4)
    }
}
