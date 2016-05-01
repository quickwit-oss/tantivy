use libc::size_t;
use std::ptr;
use std::iter;

extern {
    // fn encode_unsorted_native(data: *mut u32, num_els: size_t, output: *mut u32, output_capacity: size_t) -> size_t;
    // fn decode_unsorted_native(compressed_data: *const u32, compressed_size: size_t, uncompressed: *mut u32, output_capacity: size_t) -> size_t;

    fn intersection_native(left_data: *const u32, left_size: size_t, right_data: *const u32, right_size: size_t, output: *mut u32) -> size_t;

    // complete s4-bp128-dm
    fn encode_s4_bp128_dm_native(data: *mut u32, num_els: size_t, output: *mut u32, output_capacity: size_t) -> size_t;
    fn decode_s4_bp128_dm_native(compressed_data: *const u32, compressed_size: size_t, uncompressed: *mut u32, output_capacity: size_t) -> size_t;

    // bp128, only encodes group of 128 u32 at a time
    fn encode_sorted_block128_native(data: *mut u32, output: *mut u32, output_capacity: size_t) -> size_t;
    fn decode_sorted_block128_native(compressed_data: *const u32, compressed_size: size_t, uncompressed: *mut u32, output_capacity: size_t) -> size_t;

    // vints, used as the left over codec for the <128 remaining values
    fn encode_sorted_vint_native(data: *mut u32, num_els: size_t, output: *mut u32, output_capacity: size_t) -> size_t;
    fn decode_sorted_vint_native(compressed_data: *const u32, compressed_size: size_t, uncompressed: *mut u32, output_capacity: size_t) -> size_t;

}

pub fn intersection(left: &[u32], right: &[u32], output: &mut [u32]) -> usize {
    unsafe {
        intersection_native(
            left.as_ptr(), left.len(),
            right.as_ptr(), right.len(),
            output.as_mut_ptr())
    }
}



//-------------------------
// Vint


pub struct VIntEncoder {
    input_buffer: Vec<u32>,
    output_buffer: Vec<u32>,
}

impl VIntEncoder {

    pub fn new() -> VIntEncoder {
        VIntEncoder {
            input_buffer: Vec::with_capacity(128),
            output_buffer: iter::repeat(0u32).take(256).collect(),
        }
    }

    pub fn encode_sorted(&mut self, input: &[u32]) -> &[u32] {
        assert!(input.len() < 128);
        let input_len = input.len();
        let written_size: usize;
        // TODO use clone_from when available
        unsafe {
            ptr::copy_nonoverlapping(input.as_ptr(), self.input_buffer.as_mut_ptr(), input_len);
            written_size = encode_sorted_vint_native(
                self.input_buffer.as_mut_ptr(),
                input_len as size_t,
                self.output_buffer.as_mut_ptr(),
                256,
            );
        }
        return &self.output_buffer[0..written_size];
    }
}



pub struct VIntDecoder;

impl VIntDecoder {

    pub fn new() -> VIntDecoder {
        VIntDecoder
    }

    pub fn decode_sorted(&self,
                  compressed_data: &[u32],
                  uncompressed_values: &mut [u32]) -> size_t {
        unsafe {
            return decode_sorted_vint_native(
                compressed_data.as_ptr(),
                compressed_data.len() as size_t,
                uncompressed_values.as_mut_ptr(),
                uncompressed_values.len() as size_t);
        }
    }
}

//-------------------------
// Block128

pub struct Block128Encoder {
    input_buffer: Vec<u32>,
    output_buffer: Vec<u32>,
}

impl Block128Encoder {

    pub fn new() -> Block128Encoder {
        Block128Encoder {
            input_buffer: Vec::with_capacity(128),
            output_buffer: iter::repeat(0u32).take(256).collect(),
        }
    }

    pub fn encode_sorted(&mut self, input: &[u32]) -> &[u32] {
        assert_eq!(input.len(), 128);
        // TODO use clone_from when available
        let written_size: usize;
        unsafe {
            ptr::copy_nonoverlapping(input.as_ptr(), self.input_buffer.as_mut_ptr(), 128);
            written_size = encode_s4_bp128_dm_native(
                self.input_buffer.as_mut_ptr(),
                128,
                self.output_buffer.as_mut_ptr(),
                256,
            );
        }
        return &self.output_buffer[0..written_size];
    }
}

pub struct Block128Decoder;

impl Block128Decoder {

    pub fn new() -> Block128Decoder {
        Block128Decoder
    }

    pub fn decode_sorted(
          &self,
          compressed_data: &[u32],
          uncompressed_values: &mut [u32]) -> size_t {
        unsafe {
            return decode_s4_bp128_dm_native(
                        compressed_data.as_ptr(),
                        compressed_data.len() as size_t,
                        uncompressed_values.as_mut_ptr(),
                        uncompressed_values.len() as size_t);
        }
    }
}

//-------------------------
// s4-bp128-dm


pub struct S4BP128Encoder {
    input_buffer: Vec<u32>,
    output_buffer: Vec<u32>,
}

impl S4BP128Encoder {

    pub fn new() -> S4BP128Encoder {
        S4BP128Encoder {
            input_buffer: Vec::new(),
            output_buffer: Vec::new(),
        }
    }

    pub fn encode_sorted(&mut self, input: &[u32]) -> &[u32] {
        self.input_buffer.clear();
        let input_len = input.len();
        if input_len + 10000 >= self.input_buffer.len() {
            let target_length = input_len + 1024;
            self.input_buffer.resize(target_length, 0);
            self.output_buffer.resize(target_length, 0);
        }
        // TODO use clone_from when available
        unsafe {
            ptr::copy_nonoverlapping(input.as_ptr(), self.input_buffer.as_mut_ptr(), input_len);
            let written_size = encode_s4_bp128_dm_native(
                self.input_buffer.as_mut_ptr(),
                input_len as size_t,
                self.output_buffer.as_mut_ptr(),
                self.output_buffer.len() as size_t,
            );
            return &self.output_buffer[0..written_size];
        }
    }
}



pub struct S4BP128Decoder;

impl S4BP128Decoder {

    pub fn new() -> S4BP128Decoder {
        S4BP128Decoder
    }

    pub fn decode_sorted(&self,
                  compressed_data: &[u32],
                  uncompressed_values: &mut [u32]) -> size_t {
        unsafe {
            return decode_s4_bp128_dm_native(
                        compressed_data.as_ptr(),
                        compressed_data.len() as size_t,
                        uncompressed_values.as_mut_ptr(),
                        uncompressed_values.len() as size_t);
        }
    }

    // pub fn decode_unsorted(&self,
    //               compressed_data: &[u32],
    //               uncompressed_values: &mut [u32]) -> size_t {
    //     unsafe {
    //         return decode_unsorted_native(
    //                     compressed_data.as_ptr(),
    //                     compressed_data.len() as size_t,
    //                     uncompressed_values.as_mut_ptr(),
    //                     uncompressed_values.len() as size_t);
    //     }
    // }
}




//
// pub struct Intersector {
//     output_buffer: Vec<u32>,
// }
//
// impl Intersector {
//     fn new() -> Intersector {
//         Intersector::with_capacity(1_000_000)
//     }
//     fn with_capacity(capacity: usize) -> Intersector {
//         Intersector {
//             output_buffer: iter::repeat(0u32).take(capacity).collect()
//         }
//     }
//     fn intersection(&mut self, left: &[u32], right: &[u32]) -> &[u32] {
//         let max_intersection_length = min(left.len(), right.len());
//         if self.output_buffer.len() < max_intersection_length {
//             self.output_buffer.resize(max_intersection_length, 0);
//         }
//         unsafe {
//             let intersection_len = intersection_native(
//                 left.as_ptr(), left.len() as size_t,
//                 right.as_ptr(), right.len() as size_t,
//                 self.output_buffer.as_mut_ptr());
//             return &self.output_buffer[0..intersection_len];
//         }
//     }
// }


#[cfg(test)]
mod tests {

    use super::*;
    use test::Bencher;
    use std::iter;
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

    fn generate_array(n: usize, ratio: f32) -> Vec<u32> {
        generate_array_with_seed(n, ratio, 4)
    }

    #[test]
    fn test_encode_big() {
        let mut encoder = S4BP128Encoder::new();
        let num_ints = 10000 as usize;
        let expected_length = 1274;
        let input: Vec<u32> = (0..num_ints as u32)
            .map(|i| i * 7 / 2)
            .into_iter().collect();
        let encoded_data = encoder.encode_sorted(&input);
        assert_eq!(encoded_data.len(), expected_length);
        let decoder = S4BP128Decoder::new();
        let mut decoded_data: Vec<u32> = (0..num_ints as u32).collect();
        assert_eq!(num_ints, decoder.decode_sorted(&encoded_data[..], &mut decoded_data));
        assert_eq!(decoded_data, input);
    }

    #[test]
    fn test_encode_block() {
        let mut encoder = Block128Encoder::new();
        let expected_length = 21;
        let input: Vec<u32> = (0u32..128u32)
            .map(|i| i * 7 / 2)
            .into_iter()
            .collect();
        let encoded_data = encoder.encode_sorted(&input);
        assert_eq!(encoded_data.len(), expected_length);
        let decoder = Block128Decoder::new();
        let mut decoded_data: Vec<u32> = iter::repeat(0u32).take(128).collect();
        assert_eq!(128, decoder.decode_sorted(&encoded_data[..], &mut decoded_data));
        assert_eq!(decoded_data, input);
    }



    #[test]
    fn test_encode_vint() {
        {
            let mut encoder = VIntEncoder::new();
            let expected_length = 31;
            let input: Vec<u32> = (0u32..123u32)
                .map(|i| i * 7 / 2)
                .into_iter()
                .collect();
            let encoded_data = encoder.encode_sorted(&input);
            assert_eq!(encoded_data.len(), expected_length);
            let decoder = VIntDecoder::new();
            let mut decoded_data: Vec<u32> = iter::repeat(0u32).take(128).collect();
            assert_eq!(123, decoder.decode_sorted(&encoded_data[..], &mut decoded_data));
            assert_eq!(&decoded_data[0..123], &input[..]);
        }
        {
            let mut encoder = VIntEncoder::new();
            let input = vec!(3, 17u32, 187);
            let encoded_data = encoder.encode_sorted(&input);
            assert_eq!(encoded_data.len(), 1);
            assert_eq!(encoded_data[0], 2167049859u32);
        }
    }

    // #[test]
    // fn test_encode_unsorted() {
    //     let mut encoder = Encoder::new();
    //     let num_ints = 10_000 as usize;
    //     let expected_length = 4361;
    //     let input: Vec<u32> = (0..num_ints as u32)
    //         .map(|i| i * 213_127 % 501)
    //         .into_iter().collect();
    //     assert_eq!(input.len(), 10_000);
    //     let encoded_data = encoder.encode_unsorted(&input);
    //     assert_eq!(encoded_data.len(), expected_length);
    //     let decoder = Decoder::new();
    //     let mut decoded_data: Vec<u32> = (0..num_ints as u32).collect();
    //     assert_eq!(num_ints, decoder.decode_unsorted(&encoded_data[..], &mut decoded_data));
    //     assert_eq!(decoded_data, input);
    // }
    //
    // #[test]
    // fn test_simd_intersection() {
    //     let mut intersector = Intersector::new();
    //     let arr1 = generate_array_with_seed(1_000_000, 0.1, 2);
    //     let arr2 = generate_array_with_seed(5_000_000, 0.5, 3);
    //     let intersection = intersector.intersection(&arr1[..], &arr2[..])   ;
    //     assert_eq!(intersection.len(), 500_233);
    // }

    #[bench]
    fn bench_decode(b: &mut Bencher) {
        const TEST_SIZE: usize = 1_000_000;
        let arr = generate_array(TEST_SIZE, 0.1);
        let mut encoder = S4BP128Encoder::new();
        let encoded = encoder.encode_sorted(&arr);
        let mut uncompressed: Vec<u32> = (0..TEST_SIZE as u32).collect();
        let decoder = S4BP128Decoder;
        b.iter(|| {
            decoder.decode_sorted(&encoded, &mut uncompressed);
        });
    }


    // #[bench]
    // fn bench_simd_intersection(b: &mut Bencher) {
    //     let mut intersector = Intersector::new();
    //     let arr1 = generate_array_with_seed(1_000_000, 0.1, 2);
    //     let arr2 = generate_array_with_seed(5_000_000, 0.5, 3);
    //     b.iter(|| {
    //         intersector.intersection(&arr1[..], &arr2[..]).len()
    //     });
    // }
}
