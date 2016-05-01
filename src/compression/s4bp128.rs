
use libc::size_t;
use std::ptr;



extern {
    // complete s4-bp128-dm
    fn encode_s4_bp128_dm_native(data: *mut u32, num_els: size_t, output: *mut u32, output_capacity: size_t) -> size_t;
    fn decode_s4_bp128_dm_native(compressed_data: *const u32, compressed_size: size_t, uncompressed: *mut u32, output_capacity: size_t) -> size_t;
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



#[cfg(test)]
mod tests {

    use super::*;
    use test::Bencher;
    use compression::tests::generate_array;

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
}
