use libc::size_t;
use std::ptr;
use std::iter;

extern {
    fn encode_sorted_block128_native(data: *mut u32, output: *mut u32, output_capacity: size_t) -> size_t;
    fn decode_sorted_block128_native(compressed_data: *const u32, compressed_size: size_t, uncompressed: *mut u32, output_capacity: size_t) -> size_t;
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
            written_size = encode_sorted_block128_native(
                self.input_buffer.as_mut_ptr(),
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
            return decode_sorted_block128_native(
                        compressed_data.as_ptr(),
                        compressed_data.len() as size_t,
                        uncompressed_values.as_mut_ptr(),
                        uncompressed_values.len() as size_t);
        }
    }
}


#[cfg(test)]
mod tests {

    use super::*;
    use std::iter;

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
}
