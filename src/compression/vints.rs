use libc::size_t;
use std::ptr;
use std::iter;

extern {
    fn encode_sorted_vint_native(data: *mut u32, num_els: size_t, output: *mut u32, output_capacity: size_t) -> size_t;
    fn decode_sorted_vint_native(compressed_data: *const u32, compressed_size: size_t, uncompressed: *mut u32, output_capacity: size_t) -> size_t;
}

pub struct SortedVIntsEncoder {
    input_buffer: Vec<u32>,
    output_buffer: Vec<u32>,
}

impl SortedVIntsEncoder {

    pub fn new() -> SortedVIntsEncoder {
        SortedVIntsEncoder {
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



pub struct SortedVIntsDecoder;

impl SortedVIntsDecoder {

    pub fn new() -> SortedVIntsDecoder {
        SortedVIntsDecoder
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


#[cfg(test)]
mod tests {

    use std::iter;
    use super::*;

    #[test]
    fn test_encode_vint() {
        {
            let mut encoder = SortedVIntsEncoder::new();
            let expected_length = 31;
            let input: Vec<u32> = (0u32..123u32)
                .map(|i| i * 7 / 2)
                .into_iter()
                .collect();
            let encoded_data = encoder.encode_sorted(&input);
            assert_eq!(encoded_data.len(), expected_length);
            let decoder = SortedVIntsDecoder::new();
            let mut decoded_data: Vec<u32> = iter::repeat(0u32).take(128).collect();
            assert_eq!(123, decoder.decode_sorted(&encoded_data[..], &mut decoded_data));
            assert_eq!(&decoded_data[0..123], &input[..]);
        }
        {
            let mut encoder = SortedVIntsEncoder::new();
            let input = vec!(3, 17u32, 187);
            let encoded_data = encoder.encode_sorted(&input);
            assert_eq!(encoded_data.len(), 1);
            assert_eq!(encoded_data[0], 2167049859u32);
        }
    }

}
