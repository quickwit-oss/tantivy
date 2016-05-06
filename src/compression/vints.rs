use libc::size_t;
use std::ptr;
use std::iter;

extern {
    fn encode_sorted_vint_native(data: *mut u32, num_els: size_t, output: *mut u8, output_capacity: size_t) -> size_t;
    fn decode_sorted_vint_native(compressed_data: *const u8, compressed_size: size_t, uncompressed: *mut u32, output_capacity: size_t) -> size_t;
}

pub struct VIntsEncoder {
    input_buffer: Vec<u32>,
    output_buffer: Vec<u8>,
}

impl VIntsEncoder {

    pub fn new() -> VIntsEncoder {
        VIntsEncoder {
            input_buffer: Vec::with_capacity(128),
            output_buffer: iter::repeat(0u8).take(256 * 4).collect(),
        }
    }

    pub fn encode_sorted(&mut self, input: &[u32]) -> &[u8] {
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
                256 * 4,
            );
        }
        return &self.output_buffer[0..written_size];
    }
}



pub struct VIntsDecoder {
    output: [u32; 128],
}

impl VIntsDecoder {

    pub fn new() -> VIntsDecoder {
        VIntsDecoder {
            output: [0u32; 128]
        }
    }

    pub fn decode_sorted(&mut self,
                  compressed_data: &[u8]) -> &[u32] {
        unsafe {
            let num_uncompressed = decode_sorted_vint_native(
                compressed_data.as_ptr(),
                compressed_data.len() as size_t,
                self.output.as_mut_ptr(),
                128);
            &self.output[..num_uncompressed]
        }
    }
}


#[cfg(test)]
mod tests {
    
    use super::*;

    #[test]
    fn test_encode_vint() {
        {
            let mut encoder = VIntsEncoder::new();
            let expected_length = 124;
            let input: Vec<u32> = (0u32..123u32)
                .map(|i| i * 7 / 2)
                .into_iter()
                .collect();
            let encoded_data = encoder.encode_sorted(&input);
            assert_eq!(encoded_data.len(), expected_length);
            let mut decoder = VIntsDecoder::new();
            let decoded_data = decoder.decode_sorted(&encoded_data[..]);
            assert_eq!(123, decoded_data.len());
            assert_eq!(&decoded_data[0..123], &input[..]);
        }
        {
            let mut encoder = VIntsEncoder::new();
            let input = vec!(3u32, 17u32, 187u32);
            let encoded_data = encoder.encode_sorted(&input);
            assert_eq!(encoded_data.len(), 4);
            assert_eq!(encoded_data[0], 3u8 + 128u8);
            assert_eq!(encoded_data[1], (17u8 - 3u8) + 128u8);
            assert_eq!(encoded_data[2], (187u8 - 17u8 - 128u8));
            assert_eq!(encoded_data[3], (1u8 + 128u8));
        }
        {
            let mut encoder = VIntsEncoder::new();
            let input = vec!(0u32, 1u32, 2u32);
            let encoded_data = encoder.encode_sorted(&input);
            let mut decoder = VIntsDecoder::new();
            let decoded_data = decoder.decode_sorted(&encoded_data[..]);
            assert_eq!(3, decoded_data.len());
            assert_eq!(&decoded_data[..], &input[..]);
        }
    }

}
