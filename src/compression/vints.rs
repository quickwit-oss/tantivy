use libc::size_t;
use std::ptr;
use std::iter;

extern {
    fn encode_sorted_vint_native(data: *mut u32, num_els: size_t, output: *mut u32, output_capacity: size_t) -> size_t;
    fn decode_sorted_vint_native(compressed_data: *const u32, compressed_size: size_t, uncompressed: *mut u32, output_capacity: size_t) -> size_t;
}

pub struct VIntsEncoder {
    input_buffer: Vec<u32>,
    output_buffer: Vec<u32>,
}

impl VIntsEncoder {

    pub fn new() -> VIntsEncoder {
        VIntsEncoder {
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
                  compressed_data: &[u32]) -> &[u32] {
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
            let expected_length = 31;
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
            let input = vec!(3, 17u32, 187);
            let encoded_data = encoder.encode_sorted(&input);
            assert_eq!(encoded_data.len(), 1);
            assert_eq!(encoded_data[0], 2167049859u32);
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
