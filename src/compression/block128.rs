
use libc::size_t;
use std::ptr;

extern {
    fn encode_sorted_block128_native(data: *mut u32, output: *mut u8, output_capacity: size_t) -> size_t;
    fn decode_sorted_block128_native(compressed_data: *const u8, compressed_size: size_t, uncompressed: *mut u32) -> usize;

    fn encode_block128_native(data: *mut u32, output: *mut u8, output_capacity: size_t) -> size_t;
    fn decode_block128_native(compressed_data: *const u8, compressed_size: size_t, uncompressed: *mut u32) -> usize;

    fn encode_sorted_vint_native(data: *mut u32, num_els: size_t, output: *mut u8, output_capacity: size_t) -> size_t;
    fn decode_sorted_vint_native(compressed_data: *const u8, compressed_size: size_t, uncompressed: *mut u32, output_capacity: size_t) -> size_t;

}

//-------------------------
// Block128

pub struct Block128Encoder {
    input_buffer: [u32; 128],
    output_buffer: [u8; 256 * 4],
}

impl Block128Encoder {

    pub fn new() -> Block128Encoder {
        Block128Encoder {
            input_buffer: [0u32; 128],
            output_buffer: [0u8; 256 * 4],
        }
    }

    pub fn encode(&mut self, input: &[u32]) -> &[u8] {
        assert_eq!(input.len(), 128);
        // TODO use clone_from when available
        let written_size: usize;
        unsafe {
            ptr::copy_nonoverlapping(input.as_ptr(), self.input_buffer.as_mut_ptr(), 128);
            written_size = encode_block128_native(
                self.input_buffer.as_mut_ptr(),
                self.output_buffer.as_mut_ptr(),
                256 * 4,
            );
        }
        return &self.output_buffer[0..written_size];
    }

    pub fn encode_sorted(&mut self, input: &[u32]) -> &[u8] {
        assert_eq!(input.len(), 128);
        // TODO use clone_from when available
        let written_size: usize;
        unsafe {
            ptr::copy_nonoverlapping(input.as_ptr(), self.input_buffer.as_mut_ptr(), 128);
            written_size = encode_sorted_block128_native(
                self.input_buffer.as_mut_ptr(),
                self.output_buffer.as_mut_ptr(),
                256 * 4,
            );
        }
        return &self.output_buffer[0..written_size];
    }
}

pub struct Block128Decoder {
    output: [u32; 128],
}

impl Block128Decoder {

    pub fn new() -> Block128Decoder {
        Block128Decoder {
            output: [0u32; 128]
        }
    }
    
    pub fn decode<'a, 'b>(
          &'b mut self,
          compressed_data: &'a [u8]) -> &'a[u8] {
        unsafe {
            let consumed_num_bytes: usize = decode_block128_native(
                        compressed_data.as_ptr(),
                        compressed_data.len() as size_t,
                        self.output.as_mut_ptr());
            &compressed_data[consumed_num_bytes..]
        }
    }

    pub fn decode_sorted<'a, 'b>(
          &'b mut self,
          compressed_data: &'a [u8]) -> &'a [u8] {
        unsafe {
            let consumed_num_bytes: usize = decode_sorted_block128_native(
                        compressed_data.as_ptr(),
                        compressed_data.len() as size_t,
                        self.output.as_mut_ptr());
            &compressed_data[consumed_num_bytes..]
        }
    }
    
    pub fn decode_sorted_remaining(&mut self,
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
    
    pub fn output(&self,) -> &[u32; 128] {
        &self.output
    }
}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_encode_sorted_block() {
        for num_extra_values in [0, 2, 11].into_iter() {
            let mut encoder = Block128Encoder::new();
            let mut input = [0u32; 128];
            for i in 0u32..128u32 {
                input[i as usize] = i * 7 / 2;
            }
            let mut encoded_vec: Vec<u8> = encoder.encode_sorted(&input).to_vec();
            assert_eq!(encoded_vec.len(), 84);
            for i in 0u8..*num_extra_values as u8 {
                encoded_vec.push(i);
            }
            let mut decoder = Block128Decoder::new();
            let remaining_input = decoder.decode_sorted(&encoded_vec[..]);
            let uncompressed_values = decoder.output();
            assert_eq!(remaining_input.len(), *num_extra_values);
            for i in 0..128 {
                assert_eq!(uncompressed_values[i], input[i]);
            }
            for i in 0..*num_extra_values {
                assert_eq!(remaining_input[i], i as u8);
            }
        }
    }

    #[test]
    fn test_encode_block() {
        for num_extra_values in [0, 2, 11].into_iter() {
            let mut encoder = Block128Encoder::new();
            let mut input = [0u32; 128];
            for i in 0u32..128u32 {
                input[i as usize] = i * 7 % 31;
            }
            let mut encoded_vec: Vec<u8> = encoder.encode(&input).to_vec();
            assert_eq!(encoded_vec.len(), 100);
            for i in 0u8..*num_extra_values as u8 {
                encoded_vec.push(i);
            }
            let mut decoder = Block128Decoder::new();
            let remaining_input: &[u8] = decoder.decode(&encoded_vec[..]);
            let uncompressed_values = decoder.output();
            assert_eq!(remaining_input.len(), *num_extra_values);
            for i in 0..128 {
                assert_eq!(uncompressed_values[i], input[i]);
            }
            for i in 0..*num_extra_values {
                assert_eq!(remaining_input[i], i as u8);
            }
        }
    }


}
