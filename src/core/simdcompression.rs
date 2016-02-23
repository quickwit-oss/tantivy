
use libc::size_t;
use std::ptr;

extern {
    fn encode_native(data: *mut u32, num_els: size_t, output: *mut u32, output_capacity: size_t) -> size_t;
    fn decode_native(compressed_data: *const u32, compressed_size: size_t, uncompressed: *mut u32, output_capacity: size_t) -> size_t;
}

pub struct Encoder {
    input_buffer: Vec<u32>,
    output_buffer: Vec<u32>,
}

impl Encoder {

    pub fn new() -> Encoder {
        Encoder {
            input_buffer: Vec::new(),
            output_buffer: Vec::new(),
        }
    }

    pub fn encode(&mut self, input: &[u32]) -> &[u32] {
        self.input_buffer.clear();
        let input_len = input.len();
        if input_len + 10000 >= self.input_buffer.len() {
            self.input_buffer = (0..(input_len as u32) + 1024).collect();
            self.output_buffer = (0..(input_len as u32) + 1024).collect();
            // TODO use resize when available
        }
        // TODO use clone_from when available
        unsafe {
            ptr::copy_nonoverlapping(input.as_ptr(), self.input_buffer.as_mut_ptr(), input_len);
            let written_size = encode_native(
                self.input_buffer.as_mut_ptr(),
                input_len as size_t,
                self.output_buffer.as_mut_ptr(),
                self.output_buffer.len() as size_t,
            );
            return &self.output_buffer[0..written_size];
        }
    }
}



pub struct Decoder;

impl Decoder {

    pub fn new() -> Decoder {
        Decoder
    }

    pub fn decode(&self,
                  compressed_data: &[u32],
                  uncompressed_values: &mut [u32]) -> size_t {
        unsafe {
            return decode_native(
                        compressed_data.as_ptr(),
                        compressed_data.len() as size_t,
                        uncompressed_values.as_mut_ptr(),
                        uncompressed_values.len() as size_t);
        }
    }
}


#[test]
fn test_encode_big() {
    let mut encoder = Encoder::new();
    let num_ints = 10000 as usize;
    let expected_length = 1274;
    let input: Vec<u32> = (0..num_ints as u32)
        .map(|i| i * 7 / 2)
        .into_iter().collect();
    let encoded_data = encoder.encode(&input);
    assert_eq!(encoded_data.len(), expected_length);
    let decoder = Decoder::new();
    let mut decoded_data: Vec<u32> = (0..num_ints as u32).collect();
    assert_eq!(num_ints, decoder.decode(&encoded_data[..], &mut decoded_data));
    assert_eq!(decoded_data, input);
}
