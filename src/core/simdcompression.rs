
use libc::size_t;
use std::ptr;

// #[link(name = "simdcompression", kind = "static")]
extern {
    fn encode_native(data: *mut u32, num_els: size_t, output: *mut u32) -> size_t;
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
        if input_len >= self.input_buffer.len() {
            self.input_buffer = (0..input_len as u32).collect();
            self.output_buffer = (0..input_len as u32 + 1000).collect();
            // TODO use resize when available
        }
        // TODO use clone_from when available
        unsafe {
            ptr::copy_nonoverlapping(input.as_ptr(), self.input_buffer.as_mut_ptr(), input_len);
            let written_size = encode_native(
                self.input_buffer.as_mut_ptr(),
                input_len as size_t,
                self.output_buffer.as_mut_ptr()
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
    let input: Vec<u32> = (0..10000).into_iter().collect();
    let data = encoder.encode(&input);
    assert_eq!(data.len(), 962);
    let decoder = Decoder::new();
    let mut data_output: Vec<u32> = (0..10000).collect();
    assert_eq!(10000, decoder.decode(&data[0..962], &mut data_output));
    for i in 0..10000 {
        assert_eq!(data_output[i], input[i])    ;
    }
}
