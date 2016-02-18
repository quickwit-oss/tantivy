
use libc::size_t;
use std::ptr;
#[link(name = "simdcompression", kind = "static")]
extern {
    fn encode_native(data: *mut u32, num_els: size_t, output: *mut u32) -> size_t;
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
        unsafe {
            self.input_buffer.clear();
            let input_len = input.len();
            if input_len > self.input_buffer.len() {
                // let delta_size = self.input_buffer.len() - input_len;
                self.input_buffer = (0..input_len as u32 + 10 ).collect();
                self.output_buffer = (0..input_len as u32 + 10).collect();
                // TODO use resize when available
            }
            ptr::copy_nonoverlapping(input.as_ptr(), self.input_buffer.as_mut_ptr(), input_len);
            // TODO use clone_from when available
            let written_size = encode_native(
                self.input_buffer.as_mut_ptr(),
                input_len as size_t,
                self.output_buffer.as_mut_ptr()
            );
            return &self.output_buffer[0..written_size];
        }
    }
}
