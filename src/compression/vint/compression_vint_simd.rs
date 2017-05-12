
mod streamvbyte {

    use libc::size_t;

    extern {
        pub fn streamvbyte_delta_encode(
            data: *const u32,
            num_els: u32,
            output: *mut u8,
            offset: u32) -> size_t;

        pub fn streamvbyte_delta_decode(
            compressed_data: *const u8,
            output: *mut u32,
            num_els: u32,
            offset: u32) -> size_t;
            
        pub fn streamvbyte_encode(
            data: *const u32,
            num_els: u32,
            output: *mut u8) -> size_t;
        
        pub fn streamvbyte_decode(
            compressed_data: *const u8,
            output: *mut u32,
            num_els: usize) -> size_t;
    }
}


#[inline(always)]
pub fn compress_sorted<'a>(input: &[u32], output: &'a mut [u8], offset: u32) -> &'a [u8] {
    let compress_length = unsafe {
        streamvbyte::streamvbyte_delta_encode(
            input.as_ptr(),
            input.len() as u32,
            output.as_mut_ptr(),
            offset)
    };
    &output[..compress_length]
}

#[inline(always)]
pub fn compress_unsorted<'a>(input: &[u32], output: &'a mut [u8]) -> &'a [u8] {
    let compress_length = unsafe {
        streamvbyte::streamvbyte_encode(
            input.as_ptr(),
            input.len() as u32,
            output.as_mut_ptr())
    }; 
    &output[..compress_length]
}

#[inline(always)]
pub fn uncompress_sorted<'a>(
        compressed_data: &'a [u8],
        output: &mut [u32],
        offset: u32) -> &'a [u8] {
    let consumed_bytes = unsafe {
        streamvbyte::streamvbyte_delta_decode(
            compressed_data.as_ptr(),
            output.as_mut_ptr(),
            output.len() as u32,
            offset)
    };
    &compressed_data[consumed_bytes..]
}

#[inline(always)]
pub fn uncompress_unsorted<'a>(
    compressed_data: &'a [u8],
    output: &mut [u32]) -> &'a [u8] {
    let consumed_bytes = unsafe {
        streamvbyte::streamvbyte_decode(
            compressed_data.as_ptr(),
            output.as_mut_ptr(),
            output.len())
    };
    &compressed_data[consumed_bytes..]
}

