use libc::size_t;

extern {
    fn intersection_native(left_data: *const u32, left_size: size_t, right_data: *const u32, right_size: size_t, output: *mut u32) -> size_t;
}

pub fn intersection(left: &[u32], right: &[u32], output: &mut [u32]) -> usize {
    unsafe {
        intersection_native(
            left.as_ptr(), left.len(),
            right.as_ptr(), right.len(),
            output.as_mut_ptr())
    }
}
