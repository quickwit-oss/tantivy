mod bitpacker;

pub use crate::bitpacker::BitPacker;
pub use crate::bitpacker::BitUnpacker;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
