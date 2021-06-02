use common::BinarySerializable;
use std::io;
use tantivy_bitpacker::compute_num_bits;
use tantivy_bitpacker::BitUnpacker;
/// Depending on the field type, a different
/// fast field is required.
#[derive(Clone)]
pub struct BitpackedFastFieldReader<'data> {
    bytes: &'data [u8],
    bit_unpacker: BitUnpacker,
    pub min_value_u64: u64,
    pub max_value_u64: u64,
}

impl<'data> BitpackedFastFieldReader<'data> {
    /// Opens a fast field given a file.
    pub fn open_from_bytes(mut bytes: &'data [u8]) -> io::Result<Self> {
        let min_value = u64::deserialize(&mut bytes)?;
        let amplitude = u64::deserialize(&mut bytes)?;
        let max_value = min_value + amplitude;
        let num_bits = compute_num_bits(amplitude);
        let bit_unpacker = BitUnpacker::new(num_bits);
        Ok(BitpackedFastFieldReader {
            bytes,
            min_value_u64: min_value,
            max_value_u64: max_value,
            bit_unpacker,
        })
    }
    pub fn get_u64(&self, doc: u64) -> u64 {
        self.min_value_u64 + self.bit_unpacker.get(doc, &self.bytes)
    }
}
