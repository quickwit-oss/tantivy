use crate::FastFieldDataAccess;
use crate::FastFieldSerializerEstimate;
use crate::FastFieldStats;
use common::BinarySerializable;
use std::io::{self, Write};
use tantivy_bitpacker::compute_num_bits;
use tantivy_bitpacker::BitPacker;

pub struct BitpackedFastFieldSerializer<'a, W: 'a + Write> {
    bit_packer: BitPacker,
    write: &'a mut W,
    min_value: u64,
    num_bits: u8,
}

impl<'a, W: Write> BitpackedFastFieldSerializer<'a, W> {
    /// Creates a new fast field serializer.
    ///
    /// The serializer in fact encode the values by bitpacking
    /// `(val - min_value)`.
    ///
    /// It requires a `min_value` and a `max_value` to compute
    /// compute the minimum number of bits required to encode
    /// values.
    pub fn open(
        write: &'a mut W,
        min_value: u64,
        max_value: u64,
    ) -> io::Result<BitpackedFastFieldSerializer<'a, W>> {
        assert!(min_value <= max_value);
        min_value.serialize(write)?;
        let amplitude = max_value - min_value;
        amplitude.serialize(write)?;
        let num_bits = compute_num_bits(amplitude);
        let bit_packer = BitPacker::new();
        Ok(BitpackedFastFieldSerializer {
            bit_packer,
            write,
            min_value,
            num_bits,
        })
    }
    /// Creates a new fast field serializer.
    ///
    /// The serializer in fact encode the values by bitpacking
    /// `(val - min_value)`.
    ///
    /// It requires a `min_value` and a `max_value` to compute
    /// compute the minimum number of bits required to encode
    /// values.
    pub fn create(
        write: &'a mut W,
        _fastfield_accessor: &impl FastFieldDataAccess,
        stats: FastFieldStats,
        data_iter: impl Iterator<Item = u64>,
    ) -> io::Result<()> {
        let mut serializer = Self::open(write, stats.min_value, stats.max_value)?;

        for val in data_iter {
            serializer.add_val(val)?;
        }
        serializer.close_field()?;

        Ok(())
    }
    /// Pushes a new value to the currently open u64 fast field.
    pub fn add_val(&mut self, val: u64) -> io::Result<()> {
        let val_to_write: u64 = val - self.min_value;
        self.bit_packer
            .write(val_to_write, self.num_bits, &mut self.write)?;
        Ok(())
    }
    pub fn close_field(mut self) -> io::Result<()> {
        self.bit_packer.close(&mut self.write)
    }
}

impl<'a, W: 'a + Write> FastFieldSerializerEstimate for BitpackedFastFieldSerializer<'a, W> {
    fn estimate(
        _fastfield_accessor: &impl FastFieldDataAccess,
        stats: FastFieldStats,
    ) -> (f32, &'static str) {
        let amplitude = stats.max_value - stats.min_value;
        let num_bits = compute_num_bits(amplitude);
        let num_bits_uncompressed = 64;
        let ratio = num_bits as f32 / num_bits_uncompressed as f32;
        let name = Self::codec_id().0;
        (ratio, name)
    }
    fn codec_id() -> (&'static str, u8) {
        ("Bitpacked", 1)
    }
}
