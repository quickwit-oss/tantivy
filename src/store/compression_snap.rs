extern crate snap;

use std::io::{self, Read, Write};

pub fn compress(uncompressed: &[u8], compressed: &mut Vec<u8>) -> io::Result<()> {
    compressed.clear();
    let mut encoder = snap::Writer::new(compressed);
    encoder.write_all(&uncompressed)?;
    encoder.flush()?;
    Ok(())
}

pub fn decompress(compressed: &[u8], decompressed: &mut Vec<u8>) -> io::Result<()> {
    decompressed.clear();
    snap::Reader::new(compressed)
        .read_to_end(decompressed)?;
    Ok(())
}
