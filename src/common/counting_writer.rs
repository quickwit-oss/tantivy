use std::io;
use std::io::Write;

pub struct CountingWriter<W> {
    underlying: W,
    written_bytes: u64,
}

impl<W: Write> CountingWriter<W> {
    pub fn wrap(underlying: W) -> CountingWriter<W> {
        CountingWriter {
            underlying,
            written_bytes: 0,
        }
    }

    pub fn written_bytes(&self) -> u64 {
        self.written_bytes
    }

    pub fn finish(mut self) -> io::Result<(W, u64)> {
        self.flush()?;
        Ok((self.underlying, self.written_bytes))
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written_size = self.underlying.write(buf)?;
        self.written_bytes += written_size as u64;
        Ok(written_size)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.underlying.write_all(buf)?;
        self.written_bytes += buf.len() as u64;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.underlying.flush()
    }
}

#[cfg(test)]
mod test {

    use super::CountingWriter;
    use std::io::Write;

    #[test]
    fn test_counting_writer() {
        let buffer: Vec<u8> = vec![];
        let mut counting_writer = CountingWriter::wrap(buffer);
        let bytes = (0u8..10u8).collect::<Vec<u8>>();
        counting_writer.write_all(&bytes).unwrap();
        let (w, len): (Vec<u8>, u64) = counting_writer.finish().unwrap();
        assert_eq!(len, 10u64);
        assert_eq!(w.len(), 10);
    }
}
