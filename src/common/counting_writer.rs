use crate::directory::AntiCallToken;
use crate::directory::TerminatingWrite;
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

    /// Returns the underlying write object.
    /// Note that this method does not trigger any flushing.
    pub fn finish(self) -> W {
        self.underlying
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

impl<W: TerminatingWrite> TerminatingWrite for CountingWriter<W> {
    fn terminate_ref(&mut self, token: AntiCallToken) -> io::Result<()> {
        self.underlying.terminate_ref(token)
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
        let len = counting_writer.written_bytes();
        let buffer_restituted: Vec<u8> = counting_writer.finish();
        assert_eq!(len, 10u64);
        assert_eq!(buffer_restituted.len(), 10);
    }
}
