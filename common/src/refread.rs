use std::cell::RefCell;

use ownedbytes::OwnedBytes;

/// A struct that wraps an `OwnedBytes` and provides a read interface that can be used to read data
/// from it while borrowing and will not invalidate references
///
/// Useful for zerocopy deserialization since references remain valid after the cursor has advanced
/// past the current position.
///
/// NOT thread safe as is uses a `RefCell` internally
#[derive(Debug, Clone)]
pub struct RefReader {
    data: OwnedBytes,
    pos: RefCell<usize>,
}

impl RefReader {
    pub fn new(data: OwnedBytes) -> RefReader {
        RefReader {
            data,
            pos: RefCell::new(0),
        }
    }

    pub fn pos(&self) -> usize {
        *self.pos.borrow()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn remaining(&self) -> usize {
        self.len() - self.pos()
    }

    /// Read exactly `n` bytes from the reader and return them as a slice.
    /// If the end of the data is reached before `n` bytes are read, an `UnexpectedEof` error is
    /// returned.
    ///
    /// Advances the internal position by the number of bytes read.
    pub fn read_n(&self, n: usize) -> std::io::Result<&[u8]> {
        let mut pos = self.pos.borrow_mut();
        let remaining_data_len = self.data.len() - *pos;

        if remaining_data_len < n {
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "not enough data to read",
            ))
        } else {
            let data = &self.data[*pos..*pos + n];
            *pos += n;
            Ok(data)
        }
    }

    /// Read a single byte from the reader.
    ///
    /// Advances the internal position by one byte.
    pub fn next_byte(&self) -> Option<u8> {
        let mut pos = self.pos.borrow_mut();

        if *pos < self.data.len() {
            let byte = self.data[*pos];
            *pos += 1;
            Some(byte)
        } else {
            None
        }
    }

    /// Advances the internal position by `n` bytes.
    /// If `n` is greater than the remaining data length, then it will return an error.
    pub fn advance(&self, n: usize) -> std::io::Result<()> {
        let mut pos = self.pos.borrow_mut();
        let remaining_data_len = self.data.len() - *pos;

        if remaining_data_len < n {
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "not enough data to advance",
            ))
        } else {
            *pos += n;
            Ok(())
        }
    }

    /// Resets the interal position of the reader to the beginning.
    pub fn reset(&self) {
        *self.pos.borrow_mut() = 0;
    }
}

impl std::io::Read for RefReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut pos = self.pos.borrow_mut();
        let remaining_data_len = self.data.len() - *pos;
        let buf_len = buf.len();

        if remaining_data_len >= buf_len {
            buf.copy_from_slice(&self.data[*pos..*pos + buf_len]);
            *pos += buf_len;
            Ok(buf_len)
        } else {
            buf[..remaining_data_len].copy_from_slice(&self.data[*pos..]);
            *pos += remaining_data_len;
            Ok(remaining_data_len)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use ownedbytes::OwnedBytes;

    use super::RefReader;

    fn get_reader() -> RefReader {
        RefReader::new(OwnedBytes::new(b"Hello, world!".to_vec()))
    }

    #[test]
    fn test_read_n() {
        let reader = get_reader();

        let hello = reader.read_n(5).unwrap();
        assert_eq!(hello, b"Hello");
        assert_eq!(reader.pos(), 5);
        assert_eq!(reader.remaining(), 8);

        let comma_space = reader.read_n(2).unwrap();
        assert_eq!(comma_space, b", ");
        assert_eq!(reader.pos(), 7);
        assert_eq!(reader.remaining(), 6);

        let world = reader.read_n(6).unwrap();
        assert_eq!(world, b"world!");
        assert_eq!(reader.pos(), 13);
        assert_eq!(reader.remaining(), 0);

        assert!(reader.read_n(1).is_err());
    }

    #[test]
    fn test_next_byte() {
        let reader = get_reader();

        assert_eq!(reader.next_byte().unwrap(), b'H');
        assert_eq!(reader.next_byte().unwrap(), b'e');
        assert_eq!(reader.next_byte().unwrap(), b'l');
        assert_eq!(reader.next_byte().unwrap(), b'l');
        assert_eq!(reader.next_byte().unwrap(), b'o');
        assert_eq!(reader.next_byte().unwrap(), b',');
        assert_eq!(reader.next_byte().unwrap(), b' ');
        assert_eq!(reader.next_byte().unwrap(), b'w');
        assert_eq!(reader.next_byte().unwrap(), b'o');
        assert_eq!(reader.next_byte().unwrap(), b'r');
        assert_eq!(reader.next_byte().unwrap(), b'l');
        assert_eq!(reader.next_byte().unwrap(), b'd');
        assert_eq!(reader.next_byte().unwrap(), b'!');

        assert!(reader.next_byte().is_none());
    }

    #[test]
    fn test_advance() {
        let reader = get_reader();

        reader.advance(7).unwrap();
        assert_eq!(reader.read_n(6).unwrap(), b"world!");
        assert!(reader.advance(1).is_err());
    }

    #[test]
    fn test_reset() {
        let reader = get_reader();

        assert_eq!(reader.read_n(5).unwrap(), b"Hello");
        assert_eq!(reader.pos(), 5);
        assert_eq!(reader.remaining(), 8);
        reader.reset();
        assert_eq!(reader.read_n(5).unwrap(), b"Hello");
        assert_eq!(reader.pos(), 5);
        assert_eq!(reader.remaining(), 8);
    }

    #[test]
    fn test_read_trait_exact() {
        let mut reader = get_reader();

        let mut buffer = [0; 5];
        reader.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, b"Hello");
        assert_eq!(reader.pos(), 5);
        assert_eq!(reader.remaining(), 8);

        let mut buffer = [0; 2];
        reader.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, b", ");
        assert_eq!(reader.pos(), 7);
        assert_eq!(reader.remaining(), 6);

        let mut buffer = [0; 6];
        reader.read_exact(&mut buffer).unwrap();
        assert_eq!(&buffer, b"world!");
        assert_eq!(reader.pos(), 13);
        assert_eq!(reader.remaining(), 0);

        assert!(reader.read_exact(&mut buffer).is_err());
    }

    #[test]
    fn test_read_trait() {
        let mut reader = get_reader();

        let mut small_buffer = [0; 5];
        let bytes_read = reader.read(&mut small_buffer).unwrap();
        assert_eq!(bytes_read, 5);
        assert_eq!(&small_buffer, b"Hello");
        assert_eq!(reader.pos(), 5);
        assert_eq!(reader.remaining(), 8);

        reader.reset();
        let mut big_buffer = [0; 20];
        let bytes_read = reader.read(&mut big_buffer).unwrap();
        assert_eq!(bytes_read, 13);
        assert_eq!(&big_buffer[..13], b"Hello, world!");
        assert_eq!(reader.pos(), 13);
        assert_eq!(reader.remaining(), 0);
    }
}
