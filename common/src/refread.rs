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
