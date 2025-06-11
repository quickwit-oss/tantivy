use std::cell::RefCell;

/// A struct that implements a very similar api to std::io::Read, but with an internally managed
/// cursor and designed to run on data that is already present in memory.
///
/// Useful for zerocopy deserialization since references remain valid after the cursor has advanced
/// past the current position.
///
/// NOT thread safe as is uses a `RefCell` internally
pub struct RefReader<'a> {
    data: &'a [u8],
    pos: RefCell<usize>,
}

impl RefReader<'_> {
    pub fn new(data: &[u8]) -> RefReader {
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
}

impl<'a> RefReader<'a> {
    /// Read data from the reader into the provided buffer.
    /// Returns the number of bytes read.
    ///
    /// Advances the internal position by the number of bytes read.
    pub fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
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

    /// Read all remaining data from the reader into the provided buffer.
    /// Returns the number of bytes read.
    ///
    /// Advances the internal position to the end of the data.
    pub fn read_to_end(&self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        let mut pos = self.pos.borrow_mut();
        let remaining_data_len = self.data.len() - *pos;
        buf.extend_from_slice(&self.data[*pos..]);
        *pos += remaining_data_len;
        Ok(remaining_data_len)
    }

    /// Read exactly the specified number of bytes from the reader into the provided buffer.
    /// If the end of the data is reached before the buffer is filled, an `UnexpectedEof` error is
    /// returned.
    ///
    /// Advances the internal position by the number of bytes read.
    pub fn read_exact(&self, buf: &mut [u8]) -> std::io::Result<()> {
        let mut pos = self.pos.borrow_mut();
        let remaining_data_len = self.data.len() - *pos;
        let buf_len = buf.len();

        if remaining_data_len < buf_len {
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ))
        } else {
            buf.copy_from_slice(&self.data[*pos..*pos + buf_len]);
            *pos += buf_len;
            Ok(())
        }
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
            let slice = &self.data[*pos..*pos + n];
            *pos += n;
            Ok(slice)
        }
    }

    /// Get a reference to the remaining slice of data.
    ///
    /// Does not advance the cursor
    pub fn remaining_slice(&'a self) -> &'a [u8] {
        let pos = self.pos.borrow();
        &self.data[*pos..]
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
