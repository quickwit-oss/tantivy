use crate::common::MutableEnum;
use crate::directory::{TerminatingWrite, WritePtr};
use std::io::{self, Write};

/// Represents the state of the `SpillingWriter`.
enum SpillingState {
    Buffer {
        buffer: Vec<u8>,
        capacity: usize,
        write_factory: Box<dyn FnOnce() -> io::Result<WritePtr>>,
    },
    Spilled(WritePtr),
}

impl SpillingState {
    fn new(
        limit: usize,
        write_factory: Box<dyn FnOnce() -> io::Result<WritePtr>>,
    ) -> SpillingState {
        SpillingState::Buffer {
            buffer: Vec::with_capacity(limit),
            capacity: limit,
            write_factory,
        }
    }

    // Change the state in such a way that it is ready to accept
    // `extra_capacity` bytes.
    //
    fn reserve(self, extra_capacity: usize) -> io::Result<SpillingState> {
        match self {
            SpillingState::Buffer {
                buffer,
                capacity,
                write_factory,
            } => {
                if capacity >= extra_capacity {
                    Ok(SpillingState::Buffer {
                        buffer,
                        capacity: capacity - extra_capacity,
                        write_factory,
                    })
                } else {
                    let mut wrt = write_factory()?;
                    wrt.write_all(&buffer[..])?;
                    Ok(SpillingState::Spilled(wrt))
                }
            }
            SpillingState::Spilled(wrt) => Ok(SpillingState::Spilled(wrt)),
        }
    }
}

/// The `SpillingWriter` is a writer that start by writing in a
/// buffer.
///
/// Once a memory limit is reached, the spilling writer will
/// call a given `WritePtr` factory and start spilling into it.
///
/// Spilling here includes:
/// - writing all of the data that were written in the in-memory buffer so far
/// - writing subsequent data as well.
///
/// Once entering "spilling" mode, the `SpillingWriter` stays in this mode.
pub struct SpillingWriter {
    state: MutableEnum<SpillingState>,
}

impl SpillingWriter {
    //// Creates a new `Spilling Writer`.
    pub fn new(
        limit: usize,
        write_factory: Box<dyn FnOnce() -> io::Result<WritePtr>>,
    ) -> SpillingWriter {
        let state = SpillingState::new(limit, write_factory);
        SpillingWriter {
            state: MutableEnum::wrap(state),
        }
    }

    /// Finalizes the `SpillingWriter`.
    ///
    /// The `SpillingResult` object is an enum specific
    /// to whether the `SpillingWriter` reached the spilling limit
    /// (In that case, the buffer is returned).
    ///
    /// If the writer reached the spilling mode, the underlying `WritePtr`
    /// is terminated and SpillingResult::Spilled is returned.
    pub fn finalize(self) -> io::Result<SpillingResult> {
        match self.state.into() {
            SpillingState::Spilled(wrt) => {
                wrt.terminate()?;
                Ok(SpillingResult::Spilled)
            }
            SpillingState::Buffer { buffer, .. } => Ok(SpillingResult::Buffer(buffer)),
        }
    }
}

/// enum used as the result of `.finalize()`.
pub enum SpillingResult {
    Spilled,
    Buffer(Vec<u8>),
}

impl io::Write for SpillingWriter {
    fn write(&mut self, payload: &[u8]) -> io::Result<usize> {
        self.write_all(payload)?;
        Ok(payload.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if let SpillingState::Spilled(wrt) = &mut *self.state {
            wrt.flush()?;
        }
        Ok(())
    }

    fn write_all(&mut self, payload: &[u8]) -> io::Result<()> {
        self.state.map_mutate(|mut state| {
            state = state.reserve(payload.len())?;
            match &mut state {
                SpillingState::Buffer { buffer, .. } => {
                    buffer.extend_from_slice(payload);
                }
                SpillingState::Spilled(wrt) => {
                    wrt.write_all(payload)?;
                }
            }
            Ok(state)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SpillingWriter;
    use crate::directory::spilling_writer::SpillingResult;
    use crate::directory::RAMDirectory;
    use crate::Directory;
    use std::io::{self, Write};
    use std::path::Path;

    #[test]
    fn test_no_spilling() {
        let ram_directory = RAMDirectory::create();
        let mut ram_directory_clone = ram_directory.clone();
        let path = Path::new("test");
        let write_factory = Box::new(move || {
            ram_directory_clone
                .open_write(path)
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
        });
        let mut spilling_wrt = SpillingWriter::new(10, write_factory);
        assert!(spilling_wrt.write_all(b"abcd").is_ok());
        if let SpillingResult::Buffer(buf) = spilling_wrt.finalize().unwrap() {
            assert_eq!(buf, b"abcd")
        } else {
            panic!("spill writer should not have spilled");
        }
        assert!(!ram_directory.exists(path));
    }

    #[test]
    fn test_spilling() {
        let ram_directory = RAMDirectory::create();
        let mut ram_directory_clone = ram_directory.clone();
        let path = Path::new("test");
        let write_factory = Box::new(move || {
            ram_directory_clone
                .open_write(path)
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
        });
        let mut spilling_wrt = SpillingWriter::new(10, write_factory);
        assert!(spilling_wrt.write_all(b"abcd").is_ok());
        assert!(spilling_wrt.write_all(b"efghijklmnop").is_ok());
        if let SpillingResult::Spilled = spilling_wrt.finalize().unwrap() {
        } else {
            panic!("spill writer should have spilled");
        }
        assert_eq!(
            ram_directory.atomic_read(path).unwrap(),
            b"abcdefghijklmnop"
        );
    }
}
