use crate::directory::{TerminatingWrite, WritePtr};
use std::io::{self, Write};

pub enum SpillingState {
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

pub struct SpillingWriter {
    state: Option<SpillingState>,
}

impl SpillingWriter {
    pub fn new(
        limit: usize,
        write_factory: Box<dyn FnOnce() -> io::Result<WritePtr>>,
    ) -> SpillingWriter {
        let state = SpillingState::new(limit, write_factory);
        SpillingWriter { state: Some(state) }
    }

    pub fn finalize(self) -> io::Result<SpillingResult> {
        match self.state.expect("state cannot be None") {
            SpillingState::Spilled(wrt) => {
                wrt.terminate()?;
                Ok(SpillingResult::Spilled)
            }
            SpillingState::Buffer { buffer, .. } => Ok(SpillingResult::Buffer(buffer)),
        }
    }
}

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
        if let Some(SpillingState::Spilled(wrt)) = &mut self.state {
            wrt.flush()?;
        }
        Ok(())
    }

    fn write_all(&mut self, payload: &[u8]) -> io::Result<()> {
        let state_opt: Option<io::Result<SpillingState>> = self.state.take().map(|mut state| {
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
        });
        self.state = state_opt.transpose()?;
        Ok(())
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
