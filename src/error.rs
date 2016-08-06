use std::io;
use std::result;
use std::path::PathBuf;
use std::error;
use std::sync::PoisonError;
use directory::OpenError;

#[derive(Debug)]
pub enum Error {
    OpenError(OpenError),
    IOError(io::Error),
    Poisoned,
    CorruptedFile(PathBuf, Box<error::Error>),
    Other(Box<error::Error>), // + Send + Sync + 'static
}

impl Error {
    pub fn make_other<E: error::Error + 'static>(e: E) -> Error {
        Error::Other(Box::new(e))
    }
}

impl From<io::Error> for Error {
    fn from(io_error: io::Error) -> Error {
        Error::IOError(io_error)
    }
}

impl<Guard> From<PoisonError<Guard>> for Error {
    fn from(_: PoisonError<Guard>) -> Error {
        Error::Poisoned
    }
}

impl From<OpenError> for Error {
    fn from(error: OpenError) -> Error {
        Error::OpenError(error)
    }
}


// impl<E: 'static + error::Error + Send + Sync> From<E> for Error {
//     fn from(error: E) -> Error {
//         Error::Other(error)
//     }
// }




pub type Result<T> = result::Result<T, Error>;