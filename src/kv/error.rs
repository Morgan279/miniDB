use std::io;
use std::string::FromUtf8Error;

use failure::Fail;
use failure::_core::array::TryFromSliceError;

/// Error type for kvs.
#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "{}", _0)]
    IO(#[cause] io::Error),

    #[fail(display = "{}", _0)]
    SliceDecode(#[cause] TryFromSliceError),

    #[fail(display = "{}", _0)]
    ReprDecode(#[cause] Box<bincode::ErrorKind>),

    #[fail(display = "{}", _0)]
    StringDecode(#[cause] FromUtf8Error),

    #[fail(display = "Key not found")]
    KeyNotFound,

    #[fail(display = "Reach the file end")]
    EOF,

    #[fail(display = "invalid data path")]
    InvalidDataPath,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::IO(err)
    }
}

impl From<TryFromSliceError> for KvsError {
    fn from(err: TryFromSliceError) -> KvsError {
        KvsError::SliceDecode(err)
    }
}

impl From<Box<bincode::ErrorKind>> for KvsError {
    fn from(err: Box<bincode::ErrorKind>) -> KvsError {
        KvsError::ReprDecode(err)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> KvsError {
        KvsError::StringDecode(err)
    }
}

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, KvsError>;
