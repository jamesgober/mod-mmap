//! Error handling for memory mapping operations.

use std::fmt;
use std::io;
use std::result;

/// A specialized `Result` type for memory mapping operations.
pub type Result<T> = result::Result<T, Error>;

/// Errors that can occur during memory mapping operations.
#[derive(Debug)]
pub enum Error {
    /// An I/O error occurred.
    Io(io::Error),
    
    /// The memory map size is zero.
    ZeroSizedMapping,
    
    /// The requested memory map size exceeds system limits.
    SizeExceedsSystemLimit,
    
    /// Failed to allocate huge pages.
    HugePageAllocationFailed,
    
    /// NUMA allocation failed.
    NumaAllocationFailed,
    
    /// Memory protection error.
    ProtectionError,
    
    /// Memory alignment error.
    AlignmentError,
    
    /// Invalid argument provided.
    InvalidArgument(String),
    
    /// Platform-specific error with error code.
    PlatformError(i32),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "I/O error: {}", err),
            Error::ZeroSizedMapping => write!(f, "Memory map size cannot be zero"),
            Error::SizeExceedsSystemLimit => write!(f, "Requested memory map size exceeds system limits"),
            Error::HugePageAllocationFailed => write!(f, "Failed to allocate huge pages"),
            Error::NumaAllocationFailed => write!(f, "NUMA memory allocation failed"),
            Error::ProtectionError => write!(f, "Memory protection error"),
            Error::AlignmentError => write!(f, "Memory alignment error"),
            Error::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
            Error::PlatformError(code) => write!(f, "Platform-specific error code: {}", code),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}
