use std::fmt;

/// Errors that can occur during ubus operations.
#[derive(Debug)]
pub enum UbusError {
    /// I/O error during socket operations.
    Io(std::io::Error),
    /// Protocol error (unexpected message format).
    Protocol(&'static str),
    /// Non-zero status code returned by ubusd.
    Status(u32),
    /// Object not found.
    ObjectNotFound,
}

impl fmt::Display for UbusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UbusError::Io(e) => write!(f, "io: {}", e),
            UbusError::Protocol(s) => write!(f, "protocol: {}", s),
            UbusError::Status(c) => write!(f, "ubus status error: {}", c),
            UbusError::ObjectNotFound => write!(f, "object not found"),
        }
    }
}

impl std::error::Error for UbusError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            UbusError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for UbusError {
    fn from(e: std::io::Error) -> Self {
        UbusError::Io(e)
    }
}

/// Result type alias for ubus operations.
pub type Result<T> = std::result::Result<T, UbusError>;
