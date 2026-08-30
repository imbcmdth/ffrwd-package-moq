//! Maps wasi socket errors onto `std::io::Error`.

use std::io;

use wasi::sockets::network::ErrorCode;

/// The io kind a wasi socket error lands on. `DatagramTooLarge` maps to
/// `InvalidInput`, matching what native sends of an oversized datagram
/// surface through quinn (EMSGSIZE-shaped).
pub fn kind(code: ErrorCode) -> io::ErrorKind {
    match code {
        ErrorCode::WouldBlock => io::ErrorKind::WouldBlock,
        ErrorCode::AccessDenied => io::ErrorKind::PermissionDenied,
        ErrorCode::NotSupported => io::ErrorKind::Unsupported,
        ErrorCode::InvalidArgument | ErrorCode::DatagramTooLarge => io::ErrorKind::InvalidInput,
        ErrorCode::OutOfMemory => io::ErrorKind::OutOfMemory,
        ErrorCode::Timeout => io::ErrorKind::TimedOut,
        ErrorCode::AddressInUse => io::ErrorKind::AddrInUse,
        ErrorCode::AddressNotBindable => io::ErrorKind::AddrNotAvailable,
        ErrorCode::RemoteUnreachable => io::ErrorKind::HostUnreachable,
        ErrorCode::ConnectionRefused => io::ErrorKind::ConnectionRefused,
        ErrorCode::ConnectionReset => io::ErrorKind::ConnectionReset,
        ErrorCode::ConnectionAborted => io::ErrorKind::ConnectionAborted,
        ErrorCode::InvalidState => io::ErrorKind::InvalidInput,
        _ => io::ErrorKind::Other,
    }
}

pub fn map(code: ErrorCode) -> io::Error {
    io::Error::new(kind(code), format!("wasi socket error: {}", code.name()))
}

/// The send-budget rule: `check-send` names how many datagrams may go out
/// right now; a budget of zero is `WouldBlock`, and the caller must wait
/// on the outgoing stream's pollable before retrying.
pub fn budget(check: Result<u64, ErrorCode>) -> io::Result<u64> {
    match check {
        Ok(0) => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "send budget exhausted",
        )),
        Ok(n) => Ok(n),
        Err(code) => Err(map(code)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_budget_is_would_block() {
        assert_eq!(budget(Ok(0)).unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn positive_budget_passes_through() {
        assert_eq!(budget(Ok(16)).unwrap(), 16);
    }

    #[test]
    fn budget_errors_are_mapped() {
        assert_eq!(
            budget(Err(ErrorCode::AccessDenied)).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[test]
    fn oversized_datagram_is_invalid_input() {
        assert_eq!(
            kind(ErrorCode::DatagramTooLarge),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn would_block_keeps_its_kind() {
        assert_eq!(kind(ErrorCode::WouldBlock), io::ErrorKind::WouldBlock);
    }
}
