// Licensed under the Apache License, Version 2.0. See LICENSE.

/// Errors raised while validating or decoding Whippet values.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum CoreError {
    #[error("symbol must be 1-16 printable ASCII characters, got {0:?}")]
    InvalidSymbol(String),
    #[error("buffer too short: need {need} bytes, have {have}")]
    BufferTooShort { need: usize, have: usize },
    #[error("unknown {kind} discriminant {value}")]
    UnknownDiscriminant { kind: &'static str, value: u8 },
    #[error("invalid wire version {found}, expected {expected}")]
    InvalidVersion { found: u8, expected: u8 },
    #[error("invalid value: {0}")]
    InvalidValue(String),
    #[error("encoding failed: {0}")]
    Encode(String),
}

pub type Result<T> = std::result::Result<T, CoreError>;
