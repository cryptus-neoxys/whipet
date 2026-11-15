// Licensed under the Apache License, Version 2.0. See LICENSE.
//! Binary TCP ingress framing.
//!
//! The HTTP/JSON path stays the dashboard and control-plane path. This module
//! defines a minimal length-prefixed binary frame for clients that want to
//! submit fixed-layout commands without JSON parsing or per-order HTTP
//! requests.
//!
//! Frame layout (little-endian):
//!
//! ```text
//! [u32 body_len][u8 kind][body ...]
//! ```
//!
//! `body_len` excludes the four-byte prefix. A frame is rejected when the body
//! is larger than [`TCP_FRAME_MAX`]. The command payload inside `Submit` is the
//! existing [`Command`](crate::protocol::Command) encoding, so the engine
//! decodes exactly the same bytes whether the order arrived over HTTP or TCP.

use crate::error::{CoreError, Result};
use crate::protocol::{COMMAND_ENCODED_LEN, Command};

/// Largest accepted frame body. A full 256-command batch is 2 + 256 * 78 =
/// 19,970 bytes, so 64 KiB leaves room for a hello token and future fields.
pub const TCP_FRAME_MAX: usize = 64 * 1024;

/// Maximum commands in one `Submit` frame. Matches Iggy 0.10's default
/// `message_bus.max_batch` and the gateway's HTTP batch cap, so one TCP batch
/// is still one append.
pub const TCP_BATCH_MAX: usize = 256;

/// Largest accepted hello token.
pub const TCP_TOKEN_MAX: usize = 512;

const KIND_HELLO: u8 = 1;
const KIND_SUBMIT: u8 = 2;
const KIND_PING: u8 = 3;

const KIND_HELLO_ACK: u8 = 0x81;
const KIND_SUBMIT_ACK: u8 = 0x82;
const KIND_PONG: u8 = 0x83;
const KIND_ERROR: u8 = 0x84;

/// Client-to-server frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpRequest {
    /// Optional first frame. When the node has `WHIPPET_API_TOKEN` set, the
    /// connection is not allowed to submit until a matching hello arrives.
    Hello { token: String },
    /// A batch of fixed-layout commands. All commands must belong to one
    /// symbol so the server can make one append to one partition.
    Submit { commands: Vec<Command> },
    /// Liveness and clock-offset probe.
    Ping { client_ns: u64 },
}

/// Server-to-client frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TcpResponse {
    HelloAck {
        ok: bool,
        message: String,
    },
    /// One ack for a whole batch. Order `i` has offset `base_offset + i`.
    SubmitAck {
        partition: u32,
        base_offset: u64,
        count: u32,
        server_ns: u64,
    },
    Pong {
        client_ns: u64,
        server_ns: u64,
    },
    Error {
        code: u16,
        message: String,
    },
}

/// Error codes carried by [`TcpResponse::Error`]. These are stable numbers so a
/// non-Rust client can branch without parsing the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum TcpErrorCode {
    BadFrame = 1,
    Unauthorized = 2,
    MixedSymbols = 3,
    BatchTooLarge = 4,
    InvalidCommand = 5,
    JournalUnavailable = 6,
    RateLimited = 7,
    Internal = 8,
    /// Turbo admission rejected the command because the bounded uncommitted
    /// window is full. Retry shortly; nothing was journaled.
    WindowFull = 9,
    /// A monitored volume is at the critical threshold; ingress is refused
    /// until space is reclaimed. Nothing was journaled.
    StoragePressure = 10,
}

impl TcpErrorCode {
    pub fn as_u16(self) -> u16 {
        self as u16
    }
}

impl TcpRequest {
    /// Encode as a complete frame, including the four-byte length prefix.
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::with_capacity(16);
        match self {
            TcpRequest::Hello { token } => {
                body.push(KIND_HELLO);
                let token = token.as_bytes();
                let len = token.len().min(TCP_TOKEN_MAX) as u16;
                body.extend_from_slice(&len.to_le_bytes());
                body.extend_from_slice(&token[..len as usize]);
            }
            TcpRequest::Submit { commands } => {
                body.push(KIND_SUBMIT);
                body.extend_from_slice(&(commands.len() as u16).to_le_bytes());
                for command in commands {
                    body.extend_from_slice(&command.encode());
                }
            }
            TcpRequest::Ping { client_ns } => {
                body.push(KIND_PING);
                body.extend_from_slice(&client_ns.to_le_bytes());
            }
        }
        frame(body)
    }

    /// Decode a frame body (the bytes after the length prefix).
    pub fn decode_body(body: &[u8]) -> Result<Self> {
        let Some((&kind, rest)) = body.split_first() else {
            return Err(CoreError::InvalidValue("empty TCP frame".into()));
        };
        match kind {
            KIND_HELLO => {
                if rest.len() < 2 {
                    return Err(CoreError::InvalidValue("short hello frame".into()));
                }
                let len = u16::from_le_bytes([rest[0], rest[1]]) as usize;
                if len > TCP_TOKEN_MAX || rest.len() < 2 + len {
                    return Err(CoreError::InvalidValue("hello token length".into()));
                }
                let token = std::str::from_utf8(&rest[2..2 + len])
                    .map_err(|_| CoreError::InvalidValue("hello token is not UTF-8".into()))?;
                Ok(TcpRequest::Hello {
                    token: token.to_string(),
                })
            }
            KIND_SUBMIT => {
                if rest.len() < 2 {
                    return Err(CoreError::InvalidValue("short submit frame".into()));
                }
                let count = u16::from_le_bytes([rest[0], rest[1]]) as usize;
                if count == 0 || count > TCP_BATCH_MAX {
                    return Err(CoreError::InvalidValue(format!(
                        "submit batch must be 1..={TCP_BATCH_MAX} commands, got {count}"
                    )));
                }
                let expected = count * COMMAND_ENCODED_LEN;
                if rest.len() < 2 + expected {
                    return Err(CoreError::InvalidValue("truncated submit frame".into()));
                }
                let mut commands = Vec::with_capacity(count);
                for index in 0..count {
                    let start = 2 + index * COMMAND_ENCODED_LEN;
                    let end = start + COMMAND_ENCODED_LEN;
                    commands.push(Command::decode(&rest[start..end])?);
                }
                Ok(TcpRequest::Submit { commands })
            }
            KIND_PING => {
                if rest.len() < 8 {
                    return Err(CoreError::InvalidValue("short ping frame".into()));
                }
                let client_ns = u64::from_le_bytes(rest[..8].try_into().expect("8 bytes"));
                Ok(TcpRequest::Ping { client_ns })
            }
            other => Err(CoreError::InvalidValue(format!(
                "unknown TCP request kind {other}"
            ))),
        }
    }
}

impl TcpResponse {
    /// Encode as a complete frame, including the four-byte length prefix.
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::with_capacity(32);
        match self {
            TcpResponse::HelloAck { ok, message } => {
                body.push(KIND_HELLO_ACK);
                body.push(u8::from(*ok));
                push_string(&mut body, message);
            }
            TcpResponse::SubmitAck {
                partition,
                base_offset,
                count,
                server_ns,
            } => {
                body.push(KIND_SUBMIT_ACK);
                body.extend_from_slice(&partition.to_le_bytes());
                body.extend_from_slice(&base_offset.to_le_bytes());
                body.extend_from_slice(&count.to_le_bytes());
                body.extend_from_slice(&server_ns.to_le_bytes());
            }
            TcpResponse::Pong {
                client_ns,
                server_ns,
            } => {
                body.push(KIND_PONG);
                body.extend_from_slice(&client_ns.to_le_bytes());
                body.extend_from_slice(&server_ns.to_le_bytes());
            }
            TcpResponse::Error { code, message } => {
                body.push(KIND_ERROR);
                body.extend_from_slice(&code.to_le_bytes());
                push_string(&mut body, message);
            }
        }
        frame(body)
    }

    /// Decode a frame body (the bytes after the length prefix).
    pub fn decode_body(body: &[u8]) -> Result<Self> {
        let Some((&kind, rest)) = body.split_first() else {
            return Err(CoreError::InvalidValue("empty TCP frame".into()));
        };
        match kind {
            KIND_HELLO_ACK => {
                let (&ok, rest) = rest
                    .split_first()
                    .ok_or_else(|| CoreError::InvalidValue("short hello ack".into()))?;
                Ok(TcpResponse::HelloAck {
                    ok: ok != 0,
                    message: read_string(rest)?,
                })
            }
            KIND_SUBMIT_ACK => {
                if rest.len() < 24 {
                    return Err(CoreError::InvalidValue("short submit ack".into()));
                }
                Ok(TcpResponse::SubmitAck {
                    partition: u32::from_le_bytes(rest[0..4].try_into().expect("4 bytes")),
                    base_offset: u64::from_le_bytes(rest[4..12].try_into().expect("8 bytes")),
                    count: u32::from_le_bytes(rest[12..16].try_into().expect("4 bytes")),
                    server_ns: u64::from_le_bytes(rest[16..24].try_into().expect("8 bytes")),
                })
            }
            KIND_PONG => {
                if rest.len() < 16 {
                    return Err(CoreError::InvalidValue("short pong".into()));
                }
                Ok(TcpResponse::Pong {
                    client_ns: u64::from_le_bytes(rest[..8].try_into().expect("8 bytes")),
                    server_ns: u64::from_le_bytes(rest[8..16].try_into().expect("8 bytes")),
                })
            }
            KIND_ERROR => {
                if rest.len() < 2 {
                    return Err(CoreError::InvalidValue("short error frame".into()));
                }
                Ok(TcpResponse::Error {
                    code: u16::from_le_bytes([rest[0], rest[1]]),
                    message: read_string(&rest[2..])?,
                })
            }
            other => Err(CoreError::InvalidValue(format!(
                "unknown TCP response kind {other}"
            ))),
        }
    }
}

/// Prefix a body with its little-endian `u32` length.
pub fn frame(body: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Validate a four-byte length prefix and return the body length.
pub fn body_len(prefix: [u8; 4]) -> Result<usize> {
    let len = u32::from_le_bytes(prefix) as usize;
    if len > TCP_FRAME_MAX {
        return Err(CoreError::InvalidValue(format!(
            "TCP frame of {len} bytes exceeds the {TCP_FRAME_MAX}-byte limit"
        )));
    }
    Ok(len)
}

fn push_string(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    let len = bytes.len().min(u16::MAX as usize) as u16;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&bytes[..len as usize]);
}

fn read_string(bytes: &[u8]) -> Result<String> {
    if bytes.len() < 2 {
        return Err(CoreError::InvalidValue("short string field".into()));
    }
    let len = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    if bytes.len() < 2 + len {
        return Err(CoreError::InvalidValue("truncated string field".into()));
    }
    std::str::from_utf8(&bytes[2..2 + len])
        .map(str::to_string)
        .map_err(|_| CoreError::InvalidValue("string field is not UTF-8".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{OrderId, ParticipantId, Price, Qty, Symbol};
    use crate::protocol::{CommandKind, OrderType, Side};

    fn command(client_id: u8) -> Command {
        Command {
            kind: CommandKind::NewOrder,
            symbol: Symbol::new("BTC-USD").unwrap(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            price: Price(6_500_000),
            qty: Qty(1),
            order_id: OrderId(u128::from(client_id)),
            participant: ParticipantId(u64::from(client_id)),
            timestamp_ns: 42,
            nonce: 7,
            flags: 0,
        }
    }

    fn round_trip_request(request: TcpRequest) {
        let bytes = request.encode();
        let len = body_len(bytes[..4].try_into().unwrap()).unwrap();
        assert_eq!(len, bytes.len() - 4);
        assert_eq!(TcpRequest::decode_body(&bytes[4..]).unwrap(), request);
    }

    #[test]
    fn requests_round_trip() {
        round_trip_request(TcpRequest::Hello {
            token: "secret".into(),
        });
        round_trip_request(TcpRequest::Ping { client_ns: 99 });
        round_trip_request(TcpRequest::Submit {
            commands: vec![command(1), command(2)],
        });
    }

    #[test]
    fn responses_round_trip() {
        let responses = [
            TcpResponse::HelloAck {
                ok: true,
                message: "ok".into(),
            },
            TcpResponse::SubmitAck {
                partition: 3,
                base_offset: 11,
                count: 2,
                server_ns: 123,
            },
            TcpResponse::Pong {
                client_ns: 1,
                server_ns: 2,
            },
            TcpResponse::Error {
                code: TcpErrorCode::Unauthorized.as_u16(),
                message: "token required".into(),
            },
        ];
        for response in responses {
            let bytes = response.encode();
            assert_eq!(
                TcpResponse::decode_body(&bytes[4..]).unwrap(),
                response,
                "response round trip"
            );
        }
    }

    #[test]
    fn submit_rejects_zero_and_oversized_batches() {
        let mut body = vec![KIND_SUBMIT, 0, 0];
        assert!(TcpRequest::decode_body(&body).is_err());

        let count = (TCP_BATCH_MAX + 1) as u16;
        body = vec![KIND_SUBMIT];
        body.extend_from_slice(&count.to_le_bytes());
        assert!(TcpRequest::decode_body(&body).is_err());
    }

    #[test]
    fn frame_length_limit_is_enforced() {
        let too_big = ((TCP_FRAME_MAX + 1) as u32).to_le_bytes();
        assert!(body_len(too_big).is_err());
    }

    #[test]
    fn truncated_command_is_rejected() {
        let mut body = vec![KIND_SUBMIT];
        body.extend_from_slice(&1u16.to_le_bytes());
        body.extend_from_slice(&[0u8; 10]);
        assert!(TcpRequest::decode_body(&body).is_err());
    }
}
