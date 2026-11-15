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
    /// The ingress pre-trade risk guard refused the batch (per-order quantity
    /// or notional limit). Additive in the wire error set. Nothing was
    /// journaled and the connection stays usable. This is an ingress guard,
    /// not engine-enforced risk: a direct log producer bypasses it.
    RiskRejected = 11,
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
                // Never split a multi-byte character: a truncated token must
                // still be valid UTF-8 or the decoder rejects our own frame.
                let token = truncate_utf8(token, TCP_TOKEN_MAX).as_bytes();
                let len = token.len() as u16;
                body.extend_from_slice(&len.to_le_bytes());
                body.extend_from_slice(token);
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
                if len > TCP_TOKEN_MAX {
                    return Err(CoreError::InvalidValue(format!(
                        "hello token of {len} bytes exceeds the {TCP_TOKEN_MAX}-byte limit"
                    )));
                }
                // Exact, not `>=`: a frame with bytes after the token is a
                // desynchronised or crafted frame, not a partial decode.
                if rest.len() != 2 + len {
                    return Err(CoreError::InvalidValue(format!(
                        "hello frame carries {} bytes after the token length prefix, expected {len}",
                        rest.len() - 2
                    )));
                }
                let token = std::str::from_utf8(&rest[2..])
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
                if rest.len() != 2 + expected {
                    return Err(CoreError::InvalidValue(format!(
                        "submit frame of {} bytes does not match {count} fixed {COMMAND_ENCODED_LEN}-byte commands",
                        rest.len() - 2
                    )));
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
                if rest.len() != 8 {
                    return Err(CoreError::InvalidValue(format!(
                        "ping frame carries {} bytes, expected 8",
                        rest.len()
                    )));
                }
                let client_ns = u64::from_le_bytes(rest.try_into().expect("8 bytes"));
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
                // The encoder emits exactly 0 or 1. Accepting any other byte
                // would make the decode/re-encode cycle non-canonical.
                let ok = match ok {
                    0 => false,
                    1 => true,
                    other => {
                        return Err(CoreError::InvalidValue(format!(
                            "hello ack boolean must be 0 or 1, got {other}"
                        )));
                    }
                };
                Ok(TcpResponse::HelloAck {
                    ok,
                    message: read_string(rest)?,
                })
            }
            KIND_SUBMIT_ACK => {
                if rest.len() != 24 {
                    return Err(CoreError::InvalidValue(format!(
                        "submit ack carries {} bytes, expected 24",
                        rest.len()
                    )));
                }
                Ok(TcpResponse::SubmitAck {
                    partition: u32::from_le_bytes(rest[0..4].try_into().expect("4 bytes")),
                    base_offset: u64::from_le_bytes(rest[4..12].try_into().expect("8 bytes")),
                    count: u32::from_le_bytes(rest[12..16].try_into().expect("4 bytes")),
                    server_ns: u64::from_le_bytes(rest[16..24].try_into().expect("8 bytes")),
                })
            }
            KIND_PONG => {
                if rest.len() != 16 {
                    return Err(CoreError::InvalidValue(format!(
                        "pong carries {} bytes, expected 16",
                        rest.len()
                    )));
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
    // Cap on a UTF-8 boundary so a truncated message still decodes. The format
    // length is a `u16`; anything longer is truncated by contract.
    let value = truncate_utf8(value, u16::MAX as usize);
    let len = value.len() as u16;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value.as_bytes());
}

/// The longest prefix of `value` that is at most `max` bytes and ends on a
/// UTF-8 character boundary. A truncated frame must never contain a split
/// character, because the decoder rejects invalid UTF-8.
fn truncate_utf8(value: &str, max: usize) -> &str {
    if value.len() <= max {
        return value;
    }
    let mut end = max;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn read_string(bytes: &[u8]) -> Result<String> {
    if bytes.len() < 2 {
        return Err(CoreError::InvalidValue("short string field".into()));
    }
    let len = u16::from_le_bytes([bytes[0], bytes[1]]) as usize;
    // Exact, not `>=`: trailing bytes after the string are a malformed frame.
    if bytes.len() != 2 + len {
        return Err(CoreError::InvalidValue(format!(
            "string field carries {} bytes, expected {len}",
            bytes.len() - 2
        )));
    }
    std::str::from_utf8(&bytes[2..])
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

    #[test]
    fn decoders_reject_trailing_bytes() {
        let requests = [
            TcpRequest::Hello {
                token: "secret".into(),
            },
            TcpRequest::Ping { client_ns: 7 },
            TcpRequest::Submit {
                commands: vec![command(1)],
            },
        ];
        for request in requests {
            let mut body = request.encode()[4..].to_vec();
            body.push(0);
            assert!(
                TcpRequest::decode_body(&body).is_err(),
                "trailing byte accepted for {request:?}"
            );
        }

        let responses = [
            TcpResponse::HelloAck {
                ok: true,
                message: "ok".into(),
            },
            TcpResponse::SubmitAck {
                partition: 1,
                base_offset: 2,
                count: 3,
                server_ns: 4,
            },
            TcpResponse::Pong {
                client_ns: 5,
                server_ns: 6,
            },
            TcpResponse::Error {
                code: TcpErrorCode::BadFrame.as_u16(),
                message: "bad".into(),
            },
        ];
        for response in responses {
            let mut body = response.encode()[4..].to_vec();
            body.push(0);
            assert!(
                TcpResponse::decode_body(&body).is_err(),
                "trailing byte accepted for {response:?}"
            );
        }
    }

    #[test]
    fn hello_ack_rejects_a_non_canonical_boolean() {
        let body = [KIND_HELLO_ACK, 2, 0, 0];
        assert!(TcpResponse::decode_body(&body).is_err());
    }

    #[test]
    fn oversized_hello_token_is_not_split_mid_character() {
        // 511 ASCII bytes then a three-byte character straddles the 512-byte
        // truncation point. Encoding must cut on a boundary, not emit invalid
        // UTF-8 that its own decoder rejects.
        let token = format!("{}€", "a".repeat(TCP_TOKEN_MAX - 1));
        let frame = TcpRequest::Hello { token }.encode();
        let len = body_len(frame[..4].try_into().unwrap()).unwrap();
        let decoded = TcpRequest::decode_body(&frame[4..]).unwrap();
        assert_eq!(
            decoded,
            TcpRequest::Hello {
                token: "a".repeat(TCP_TOKEN_MAX - 1)
            },
            "frame body of {len} bytes must decode"
        );
    }

    #[test]
    fn declared_counts_are_rejected_before_reading_the_payload() {
        // Maximum batch count with no commands behind it.
        let mut body = vec![KIND_SUBMIT];
        body.extend_from_slice(&u16::MAX.to_le_bytes());
        assert!(TcpRequest::decode_body(&body).is_err());

        // Maximum hello token length with no token behind it.
        let mut body = vec![KIND_HELLO];
        body.extend_from_slice(&u16::MAX.to_le_bytes());
        assert!(TcpRequest::decode_body(&body).is_err());
    }
}
