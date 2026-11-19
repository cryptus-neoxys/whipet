// Licensed under the Apache License, Version 2.0. See LICENSE.

//! Executable untrusted-input contract for the Whippet wire decoders.
//!
//! Every decoder is a trust boundary: the bytes it reads come from the journal,
//! a socket, or a peer process. The contract these tests make executable is:
//!
//! 1. Decoding either returns a value or a typed [`CoreError`]; it never panics,
//!    reads out of bounds, or allocates on an attacker-chosen count.
//! 2. A successful decode is canonical: re-encoding the value reproduces the
//!    exact input bytes, so a decoder cannot silently accept trailing bytes or
//!    a non-canonical field encoding. The one documented exception is the
//!    pre-status-byte [`MdSnapshot`] form (see [`assert_snapshot_canonical`]).
//! 3. The `*_ENCODED_LEN` constants describe the fixed-size layouts exactly.
//! 4. `Symbol` accepts only 1-16 printable ASCII bytes, NUL-padded on the wire,
//!    and every other byte string is a typed error rather than a panic.
//!
//! Proptest turns a panic into a failing case, so the "arbitrary bytes" tests
//! are the executable form of "arbitrary input never panics".

use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use whippet_core::CoreError;
use whippet_core::WIRE_VERSION;
use whippet_core::ids::{OrderId, ParticipantId, Price, Qty, Seq, Symbol};
use whippet_core::protocol::{
    COMMAND_ENCODED_LEN, Command, CommandKind, EXEC_ENCODED_LEN, ExecKind, ExecutionReport,
    MD_EVENT_ENCODED_LEN, MD_FLAG_HALTED, MD_LEVEL_ENCODED_LEN, MD_ORDER_ENCODED_LEN,
    MD_SESSION_MASK, MD_SESSION_SHIFT, MD_SNAPSHOT_HEADER_LEN, MdEvent, MdKind, MdLevel, MdOrder,
    MdSnapshot, OrderType, RejectReason, SessionState, Side, TradingStatus,
};
use whippet_core::tcp::{TCP_BATCH_MAX, TCP_FRAME_MAX, TcpRequest, TcpResponse, body_len, frame};

// ---------------------------------------------------------------------------
// Strategies
// ---------------------------------------------------------------------------

const SYMBOLS: [&str; 5] = ["A", "BTC-USD", "ETH-USD", "SOL-USD", "ABCDEFGHIJKLMNOP"];

fn symbol() -> Symbol {
    Symbol::new("BTC-USD").expect("static symbol")
}

fn symbol_strategy() -> impl Strategy<Value = Symbol> {
    prop::sample::select(SYMBOLS.to_vec())
        .prop_map(|value| Symbol::new(value).expect("listed symbols are valid"))
}

/// Integers biased towards the edges that overflow, sign-extend, or shrink
/// guests, plus the whole range.
fn any_i64() -> impl Strategy<Value = i64> {
    prop_oneof![
        Just(0i64),
        Just(1),
        Just(-1),
        Just(i64::MIN),
        Just(i64::MAX),
        any::<i64>(),
    ]
}

fn any_u64() -> impl Strategy<Value = u64> {
    prop_oneof![
        Just(0u64),
        Just(1),
        Just(u64::from(u32::MAX)),
        Just(u64::MAX),
        any::<u64>(),
    ]
}

fn any_u128() -> impl Strategy<Value = u128> {
    prop_oneof![
        Just(0u128),
        Just(1),
        Just(u128::from(u64::MAX)),
        Just(u128::MAX),
        any::<u128>(),
    ]
}

fn any_u16() -> impl Strategy<Value = u16> {
    prop_oneof![Just(0u16), Just(1), Just(u16::MAX), any::<u16>()]
}

fn side_strategy() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

fn order_type_strategy() -> impl Strategy<Value = OrderType> {
    prop_oneof![
        Just(OrderType::Limit),
        Just(OrderType::Market),
        Just(OrderType::Ioc),
        Just(OrderType::Fok),
        Just(OrderType::PostOnly),
    ]
}

fn command_kind_strategy() -> impl Strategy<Value = CommandKind> {
    prop_oneof![
        Just(CommandKind::NewOrder),
        Just(CommandKind::Cancel),
        Just(CommandKind::Halt),
        Just(CommandKind::Resume),
        Just(CommandKind::SetSession),
        Just(CommandKind::Amend),
    ]
}

fn exec_kind_strategy() -> impl Strategy<Value = ExecKind> {
    prop_oneof![
        Just(ExecKind::Accepted),
        Just(ExecKind::Rejected),
        Just(ExecKind::Filled),
        Just(ExecKind::PartiallyFilled),
        Just(ExecKind::Cancelled),
        Just(ExecKind::Amended),
    ]
}

fn reject_reason_strategy() -> impl Strategy<Value = RejectReason> {
    prop_oneof![
        Just(RejectReason::None),
        Just(RejectReason::UnknownSymbol),
        Just(RejectReason::InvalidPrice),
        Just(RejectReason::InvalidQty),
        Just(RejectReason::DuplicateOrderId),
        Just(RejectReason::PriceOutOfBand),
        Just(RejectReason::PostOnlyWouldCross),
        Just(RejectReason::FokUnfillable),
        Just(RejectReason::UnknownOrder),
        Just(RejectReason::SelfTradePrevention),
        Just(RejectReason::Halted),
        Just(RejectReason::Internal),
        Just(RejectReason::SessionNotOpen),
        Just(RejectReason::RiskLimit),
        Just(RejectReason::AmendQtyIncrease),
        Just(RejectReason::AmendWouldCross),
    ]
}

fn md_kind_strategy() -> impl Strategy<Value = MdKind> {
    prop_oneof![
        Just(MdKind::Add),
        Just(MdKind::Reduce),
        Just(MdKind::Remove),
        Just(MdKind::Trade),
        Just(MdKind::Bbo),
        Just(MdKind::Status),
        Just(MdKind::Snapshot),
        Just(MdKind::Replace),
    ]
}

fn command_strategy() -> impl Strategy<Value = Command> {
    (
        command_kind_strategy(),
        symbol_strategy(),
        side_strategy(),
        order_type_strategy(),
        any_i64(),
        any_u64(),
        any_u128(),
        any_u64(),
        any_u64(),
        any_u64(),
        any_u16(),
    )
        .prop_map(
            |(
                kind,
                symbol,
                side,
                order_type,
                price,
                qty,
                order_id,
                participant,
                timestamp_ns,
                nonce,
                flags,
            )| Command {
                kind,
                symbol,
                side,
                order_type,
                price: Price(price),
                qty: Qty(qty),
                order_id: OrderId(order_id),
                participant: ParticipantId(participant),
                timestamp_ns,
                nonce,
                flags,
            },
        )
}

fn execution_report_strategy() -> impl Strategy<Value = ExecutionReport> {
    (
        (
            exec_kind_strategy(),
            symbol_strategy(),
            side_strategy(),
            order_type_strategy(),
            any_i64(),
            any_u64(),
            any_u64(),
        ),
        (
            any_u128(),
            any_u128(),
            any_u64(),
            any_u64(),
            any_u64(),
            reject_reason_strategy(),
            any_u16(),
        ),
    )
        .prop_map(|(head, tail)| {
            let (kind, symbol, side, order_type, price, qty, remaining) = head;
            let (order_id, maker_order_id, seq, cause_offset, timestamp_ns, reason, flags) = tail;
            ExecutionReport {
                kind,
                symbol,
                side,
                order_type,
                price: Price(price),
                qty: Qty(qty),
                remaining: Qty(remaining),
                order_id: OrderId(order_id),
                maker_order_id: OrderId(maker_order_id),
                seq: Seq(seq),
                cause_offset,
                timestamp_ns,
                reason,
                flags,
            }
        })
}

fn md_event_strategy() -> impl Strategy<Value = MdEvent> {
    (
        md_kind_strategy(),
        symbol_strategy(),
        any_u64(),
        any_u64(),
        any_u64(),
        side_strategy(),
        any::<u8>(),
        any_i64(),
        any_u64(),
        any_u128(),
        any_u64(),
    )
        .prop_map(
            |(
                kind,
                symbol,
                seq,
                cause_offset,
                timestamp_ns,
                side,
                flags,
                price,
                qty,
                order_id,
                participant,
            )| MdEvent {
                kind,
                symbol,
                seq: Seq(seq),
                cause_offset,
                timestamp_ns,
                side,
                flags,
                price: Price(price),
                qty: Qty(qty),
                order_id: OrderId(order_id),
                participant: ParticipantId(participant),
            },
        )
}

fn md_level_strategy() -> impl Strategy<Value = MdLevel> {
    (any_i64(), any_u64(), any::<u32>()).prop_map(|(price, qty, orders)| MdLevel {
        price: Price(price),
        qty: Qty(qty),
        orders,
    })
}

fn md_order_strategy() -> impl Strategy<Value = MdOrder> {
    (any_u128(), side_strategy(), any_i64(), any_u64(), any_u64()).prop_map(
        |(order_id, side, price, qty, participant)| MdOrder {
            order_id: OrderId(order_id),
            side,
            price: Price(price),
            qty: Qty(qty),
            participant: ParticipantId(participant),
        },
    )
}

/// Snapshot status flags: valid halt/session combinations plus every raw byte.
/// The wire field is a raw `u8`; the status helpers are a separate contract.
fn snapshot_flags_strategy() -> impl Strategy<Value = u8> {
    prop_oneof![
        Just(0u8),
        Just(MD_FLAG_HALTED),
        Just(SessionState::PreOpen.to_md_flags()),
        Just(SessionState::Open.to_md_flags()),
        Just(SessionState::Closed.to_md_flags()),
        Just(MD_FLAG_HALTED | SessionState::Closed.to_md_flags()),
        any::<u8>(),
    ]
}

fn md_snapshot_strategy() -> impl Strategy<Value = MdSnapshot> {
    (
        symbol_strategy(),
        any_u64(),
        any_u64(),
        any_u64(),
        any_i64(),
        any_u64(),
        prop::collection::vec(md_level_strategy(), 0..=8),
        prop::collection::vec(md_level_strategy(), 0..=8),
        prop::collection::vec(md_order_strategy(), 0..=8),
        snapshot_flags_strategy(),
    )
        .prop_map(
            |(
                symbol,
                seq,
                cause_offset,
                timestamp_ns,
                last_trade_price,
                last_trade_qty,
                bids,
                asks,
                orders,
                flags,
            )| MdSnapshot {
                symbol,
                seq: Seq(seq),
                cause_offset,
                timestamp_ns,
                last_trade_price: Price(last_trade_price),
                last_trade_qty: Qty(last_trade_qty),
                bids,
                asks,
                orders,
                flags,
            },
        )
}

/// UTF-8 text bounded by character count. The wire stores a `u16` byte length,
/// so the strategies stay far below that and never exercise encoder truncation.
fn text_strategy(max_chars: usize) -> impl Strategy<Value = String> {
    prop::collection::vec(any::<char>(), 0..=max_chars)
        .prop_map(|chars| chars.into_iter().collect())
}

fn tcp_request_strategy() -> impl Strategy<Value = TcpRequest> {
    prop_oneof![
        text_strategy(32).prop_map(|token| TcpRequest::Hello { token }),
        prop::collection::vec(command_strategy(), 1..=4)
            .prop_map(|commands| TcpRequest::Submit { commands }),
        any_u64().prop_map(|client_ns| TcpRequest::Ping { client_ns }),
    ]
}

fn tcp_response_strategy() -> impl Strategy<Value = TcpResponse> {
    prop_oneof![
        (any::<bool>(), text_strategy(32))
            .prop_map(|(ok, message)| TcpResponse::HelloAck { ok, message }),
        (any::<u32>(), any_u64(), any::<u32>(), any_u64()).prop_map(
            |(partition, base_offset, count, server_ns)| TcpResponse::SubmitAck {
                partition,
                base_offset,
                count,
                server_ns,
            }
        ),
        (any_u64(), any_u64()).prop_map(|(client_ns, server_ns)| TcpResponse::Pong {
            client_ns,
            server_ns
        }),
        (any::<u16>(), text_strategy(32))
            .prop_map(|(code, message)| TcpResponse::Error { code, message }),
    ]
}

// ---------------------------------------------------------------------------
// Fixed test fixtures and helpers
// ---------------------------------------------------------------------------

fn minimal_report() -> ExecutionReport {
    ExecutionReport {
        kind: ExecKind::Accepted,
        symbol: symbol(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        price: Price(1),
        qty: Qty(1),
        remaining: Qty(1),
        order_id: OrderId(1),
        maker_order_id: OrderId(0),
        seq: Seq(1),
        cause_offset: 0,
        timestamp_ns: 0,
        reason: RejectReason::None,
        flags: 0,
    }
}

fn minimal_event() -> MdEvent {
    MdEvent {
        kind: MdKind::Trade,
        symbol: symbol(),
        seq: Seq(1),
        cause_offset: 0,
        timestamp_ns: 0,
        side: Side::Buy,
        flags: 0,
        price: Price(1),
        qty: Qty(1),
        order_id: OrderId(1),
        participant: ParticipantId(1),
    }
}

fn minimal_snapshot() -> MdSnapshot {
    MdSnapshot {
        symbol: symbol(),
        seq: Seq(1),
        cause_offset: 0,
        timestamp_ns: 0,
        last_trade_price: Price(0),
        last_trade_qty: Qty(0),
        bids: Vec::new(),
        asks: Vec::new(),
        orders: Vec::new(),
        flags: 0,
    }
}

/// The snapshot decoder accepts one historical non-canonical form: a snapshot
/// written before the status byte existed has no trailing flags byte and
/// decodes as running (`flags == 0`). Re-encoding adds that zero byte. Every
/// other successful decode must reproduce its input exactly.
fn assert_snapshot_canonical(snapshot: &MdSnapshot, bytes: &[u8]) -> Result<(), TestCaseError> {
    let canonical = snapshot.encode();
    if canonical.as_slice() == bytes {
        return Ok(());
    }
    prop_assert_eq!(
        snapshot.flags,
        0,
        "only the legacy flag-less form may omit bytes"
    );
    prop_assert_eq!(
        canonical.len(),
        bytes.len() + 1,
        "at most the single legacy status byte may be missing"
    );
    prop_assert_eq!(&canonical[..bytes.len()], bytes);
    prop_assert_eq!(canonical.last().copied(), Some(0u8));
    Ok(())
}

/// Apply one of four deterministic mutations: replace, insert, truncate, append.
fn mutate_bytes(bytes: &mut Vec<u8>, position: usize, replacement: u8, mode: u8) {
    match mode % 4 {
        0 => {
            if !bytes.is_empty() {
                let index = position % bytes.len();
                bytes[index] = replacement;
            }
        }
        1 => {
            let index = position % (bytes.len() + 1);
            bytes.insert(index, replacement);
        }
        2 => {
            let keep = position % (bytes.len() + 1);
            bytes.truncate(keep);
        }
        _ => bytes.push(replacement),
    }
}

fn assert_invalid_version<T: std::fmt::Debug + PartialEq>(
    template: &[u8],
    decode: impl Fn(&[u8]) -> whippet_core::Result<T>,
) {
    for found in (0..=255u8).filter(|&value| value != WIRE_VERSION) {
        let mut bytes = template.to_vec();
        bytes[0] = found;
        assert_eq!(
            decode(&bytes),
            Err(CoreError::InvalidVersion {
                found,
                expected: WIRE_VERSION,
            }),
            "version byte {found} must be rejected"
        );
    }
}

// ---------------------------------------------------------------------------
// Round trips: decode(encode(value)) == value
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn command_decodes_its_own_encoding(command in command_strategy()) {
        let bytes = command.encode();
        prop_assert_eq!(bytes.len(), COMMAND_ENCODED_LEN);
        prop_assert_eq!(Command::decode(&bytes), Ok(command));
    }

    #[test]
    fn execution_report_decodes_its_own_encoding(report in execution_report_strategy()) {
        let bytes = report.encode();
        prop_assert_eq!(bytes.len(), EXEC_ENCODED_LEN);
        prop_assert_eq!(ExecutionReport::decode(&bytes), Ok(report));
    }

    #[test]
    fn md_event_decodes_its_own_encoding(event in md_event_strategy()) {
        let bytes = event.encode();
        prop_assert_eq!(bytes.len(), MD_EVENT_ENCODED_LEN);
        prop_assert_eq!(MdEvent::decode(&bytes), Ok(event));
    }

    #[test]
    fn md_snapshot_decodes_its_own_encoding(snapshot in md_snapshot_strategy()) {
        let bytes = snapshot.encode();
        prop_assert_eq!(MdSnapshot::decode(&bytes), Ok(snapshot));
    }

    #[test]
    fn tcp_request_decodes_its_own_frame(request in tcp_request_strategy()) {
        let bytes = request.encode();
        let prefix: [u8; 4] = bytes[..4].try_into().expect("four-byte prefix");
        prop_assert_eq!(body_len(prefix), Ok(bytes.len() - 4));
        prop_assert_eq!(TcpRequest::decode_body(&bytes[4..]), Ok(request.clone()));
        prop_assert_eq!(request.encode(), bytes);
    }

    #[test]
    fn tcp_response_decodes_its_own_frame(response in tcp_response_strategy()) {
        let bytes = response.encode();
        let prefix: [u8; 4] = bytes[..4].try_into().expect("four-byte prefix");
        prop_assert_eq!(body_len(prefix), Ok(bytes.len() - 4));
        prop_assert_eq!(TcpResponse::decode_body(&bytes[4..]), Ok(response.clone()));
        prop_assert_eq!(response.encode(), bytes);
    }

    /// The declared fixed widths are the actual encoded widths.
    #[test]
    fn fixed_width_constants_match_the_encoder(
        command in command_strategy(),
        report in execution_report_strategy(),
        event in md_event_strategy(),
        snapshot in md_snapshot_strategy(),
    ) {
        prop_assert_eq!(command.encode().len(), COMMAND_ENCODED_LEN);
        prop_assert_eq!(report.encode().len(), EXEC_ENCODED_LEN);
        prop_assert_eq!(event.encode().len(), MD_EVENT_ENCODED_LEN);
        let expected = MD_SNAPSHOT_HEADER_LEN
            + (snapshot.bids.len() + snapshot.asks.len()) * MD_LEVEL_ENCODED_LEN
            + snapshot.orders.len() * MD_ORDER_ENCODED_LEN
            + 1;
        prop_assert_eq!(snapshot.encode().len(), expected);
    }
}

// ---------------------------------------------------------------------------
// Arbitrary bytes: never panic, and every success is canonical
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn arbitrary_bytes_never_panic_and_are_canonical(
        bytes in prop::collection::vec(any::<u8>(), 0..=4096)
    ) {
        if let Ok(command) = Command::decode(&bytes) {
            prop_assert_eq!(command.encode(), bytes.clone());
        }
        if let Ok(report) = ExecutionReport::decode(&bytes) {
            prop_assert_eq!(report.encode(), bytes.clone());
        }
        if let Ok(event) = MdEvent::decode(&bytes) {
            prop_assert_eq!(event.encode(), bytes.clone());
        }
        if let Ok(snapshot) = MdSnapshot::decode(&bytes) {
            assert_snapshot_canonical(&snapshot, &bytes)?;
        }
        if let Ok(request) = TcpRequest::decode_body(&bytes) {
            prop_assert_eq!(request.encode(), frame(bytes.clone()));
        }
        if let Ok(response) = TcpResponse::decode_body(&bytes) {
            prop_assert_eq!(response.encode(), frame(bytes.clone()));
        }
    }

    #[test]
    fn mutated_command_encodings_are_rejected_or_canonical(
        command in command_strategy(),
        position in any::<usize>(),
        replacement in any::<u8>(),
        mode in 0u8..4,
    ) {
        let mut bytes = command.encode();
        mutate_bytes(&mut bytes, position, replacement, mode);
        if let Ok(decoded) = Command::decode(&bytes) {
            prop_assert_eq!(decoded.encode(), bytes);
        }
    }

    #[test]
    fn mutated_report_encodings_are_rejected_or_canonical(
        report in execution_report_strategy(),
        position in any::<usize>(),
        replacement in any::<u8>(),
        mode in 0u8..4,
    ) {
        let mut bytes = report.encode();
        mutate_bytes(&mut bytes, position, replacement, mode);
        if let Ok(decoded) = ExecutionReport::decode(&bytes) {
            prop_assert_eq!(decoded.encode(), bytes);
        }
    }

    #[test]
    fn mutated_event_encodings_are_rejected_or_canonical(
        event in md_event_strategy(),
        position in any::<usize>(),
        replacement in any::<u8>(),
        mode in 0u8..4,
    ) {
        let mut bytes = event.encode();
        mutate_bytes(&mut bytes, position, replacement, mode);
        if let Ok(decoded) = MdEvent::decode(&bytes) {
            prop_assert_eq!(decoded.encode(), bytes);
        }
    }

    #[test]
    fn mutated_snapshot_encodings_are_rejected_or_canonical(
        snapshot in md_snapshot_strategy(),
        position in any::<usize>(),
        replacement in any::<u8>(),
        mode in 0u8..4,
    ) {
        let mut bytes = snapshot.encode();
        mutate_bytes(&mut bytes, position, replacement, mode);
        if let Ok(decoded) = MdSnapshot::decode(&bytes) {
            assert_snapshot_canonical(&decoded, &bytes)?;
        }
    }

    #[test]
    fn mutated_tcp_bodies_are_rejected_or_canonical(
        request in tcp_request_strategy(),
        response in tcp_response_strategy(),
        position in any::<usize>(),
        replacement in any::<u8>(),
        mode in 0u8..4,
    ) {
        let frame_bytes = request.encode();
        let mut body = frame_bytes[4..].to_vec();
        mutate_bytes(&mut body, position, replacement, mode);
        if let Ok(decoded) = TcpRequest::decode_body(&body) {
            prop_assert_eq!(decoded.encode(), frame(body.clone()));
        }

        let frame_bytes = response.encode();
        let mut body = frame_bytes[4..].to_vec();
        mutate_bytes(&mut body, position, replacement, mode);
        if let Ok(decoded) = TcpResponse::decode_body(&body) {
            prop_assert_eq!(decoded.encode(), frame(body));
        }
    }
}

// ---------------------------------------------------------------------------
// Deterministic edge cases
// ---------------------------------------------------------------------------

#[test]
fn version_byte_is_authoritative() {
    assert_invalid_version(&minimal_command().encode(), Command::decode);
    assert_invalid_version(&minimal_report().encode(), ExecutionReport::decode);
    assert_invalid_version(&minimal_event().encode(), MdEvent::decode);
    assert_invalid_version(&minimal_snapshot().encode(), MdSnapshot::decode);
}

fn minimal_command() -> Command {
    Command::cancel(symbol(), OrderId(1), ParticipantId(1), 0)
}

#[test]
fn amend_command_roundtrips_in_the_fixed_layout() {
    let command = Command::amend(
        symbol(),
        OrderId(7),
        Price(1_234),
        Qty(5),
        ParticipantId(9),
        42,
    );
    let bytes = command.encode();
    assert_eq!(bytes.len(), COMMAND_ENCODED_LEN);
    let decoded = Command::decode(&bytes).expect("an amend command decodes");
    assert_eq!(decoded, command);
    assert_eq!(decoded.kind, CommandKind::Amend);
    assert_eq!(decoded.order_id, OrderId(7));
    assert_eq!(decoded.price, Price(1_234));
    assert_eq!(decoded.qty, Qty(5));
    assert_eq!(decoded.participant, ParticipantId(9));
}

#[test]
fn empty_input_is_a_typed_error() {
    assert!(Command::decode(&[]).is_err());
    assert!(ExecutionReport::decode(&[]).is_err());
    assert!(MdEvent::decode(&[]).is_err());
    assert!(MdSnapshot::decode(&[]).is_err());
    assert!(TcpRequest::decode_body(&[]).is_err());
    assert!(TcpResponse::decode_body(&[]).is_err());

    // A zero-length frame is a valid length prefix with no body; the body
    // decoder must still reject it rather than returning a default value.
    assert_eq!(body_len([0, 0, 0, 0]), Ok(0));
    assert!(TcpRequest::decode_body(&frame(Vec::new())[4..]).is_err());
}

#[test]
fn trailing_bytes_are_rejected() {
    let mut bytes = minimal_command().encode();
    bytes.push(0);
    assert!(Command::decode(&bytes).is_err());

    let mut bytes = minimal_report().encode();
    bytes.push(0);
    assert!(ExecutionReport::decode(&bytes).is_err());

    let mut bytes = minimal_event().encode();
    bytes.push(0);
    assert!(MdEvent::decode(&bytes).is_err());

    let mut bytes = minimal_snapshot().encode();
    bytes.push(0);
    assert!(MdSnapshot::decode(&bytes).is_err());

    let mut body = TcpRequest::Ping { client_ns: 1 }.encode()[4..].to_vec();
    body.push(0);
    assert!(TcpRequest::decode_body(&body).is_err());

    let mut body = TcpResponse::Pong {
        client_ns: 1,
        server_ns: 2,
    }
    .encode()[4..]
        .to_vec();
    body.push(0);
    assert!(TcpResponse::decode_body(&body).is_err());
}

#[test]
fn truncation_is_never_a_partial_decode() {
    let command = minimal_command().encode();
    for len in 0..command.len() {
        assert!(
            Command::decode(&command[..len]).is_err(),
            "command prefix of {len} bytes decoded"
        );
    }

    let report = minimal_report().encode();
    for len in 0..report.len() {
        assert!(
            ExecutionReport::decode(&report[..len]).is_err(),
            "report prefix of {len} bytes decoded"
        );
    }

    let event = minimal_event().encode();
    for len in 0..event.len() {
        assert!(
            MdEvent::decode(&event[..len]).is_err(),
            "event prefix of {len} bytes decoded"
        );
    }

    // The snapshot's optional status byte means the last proper prefix is the
    // documented legacy form; every shorter prefix is an error.
    let snapshot = minimal_snapshot().encode();
    for len in 0..snapshot.len() - 1 {
        assert!(
            MdSnapshot::decode(&snapshot[..len]).is_err(),
            "snapshot prefix of {len} bytes decoded"
        );
    }
    let legacy = MdSnapshot::decode(&snapshot[..snapshot.len() - 1]).expect("legacy form");
    assert_eq!(legacy.flags, 0);
    assert_eq!(legacy, minimal_snapshot());

    for request in [
        TcpRequest::Hello {
            token: "secret".into(),
        },
        TcpRequest::Ping { client_ns: 9 },
        TcpRequest::Submit {
            commands: vec![minimal_command()],
        },
    ] {
        let body = request.encode()[4..].to_vec();
        for len in 0..body.len() {
            assert!(
                TcpRequest::decode_body(&body[..len]).is_err(),
                "request prefix of {len} bytes decoded for {request:?}"
            );
        }
    }
}

#[test]
fn oversized_count_prefixes_fail_without_a_long_payload() {
    // Submit: zero, too large, and the maximum u16 count with no commands.
    for count in [0u16, TCP_BATCH_MAX as u16 + 1, u16::MAX] {
        let mut body = vec![2u8];
        body.extend_from_slice(&count.to_le_bytes());
        assert!(
            TcpRequest::decode_body(&body).is_err(),
            "submit count {count} accepted"
        );
    }

    // Hello: maximum u16 token length with no token bytes.
    let mut body = vec![1u8];
    body.extend_from_slice(&u16::MAX.to_le_bytes());
    assert!(TcpRequest::decode_body(&body).is_err());

    // Responses: maximum u16 string length with no string bytes.
    assert!(TcpResponse::decode_body(&[0x81, 1, 0xff, 0xff]).is_err());
    assert!(TcpResponse::decode_body(&[0x84, 0, 0, 0xff, 0xff]).is_err());

    // Snapshot: every count field at its maximum with no levels or orders.
    const BID_COUNT_AT: usize = MD_SNAPSHOT_HEADER_LEN - 2 - 2 - 4;
    let mut bytes = minimal_snapshot().encode();
    bytes[BID_COUNT_AT..BID_COUNT_AT + 2].copy_from_slice(&u16::MAX.to_le_bytes());
    bytes[BID_COUNT_AT + 2..BID_COUNT_AT + 4].copy_from_slice(&u16::MAX.to_le_bytes());
    bytes[BID_COUNT_AT + 4..BID_COUNT_AT + 8].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(MdSnapshot::decode(&bytes).is_err());

    // Frame length prefix: u32 maximum and one byte over the cap.
    assert!(body_len([0xff; 4]).is_err());
    assert_eq!(
        body_len((TCP_FRAME_MAX as u32).to_le_bytes()),
        Ok(TCP_FRAME_MAX)
    );
    assert!(body_len(((TCP_FRAME_MAX as u32) + 1).to_le_bytes()).is_err());
}

#[test]
fn every_unknown_discriminant_is_a_typed_error() {
    for code in 0..=255u8 {
        assert_eq!(Side::from_code(code).is_ok(), matches!(code, 1 | 2));
        assert_eq!(
            OrderType::from_code(code).is_ok(),
            (1u8..=5).contains(&code)
        );
        assert_eq!(
            CommandKind::from_code(code).is_ok(),
            (1u8..=7).contains(&code)
        );
        assert_eq!(ExecKind::from_code(code).is_ok(), (1u8..=6).contains(&code));
        assert_eq!(MdKind::from_code(code).is_ok(), (1u8..=8).contains(&code));
        assert_eq!(SessionState::from_code(code).is_ok(), code <= 2);
    }
    for code in 0..=u16::MAX {
        assert_eq!(RejectReason::from_code(code).is_ok(), code <= 15);
    }

    // A patch to the kind byte must be caught, not ignored.
    let command = minimal_command().encode();
    let report = minimal_report().encode();
    let event = minimal_event().encode();
    let snapshot = minimal_snapshot().encode();
    for code in 0..=255u8 {
        if CommandKind::from_code(code).is_err() {
            let mut bytes = command.clone();
            bytes[1] = code;
            assert!(matches!(
                Command::decode(&bytes),
                Err(CoreError::UnknownDiscriminant {
                    kind: "CommandKind",
                    value
                }) if value == code
            ));
        }
        if ExecKind::from_code(code).is_err() {
            let mut bytes = report.clone();
            bytes[1] = code;
            assert!(ExecutionReport::decode(&bytes).is_err());
        }
        if MdKind::from_code(code).is_err() {
            let mut bytes = event.clone();
            bytes[1] = code;
            assert!(MdEvent::decode(&bytes).is_err());
        }
        if code != MdKind::Snapshot.as_code() {
            let mut bytes = snapshot.clone();
            bytes[1] = code;
            assert!(MdSnapshot::decode(&bytes).is_err());
        }
    }
}

// ---------------------------------------------------------------------------
// Symbol contract
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn arbitrary_wire_symbols_never_panic_and_round_trip(bytes in any::<[u8; 16]>()) {
        if let Ok(symbol) = Symbol::from_wire(bytes) {
            prop_assert_eq!(symbol.as_bytes(), &bytes);
            prop_assert!(!symbol.as_str().is_empty());
            prop_assert!(symbol.as_str().len() <= Symbol::WIDTH);
            prop_assert_eq!(Symbol::from_wire(*symbol.as_bytes()), Ok(symbol));
            prop_assert_eq!(Symbol::new(symbol.as_str()), Ok(symbol));
        }
    }

    #[test]
    fn arbitrary_utf8_never_panics_in_symbol_new(value in any::<String>()) {
        if let Ok(symbol) = Symbol::new(&value) {
            prop_assert_eq!(symbol.as_str(), value.as_str());
            prop_assert_eq!(Symbol::new(symbol.as_str()), Ok(symbol));
            prop_assert_eq!(Symbol::from_wire(*symbol.as_bytes()), Ok(symbol));
        }
    }

    #[test]
    fn arbitrary_byte_strings_never_panic_in_symbol_new(
        bytes in prop::collection::vec(any::<u8>(), 0..=32)
    ) {
        if let Ok(text) = std::str::from_utf8(&bytes) {
            // Must be a value or an error, never a panic on invalid lengths or
            // characters.
            let _ = Symbol::new(text);
        }
    }

    #[test]
    fn accepted_symbols_round_trip(value in "[!-~]{1,16}") {
        let symbol = Symbol::new(&value).expect("printable ASCII fits");
        prop_assert_eq!(symbol.as_str(), value.as_str());
        prop_assert_eq!(Symbol::new(symbol.as_str()), Ok(symbol));
        prop_assert_eq!(Symbol::from_wire(*symbol.as_bytes()), Ok(symbol));
    }
}

#[test]
fn symbol_rules_are_explicit() {
    // Length is measured in bytes and must be 1..=16.
    assert!(Symbol::new("").is_err());
    assert!(Symbol::new("A").is_ok());
    assert!(Symbol::new("ABCDEFGHIJKLMNOP").is_ok());
    assert!(Symbol::new("ABCDEFGHIJKLMNOPQ").is_err());

    // Printable ASCII 0x21..=0x7e only: no spaces, NUL, DEL, or non-ASCII.
    assert!(Symbol::new("A B").is_err());
    assert!(Symbol::new("\0").is_err());
    assert!(Symbol::new("\u{7f}").is_err());
    assert!(Symbol::new("é").is_err());

    // Wire form is NUL padded with no interior NUL.
    assert!(Symbol::from_wire([0u8; 16]).is_err());
    let mut interior_nul = [0u8; 16];
    interior_nul[0] = b'A';
    interior_nul[2] = b'B';
    assert!(Symbol::from_wire(interior_nul).is_err());

    let mut non_ascii = [0u8; 16];
    non_ascii[0] = 0x80;
    assert!(Symbol::from_wire(non_ascii).is_err());

    // A full 16-byte field needs no padding and is valid.
    assert!(Symbol::from_wire([b'A'; 16]).is_ok());
}

// ---------------------------------------------------------------------------
// Status / session helper contract
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn set_session_flags_are_validated(flags in any::<u16>()) {
        let mut command = Command::set_session(symbol(), SessionState::Open, ParticipantId(1), 0);
        command.flags = flags;
        let result = command.session_state();
        let high_bits_clear = flags & 0xff00 == 0;
        let low_code_valid = (flags & 0x00ff) <= 2;
        prop_assert_eq!(result.is_ok(), high_bits_clear && low_code_valid);
        if let Ok(session) = result {
            prop_assert_eq!(u16::from(session.as_code()), flags);
        }
    }

    #[test]
    fn trading_status_flags_are_bounded(flags in any::<u8>()) {
        match TradingStatus::from_md_flags(flags) {
            Ok(status) => {
                // Only the halted bit and the two session bits carry meaning;
                // higher bits are reserved and ignored by this helper (the raw
                // wire field still round-trips unchanged).
                prop_assert_eq!(status.to_md_flags(), flags & 0b0000_0111);
            }
            Err(_) => {
                let session_code = (flags & MD_SESSION_MASK) >> MD_SESSION_SHIFT;
                prop_assert!(session_code > 2);
            }
        }
    }
}
