//! Codec goldens and reassembly boundary tests for the pbbp2 frame layer.

use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use lark_codex_bridge::lark::fragments::{Reassembler, ReassemblyError};
use lark_codex_bridge::lark::frame::{
    Frame, FrameHeaders, FrameMethod, Header, MessageType, header_key,
};
use lark_codex_bridge::limits::{
    LARK_FRAGMENT_MAX_IN_FLIGHT, LARK_FRAGMENT_MESSAGE_BYTES, LARK_FRAGMENT_MESSAGE_MAX_FRAGMENTS,
    LARK_FRAGMENT_TOTAL_BYTES, LARK_FRAGMENT_TTL,
};

fn header(key: &str, value: &str) -> Header {
    Header {
        key: key.to_owned(),
        value: value.to_owned(),
    }
}

fn headers(pairs: &[(&str, &str)]) -> FrameHeaders {
    FrameHeaders::new(pairs.iter().map(|(k, v)| header(k, v)).collect())
}

fn event_headers(message_id: &str, sum: u32, seq: u32) -> FrameHeaders {
    headers(&[
        (header_key::TYPE, "event"),
        (header_key::MESSAGE_ID, message_id),
        (header_key::SUM, &sum.to_string()),
        (header_key::SEQ, &seq.to_string()),
        (header_key::TRACE_ID, "tr-1"),
    ])
}

struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer lock")
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Codec goldens (hand-computed wire bytes)
// ---------------------------------------------------------------------------

#[test]
fn ping_frame_matches_hand_computed_golden() {
    // The reference's proto2-style encoder writes fields 1–4 unconditionally,
    // so the real ping frame bytes start with the zero-valued SeqID/LogID and
    // method fields; our encoder must be byte-identical.
    let expected: Vec<u8> = vec![
        0x08, 0x00, // field 1: SeqID = 0
        0x10, 0x00, // field 2: LogID = 0
        0x18, 0x07, // field 3: service = 7
        0x20, 0x00, // field 4: method = 0 (control)
        0x2A, 0x0C, // field 5: headers, 12 bytes
        0x0A, 0x04, b't', b'y', b'p', b'e', // header key
        0x12, 0x04, b'p', b'i', b'n', b'g', // header value
    ];
    let frame = Frame::ping(7);
    assert_eq!(frame.method, FrameMethod::Control.as_wire());
    let encoded = frame.encode_to_vec();
    assert_eq!(encoded, expected);
    let decoded = Frame::decode_bytes(&expected).expect("golden decodes");
    assert_eq!(decoded, frame);
    assert_eq!(decoded.frame_headers().ty(), Some(MessageType::Ping));
}

#[test]
fn single_fragment_event_frame_matches_hand_computed_golden() {
    let mut frame = Frame::ping(7);
    frame.method = FrameMethod::Data.as_wire();
    frame.headers = vec![
        header("type", "event"),
        header("message_id", "m-1"),
        header("sum", "1"),
        header("seq", "0"),
        header("trace_id", "tr-1"),
    ];
    frame.payload = Some(Bytes::from_static(b"{}"));

    let expected: Vec<u8> = vec![
        0x08, 0x00, // SeqID = 0
        0x10, 0x00, // LogID = 0
        0x18, 0x07, // service = 7
        0x20, 0x01, // method = 1 (data)
        0x2A, 0x0D, // header {type, event}: 13 bytes
        0x0A, 0x04, b't', b'y', b'p', b'e', 0x12, 0x05, b'e', b'v', b'e', b'n', b't', 0x2A,
        0x11, // header {message_id, m-1}: 17 bytes
        0x0A, 0x0A, b'm', b'e', b's', b's', b'a', b'g', b'e', b'_', b'i', b'd', 0x12, 0x03, b'm',
        b'-', b'1', //
        0x2A, 0x08, // header {sum, 1}
        0x0A, 0x03, b's', b'u', b'm', 0x12, 0x01, b'1', //
        0x2A, 0x08, // header {seq, 0}
        0x0A, 0x03, b's', b'e', b'q', 0x12, 0x01, b'0', //
        0x2A, 0x10, // header {trace_id, tr-1}: 16 bytes
        0x0A, 0x08, b't', b'r', b'a', b'c', b'e', b'_', b'i', b'd', 0x12, 0x04, b't', b'r', b'-',
        b'1', //
        0x42, 0x02, b'{', b'}', // field 8: payload
    ];
    assert_eq!(frame.encode_to_vec(), expected);
    let decoded = Frame::decode_bytes(&expected).expect("golden decodes");
    assert_eq!(decoded, frame);
    let view = decoded.frame_headers();
    assert_eq!(view.ty(), Some(MessageType::Event));
    assert_eq!(view.message_id(), Some("m-1"));
    assert_eq!(view.sum(), Some(1));
    assert_eq!(view.seq(), Some(0));
    assert_eq!(view.trace_id(), Some("tr-1"));
}

#[test]
fn round_trip_preserves_optional_fields() {
    let mut frame = Frame::ping(3);
    frame.seq_id = 41;
    frame.log_id = 42;
    frame.method = FrameMethod::Data.as_wire();
    frame.payload_encoding = Some("utf-8".to_owned());
    frame.payload_type = Some("json".to_owned());
    frame.payload = Some(Bytes::from_static(b"payload-bytes"));
    frame.log_id_new = Some("log-x".to_owned());
    let decoded = Frame::decode_bytes(&frame.encode_to_vec()).expect("round trip decodes");
    assert_eq!(decoded, frame);
}

#[test]
fn decode_tolerates_unknown_wire_fields() {
    // Append an unknown field (tag 100, varint) to a valid ping frame: the
    // decode succeeds and known fields survive; the unknown field is dropped
    // on re-encode (prost does not retain unknown fields).
    let mut bytes = Frame::ping(7).encode_to_vec();
    bytes.extend_from_slice(&[0xA0, 0x06, 0x2A]); // field 100, varint 42
    let decoded = Frame::decode_bytes(&bytes).expect("unknown fields are skipped");
    assert_eq!(decoded, Frame::ping(7));
}

#[test]
fn decode_rejects_truncated_frames() {
    let bytes = Frame::ping(7).encode_to_vec();
    let truncated = &bytes[..bytes.len() - 3];
    assert!(Frame::decode_bytes(truncated).is_err());
}

#[test]
fn frame_debug_redacts_payload_bytes() {
    let mut frame = Frame::ping(7);
    frame.payload = Some(Bytes::from_static(b"secret-payload-contents"));
    let debug = format!("{frame:?}");
    assert!(debug.contains("payload_len"));
    assert!(!debug.contains("secret-payload-contents"));
    assert!(!debug.contains("secret"));
}

#[test]
fn frame_headers_debug_redacts_handshake_msg() {
    let view = headers(&[
        (header_key::HANDSHAKE_STATUS, "403"),
        (header_key::HANDSHAKE_MSG, "server said something sensitive"),
        (header_key::HANDSHAKE_AUTHERRCODE, "1000040351"),
    ]);
    let debug = format!("{view:?}");
    assert!(debug.contains("handshake-status=403"));
    assert!(debug.contains("handshake-msg=<redacted>"));
    assert!(!debug.contains("sensitive"));
    assert_eq!(view.handshake_status(), Some("403"));
    assert!(view.has_handshake_msg());
    assert_eq!(view.handshake_autherrcode(), Some("1000040351"));
}

#[test]
fn frame_method_and_message_type_wire_mapping() {
    assert_eq!(FrameMethod::from_wire(0), Some(FrameMethod::Control));
    assert_eq!(FrameMethod::from_wire(1), Some(FrameMethod::Data));
    assert_eq!(FrameMethod::from_wire(2), None);
    assert_eq!(MessageType::parse("card"), Some(MessageType::Card));
    assert_eq!(MessageType::parse("pong"), Some(MessageType::Pong));
    assert_eq!(MessageType::parse("mystery"), None);
}

#[test]
fn fragment_warning_does_not_log_ids_or_payload_content() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = Arc::clone(&output);
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || LogBuffer(Arc::clone(&writer)))
        .with_ansi(false)
        .without_time()
        .finish();
    let message_id = "SECRET_MESSAGE_ID";
    let payload = Bytes::from_static(b"SECRET_MESSAGE_BODY_AND_TOKEN");

    tracing::subscriber::with_default(subscriber, || {
        let mut reassembler = Reassembler::new();
        let error = reassembler
            .ingest(&event_headers(message_id, 0, 0), payload, Instant::now())
            .expect_err("sum zero must be rejected");
        assert_eq!(error, ReassemblyError::OutOfRange);
    });

    let output = output.lock().expect("log buffer lock");
    let output = String::from_utf8_lossy(&output);
    assert!(output.contains("lark fragment rejected"));
    assert!(output.contains("fragment-out-of-range"));
    assert!(!output.contains(message_id));
    assert!(!output.contains("SECRET_MESSAGE_BODY_AND_TOKEN"));
    assert!(!output.contains("tr-SECRET_MESSAGE_ID"));
}

// ---------------------------------------------------------------------------
// Reassembly
// ---------------------------------------------------------------------------

#[test]
fn single_fragment_passes_through_without_caching() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    let done = reassembler
        .ingest(&event_headers("m-1", 1, 0), Bytes::from_static(b"{}"), now)
        .expect("single fragment ingests")
        .expect("single fragment completes immediately");
    assert_eq!(done.message_id, "m-1");
    assert_eq!(done.trace_id.as_deref(), Some("tr-1"));
    assert_eq!(done.payload.as_ref(), b"{}");
    assert_eq!(reassembler.in_flight(), 0);
    assert_eq!(reassembler.buffered_bytes(), 0);

    // A missing sum header also means a single fragment.
    let without_sum = headers(&[(header_key::TYPE, "event"), (header_key::MESSAGE_ID, "m-2")]);
    let done = reassembler
        .ingest(&without_sum, Bytes::from_static(b"x"), now)
        .expect("missing sum ingests")
        .expect("missing sum completes immediately");
    assert_eq!(done.payload.as_ref(), b"x");
}

#[test]
fn out_of_order_fragments_reassemble_in_seq_order() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    assert!(
        reassembler
            .ingest(&event_headers("m", 3, 1), Bytes::from_static(b"-B-"), now)
            .expect("seq 1")
            .is_none()
    );
    assert!(
        reassembler
            .ingest(&event_headers("m", 3, 2), Bytes::from_static(b"-C"), now)
            .expect("seq 2")
            .is_none()
    );
    assert_eq!(reassembler.in_flight(), 1);
    assert_eq!(reassembler.buffered_bytes(), 5);
    let done = reassembler
        .ingest(&event_headers("m", 3, 0), Bytes::from_static(b"A"), now)
        .expect("seq 0")
        .expect("message completes");
    assert_eq!(done.payload.as_ref(), b"A-B--C");
    assert_eq!(reassembler.in_flight(), 0);
    assert_eq!(reassembler.buffered_bytes(), 0);
}

#[test]
fn duplicate_fragment_is_rejected() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    reassembler
        .ingest(&event_headers("m", 2, 0), Bytes::from_static(b"a"), now)
        .expect("first fragment");
    let error = reassembler
        .ingest(&event_headers("m", 2, 0), Bytes::from_static(b"a"), now)
        .expect_err("duplicate seq is rejected");
    assert_eq!(error, ReassemblyError::Duplicate);
    // The buffered fragment is untouched and the message still completes.
    let done = reassembler
        .ingest(&event_headers("m", 2, 1), Bytes::from_static(b"b"), now)
        .expect("second fragment")
        .expect("message completes");
    assert_eq!(done.payload.as_ref(), b"ab");
}

#[test]
fn out_of_range_seq_and_sum_anomalies_are_rejected() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    // sum == 0
    assert_eq!(
        reassembler
            .ingest(&event_headers("m", 0, 0), Bytes::from_static(b"a"), now)
            .expect_err("sum 0"),
        ReassemblyError::OutOfRange
    );
    // seq >= sum
    assert_eq!(
        reassembler
            .ingest(&event_headers("m", 2, 2), Bytes::from_static(b"a"), now)
            .expect_err("seq >= sum"),
        ReassemblyError::OutOfRange
    );
    // missing seq on a multi-fragment message
    let no_seq = headers(&[
        (header_key::TYPE, "event"),
        (header_key::MESSAGE_ID, "m"),
        (header_key::SUM, "2"),
    ]);
    assert_eq!(
        reassembler
            .ingest(&no_seq, Bytes::from_static(b"a"), now)
            .expect_err("missing seq"),
        ReassemblyError::OutOfRange
    );
    // missing message_id on a multi-fragment message
    let no_id = headers(&[(header_key::SUM, "2"), (header_key::SEQ, "0")]);
    assert_eq!(
        reassembler
            .ingest(&no_id, Bytes::from_static(b"a"), now)
            .expect_err("missing message_id"),
        ReassemblyError::OutOfRange
    );
    // conflicting sum for an in-flight message
    reassembler
        .ingest(&event_headers("m", 2, 0), Bytes::from_static(b"a"), now)
        .expect("first fragment");
    assert_eq!(
        reassembler
            .ingest(&event_headers("m", 3, 1), Bytes::from_static(b"b"), now)
            .expect_err("sum conflict"),
        ReassemblyError::OutOfRange
    );
}

#[test]
fn fragment_count_bound_is_enforced() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    let too_many = u32::try_from(LARK_FRAGMENT_MESSAGE_MAX_FRAGMENTS + 1).expect("fits in u32");
    assert_eq!(
        reassembler
            .ingest(
                &event_headers("m", too_many, 0),
                Bytes::from_static(b"a"),
                now
            )
            .expect_err("sum above the fragment cap"),
        ReassemblyError::TooManyFragments
    );
    assert_eq!(reassembler.in_flight(), 0);
}

#[test]
fn in_flight_message_bound_is_enforced() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    for index in 0..LARK_FRAGMENT_MAX_IN_FLIGHT {
        let message_id = format!("m-{index}");
        reassembler
            .ingest(
                &event_headers(&message_id, 2, 0),
                Bytes::from_static(b"a"),
                now,
            )
            .expect("in-flight message accepted");
    }
    assert_eq!(
        reassembler
            .ingest(
                &event_headers("m-overflow", 2, 0),
                Bytes::from_static(b"a"),
                now
            )
            .expect_err("in-flight cap"),
        ReassemblyError::TooManyFragments
    );
}

#[test]
fn per_message_byte_bound_is_enforced() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    let first = vec![0_u8; LARK_FRAGMENT_MESSAGE_BYTES - 10];
    reassembler
        .ingest(&event_headers("m", 2, 0), Bytes::from(first), now)
        .expect("first fragment under the cap");
    assert_eq!(
        reassembler
            .ingest(&event_headers("m", 2, 1), Bytes::from(vec![0_u8; 11]), now)
            .expect_err("per-message byte cap"),
        ReassemblyError::OverBytes
    );
}

#[test]
fn oversized_single_fragment_is_rejected() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    let payload = vec![0_u8; LARK_FRAGMENT_MESSAGE_BYTES + 1];
    assert_eq!(
        reassembler
            .ingest(&event_headers("m", 1, 0), Bytes::from(payload), now)
            .expect_err("single fragment byte cap"),
        ReassemblyError::OverBytes
    );
}

#[test]
fn total_byte_bound_is_enforced_across_messages() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    // Each message buffers one fragment just under the per-message cap; the
    // total cap is 8x that, so the ninth distinct message must fail.
    let chunk = LARK_FRAGMENT_MESSAGE_BYTES - 1;
    let mut accepted = 0_usize;
    for index in 0..(LARK_FRAGMENT_TOTAL_BYTES / chunk + 2) {
        let message_id = format!("m-{index}");
        match reassembler.ingest(
            &event_headers(&message_id, 2, 0),
            Bytes::from(vec![0_u8; chunk]),
            now,
        ) {
            Ok(None) => accepted += 1,
            Err(ReassemblyError::OverBytes) => break,
            other => panic!("unexpected ingest outcome: {other:?}"),
        }
    }
    assert!(accepted >= LARK_FRAGMENT_TOTAL_BYTES / chunk);
    assert!(reassembler.buffered_bytes() <= LARK_FRAGMENT_TOTAL_BYTES);
    // After expiry the budget is usable again.
    let later = now + LARK_FRAGMENT_TTL + Duration::from_secs(1);
    assert!(
        reassembler
            .ingest(
                &event_headers("m-fresh", 2, 0),
                Bytes::from_static(b"a"),
                later
            )
            .expect("fresh message ingests after sweep")
            .is_none()
    );
}

#[test]
fn ttl_expiry_mid_sequence_rejects_late_continuation() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    reassembler
        .ingest(&event_headers("m", 2, 0), Bytes::from_static(b"a"), now)
        .expect("first fragment");
    let later = now + LARK_FRAGMENT_TTL + Duration::from_millis(1);
    assert_eq!(
        reassembler
            .ingest(&event_headers("m", 2, 1), Bytes::from_static(b"b"), later)
            .expect_err("late continuation"),
        ReassemblyError::Expired
    );
    assert_eq!(reassembler.in_flight(), 0);
    assert_eq!(reassembler.buffered_bytes(), 0);
    // The same message_id can start over cleanly after expiry.
    assert!(
        reassembler
            .ingest(&event_headers("m", 2, 0), Bytes::from_static(b"a"), later)
            .expect("restarted message")
            .is_none()
    );
}

#[test]
fn ingest_sweeps_other_expired_messages() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    reassembler
        .ingest(&event_headers("old", 2, 0), Bytes::from_static(b"a"), now)
        .expect("old message");
    reassembler
        .ingest(&event_headers("fresh", 2, 0), Bytes::from_static(b"b"), now)
        .expect("fresh message");
    let later = now + LARK_FRAGMENT_TTL + Duration::from_millis(1);
    // A brand-new message id sweeps both stale entries on ingest.
    reassembler
        .ingest(&event_headers("new", 2, 0), Bytes::from_static(b"c"), later)
        .expect("new message triggers the sweep");
    assert_eq!(reassembler.in_flight(), 1);
    assert_eq!(reassembler.buffered_bytes(), 1);
}

#[test]
fn unparsable_sum_header_is_rejected_instead_of_passing_through() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    let unparsable = headers(&[
        (header_key::TYPE, "event"),
        (header_key::MESSAGE_ID, "m"),
        (header_key::SUM, "not-a-number"),
        (header_key::SEQ, "0"),
    ]);
    assert_eq!(
        reassembler
            .ingest(&unparsable, Bytes::from_static(b"a"), now)
            .expect_err("unparsable sum is a protocol anomaly"),
        ReassemblyError::OutOfRange
    );
}

#[test]
fn sweep_runs_even_when_the_fragment_is_rejected() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    reassembler
        .ingest(&event_headers("old", 2, 0), Bytes::from_static(b"a"), now)
        .expect("old message");
    assert_eq!(reassembler.in_flight(), 1);
    let later = now + LARK_FRAGMENT_TTL + Duration::from_millis(1);
    // A malformed fragment is rejected, but the sweep must still run first.
    assert_eq!(
        reassembler
            .ingest(
                &event_headers("junk", 0, 0),
                Bytes::from_static(b"x"),
                later
            )
            .expect_err("sum 0 rejected"),
        ReassemblyError::OutOfRange
    );
    assert_eq!(reassembler.in_flight(), 0, "stale entry swept on ingest");
    assert_eq!(reassembler.buffered_bytes(), 0);
}

#[test]
fn reassembly_debug_redacts_payload_bytes() {
    let mut reassembler = Reassembler::new();
    let now = Instant::now();
    let done = reassembler
        .ingest(
            &event_headers("m", 1, 0),
            Bytes::from_static(b"top-secret-body"),
            now,
        )
        .expect("ingest")
        .expect("complete");
    let debug = format!("{done:?}");
    assert!(debug.contains("payload_len"));
    assert!(!debug.contains("top-secret-body"));
}

#[test]
fn golden_fixture_documents_frame_shapes() {
    // The fixture is documentation; parse it to keep it honest JSON.
    let text = include_str!("fixtures/lark/frame_data_fragment.json");
    let value: serde_json::Value = serde_json::from_str(text).expect("fixture is valid JSON");
    assert!(value.get("single_fragment_event_frame").is_some());
    assert!(value.get("multi_fragment_event_frame").is_some());
}
