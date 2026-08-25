//! Process-isolated tracing regression tests for the Lark fragment reassembler.
//!
//! This is intentionally a separate integration-test binary. Tracing callsite
//! interest is cached process-wide, so capture assertions must not race other
//! tests invoking the same production callsite under a different dispatcher.

use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use lark_codex_bridge::lark::fragments::{Reassembler, ReassemblyError};
use lark_codex_bridge::lark::frame::{FrameHeaders, Header, header_key};

const SECRET_MESSAGE_ID: &str = "SECRET_FRAGMENT_MESSAGE_ID";
const SECRET_TRACE_ID: &str = "SECRET_FRAGMENT_TRACE_ID";
const SECRET_PAYLOAD: &[u8] = b"SECRET_FRAGMENT_BODY_AND_TOKEN";

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

fn secret_event_headers() -> FrameHeaders {
    FrameHeaders::new(
        [
            (header_key::TYPE, "event"),
            (header_key::MESSAGE_ID, SECRET_MESSAGE_ID),
            (header_key::SUM, "0"),
            (header_key::SEQ, "0"),
            (header_key::TRACE_ID, SECRET_TRACE_ID),
        ]
        .into_iter()
        .map(|(key, value)| Header {
            key: key.to_owned(),
            value: value.to_owned(),
        })
        .collect(),
    )
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

    tracing::subscriber::with_default(subscriber, || {
        let mut reassembler = Reassembler::new();
        let error = reassembler
            .ingest(
                &secret_event_headers(),
                Bytes::from_static(SECRET_PAYLOAD),
                Instant::now(),
            )
            .expect_err("sum zero must be rejected");
        assert_eq!(error, ReassemblyError::OutOfRange);
    });

    let output = output.lock().expect("log buffer lock");
    let output = String::from_utf8_lossy(&output);
    assert!(output.contains("lark fragment rejected"));
    assert!(output.contains("fragment-out-of-range"));
    assert!(!output.contains(SECRET_MESSAGE_ID));
    assert!(!output.contains(SECRET_TRACE_ID));
    assert!(!output.contains(String::from_utf8_lossy(SECRET_PAYLOAD).as_ref()));
}
