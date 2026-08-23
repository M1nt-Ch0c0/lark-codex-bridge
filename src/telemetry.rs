//! Process-wide, stderr-only terminal tracing.
//!
//! The filter is selected before any runtime component starts. `RUST_LOG`
//! takes precedence over CLI verbosity, and malformed values fail closed with
//! a static diagnostic that never echoes environment contents.

use std::ffi::OsStr;
use std::io;

use thiserror::Error;
use tracing_subscriber::filter::filter_fn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

const APPLICATION_TARGET: &str = "lark_codex_bridge";

/// Human-readable output is the default; JSON is useful for service managers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OutputFormat {
    /// Compact terminal-oriented lines.
    #[default]
    Human,
    /// One structured JSON object per event.
    Json,
}

/// Static tracing initialization failures safe to print at the process boundary.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum InitError {
    /// `RUST_LOG` was present but could not be represented as UTF-8.
    #[error(
        "RUST_LOG must be valid UTF-8; use directives such as `info` or `lark_codex_bridge=debug`"
    )]
    NonUnicodeFilter,
    /// `RUST_LOG` was empty or did not use valid `EnvFilter` syntax.
    #[error("invalid RUST_LOG filter; use directives such as `info` or `lark_codex_bridge=debug`")]
    InvalidFilter,
    /// Another library installed a global tracing subscriber first.
    #[error("terminal tracing is already initialized")]
    AlreadyInitialized,
}

/// Identifies which operator input selected the effective filter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FilterSource {
    Verbosity,
    RustLog,
}

/// Returns the bounded default directive for one CLI verbosity count.
///
/// Only audited bridge events are enabled. Third-party dependency targets are
/// excluded by a second, non-overridable safety filter in [`init`].
#[must_use]
pub const fn default_filter(verbosity: u8) -> &'static str {
    match verbosity {
        0 => "lark_codex_bridge=warn",
        1 => "lark_codex_bridge=info",
        _ => "lark_codex_bridge=debug",
    }
}

fn resolve_filter(
    verbosity: u8,
    rust_log: Option<&OsStr>,
) -> Result<(EnvFilter, FilterSource), InitError> {
    let Some(value) = rust_log else {
        let filter = EnvFilter::try_new(default_filter(verbosity))
            .expect("static tracing directives are valid");
        return Ok((filter, FilterSource::Verbosity));
    };
    let value = value.to_str().ok_or(InitError::NonUnicodeFilter)?;
    if value
        .split(',')
        .all(|directive| directive.trim().is_empty())
    {
        return Err(InitError::InvalidFilter);
    }
    let filter = EnvFilter::try_new(value).map_err(|_| InitError::InvalidFilter)?;
    Ok((filter, FilterSource::RustLog))
}

fn is_application_target(target: &str) -> bool {
    target == APPLICATION_TARGET
        || target
            .strip_prefix(APPLICATION_TARGET)
            .is_some_and(|suffix| suffix.starts_with("::"))
}

/// Installs the process-wide terminal subscriber.
///
/// `RUST_LOG`, when present, replaces the verbosity-derived defaults for
/// audited bridge targets. A non-overridable target filter drops dependency
/// diagnostics even when a broad directive such as `trace` is requested;
/// upstream HTTP/WebSocket crates may otherwise log endpoints or payloads.
/// All formatted events are explicitly written to stderr so command stdout
/// stays machine-readable.
///
/// # Errors
///
/// Returns a static, actionable error for an invalid `RUST_LOG` value or when
/// another global subscriber was already installed.
pub fn init(verbosity: u8, format: OutputFormat) -> Result<(), InitError> {
    let (filter, source) = resolve_filter(verbosity, std::env::var_os("RUST_LOG").as_deref())?;
    let result = match format {
        OutputFormat::Human => tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(io::stderr)
                    .with_ansi(false)
                    .compact()
                    .with_filter(filter)
                    .with_filter(filter_fn(|metadata| {
                        is_application_target(metadata.target())
                    })),
            )
            .try_init(),
        OutputFormat::Json => tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(io::stderr)
                    .with_ansi(false)
                    .json()
                    .flatten_event(true)
                    .with_filter(filter)
                    .with_filter(filter_fn(|metadata| {
                        is_application_target(metadata.target())
                    })),
            )
            .try_init(),
    };
    result.map_err(|_| InitError::AlreadyInitialized)?;
    tracing::debug!(
        filter_source = ?source,
        output_format = ?format,
        "terminal tracing initialized"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    use tracing::Level;
    use tracing_subscriber::Layer;
    use tracing_subscriber::filter::filter_fn;
    use tracing_subscriber::fmt::MakeWriter;
    use tracing_subscriber::layer::SubscriberExt;

    use super::{FilterSource, InitError, default_filter, is_application_target, resolve_filter};

    #[derive(Clone, Default)]
    struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

    struct CapturedGuard(Arc<Mutex<Vec<u8>>>);

    impl Write for CapturedGuard {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("capture lock")
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> MakeWriter<'writer> for CapturedWriter {
        type Writer = CapturedGuard;

        fn make_writer(&'writer self) -> Self::Writer {
            CapturedGuard(Arc::clone(&self.0))
        }
    }

    impl CapturedWriter {
        fn output(&self) -> String {
            String::from_utf8(self.0.lock().expect("capture lock").clone())
                .expect("tracing output is UTF-8")
        }
    }

    #[test]
    fn verbosity_maps_to_bounded_default_filters() {
        assert_eq!(default_filter(0), "lark_codex_bridge=warn");
        assert_eq!(default_filter(1), "lark_codex_bridge=info");
        assert_eq!(default_filter(2), "lark_codex_bridge=debug");
        assert_eq!(default_filter(u8::MAX), "lark_codex_bridge=debug");
    }

    #[test]
    fn rust_log_replaces_the_verbosity_filter() {
        let (filter, source) =
            resolve_filter(2, Some(OsStr::new("lark_codex_bridge=trace"))).expect("filter");

        assert_eq!(source, FilterSource::RustLog);
        assert_eq!(filter.to_string(), "lark_codex_bridge=trace");
    }

    #[test]
    fn absent_rust_log_uses_the_verbosity_filter() {
        let (filter, source) = resolve_filter(1, None).expect("filter");

        assert_eq!(source, FilterSource::Verbosity);
        assert_eq!(filter.to_string(), "lark_codex_bridge=info");
    }

    #[test]
    fn invalid_filter_error_is_actionable_without_echoing_input() {
        let secret = "[secret-app-token";
        let error = resolve_filter(2, Some(OsStr::new(secret))).expect_err("invalid filter");
        let rendered = error.to_string();

        assert_eq!(error, InitError::InvalidFilter);
        assert!(rendered.contains("invalid RUST_LOG filter"));
        assert!(rendered.contains("lark_codex_bridge=debug"));
        assert!(!rendered.contains(secret));
    }

    #[test]
    fn delimiter_only_filter_is_rejected() {
        for value in [",", " , ", ", ,\t,"] {
            assert_eq!(
                resolve_filter(2, Some(OsStr::new(value))).expect_err("empty filter"),
                InitError::InvalidFilter
            );
        }
    }

    #[test]
    fn broad_filter_cannot_enable_dependency_payload_logs() {
        const SECRET_ENDPOINT: &str =
            "wss://open.feishu.cn/callback?ticket=SECRET_WEBSOCKET_TICKET";
        const SECRET_PAYLOAD: &str = "SECRET_RAW_MESSAGE_AND_MEDIA_PAYLOAD";
        let writer = CapturedWriter::default();
        let filter = tracing_subscriber::EnvFilter::try_new("trace").expect("broad filter");
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_ansi(false)
                .with_writer(writer.clone())
                .with_filter(filter)
                .with_filter(filter_fn(|metadata| {
                    is_application_target(metadata.target())
                })),
        );

        tracing::subscriber::with_default(subscriber, || {
            tracing::event!(
                target: "tungstenite::protocol",
                Level::TRACE,
                endpoint = SECRET_ENDPOINT,
                payload = SECRET_PAYLOAD,
                "unredacted dependency frame"
            );
            tracing::event!(
                target: "lark_codex_bridge::telemetry::test",
                Level::INFO,
                "audited bridge event"
            );
        });

        let output = writer.output();
        assert!(output.contains("audited bridge event"));
        assert!(!output.contains(SECRET_ENDPOINT));
        assert!(!output.contains(SECRET_PAYLOAD));
        assert!(!output.contains("unredacted dependency frame"));
    }
}
