//! Process-wide, stderr-only terminal tracing.
//!
//! The filter is selected before any runtime component starts. `RUST_LOG`
//! takes precedence over CLI verbosity, and malformed values fail closed with
//! a static diagnostic that never echoes environment contents.

use std::ffi::OsStr;
use std::io;

use thiserror::Error;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::util::SubscriberInitExt;

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
/// Dependency warnings remain visible at every level while increasingly
/// verbose bridge events are enabled only for this crate.
#[must_use]
pub const fn default_filter(verbosity: u8) -> &'static str {
    match verbosity {
        0 => "warn",
        1 => "warn,lark_codex_bridge=info",
        _ => "warn,lark_codex_bridge=debug",
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
    if value.trim().is_empty() {
        return Err(InitError::InvalidFilter);
    }
    let filter = EnvFilter::try_new(value).map_err(|_| InitError::InvalidFilter)?;
    Ok((filter, FilterSource::RustLog))
}

/// Installs the process-wide terminal subscriber.
///
/// `RUST_LOG`, when present, replaces the verbosity-derived defaults. All
/// formatted events are explicitly written to stderr so command stdout stays
/// machine-readable.
///
/// # Errors
///
/// Returns a static, actionable error for an invalid `RUST_LOG` value or when
/// another global subscriber was already installed.
pub fn init(verbosity: u8, format: OutputFormat) -> Result<(), InitError> {
    let (filter, source) = resolve_filter(verbosity, std::env::var_os("RUST_LOG").as_deref())?;
    let result = match format {
        OutputFormat::Human => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(io::stderr)
            .with_ansi(false)
            .compact()
            .finish()
            .try_init(),
        OutputFormat::Json => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(io::stderr)
            .with_ansi(false)
            .json()
            .flatten_event(true)
            .finish()
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

    use super::{FilterSource, InitError, default_filter, resolve_filter};

    #[test]
    fn verbosity_maps_to_bounded_default_filters() {
        assert_eq!(default_filter(0), "warn");
        assert_eq!(default_filter(1), "warn,lark_codex_bridge=info");
        assert_eq!(default_filter(2), "warn,lark_codex_bridge=debug");
        assert_eq!(default_filter(u8::MAX), "warn,lark_codex_bridge=debug");
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
        assert_eq!(filter.to_string(), "lark_codex_bridge=info,warn");
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
}
