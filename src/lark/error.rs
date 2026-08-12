//! Classified error taxonomy shared by the Lark token, API, and transport
//! layers.

use std::fmt;

/// Coarse retry classification for [`LarkError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LarkErrorKind {
    /// Bad credentials or a forbidden app; retrying will not help.
    PermanentAuth,
    /// Network, timeout, rate limit, or transient server failure.
    Retryable,
    /// The peer (or a local file) violated the expected protocol shape.
    ProtocolViolation,
    /// A configured count/byte/time bound was hit.
    Exhausted,
}

/// Lark client failures with explicit retry semantics.
///
/// Server-provided messages, App Secrets, tenant tokens, and payload content
/// are deliberately discarded: errors carry only static contexts, numeric
/// codes, and sizes.
#[derive(Clone, PartialEq, Eq)]
pub enum LarkError {
    /// Bad credentials or forbidden access; do not retry.
    PermanentAuth {
        /// Static description of the operation that failed.
        context: &'static str,
        /// HTTP status or Lark `code` when one was provided.
        code: Option<i64>,
    },
    /// Transient failure; retrying with backoff is reasonable.
    Retryable {
        /// Static description of the operation that failed.
        context: &'static str,
        /// HTTP status or Lark `code` when one was provided.
        code: Option<i64>,
    },
    /// Malformed response, file, or frame; the peer broke the contract.
    ProtocolViolation {
        /// Static description of what was malformed.
        context: &'static str,
    },
    /// A configured bound was hit.
    Exhausted {
        /// Static description of the bound.
        context: &'static str,
        /// The configured limit that was exceeded.
        limit: u64,
    },
}

impl LarkError {
    /// Builds a [`LarkError::PermanentAuth`] without a code.
    #[must_use]
    pub fn permanent_auth(context: &'static str) -> Self {
        Self::PermanentAuth {
            context,
            code: None,
        }
    }

    /// Builds a [`LarkError::Retryable`] without a code.
    #[must_use]
    pub fn retryable(context: &'static str) -> Self {
        Self::Retryable {
            context,
            code: None,
        }
    }

    /// Builds a [`LarkError::ProtocolViolation`].
    #[must_use]
    pub fn protocol(context: &'static str) -> Self {
        Self::ProtocolViolation { context }
    }

    /// Builds a [`LarkError::Exhausted`] with the exceeded limit.
    #[must_use]
    pub fn exhausted(context: &'static str, limit: u64) -> Self {
        Self::Exhausted { context, limit }
    }

    /// Returns the coarse retry classification of this error.
    #[must_use]
    pub fn kind(&self) -> LarkErrorKind {
        match self {
            Self::PermanentAuth { .. } => LarkErrorKind::PermanentAuth,
            Self::Retryable { .. } => LarkErrorKind::Retryable,
            Self::ProtocolViolation { .. } => LarkErrorKind::ProtocolViolation,
            Self::Exhausted { .. } => LarkErrorKind::Exhausted,
        }
    }
}

impl fmt::Display for LarkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = match self {
            Self::PermanentAuth { context, code } => {
                write!(
                    formatter,
                    "permanent Lark authentication failure while {context}"
                )?;
                *code
            }
            Self::Retryable { context, code } => {
                write!(formatter, "retryable Lark failure while {context}")?;
                *code
            }
            Self::ProtocolViolation { context } => {
                return write!(formatter, "Lark protocol violation: {context}");
            }
            Self::Exhausted { context, limit } => {
                return write!(formatter, "Lark bound exhausted: {context} (limit {limit})");
            }
        };
        if let Some(code) = code {
            write!(formatter, " (code {code})")?;
        }
        Ok(())
    }
}

impl fmt::Debug for LarkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl std::error::Error for LarkError {}
