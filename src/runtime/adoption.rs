//! Fail-closed capability boundary for persisted-thread adoption.
//!
//! The supported Codex app-server contract can acquire a writer with
//! `thread/resume`, but it has no verified operation that releases that writer
//! while the owning app-server remains alive. Dropping the bridge's local
//! subscription or route is not a remote ownership release. Until both sides
//! of that lifecycle are authoritative, discovery, adoption, and release stay
//! behind this dependency-free gate and cannot reach an RPC client or store.

use serde::{Serialize, Serializer};

/// Persisted-thread control operations that require reliable writer release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadAdoptionOperation {
    /// List redacted persisted-thread candidates.
    Discover,
    /// Acquire a selected persisted thread after explicit handoff.
    Adopt,
    /// Release bridge ownership without changing the remote thread lifecycle.
    Release,
}

/// Stable availability classification exposed to operators and command handlers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadAdoptionAvailability {
    /// The app-server offers no verified writer-release operation.
    UnavailableNoReliableWriterRelease,
}

impl ThreadAdoptionAvailability {
    /// Machine-readable classification used by diagnostics and durable replies.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::UnavailableNoReliableWriterRelease => "unavailable_no_reliable_writer_release",
        }
    }

    /// Whether persisted-thread adoption can pass the capability gate.
    #[must_use]
    pub const fn is_available(self) -> bool {
        match self {
            Self::UnavailableNoReliableWriterRelease => false,
        }
    }

    /// Static, path-free guidance safe to show to an authorized operator.
    #[must_use]
    pub const fn guidance(self) -> &'static str {
        match self {
            Self::UnavailableNoReliableWriterRelease => {
                "persisted-thread adoption is disabled because the supported Codex app-server cannot reliably release writer ownership; keep using bridge-created threads and see issue #8 for shared-endpoint research"
            }
        }
    }
}

impl Serialize for ThreadAdoptionAvailability {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.code())
    }
}

/// Stable fail-closed error returned before any discovery, RPC, or persistence.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ThreadAdoptionError {
    /// No safe acquire-and-release lifecycle is available.
    #[error("{availability}", availability = .0.guidance())]
    Unavailable(ThreadAdoptionAvailability),
}

/// Zero-state guard for every persisted-thread control entry point.
///
/// This type deliberately owns no app-server client, store handle, workspace,
/// or thread identifier. A disabled call therefore cannot accidentally list,
/// resume, subscribe, mutate a mapping, or terminate another client.
#[derive(Clone, Copy, Debug, Default)]
pub struct ThreadAdoptionGate;

impl ThreadAdoptionGate {
    /// Current capability classification.
    #[must_use]
    pub const fn availability(self) -> ThreadAdoptionAvailability {
        ThreadAdoptionAvailability::UnavailableNoReliableWriterRelease
    }

    /// Authorizes one persisted-thread operation, or fails before side effects.
    ///
    /// # Errors
    ///
    /// Always returns [`ThreadAdoptionError::Unavailable`] for the current
    /// supported Codex contract.
    pub const fn require(
        self,
        _operation: ThreadAdoptionOperation,
    ) -> Result<(), ThreadAdoptionError> {
        Err(ThreadAdoptionError::Unavailable(self.availability()))
    }
}
