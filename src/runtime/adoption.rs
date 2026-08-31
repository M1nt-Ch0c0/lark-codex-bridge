//! Backend-aware capability boundary and bounded projection contracts for
//! persisted-thread adoption.
//!
//! `thread/resume` is the acquisition attempt. The corresponding release
//! authority is not unsubscribe or a route drop: it is confirmed termination
//! and reap of the dedicated bridge-owned app-server process tree. On Linux
//! and macOS, managed stdio and sidecar backends can prove that ownership
//! domain is empty. Windows and socket-only shared external endpoints cannot,
//! so both remain fail closed for adoption.

use std::{collections::HashSet, fmt, hash::BuildHasher, path::PathBuf};

use serde::{Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::{
    codex::{
        external::CodexBackendConfig,
        types::{SortDirection, Thread, ThreadListParams, ThreadListResult, ThreadSortKey},
    },
    limits::{
        THREAD_ADOPTION_SELECTOR_MAX_BYTES, THREAD_DISCOVERY_CURSOR_MAX_BYTES,
        THREAD_DISCOVERY_MAX_PAGE_BYTES, THREAD_DISCOVERY_MAX_RESULTS,
    },
    runtime::policy::AccessPolicy,
};

const CANDIDATE_TITLE_MAX_BYTES: usize = 160;
const WORKSPACE_ALIAS_HEX_BYTES: usize = 12;
const OWNERSHIP_UNVERIFIED_NOTE: &str = "read-only thread metadata cannot prove writer ownership; close every other Desktop, CLI, or app-server before adoption";
/// Platforms where the issue #4 release authority has an independent,
/// side-effect-free process-group absence proof after the owned kill/wait.
pub const THREAD_ADOPTION_SUPPORTED_PLATFORMS: &[&str] = &["linux", "macos"];

/// Whether this build target can prove that an adopted POSIX process group is
/// empty. Windows Job-object leader wait is deliberately not treated as
/// `ACTIVE_PROCESS_ZERO` evidence.
#[must_use]
pub const fn thread_adoption_platform_supported() -> bool {
    cfg!(any(target_os = "linux", target_os = "macos"))
}

/// Read-only ownership observation attached to every discovery result.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateOwnership {
    /// `thread/list` and `thread/read` have not acquired the single writer.
    Unverified,
}

/// Bounded, owner-visible summary of one persisted-thread candidate.
#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadCandidate {
    /// Exact, explicitly selectable thread identifier.
    pub selector: String,
    /// Bounded persisted title, or a static fallback when none is safe.
    pub title: String,
    /// Non-reversible display alias for the validated workspace.
    pub workspace_alias: String,
    /// Sanitized source kind from the reviewed typed contract.
    pub source: &'static str,
    /// Last persisted update timestamp reported by Codex.
    pub updated_at: i64,
    /// The only discovery state admitted into a selectable page.
    pub observable_state: &'static str,
    /// Always unverified until authoritative `thread/resume` succeeds.
    pub ownership: CandidateOwnership,
}

impl fmt::Debug for ThreadCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ThreadCandidate")
            .field("selector_bytes", &self.selector.len())
            .field("title_bytes", &self.title.len())
            .field("workspace_alias_bytes", &self.workspace_alias.len())
            .field("source", &self.source)
            .field("updated_at", &self.updated_at)
            .field("observable_state", &self.observable_state)
            .field("ownership", &self.ownership)
            .finish()
    }
}

/// One strictly bounded persisted-thread discovery page.
#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadCandidatePage {
    pub candidates: Vec<ThreadCandidate>,
    pub next_cursor: Option<String>,
    pub ownership_note: &'static str,
}

impl fmt::Debug for ThreadCandidatePage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ThreadCandidatePage")
            .field("candidate_count", &self.candidates.len())
            .field(
                "next_cursor_bytes",
                &self.next_cursor.as_ref().map(String::len),
            )
            .finish_non_exhaustive()
    }
}

/// Static discovery/projection refusal classifications.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ThreadDiscoveryError {
    #[error("the persisted-thread cursor exceeds its byte limit")]
    CursorTooLong,
    #[error("the app-server returned too many persisted threads")]
    TooManyResults,
    #[error("the app-server returned an invalid persisted-thread identifier")]
    InvalidSelector,
    #[error("the persisted-thread discovery page exceeds its byte limit")]
    PageTooLarge,
}

/// Static pre-adoption validation refusals. No path, title, or identifier is
/// retained by the error.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CandidateValidationError {
    #[error("the selected persisted thread no longer exists in the reviewed page")]
    SelectorMismatch,
    #[error("the selected persisted thread is not an idle persisted thread")]
    NotIdlePersisted,
    #[error("the selected persisted thread workspace is not allowed")]
    WorkspaceDenied,
    #[error("the selected persisted thread is already reserved by this bridge")]
    AlreadyBound,
}

/// Canonical target facts returned only after fresh read-time validation.
#[derive(Clone, Eq, PartialEq)]
pub struct ValidatedCandidate {
    pub thread_id: String,
    pub cwd: PathBuf,
}

impl fmt::Debug for ValidatedCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedCandidate")
            .field("thread_id_bytes", &self.thread_id.len())
            .field("cwd_bytes", &self.cwd.as_os_str().len())
            .finish()
    }
}

/// Builds the only read-only `thread/list` shape accepted by adoption.
///
/// The response remains preflight-only: this request never proves or acquires
/// writer ownership.
///
/// # Errors
///
/// Returns a bounded static classification when the cursor exceeds the
/// reviewed wire limit.
pub fn discovery_params(cursor: Option<String>) -> Result<ThreadListParams, ThreadDiscoveryError> {
    if cursor
        .as_ref()
        .is_some_and(|value| value.len() > THREAD_DISCOVERY_CURSOR_MAX_BYTES)
    {
        return Err(ThreadDiscoveryError::CursorTooLong);
    }
    Ok(ThreadListParams {
        cursor,
        limit: Some(u32::try_from(THREAD_DISCOVERY_MAX_RESULTS).unwrap_or(u32::MAX)),
        sort_key: Some(ThreadSortKey::UpdatedAt),
        sort_direction: Some(SortDirection::Descending),
        archived: Some(false),
        ..ThreadListParams::default()
    })
}

/// Projects a typed `thread/list` response through workspace policy and
/// bridge-wide binding exclusions.
///
/// Non-persisted, non-selectable, denied-workspace, and already-bound rows are
/// not selectable. A fresh owner may report an active persisted thread as
/// `notLoaded`, so both `idle` and `notLoaded` remain read-only preflight
/// states. Structural limit violations reject the entire page instead of
/// trusting a partially decoded provider response.
///
/// # Errors
///
/// Returns a bounded static classification for an oversized or structurally
/// invalid provider page.
pub fn project_candidate_page<S: BuildHasher>(
    result: ThreadListResult,
    policy: &AccessPolicy,
    bridge_bound: &HashSet<String, S>,
) -> Result<ThreadCandidatePage, ThreadDiscoveryError> {
    project_candidate_page_with(result, bridge_bound, |cwd| {
        policy.validate_workspace(cwd).ok()
    })
}

fn project_candidate_page_with<S: BuildHasher>(
    result: ThreadListResult,
    bridge_bound: &HashSet<String, S>,
    mut validate_workspace: impl FnMut(&std::path::Path) -> Option<PathBuf>,
) -> Result<ThreadCandidatePage, ThreadDiscoveryError> {
    if result.data.len() > THREAD_DISCOVERY_MAX_RESULTS {
        return Err(ThreadDiscoveryError::TooManyResults);
    }
    if result
        .next_cursor
        .as_ref()
        .is_some_and(|value| value.len() > THREAD_DISCOVERY_CURSOR_MAX_BYTES)
    {
        return Err(ThreadDiscoveryError::CursorTooLong);
    }

    let mut candidates = Vec::with_capacity(result.data.len());
    for thread in result.data {
        validate_selector(&thread.id)?;
        let Some(observable_state) = selectable_preflight_state(&thread) else {
            continue;
        };
        if thread.ephemeral || bridge_bound.contains(&thread.id) {
            continue;
        }
        let Some(cwd) = validate_workspace(&thread.cwd) else {
            continue;
        };
        candidates.push(ThreadCandidate {
            selector: thread.id,
            title: candidate_title(thread.name.as_deref()),
            workspace_alias: workspace_alias(&cwd),
            source: source_label(&thread.source),
            updated_at: thread.updated_at,
            observable_state,
            ownership: CandidateOwnership::Unverified,
        });
    }

    let page = ThreadCandidatePage {
        candidates,
        next_cursor: result.next_cursor,
        ownership_note: OWNERSHIP_UNVERIFIED_NOTE,
    };
    let encoded = serde_json::to_vec(&page).map_err(|_| ThreadDiscoveryError::PageTooLarge)?;
    if encoded.len() > THREAD_DISCOVERY_MAX_PAGE_BYTES {
        return Err(ThreadDiscoveryError::PageTooLarge);
    }
    Ok(page)
}

/// Revalidates one exact `thread/read` result immediately before acquisition.
/// A `notLoaded` read is accepted only because the coordinator first requires
/// a fresh exact match in the same bounded `archived: false` discovery page.
/// This check is intentionally insufficient on its own: callers must still
/// acquire ownership with `thread/resume` before writing a scope mapping.
///
/// # Errors
///
/// Returns a static classification when the exact target, persisted idle
/// state, workspace policy, or bridge-wide uniqueness check no longer holds.
pub fn validate_candidate_for_resume<S: BuildHasher>(
    thread: &Thread,
    selector: &str,
    policy: &AccessPolicy,
    bridge_bound: &HashSet<String, S>,
) -> Result<ValidatedCandidate, CandidateValidationError> {
    if thread.id != selector || validate_selector(selector).is_err() {
        return Err(CandidateValidationError::SelectorMismatch);
    }
    if thread.ephemeral || selectable_preflight_state(thread).is_none() {
        return Err(CandidateValidationError::NotIdlePersisted);
    }
    if bridge_bound.contains(selector) {
        return Err(CandidateValidationError::AlreadyBound);
    }
    let cwd = policy
        .validate_workspace(&thread.cwd)
        .map_err(|_| CandidateValidationError::WorkspaceDenied)?;
    Ok(ValidatedCandidate {
        thread_id: thread.id.clone(),
        cwd,
    })
}

fn validate_selector(selector: &str) -> Result<(), ThreadDiscoveryError> {
    if selector.is_empty()
        || selector.len() > THREAD_ADOPTION_SELECTOR_MAX_BYTES
        || selector.bytes().any(|byte| byte.is_ascii_control())
    {
        Err(ThreadDiscoveryError::InvalidSelector)
    } else {
        Ok(())
    }
}

fn selectable_preflight_state(thread: &Thread) -> Option<&'static str> {
    match thread
        .status
        .get("type")
        .and_then(serde_json::Value::as_str)
    {
        Some("idle") => Some("idle_preflight_only"),
        Some("notLoaded") => Some("not_loaded_preflight_only"),
        _ => None,
    }
}

fn candidate_title(name: Option<&str>) -> String {
    let Some(name) = name
        .map(str::trim)
        .filter(|name| !name.is_empty() && !name.chars().any(char::is_control))
    else {
        return "Untitled persisted thread".to_owned();
    };
    if name.len() <= CANDIDATE_TITLE_MAX_BYTES {
        return name.to_owned();
    }
    let mut end = CANDIDATE_TITLE_MAX_BYTES.saturating_sub(3).min(name.len());
    while !name.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    format!("{}...", &name[..end])
}

fn source_label(source: &serde_json::Value) -> &'static str {
    let value = source
        .as_str()
        .or_else(|| source.get("type").and_then(serde_json::Value::as_str));
    match value {
        Some("cli") => "cli",
        Some("vscode") => "vscode",
        Some("exec") => "exec",
        Some("appServer") => "app_server",
        Some(
            "subAgent"
            | "subAgentReview"
            | "subAgentCompact"
            | "subAgentThreadSpawn"
            | "subAgentOther",
        ) => "subagent",
        _ => "unknown",
    }
}

fn workspace_alias(cwd: &std::path::Path) -> String {
    let mut digest = Sha256::new();
    digest.update(b"lark-codex-thread-workspace-v1\0");
    digest.update(cwd.as_os_str().as_encoded_bytes());
    let digest = digest.finalize();
    let mut alias = String::with_capacity(3 + WORKSPACE_ALIAS_HEX_BYTES * 2);
    alias.push_str("ws-");
    for byte in digest.iter().take(WORKSPACE_ALIAS_HEX_BYTES) {
        use fmt::Write as _;
        write!(&mut alias, "{byte:02x}").expect("writing to String cannot fail");
    }
    alias
}

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

/// Process-ownership shape used by persisted-thread adoption.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ThreadAdoptionBackend {
    /// The bridge starts and reaps a native Codex app-server process tree.
    #[serde(rename = "spawned_stdio")]
    ManagedStdio,
    /// The bridge starts and reaps a sidecar plus its Codex process tree.
    #[serde(rename = "protocol_sidecar")]
    ManagedSidecar,
    /// The bridge owns a socket only; the remote server process is shared.
    #[serde(rename = "external_endpoint")]
    ExternalEndpoint,
}

impl ThreadAdoptionBackend {
    /// Stable configuration spelling used by diagnostics.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::ManagedStdio => "spawned_stdio",
            Self::ManagedSidecar => "protocol_sidecar",
            Self::ExternalEndpoint => "external_endpoint",
        }
    }
}

impl From<&CodexBackendConfig> for ThreadAdoptionBackend {
    fn from(backend: &CodexBackendConfig) -> Self {
        match backend {
            CodexBackendConfig::SpawnedStdio { .. } => Self::ManagedStdio,
            CodexBackendConfig::ProtocolSidecar { .. } => Self::ManagedSidecar,
            CodexBackendConfig::ExternalEndpoint { .. } => Self::ExternalEndpoint,
        }
    }
}

/// Stable availability classification exposed to operators and command handlers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadAdoptionAvailability {
    /// A dedicated bridge-owned process tree is the writer-release authority.
    AvailableDedicatedProcessOwnership,
    /// This platform cannot prove the owned process tree is empty after reap.
    UnavailablePlatformProcessTreeProof,
    /// A socket-only shared endpoint cannot prove or perform process release.
    UnavailableSharedExternalEndpoint,
}

impl ThreadAdoptionAvailability {
    /// Machine-readable classification used by diagnostics and durable replies.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::AvailableDedicatedProcessOwnership => "available_dedicated_process_ownership",
            Self::UnavailablePlatformProcessTreeProof => "unavailable_platform_process_tree_proof",
            Self::UnavailableSharedExternalEndpoint => "unavailable_shared_external_endpoint",
        }
    }

    /// Whether persisted-thread adoption can pass the capability gate.
    #[must_use]
    pub const fn is_available(self) -> bool {
        match self {
            Self::AvailableDedicatedProcessOwnership => true,
            Self::UnavailablePlatformProcessTreeProof | Self::UnavailableSharedExternalEndpoint => {
                false
            }
        }
    }

    /// Stable release authority, when this availability is enabled.
    #[must_use]
    pub const fn release_authority(self) -> Option<&'static str> {
        match self {
            Self::AvailableDedicatedProcessOwnership => Some("dedicated_process_tree_reap"),
            Self::UnavailablePlatformProcessTreeProof | Self::UnavailableSharedExternalEndpoint => {
                None
            }
        }
    }

    /// Static, path-free guidance safe to show to an authorized operator.
    #[must_use]
    pub const fn guidance(self) -> &'static str {
        match self {
            Self::AvailableDedicatedProcessOwnership => {
                "persisted-thread adoption is available only through an explicit handoff and a dedicated bridge-owned local app-server; release completes after that process tree is reaped"
            }
            Self::UnavailablePlatformProcessTreeProof => {
                "persisted-thread adoption is available only on Linux and macOS because this platform cannot prove that the owned process tree is empty after termination"
            }
            Self::UnavailableSharedExternalEndpoint => {
                "persisted-thread adoption is unavailable for a shared external endpoint because the bridge cannot reap its app-server process; use a managed local backend or see issue #8 for shared-endpoint coordination"
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

/// Backend-aware guard for every persisted-thread control entry point.
///
/// The gate owns no client, store, workspace, or identifier. It only proves
/// that the selected backend gives the bridge a dedicated process tree whose
/// confirmed reap can serve as writer-release authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ThreadAdoptionGate {
    backend: ThreadAdoptionBackend,
}

impl Default for ThreadAdoptionGate {
    fn default() -> Self {
        Self::managed_stdio()
    }
}

impl ThreadAdoptionGate {
    /// Gate for the native process-owning backend.
    #[must_use]
    pub const fn managed_stdio() -> Self {
        Self {
            backend: ThreadAdoptionBackend::ManagedStdio,
        }
    }

    /// Gate for the bridge-owned protocol sidecar process tree.
    #[must_use]
    pub const fn managed_sidecar() -> Self {
        Self {
            backend: ThreadAdoptionBackend::ManagedSidecar,
        }
    }

    /// Fail-closed gate for a socket-only shared endpoint.
    #[must_use]
    pub const fn external_endpoint() -> Self {
        Self {
            backend: ThreadAdoptionBackend::ExternalEndpoint,
        }
    }

    /// Builds the gate from the exhaustive runtime backend choice.
    #[must_use]
    pub fn for_backend(backend: &CodexBackendConfig) -> Self {
        Self {
            backend: ThreadAdoptionBackend::from(backend),
        }
    }

    /// Backend ownership shape checked by this gate.
    #[must_use]
    pub const fn backend(self) -> ThreadAdoptionBackend {
        self.backend
    }

    /// Current capability classification.
    #[must_use]
    pub const fn availability(self) -> ThreadAdoptionAvailability {
        match self.backend {
            ThreadAdoptionBackend::ManagedStdio | ThreadAdoptionBackend::ManagedSidecar => {
                if thread_adoption_platform_supported() {
                    ThreadAdoptionAvailability::AvailableDedicatedProcessOwnership
                } else {
                    ThreadAdoptionAvailability::UnavailablePlatformProcessTreeProof
                }
            }
            ThreadAdoptionBackend::ExternalEndpoint => {
                ThreadAdoptionAvailability::UnavailableSharedExternalEndpoint
            }
        }
    }

    /// Authorizes one persisted-thread operation, or fails before side effects.
    ///
    /// # Errors
    ///
    /// Shared external endpoints always fail before side effects. Managed
    /// local backends pass this capability boundary; callers must still apply
    /// actor serialization, policy, store-saga, and fresh RPC checks.
    pub const fn require(
        self,
        _operation: ThreadAdoptionOperation,
    ) -> Result<(), ThreadAdoptionError> {
        let availability = self.availability();
        if availability.is_available() {
            Ok(())
        } else {
            Err(ThreadAdoptionError::Unavailable(availability))
        }
    }
}

#[cfg(test)]
mod projection_tests {
    use std::{collections::HashSet, path::Path};

    use serde_json::json;

    use super::*;

    fn thread(id: &str, cwd: &Path, status: &str) -> Thread {
        serde_json::from_value(json!({
            "id": id,
            "sessionId": id,
            "preview": "private preview must never be projected",
            "modelProvider": "openai",
            "createdAt": 1_786_478_400_i64,
            "updatedAt": 1_786_478_500_i64,
            "status": {"type": status},
            "ephemeral": false,
            "turns": [],
            "source": "appServer",
            "cliVersion": "0.149.0",
            "cwd": cwd,
            "name": "Reviewed title"
        }))
        .expect("typed thread fixture")
    }

    #[test]
    fn discovery_request_and_response_enforce_both_bounds() {
        let params = discovery_params(Some("next".to_owned())).expect("bounded cursor");
        assert_eq!(params.limit, Some(20));
        assert_eq!(params.archived, Some(false));
        assert_eq!(params.sort_key, Some(ThreadSortKey::UpdatedAt));
        assert_eq!(params.sort_direction, Some(SortDirection::Descending));
        assert!(matches!(
            discovery_params(Some("x".repeat(THREAD_DISCOVERY_CURSOR_MAX_BYTES + 1))),
            Err(ThreadDiscoveryError::CursorTooLong)
        ));

        let cwd = PathBuf::from("/validated/workspace");
        let too_many = ThreadListResult {
            data: (0..=THREAD_DISCOVERY_MAX_RESULTS)
                .map(|index| thread(&format!("thread-{index}"), &cwd, "idle"))
                .collect(),
            next_cursor: None,
            backwards_cursor: None,
        };
        assert!(matches!(
            project_candidate_page_with(too_many, &HashSet::new(), |path| {
                Some(path.to_path_buf())
            }),
            Err(ThreadDiscoveryError::TooManyResults)
        ));
    }

    #[test]
    fn projection_is_preflight_only_bounded_and_redacted_in_debug() {
        let cwd = PathBuf::from("/validated/private-customer-workspace");
        let mut active = thread("active-thread", &cwd, "active");
        active.name = Some("active title".to_owned());
        let mut ephemeral = thread("ephemeral-thread", &cwd, "idle");
        ephemeral.ephemeral = true;
        let bound = HashSet::from(["bound-thread".to_owned()]);
        let result = ThreadListResult {
            data: vec![
                thread("selectable-thread", &cwd, "idle"),
                thread("bound-thread", &cwd, "idle"),
                active,
                ephemeral,
            ],
            next_cursor: Some("opaque-cursor".to_owned()),
            backwards_cursor: None,
        };
        let page = project_candidate_page_with(result, &bound, |path| Some(path.to_path_buf()))
            .expect("bounded projection");
        assert_eq!(page.candidates.len(), 1);
        assert_eq!(page.candidates[0].selector, "selectable-thread");
        assert_eq!(page.candidates[0].ownership, CandidateOwnership::Unverified);
        assert_eq!(page.candidates[0].observable_state, "idle_preflight_only");
        assert!(
            page.ownership_note
                .contains("cannot prove writer ownership")
        );
        let encoded = serde_json::to_vec(&page).expect("page JSON");
        assert!(encoded.len() <= THREAD_DISCOVERY_MAX_PAGE_BYTES);
        let debug = format!("{page:?}");
        assert!(!debug.contains("selectable-thread"));
        assert!(!debug.contains("private-customer-workspace"));
        assert!(!debug.contains("Reviewed title"));
    }

    #[test]
    fn titles_are_safely_bounded_and_raw_preview_is_never_used() {
        let cwd = PathBuf::from("/validated/workspace");
        let mut candidate = thread("thread-one", &cwd, "idle");
        candidate.name = Some("界".repeat(CANDIDATE_TITLE_MAX_BYTES));
        let result = ThreadListResult {
            data: vec![candidate],
            next_cursor: None,
            backwards_cursor: None,
        };
        let page =
            project_candidate_page_with(result, &HashSet::new(), |path| Some(path.to_path_buf()))
                .expect("candidate page");
        assert!(page.candidates[0].title.len() <= CANDIDATE_TITLE_MAX_BYTES);
        assert!(!page.candidates[0].title.contains("private preview"));
    }

    #[test]
    fn fresh_validation_rejects_target_drift_before_resume() {
        let temporary = tempfile::tempdir().expect("temporary root");
        let home = temporary.path().join("home");
        let workspace = temporary.path().join("workspace");
        std::fs::create_dir_all(&home).expect("home");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let roots =
            crate::runtime::policy::PlatformRoots::new(&home, Vec::new(), Vec::new(), Vec::new())
                .expect("platform roots");
        let mut config = crate::config::BridgeConfig {
            owners: vec!["owner".to_owned()],
            ..crate::config::BridgeConfig::default()
        };
        config.workspace.allow_roots = vec![workspace.clone()];
        let policy = AccessPolicy::with_platform_roots(&config, &roots).expect("policy");
        let candidate = thread("thread-one", &workspace, "idle");

        assert!(matches!(
            validate_candidate_for_resume(&candidate, "different-thread", &policy, &HashSet::new()),
            Err(CandidateValidationError::SelectorMismatch)
        ));
        assert!(matches!(
            validate_candidate_for_resume(
                &candidate,
                "thread-one",
                &policy,
                &HashSet::from(["thread-one".to_owned()])
            ),
            Err(CandidateValidationError::AlreadyBound)
        ));
        let validated =
            validate_candidate_for_resume(&candidate, "thread-one", &policy, &HashSet::new())
                .expect("fresh valid target");
        assert_eq!(validated.cwd, std::fs::canonicalize(workspace).unwrap());
    }
}
