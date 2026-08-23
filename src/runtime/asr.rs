//! Local speech-to-text sidecar used for Feishu/Lark audio parts.
//!
//! The bridge never links ONNX or Python ASR. A configured external command
//! receives a 16 kHz WAV path as its last argument and must print a transcript
//! on stdout. ffmpeg is used only to decode Feishu Opus/Ogg into that WAV.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, SystemTime};

use command_group::{AsyncCommandGroup, AsyncGroupChild};
use tempfile::{Builder as TempDirBuilder, TempDir};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::AsrSection;
use crate::lark::normalize::normalize_transcript;
use crate::limits::{
    ASR_ABSOLUTE_MAX_DURATION_MS, ASR_DECODED_PCM_BYTES_PER_SECOND, ASR_DECODED_WAV_MAX_BYTES,
    ASR_FFMPEG_TIMEOUT, ASR_SIDECAR_TIMEOUT,
};

const ASR_PRIVATE_ROOT: &str = "lark-codex-bridge-asr";
const ASR_TEMP_PREFIX: &str = "workspace-";
const ASR_TEMP_MARKER: &str = ".lark-codex-bridge-asr-v1";
const ASR_TEMP_MARKER_CONTENTS: &[u8] = b"lark-codex-bridge private ASR workspace v1\n";
const ASR_QUARANTINE_PREFIX: &str = ".quarantine-";
const ASR_CLAIM_PREFIX: &str = ".cleanup-claim-";
const ASR_STALE_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const ASR_STALE_SCAN_LIMIT: usize = 256;
/// Runtime cadence for retrying cleanup of stale, bridge-owned workspaces.
pub(crate) const ASR_STALE_SWEEP_INTERVAL: Duration = Duration::from_secs(15 * 60);
const ASR_CLEANUP_ATTEMPTS: usize = 20;
const ASR_CLEANUP_RETRY: Duration = Duration::from_millis(50);
const ASR_PROCESS_POLL: Duration = Duration::from_millis(10);

/// Why local transcription could not produce text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsrError {
    /// No sidecar command is configured.
    SidecarMissing,
    /// Declared or decoded duration exceeds [`AsrSection::max_duration_ms`].
    TooLong,
    /// ffmpeg could not decode the inbound audio.
    UnsupportedCodec,
    /// Sidecar could not be spawned or exited unsuccessfully.
    SidecarFailed,
    /// Sidecar succeeded but stdout was empty after trimming.
    EmptyTranscript,
    /// Sidecar stdout exceeded the configured transcript byte limit.
    TranscriptTooLarge,
    /// A supplied inbound transcript was malformed.
    InvalidTranscript,
    /// A valid live-delivery transcript was intentionally not persisted and
    /// is no longer available after recovery.
    TranscriptUnavailable,
    /// Downloaded audio exceeded the attachment byte cap.
    Oversize,
    /// The owning turn was cancelled while ASR work was active.
    Cancelled,
    /// A private temporary workspace could not be created.
    TemporaryStorage,
}

impl AsrError {
    /// Stable tool-result error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::SidecarMissing => "sidecar_missing",
            Self::TooLong => "too_long",
            Self::UnsupportedCodec => "unsupported_codec",
            Self::SidecarFailed => "sidecar_failed",
            Self::EmptyTranscript => "empty_transcript",
            Self::TranscriptTooLarge => "transcript_too_large",
            Self::InvalidTranscript => "invalid_transcript",
            Self::TranscriptUnavailable => "transcript_unavailable",
            Self::Oversize => "oversize",
            Self::Cancelled => "cancelled",
            Self::TemporaryStorage => "temporary_storage_failed",
        }
    }

    /// Operator-readable, non-leaky explanation.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::SidecarMissing => "local ASR sidecar is not configured",
            Self::TooLong => "audio duration exceeds the configured limit",
            Self::UnsupportedCodec => "audio could not be decoded",
            Self::SidecarFailed => "local ASR sidecar failed",
            Self::EmptyTranscript => "local ASR sidecar produced an empty transcript",
            Self::TranscriptTooLarge => "local ASR transcript exceeds the configured limit",
            Self::InvalidTranscript => "inbound audio transcript is invalid",
            Self::TranscriptUnavailable => {
                "inbound audio transcript is unavailable after durable recovery"
            }
            Self::Oversize => "audio is too large to transcribe",
            Self::Cancelled => "audio transcription was cancelled",
            Self::TemporaryStorage => "private audio workspace is unavailable",
        }
    }
}

/// Source of a transcript returned to Codex.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TranscriptSource {
    /// Client-supplied recognition text from the inbound Feishu/Lark payload.
    Inbound,
    /// Text produced by the local sidecar.
    Sidecar,
}

impl TranscriptSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Sidecar => "sidecar",
        }
    }
}

/// Decodes `input` to 16 kHz mono WAV and runs the configured sidecar.
///
/// # Errors
///
/// Returns a classified [`AsrError`]. Paths and stdout are never included.
pub async fn transcribe_file(
    config: &AsrSection,
    input: &Path,
    duration_ms: Option<u64>,
) -> Result<String, AsrError> {
    let turn_cancellation = CancellationToken::new();
    let shutdown = CancellationToken::new();
    transcribe_file_cancellable(config, input, duration_ms, &turn_cancellation, &shutdown).await
}

/// Process-local stale-workspace traversal retained across periodic ticks.
///
/// Each round advances one live directory iterator by at most the configured
/// entry budget. A restart discards only traversal progress; ownership is
/// revalidated independently before every cleanup attempt.
pub(crate) struct StaleWorkspaceSweeper {
    root: PathBuf,
    entries: Option<fs::ReadDir>,
}

#[derive(Debug, Default, Eq, PartialEq)]
struct SweepRound {
    read_attempts: usize,
    metadata_attempts: usize,
    cleanup_attempts: usize,
    cycle_complete: bool,
}

impl StaleWorkspaceSweeper {
    /// Creates the process-local traversal without touching storage.
    pub(crate) fn for_private_root() -> Self {
        Self::new(private_root())
    }

    fn new(root: PathBuf) -> Self {
        Self {
            root,
            entries: None,
        }
    }

    /// Verifies storage and performs one hard-bounded startup or periodic
    /// cleanup round.
    pub(crate) fn sweep_once(&mut self) -> Result<(), AsrError> {
        self.sweep_round(ASR_STALE_AGE, ASR_STALE_SCAN_LIMIT)
            .map(|_| ())
    }

    fn sweep_round(
        &mut self,
        stale_age: Duration,
        scan_limit: usize,
    ) -> Result<SweepRound, AsrError> {
        let mut round = SweepRound::default();
        if scan_limit == 0 {
            return Ok(round);
        }
        if ensure_private_directory(&self.root).is_err() {
            self.entries = None;
            return Err(AsrError::TemporaryStorage);
        }
        if self.entries.is_none() {
            self.entries = Some(fs::read_dir(&self.root).map_err(|_| AsrError::TemporaryStorage)?);
        }

        let now = SystemTime::now();
        for _ in 0..scan_limit {
            round.read_attempts += 1;
            let entry = match self
                .entries
                .as_mut()
                .expect("directory iterator exists during a sweep cycle")
                .next()
            {
                Some(Ok(entry)) => entry,
                Some(Err(_)) => continue,
                None => {
                    round.cycle_complete = true;
                    self.entries = None;
                    break;
                }
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let is_workspace = name.starts_with(ASR_TEMP_PREFIX);
            let is_quarantine = name.starts_with(ASR_QUARANTINE_PREFIX);
            if !is_workspace && !is_quarantine {
                continue;
            }

            round.metadata_attempts += 1;
            let path = entry.path();
            let Ok(metadata) = fs::symlink_metadata(&path) else {
                continue;
            };
            if !metadata.file_type().is_dir() {
                continue;
            }
            if is_workspace
                && metadata
                    .modified()
                    .ok()
                    .and_then(|modified| now.duration_since(modified).ok())
                    .is_none_or(|age| age < stale_age)
            {
                continue;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if metadata.permissions().mode() & 0o077 != 0 {
                    continue;
                }
            }

            round.cleanup_attempts += 1;
            if cleanup_owned_workspace_once(&path).is_err() {
                tracing::warn!("stale private ASR workspace cleanup failed");
            }
        }
        Ok(round)
    }
}

pub(crate) async fn transcribe_file_cancellable(
    config: &AsrSection,
    input: &Path,
    duration_ms: Option<u64>,
    turn_cancellation: &CancellationToken,
    shutdown: &CancellationToken,
) -> Result<String, AsrError> {
    let effective_max_duration = config.max_duration_ms.min(ASR_ABSOLUTE_MAX_DURATION_MS);
    if config.max_duration_ms == 0
        || duration_ms.is_some_and(|duration| duration > effective_max_duration)
    {
        return Err(AsrError::TooLong);
    }
    if !config.is_configured() {
        return Err(AsrError::SidecarMissing);
    }
    let root = private_root();
    ensure_private_directory(&root)?;
    transcribe_file_in(
        config,
        input,
        &root,
        effective_max_duration,
        turn_cancellation,
        shutdown,
    )
    .await
}

fn private_root() -> PathBuf {
    std::env::temp_dir().join(ASR_PRIVATE_ROOT)
}

async fn transcribe_file_in(
    config: &AsrSection,
    input: &Path,
    temp_root: &Path,
    effective_max_duration: u64,
    turn_cancellation: &CancellationToken,
    shutdown: &CancellationToken,
) -> Result<String, AsrError> {
    ensure_private_directory(temp_root)?;
    let workspace = AsrWorkspace::new_in(temp_root)?;
    let wav_path = workspace.path().join("decoded.wav");
    create_private_file(&wav_path, &[])?;
    let decode = decode_to_wav(
        &config.ffmpeg,
        input,
        &wav_path,
        effective_max_duration,
        turn_cancellation,
        shutdown,
    )
    .await;
    let result = match decode {
        Ok(()) => {
            let command = config.command.as_ref().ok_or(AsrError::SidecarMissing)?;
            run_sidecar(config, command, &wav_path, turn_cancellation, shutdown).await
        }
        Err(error) => Err(error),
    };
    workspace.cleanup().await;
    result
}

struct AsrWorkspace {
    directory: Option<TempDir>,
}

impl AsrWorkspace {
    fn new_in(root: &Path) -> Result<Self, AsrError> {
        let mut builder = TempDirBuilder::new();
        builder.prefix(ASR_TEMP_PREFIX);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(fs::Permissions::from_mode(0o700));
        }
        let directory = builder
            .tempdir_in(root)
            .map_err(|_| AsrError::TemporaryStorage)?;
        ensure_private_directory(directory.path())?;
        create_private_file(
            &directory.path().join(ASR_TEMP_MARKER),
            ASR_TEMP_MARKER_CONTENTS,
        )?;
        Ok(Self {
            directory: Some(directory),
        })
    }

    fn path(&self) -> &Path {
        self.directory
            .as_ref()
            .expect("workspace exists until cleanup")
            .path()
    }

    async fn cleanup(mut self) {
        let Some(directory) = self.directory.take() else {
            return;
        };
        cleanup_workspace(directory.keep()).await;
    }
}

impl Drop for AsrWorkspace {
    fn drop(&mut self) {
        let Some(directory) = self.directory.take() else {
            return;
        };
        let path = directory.keep();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(cleanup_workspace(path));
        } else if cleanup_owned_workspace_once(&path).is_err() {
            tracing::warn!("private ASR workspace cleanup could not be scheduled");
        }
    }
}

async fn cleanup_workspace(path: PathBuf) {
    let path = match prepare_quarantine(&path) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(_) => {
            tracing::warn!("private ASR workspace quarantine failed");
            return;
        }
    };
    for attempt in 0..ASR_CLEANUP_ATTEMPTS {
        match cleanup_quarantined_workspace_once(&path) {
            Ok(()) => return,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            Err(_) if attempt + 1 < ASR_CLEANUP_ATTEMPTS => {
                tokio::time::sleep(ASR_CLEANUP_RETRY).await;
            }
            Err(_) => {
                tracing::warn!("private ASR workspace cleanup failed");
                return;
            }
        }
    }
}

fn is_owned_stale_workspace(path: &Path) -> bool {
    let marker_path = path.join(ASR_TEMP_MARKER);
    let Ok(marker_metadata) = fs::symlink_metadata(&marker_path) else {
        return false;
    };
    if !marker_metadata.file_type().is_file() {
        return false;
    }
    let Ok(mut marker) = open_no_follow_read(&marker_path) else {
        return false;
    };
    let mut contents = Vec::with_capacity(ASR_TEMP_MARKER_CONTENTS.len());
    if std::io::Read::by_ref(&mut marker)
        .take(u64::try_from(ASR_TEMP_MARKER_CONTENTS.len() + 1).unwrap_or(u64::MAX))
        .read_to_end(&mut contents)
        .is_err()
        || contents != ASR_TEMP_MARKER_CONTENTS
    {
        return false;
    }
    true
}

fn cleanup_owned_workspace_once(path: &Path) -> std::io::Result<()> {
    let quarantined = prepare_quarantine(path)?;
    cleanup_quarantined_workspace_once(&quarantined)
}

fn prepare_quarantine(path: &Path) -> std::io::Result<PathBuf> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "invalid workspace name",
            )
        })?;
    if name.starts_with(ASR_QUARANTINE_PREFIX) {
        ensure_cleanup_claim(path)?;
        return Ok(path.to_owned());
    }
    if !name.starts_with(ASR_TEMP_PREFIX) || !is_owned_stale_workspace(path) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "not a bridge-owned ASR workspace",
        ));
    }
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "workspace has no parent",
        )
    })?;
    let quarantined = parent.join(format!(
        "{ASR_QUARANTINE_PREFIX}{}",
        Uuid::new_v4().simple()
    ));
    fs::rename(path, &quarantined)?;
    // The marker is deliberately retained if this fails, so a later sweep can
    // safely recreate the external claim after a crash or transient failure.
    ensure_cleanup_claim(&quarantined)?;
    Ok(quarantined)
}

fn cleanup_quarantined_workspace_once(path: &Path) -> std::io::Result<()> {
    let claim = ensure_cleanup_claim(path)?;
    secure_private_path(path, true).map_err(as_io_error)?;
    let marker = path.join(ASR_TEMP_MARKER);
    match fs::symlink_metadata(&marker) {
        Ok(_) if !is_owned_stale_workspace(path) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "workspace marker is invalid",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    erase_known_decoded_audio(&path.join("decoded.wav"))?;

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_name() != ASR_TEMP_MARKER {
            // Unknown/attacker-controlled entries are never removed. The
            // external claim and marker remain so decoded speech stays erased
            // and a later sweep can retry safely.
            return Err(std::io::Error::new(
                std::io::ErrorKind::DirectoryNotEmpty,
                "workspace contains an unknown entry",
            ));
        }
    }
    match fs::remove_file(&marker) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    fs::remove_dir(path)?;
    match fs::remove_file(claim) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn erase_known_decoded_audio(path: &Path) -> std::io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "decoded audio is not a regular file",
        ));
    }
    let mut options = OpenOptions::new();
    options.write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        if metadata.nlink() != 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "decoded audio has unexpected links",
            ));
        }
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    file.set_len(0)?;
    file.sync_all()?;
    fs::remove_file(path)
}

fn cleanup_claim_path(path: &Path) -> std::io::Result<PathBuf> {
    let id = path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix(ASR_QUARANTINE_PREFIX))
        .filter(|id| !id.is_empty())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "invalid quarantine name",
            )
        })?;
    Ok(path
        .parent()
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "quarantine has no parent",
            )
        })?
        .join(format!("{ASR_CLAIM_PREFIX}{id}")))
}

fn cleanup_claim_contents(path: &Path) -> std::io::Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "quarantine is not a directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(format!("asr-cleanup-v1\n{}:{}\n", metadata.dev(), metadata.ino()).into_bytes())
    }
    #[cfg(not(unix))]
    Ok(format!("asr-cleanup-v1\n{}\n", path.display()).into_bytes())
}

fn ensure_cleanup_claim(path: &Path) -> std::io::Result<PathBuf> {
    let claim = cleanup_claim_path(path)?;
    let expected = cleanup_claim_contents(path)?;
    if fs::symlink_metadata(&claim).is_err() && !is_owned_stale_workspace(path) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "unclaimed quarantine has no valid ownership marker",
        ));
    }
    match create_private_file(&claim, &expected) {
        Ok(()) => return Ok(claim),
        Err(_) if fs::symlink_metadata(&claim).is_ok() => {}
        Err(_) => {
            return Err(std::io::Error::other("cleanup claim creation failed"));
        }
    }
    let file = open_no_follow_read(&claim)?;
    let mut actual = Vec::with_capacity(expected.len());
    file.take(u64::try_from(expected.len() + 1).unwrap_or(u64::MAX))
        .read_to_end(&mut actual)?;
    if actual != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "cleanup claim does not match the directory identity",
        ));
    }
    Ok(claim)
}

fn as_io_error(_error: AsrError) -> std::io::Error {
    std::io::Error::other("private ASR path verification failed")
}

fn create_private_file(path: &Path, contents: &[u8]) -> Result<(), AsrError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(|_| AsrError::TemporaryStorage)?;
    secure_private_path(path, false)?;
    file.write_all(contents)
        .and_then(|()| file.sync_all())
        .map_err(|_| AsrError::TemporaryStorage)?;
    secure_private_path(path, false)
}

fn ensure_private_directory(path: &Path) -> Result<(), AsrError> {
    match create_private_directory(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(_) => return Err(AsrError::TemporaryStorage),
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| AsrError::TemporaryStorage)?;
    if !metadata.file_type().is_dir() {
        return Err(AsrError::TemporaryStorage);
    }
    secure_private_path(path, true)
}

#[cfg(unix)]
fn create_private_directory(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    // 0700 contains no group/other bits, so the directory is private at the
    // instant of creation under every umask; there is no post-create window.
    builder.mode(0o700).create(path)
}

#[cfg(not(unix))]
fn create_private_directory(path: &Path) -> std::io::Result<()> {
    fs::create_dir(path)
}

#[cfg(unix)]
fn secure_private_path(path: &Path, directory: bool) -> Result<(), AsrError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    let expected = if directory { 0o700 } else { 0o600 };
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | if directory { libc::O_DIRECTORY } else { 0 });
    let handle = options.open(path).map_err(|_| AsrError::TemporaryStorage)?;
    let metadata = handle.metadata().map_err(|_| AsrError::TemporaryStorage)?;
    let expected_type = if directory {
        metadata.file_type().is_dir()
    } else {
        metadata.file_type().is_file()
    };
    if !expected_type || (!directory && metadata.nlink() != 1) {
        return Err(AsrError::TemporaryStorage);
    }
    handle
        .set_permissions(fs::Permissions::from_mode(expected))
        .map_err(|_| AsrError::TemporaryStorage)?;
    let handle_metadata = handle.metadata().map_err(|_| AsrError::TemporaryStorage)?;
    let path_metadata = fs::symlink_metadata(path).map_err(|_| AsrError::TemporaryStorage)?;
    if handle_metadata.dev() != path_metadata.dev()
        || handle_metadata.ino() != path_metadata.ino()
        || path_metadata.permissions().mode() & 0o777 != expected
    {
        return Err(AsrError::TemporaryStorage);
    }
    Ok(())
}

#[cfg(windows)]
fn secure_private_path(path: &Path, directory: bool) -> Result<(), AsrError> {
    windows_private_acl::apply_and_verify(path, directory)
}

#[cfg(not(any(unix, windows)))]
fn secure_private_path(_path: &Path, _directory: bool) -> Result<(), AsrError> {
    Err(AsrError::TemporaryStorage)
}

fn open_no_follow_read(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path)
}

#[cfg(windows)]
mod windows_private_acl {
    use std::path::Path;

    use windows_acl::acl::{ACL, AceType};
    use windows_acl::helper::{current_user, name_to_sid, string_to_sid};

    use super::AsrError;

    const SYSTEM_SID: &str = "S-1-5-18";
    const FILE_ALL_ACCESS: u32 = 0x001f_01ff;

    pub(super) fn apply_and_verify(path: &Path, directory: bool) -> Result<(), AsrError> {
        let path = path.to_str().ok_or(AsrError::TemporaryStorage)?;
        let user_name = current_user().ok_or(AsrError::TemporaryStorage)?;
        let user_sid = name_to_sid(&user_name, None).map_err(|_| AsrError::TemporaryStorage)?;
        let user_sid_string =
            windows_acl::helper::sid_to_string(user_sid.as_ptr().cast_mut().cast())
                .map_err(|_| AsrError::TemporaryStorage)?;
        let system_sid = string_to_sid(SYSTEM_SID).map_err(|_| AsrError::TemporaryStorage)?;
        let mut acl = ACL::from_file_path(path, false).map_err(|_| AsrError::TemporaryStorage)?;

        // Start from an empty protected DACL. windows-acl applies DACL changes
        // with PROTECTED_DACL_SECURITY_INFORMATION, so inherited grants cannot
        // reappear between creation and the first content write.
        for entry in acl.all().map_err(|_| AsrError::TemporaryStorage)? {
            let raw_sid =
                string_to_sid(&entry.string_sid).map_err(|_| AsrError::TemporaryStorage)?;
            acl.remove(
                raw_sid.as_ptr().cast_mut().cast(),
                Some(entry.entry_type),
                None,
            )
            .map_err(|_| AsrError::TemporaryStorage)?;
        }
        acl.allow(
            user_sid.as_ptr().cast_mut().cast(),
            directory,
            FILE_ALL_ACCESS,
        )
        .map_err(|_| AsrError::TemporaryStorage)?;
        acl.allow(
            system_sid.as_ptr().cast_mut().cast(),
            directory,
            FILE_ALL_ACCESS,
        )
        .map_err(|_| AsrError::TemporaryStorage)?;

        let entries = ACL::from_file_path(path, false)
            .and_then(|acl| acl.all())
            .map_err(|_| AsrError::TemporaryStorage)?;
        let expected_flags = if directory { 0x03 } else { 0x00 };
        let mut saw_user = false;
        let mut saw_system = false;
        for entry in &entries {
            if entry.entry_type != AceType::AccessAllow
                || entry.mask & FILE_ALL_ACCESS != FILE_ALL_ACCESS
                || entry.flags != expected_flags
            {
                return Err(AsrError::TemporaryStorage);
            }
            match entry.string_sid.as_str() {
                sid if sid == user_sid_string => saw_user = true,
                SYSTEM_SID => saw_system = true,
                _ => return Err(AsrError::TemporaryStorage),
            }
        }
        if entries.len() != 2 || !saw_user || !saw_system {
            return Err(AsrError::TemporaryStorage);
        }
        Ok(())
    }
}

async fn decode_to_wav(
    ffmpeg: &Path,
    input: &Path,
    output: &Path,
    max_duration_ms: u64,
    turn_cancellation: &CancellationToken,
    shutdown: &CancellationToken,
) -> Result<(), AsrError> {
    let decode_limit_ms = max_duration_ms.saturating_add(1);
    let decode_limit = format!("{}.{:03}", decode_limit_ms / 1_000, decode_limit_ms % 1_000);
    let decoded_byte_limit = decoded_byte_limit(max_duration_ms);
    let mut command = Command::new(ffmpeg);
    command
        .arg("-hide_banner")
        .arg("-nostdin")
        .arg("-y")
        .arg("-loglevel")
        .arg("error")
        .arg("-i")
        .arg(input)
        .arg("-t")
        .arg(decode_limit)
        .arg("-ac")
        .arg("1")
        .arg("-ar")
        .arg("16000")
        .arg("-c:a")
        .arg("pcm_s16le")
        .arg("-f")
        .arg("s16le")
        .arg("-fs")
        .arg(decoded_byte_limit.saturating_sub(44).to_string())
        .arg("pipe:1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let file = open_no_follow_write(output)?;
    let mut process = SupervisedProcess::spawn(&mut command, AsrError::UnsupportedCodec)?;
    let status = write_bounded_decode(
        &mut process,
        file,
        decoded_byte_limit,
        turn_cancellation,
        shutdown,
    )
    .await?;
    if !status.success() {
        return Err(AsrError::UnsupportedCodec);
    }
    secure_private_path(output, false)?;
    let metadata = fs::symlink_metadata(output).map_err(|_| AsrError::UnsupportedCodec)?;
    if !metadata.file_type().is_file() {
        return Err(AsrError::UnsupportedCodec);
    }
    if metadata.len() > decoded_byte_limit || metadata.len() > ASR_DECODED_WAV_MAX_BYTES {
        return Err(AsrError::Oversize);
    }
    if wav_exceeds_duration(output, max_duration_ms)? {
        Err(AsrError::TooLong)
    } else {
        Ok(())
    }
}

async fn write_bounded_decode(
    process: &mut SupervisedProcess,
    mut file: File,
    byte_limit: u64,
    turn_cancellation: &CancellationToken,
    shutdown: &CancellationToken,
) -> Result<ExitStatus, AsrError> {
    let Some(mut stdout) = process.take_stdout() else {
        process.terminate_and_wait().await;
        return Err(AsrError::UnsupportedCodec);
    };
    let deadline = Instant::now() + ASR_FFMPEG_TIMEOUT;
    let mut bytes_written = 0_u64;
    if file.write_all(&canonical_wav_header(0)).is_err() {
        process.terminate_and_wait().await;
        return Err(AsrError::TemporaryStorage);
    }
    let mut chunk = [0_u8; 8 * 1024];
    let mut stdout_eof = false;
    let mut exit_status = None;
    loop {
        if exit_status.is_none() {
            let Ok(leader) = process.try_wait_leader() else {
                process.terminate_and_wait().await;
                return Err(AsrError::UnsupportedCodec);
            };
            if leader.is_some() {
                exit_status = Some(
                    process
                        .finish_after_leader()
                        .await
                        .map_err(|_| AsrError::UnsupportedCodec)?,
                );
            }
        }
        if exit_status.is_some() && stdout_eof {
            break;
        }
        tokio::select! {
            biased;
            () = turn_cancellation.cancelled() => {
                process.terminate_and_wait().await;
                return Err(AsrError::Cancelled);
            }
            () = shutdown.cancelled() => {
                process.terminate_and_wait().await;
                return Err(AsrError::Cancelled);
            }
            () = tokio::time::sleep_until(deadline) => {
                process.terminate_and_wait().await;
                return Err(AsrError::UnsupportedCodec);
            }
            read = stdout.read(&mut chunk), if !stdout_eof => {
                let Ok(read) = read else {
                    process.terminate_and_wait().await;
                    return Err(AsrError::UnsupportedCodec);
                };
                if read == 0 {
                    stdout_eof = true;
                    continue;
                }
                let read = u64::try_from(read).map_err(|_| AsrError::Oversize)?;
                let Some(next) = bytes_written.checked_add(read) else {
                    process.terminate_and_wait().await;
                    return Err(AsrError::Oversize);
                };
                let file_bytes = next.saturating_add(44);
                if file_bytes > byte_limit || file_bytes > ASR_DECODED_WAV_MAX_BYTES {
                    process.terminate_and_wait().await;
                    return Err(AsrError::Oversize);
                }
                let Ok(read) = usize::try_from(read) else {
                    process.terminate_and_wait().await;
                    return Err(AsrError::Oversize);
                };
                if file.write_all(&chunk[..read]).is_err() {
                    process.terminate_and_wait().await;
                    return Err(AsrError::TemporaryStorage);
                }
                bytes_written = next;
            }
            () = sleep(ASR_PROCESS_POLL) => {}
        }
    }
    if bytes_written % 2 != 0 || bytes_written > u64::from(u32::MAX) {
        return Err(AsrError::UnsupportedCodec);
    }
    let data_bytes = u32::try_from(bytes_written).map_err(|_| AsrError::UnsupportedCodec)?;
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.write_all(&canonical_wav_header(data_bytes)))
        .map_err(|_| AsrError::TemporaryStorage)?;
    file.flush()
        .and_then(|()| file.sync_all())
        .map_err(|_| AsrError::TemporaryStorage)?;
    Ok(exit_status.expect("decode status exists after stdout EOF"))
}

fn canonical_wav_header(data_bytes: u32) -> [u8; 44] {
    let mut header = [0_u8; 44];
    header[0..4].copy_from_slice(b"RIFF");
    header[4..8].copy_from_slice(&36_u32.saturating_add(data_bytes).to_le_bytes());
    header[8..16].copy_from_slice(b"WAVEfmt ");
    header[16..20].copy_from_slice(&16_u32.to_le_bytes());
    header[20..22].copy_from_slice(&1_u16.to_le_bytes());
    header[22..24].copy_from_slice(&1_u16.to_le_bytes());
    header[24..28].copy_from_slice(&16_000_u32.to_le_bytes());
    header[28..32].copy_from_slice(&32_000_u32.to_le_bytes());
    header[32..34].copy_from_slice(&2_u16.to_le_bytes());
    header[34..36].copy_from_slice(&16_u16.to_le_bytes());
    header[36..40].copy_from_slice(b"data");
    header[40..44].copy_from_slice(&data_bytes.to_le_bytes());
    header
}

fn open_no_follow_write(path: &Path) -> Result<File, AsrError> {
    let mut options = OpenOptions::new();
    options.write(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).map_err(|_| AsrError::TemporaryStorage)?;
    secure_private_path(path, false)?;
    Ok(file)
}

fn decoded_byte_limit(max_duration_ms: u64) -> u64 {
    let pcm = ASR_DECODED_PCM_BYTES_PER_SECOND
        .saturating_mul(max_duration_ms.saturating_add(1))
        .saturating_add(999)
        / 1_000;
    pcm.saturating_add(44).min(ASR_DECODED_WAV_MAX_BYTES)
}

#[derive(Default)]
struct WavLayout {
    byte_rate: Option<u32>,
    data_size: Option<u64>,
}

fn wav_exceeds_duration(path: &Path, max_duration_ms: u64) -> Result<bool, AsrError> {
    const MAX_CHUNKS: usize = 128;

    let mut file = File::open(path).map_err(|_| AsrError::UnsupportedCodec)?;
    let file_len = file
        .metadata()
        .map_err(|_| AsrError::UnsupportedCodec)?
        .len();
    let mut header = [0_u8; 12];
    file.read_exact(&mut header)
        .map_err(|_| AsrError::UnsupportedCodec)?;
    if &header[..4] != b"RIFF" || &header[8..] != b"WAVE" {
        return Err(AsrError::UnsupportedCodec);
    }
    let riff_size = u64::from(u32::from_le_bytes(
        header[4..8]
            .try_into()
            .map_err(|_| AsrError::UnsupportedCodec)?,
    ));
    if riff_size.checked_add(8) != Some(file_len) {
        return Err(AsrError::UnsupportedCodec);
    }

    let mut layout = WavLayout::default();
    let mut chunks = 0_usize;
    while file
        .stream_position()
        .map_err(|_| AsrError::UnsupportedCodec)?
        < file_len
    {
        chunks = chunks.saturating_add(1);
        if chunks > MAX_CHUNKS {
            return Err(AsrError::UnsupportedCodec);
        }
        let position = file
            .stream_position()
            .map_err(|_| AsrError::UnsupportedCodec)?;
        if file_len.saturating_sub(position) < 8 {
            return Err(AsrError::UnsupportedCodec);
        }
        let mut chunk = [0_u8; 8];
        file.read_exact(&mut chunk)
            .map_err(|_| AsrError::UnsupportedCodec)?;
        let size = u64::from(u32::from_le_bytes(
            chunk[4..8]
                .try_into()
                .map_err(|_| AsrError::UnsupportedCodec)?,
        ));
        let padded_size = size
            .checked_add(size % 2)
            .ok_or(AsrError::UnsupportedCodec)?;
        let payload_start = file
            .stream_position()
            .map_err(|_| AsrError::UnsupportedCodec)?;
        if payload_start
            .checked_add(padded_size)
            .is_none_or(|end| end > file_len)
        {
            return Err(AsrError::UnsupportedCodec);
        }
        parse_wav_chunk(&mut file, &chunk[..4], size, &mut layout)?;
    }
    if file
        .stream_position()
        .map_err(|_| AsrError::UnsupportedCodec)?
        != file_len
    {
        return Err(AsrError::UnsupportedCodec);
    }
    let rate = layout.byte_rate.ok_or(AsrError::UnsupportedCodec)?;
    let size = layout.data_size.ok_or(AsrError::UnsupportedCodec)?;
    Ok(u128::from(size) * 1_000 > u128::from(rate) * u128::from(max_duration_ms))
}

fn parse_wav_chunk(
    file: &mut File,
    kind: &[u8],
    size: u64,
    layout: &mut WavLayout,
) -> Result<(), AsrError> {
    match kind {
        b"fmt " => {
            if size < 16 || layout.byte_rate.is_some() {
                return Err(AsrError::UnsupportedCodec);
            }
            let mut format = [0_u8; 16];
            file.read_exact(&mut format)
                .map_err(|_| AsrError::UnsupportedCodec)?;
            let audio_format = u16::from_le_bytes([format[0], format[1]]);
            let channels = u16::from_le_bytes([format[2], format[3]]);
            let sample_rate = u32::from_le_bytes([format[4], format[5], format[6], format[7]]);
            let rate = u32::from_le_bytes([format[8], format[9], format[10], format[11]]);
            let block_align = u16::from_le_bytes([format[12], format[13]]);
            let bits_per_sample = u16::from_le_bytes([format[14], format[15]]);
            if audio_format != 1
                || channels != 1
                || sample_rate != 16_000
                || rate != 32_000
                || block_align != 2
                || bits_per_sample != 16
            {
                return Err(AsrError::UnsupportedCodec);
            }
            layout.byte_rate = Some(rate);
            skip_chunk_remainder(file, size, 16)
        }
        b"data" => {
            if layout.byte_rate.is_none()
                || layout.data_size.replace(size).is_some()
                || size % 2 != 0
            {
                return Err(AsrError::UnsupportedCodec);
            }
            skip_chunk_remainder(file, size, 0)
        }
        _ => skip_chunk_remainder(file, size, 0),
    }
}

fn skip_chunk_remainder(file: &mut File, size: u64, consumed: u64) -> Result<(), AsrError> {
    let remainder = size
        .checked_sub(consumed)
        .and_then(|value| value.checked_add(size % 2))
        .ok_or(AsrError::UnsupportedCodec)?;
    let offset = i64::try_from(remainder).map_err(|_| AsrError::UnsupportedCodec)?;
    file.seek(SeekFrom::Current(offset))
        .map_err(|_| AsrError::UnsupportedCodec)?;
    Ok(())
}

async fn run_sidecar(
    config: &AsrSection,
    command: &Path,
    wav_path: &Path,
    turn_cancellation: &CancellationToken,
    shutdown: &CancellationToken,
) -> Result<String, AsrError> {
    let mut process = Command::new(command);
    process
        .args(&config.args)
        .arg(wav_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = SupervisedProcess::spawn(&mut process, AsrError::SidecarFailed)?;
    let mut stdout = child.take_stdout().ok_or(AsrError::SidecarFailed)?;
    let stdout_limit = config
        .max_transcript_bytes
        .saturating_mul(4)
        .saturating_add(4 * 1024);
    let deadline = Instant::now() + ASR_SIDECAR_TIMEOUT;
    let mut bytes = Vec::with_capacity(stdout_limit.min(8 * 1024));
    let mut chunk = vec![0_u8; 8 * 1024];
    let mut stdout_eof = false;
    let mut exit_status = None;
    loop {
        if exit_status.is_none() {
            let Ok(leader) = child.try_wait_leader() else {
                child.terminate_and_wait().await;
                return Err(AsrError::SidecarFailed);
            };
            if leader.is_some() {
                exit_status = Some(
                    child
                        .finish_after_leader()
                        .await
                        .map_err(|_| AsrError::SidecarFailed)?,
                );
            }
        }
        if exit_status.is_some() && stdout_eof {
            break;
        }
        tokio::select! {
            biased;
            () = turn_cancellation.cancelled() => {
                child.terminate_and_wait().await;
                return Err(AsrError::Cancelled);
            }
            () = shutdown.cancelled() => {
                child.terminate_and_wait().await;
                return Err(AsrError::Cancelled);
            }
            () = tokio::time::sleep_until(deadline) => {
                child.terminate_and_wait().await;
                return Err(AsrError::SidecarFailed);
            }
            read = stdout.read(&mut chunk), if !stdout_eof => {
                let Ok(read) = read else {
                    child.terminate_and_wait().await;
                    return Err(AsrError::SidecarFailed);
                };
                if read == 0 {
                    stdout_eof = true;
                } else if bytes.len().saturating_add(read) > stdout_limit {
                    child.terminate_and_wait().await;
                    return Err(AsrError::TranscriptTooLarge);
                } else {
                    bytes.extend_from_slice(&chunk[..read]);
                }
            }
            () = sleep(ASR_PROCESS_POLL) => {}
        }
    }
    let status = exit_status.expect("sidecar status exists when bounded drain completes");
    if !status.success() {
        return Err(AsrError::SidecarFailed);
    }
    let stdout = String::from_utf8_lossy(&bytes);
    parse_sidecar_transcript(&stdout, config.max_transcript_bytes)
}

fn parse_sidecar_transcript(stdout: &str, max_bytes: usize) -> Result<String, AsrError> {
    // sherpa-onnx SenseVoice commonly emits one JSON object with a `text`
    // field. Parsing that envelope in Rust keeps the wrapper minimal; process
    // group / Job Object supervision covers wrappers with or without `exec`.
    // Plain-text sidecars remain supported.
    for line in stdout.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line.trim()) else {
            continue;
        };
        if let Some(text) = value.get("text").and_then(serde_json::Value::as_str) {
            return bounded_transcript(text, max_bytes);
        }
    }
    bounded_transcript(stdout, max_bytes)
}

fn bounded_transcript(text: &str, max_bytes: usize) -> Result<String, AsrError> {
    if text.trim().len() > max_bytes {
        return Err(AsrError::TranscriptTooLarge);
    }
    normalize_transcript(text, max_bytes).ok_or(AsrError::EmptyTranscript)
}

struct SupervisedProcess {
    child: Option<AsyncGroupChild>,
}

impl SupervisedProcess {
    fn spawn(command: &mut Command, spawn_error: AsrError) -> Result<Self, AsrError> {
        let mut group = command.group();
        group.kill_on_drop(true);
        let child = group.spawn().map_err(|_| spawn_error)?;
        Ok(Self { child: Some(child) })
    }

    fn take_stdout(&mut self) -> Option<tokio::process::ChildStdout> {
        self.child.as_mut()?.inner().stdout.take()
    }

    fn try_wait_leader(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.child
            .as_mut()
            .expect("supervised child exists until reaped")
            .inner()
            .try_wait()
    }

    async fn finish_after_leader(&mut self) -> std::io::Result<ExitStatus> {
        let child = self
            .child
            .as_mut()
            .expect("supervised child exists until reaped");
        // A successful leader may still have spawned descendants. Always close
        // the entire group/job before releasing the private workspace.
        let _ = child.start_kill();
        let waited = child.wait().await;
        self.child.take();
        waited
    }

    async fn terminate_and_wait(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }

    #[cfg(test)]
    async fn wait_bounded(
        &mut self,
        timeout: Duration,
        turn_cancellation: &CancellationToken,
        shutdown: &CancellationToken,
        output_limit: Option<(&Path, u64)>,
        process_error: AsrError,
    ) -> Result<ExitStatus, AsrError> {
        let deadline = Instant::now() + timeout;
        loop {
            if output_limit.is_some_and(|(path, limit)| {
                fs::symlink_metadata(path).is_ok_and(|metadata| metadata.len() > limit)
            }) {
                self.terminate_and_wait().await;
                return Err(AsrError::Oversize);
            }
            let Ok(leader_status) = self.try_wait_leader() else {
                self.terminate_and_wait().await;
                return Err(process_error);
            };
            if leader_status.is_some() {
                let status = self
                    .finish_after_leader()
                    .await
                    .map_err(|_| process_error)?;
                if output_limit.is_some_and(|(path, limit)| {
                    fs::symlink_metadata(path).is_ok_and(|metadata| metadata.len() > limit)
                }) {
                    return Err(AsrError::Oversize);
                }
                return Ok(status);
            }
            tokio::select! {
                biased;
                () = turn_cancellation.cancelled() => {
                    self.terminate_and_wait().await;
                    return Err(AsrError::Cancelled);
                }
                () = shutdown.cancelled() => {
                    self.terminate_and_wait().await;
                    return Err(AsrError::Cancelled);
                }
                () = tokio::time::sleep_until(deadline) => {
                    self.terminate_and_wait().await;
                    return Err(process_error);
                }
                () = sleep(ASR_PROCESS_POLL) => {}
            }
        }
    }
}

impl Drop for SupervisedProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.start_kill();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = child.wait().await;
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs2::FileExt;
    use std::fs::{self, File, OpenOptions};
    use tempfile::tempdir;
    #[cfg(unix)]
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::time::timeout;
    use uuid::Uuid;

    fn lock_asr_process_tests() -> File {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(std::env::temp_dir().join("lark-codex-bridge-asr-process-tests.lock"))
            .expect("open shared ASR process-test lock");
        file.lock_exclusive()
            .expect("lock shared ASR process-test file");
        file
    }

    fn sweep_complete_cycle(root: &Path, stale_age: Duration, scan_limit: usize) {
        let mut sweeper = StaleWorkspaceSweeper::new(root.to_owned());
        for _ in 0..16_384 {
            let round = sweeper
                .sweep_round(stale_age, scan_limit)
                .expect("bounded stale sweep round");
            assert!(round.read_attempts <= scan_limit);
            assert!(round.metadata_attempts <= round.read_attempts);
            assert!(round.cleanup_attempts <= round.metadata_attempts);
            if round.cycle_complete {
                return;
            }
        }
        panic!("stateful stale sweep must eventually finish one directory cycle");
    }

    #[cfg(unix)]
    async fn read_descendant_pid_handshake(process: &mut SupervisedProcess) -> u32 {
        let stdout = process.take_stdout().expect("handshake stdout");
        let mut stdout = BufReader::new(stdout);
        let mut line = String::new();
        let read = timeout(Duration::from_secs(30), stdout.read_line(&mut line))
            .await
            .expect("descendant startup handshake deadline")
            .expect("read descendant startup handshake");
        assert_ne!(read, 0, "descendant startup handshake must not reach EOF");
        line.trim()
            .parse::<u32>()
            .expect("descendant pid handshake")
    }

    #[cfg(unix)]
    async fn wait_for_process_exit(pid: u32, context: &'static str) {
        timeout(Duration::from_secs(30), async {
            while std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
            {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect(context);
    }

    fn stub(
        dir: &Path,
        name: &str,
        unix: &str,
        #[allow(unused_variables)] windows: &str,
    ) -> PathBuf {
        #[cfg(windows)]
        {
            let path = dir.join(format!("{name}.cmd"));
            fs::write(&path, windows).expect("write windows stub");
            path
        }
        #[cfg(not(windows))]
        {
            use std::os::unix::fs::PermissionsExt;
            let path = dir.join(name);
            fs::write(&path, format!("#!/bin/sh\n{unix}\n")).expect("write unix stub");
            fs::set_permissions(&path, PermissionsExt::from_mode(0o755)).expect("chmod stub");
            path
        }
    }

    fn pcm_wav_bytes(data_bytes: u32) -> Vec<u8> {
        let riff_bytes = 36_u32.checked_add(data_bytes).expect("bounded WAV fixture");
        let mut wav = Vec::with_capacity(44 + data_bytes as usize);
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&riff_bytes.to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16_u32.to_le_bytes());
        wav.extend_from_slice(&1_u16.to_le_bytes());
        wav.extend_from_slice(&1_u16.to_le_bytes());
        wav.extend_from_slice(&16_000_u32.to_le_bytes());
        wav.extend_from_slice(&32_000_u32.to_le_bytes());
        wav.extend_from_slice(&2_u16.to_le_bytes());
        wav.extend_from_slice(&16_u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&data_bytes.to_le_bytes());
        wav.resize(44 + data_bytes as usize, 0);
        wav
    }

    fn fix_riff_extent(wav: &mut [u8]) {
        let size = u32::try_from(wav.len() - 8).expect("bounded RIFF fixture");
        wav[4..8].copy_from_slice(&size.to_le_bytes());
    }

    fn ffmpeg_stub_with_samples(dir: &Path, samples: u32) -> PathBuf {
        let source = dir.join(format!("decoded-{}.pcm", Uuid::new_v4().simple()));
        fs::write(
            &source,
            vec![0_u8; usize::try_from(samples.saturating_mul(2)).expect("fixture size")],
        )
        .expect("write PCM fixture");
        let source = source.to_string_lossy();
        let unix = format!(r#"cat "{}""#, source.replace('"', r#"\""#));
        let windows = format!("@echo off\r\ntype \"{source}\"\r\n");
        stub(dir, "ffmpeg", &unix, &windows)
    }

    fn ffmpeg_stub(dir: &Path) -> PathBuf {
        ffmpeg_stub_with_samples(dir, 160)
    }

    fn config_with(command: PathBuf, ffmpeg: PathBuf, args: Vec<String>) -> AsrSection {
        AsrSection {
            command: Some(command),
            args,
            ffmpeg,
            ..AsrSection::default()
        }
    }

    #[test]
    fn missing_command_is_sidecar_missing() {
        let config = AsrSection::default();
        assert!(!config.is_configured());
        let error = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(transcribe_file(&config, Path::new("missing.ogg"), None))
            .expect_err("missing sidecar");
        assert_eq!(error, AsrError::SidecarMissing);
        assert_eq!(error.code(), "sidecar_missing");
    }

    #[test]
    fn duration_over_limit_does_not_spawn_sidecar() {
        let dir = tempdir().expect("tempdir");
        let exploding = stub(
            dir.path(),
            "explode",
            "exit 99",
            "@echo off\r\nexit /b 99\r\n",
        );
        let config = AsrSection {
            max_duration_ms: 1_000,
            ..config_with(exploding, PathBuf::from("ffmpeg"), Vec::new())
        };
        let error = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(transcribe_file(
                &config,
                Path::new("voice.ogg"),
                Some(1_001),
            ))
            .expect_err("too long");
        assert_eq!(error, AsrError::TooLong);
        assert_eq!(error.code(), "too_long");
    }

    #[tokio::test]
    async fn stub_sidecar_prints_transcript_after_ffmpeg_decode() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let marker = dir.path().join("marker.txt");
        let ffmpeg = ffmpeg_stub(dir.path());
        let asr = stub(
            dir.path(),
            "asr",
            r#"printf 'invoked\n' >> "$1"; printf 'KNOWN TRANSCRIPT\n'"#,
            "@echo off\r\necho invoked>>\"%~1\"\r\necho KNOWN TRANSCRIPT\r\n",
        );
        let input = dir.path().join("voice.ogg");
        fs::write(&input, b"fake-opus").expect("input");
        let config = config_with(asr, ffmpeg, vec![marker.to_string_lossy().into_owned()]);
        let text = transcribe_file(&config, &input, Some(800))
            .await
            .expect("transcript");
        assert_eq!(text, "KNOWN TRANSCRIPT");
        let marker_text = fs::read_to_string(&marker).expect("marker");
        assert!(marker_text.contains("invoked"));
    }

    #[tokio::test]
    async fn empty_sidecar_stdout_is_empty_transcript() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let ffmpeg = ffmpeg_stub(dir.path());
        let asr = stub(dir.path(), "asr", "exit 0", "@echo off\r\nexit /b 0\r\n");
        let input = dir.path().join("voice.ogg");
        fs::write(&input, b"fake-opus").expect("input");
        let error = transcribe_file(&config_with(asr, ffmpeg, Vec::new()), &input, None)
            .await
            .expect_err("empty");
        assert_eq!(error, AsrError::EmptyTranscript);
        assert_eq!(error.code(), "empty_transcript");
    }

    #[tokio::test]
    async fn failing_sidecar_is_sidecar_failed() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let ffmpeg = ffmpeg_stub(dir.path());
        let asr = stub(dir.path(), "asr", "exit 2", "@echo off\r\nexit /b 2\r\n");
        let input = dir.path().join("voice.ogg");
        fs::write(&input, b"fake-opus").expect("input");
        let error = transcribe_file(&config_with(asr, ffmpeg, Vec::new()), &input, None)
            .await
            .expect_err("failed");
        assert_eq!(error, AsrError::SidecarFailed);
        assert_eq!(error.code(), "sidecar_failed");
    }

    #[tokio::test]
    async fn missing_ffmpeg_is_unsupported_codec() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let asr = stub(
            dir.path(),
            "asr",
            "printf 'should-not-run\n'",
            "@echo off\r\necho should-not-run\r\n",
        );
        let input = dir.path().join("voice.ogg");
        fs::write(&input, b"fake-opus").expect("input");
        let config = config_with(asr, dir.path().join("missing-ffmpeg"), Vec::new());
        let error = transcribe_file(&config, &input, None)
            .await
            .expect_err("codec");
        assert_eq!(error, AsrError::UnsupportedCodec);
        assert_eq!(error.code(), "unsupported_codec");
    }

    #[tokio::test]
    async fn decoded_duration_limit_applies_without_inbound_metadata() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let marker = dir.path().join("sidecar-must-not-run");
        let asr = stub(
            dir.path(),
            "asr-duration",
            r#"printf 'invoked\n' > "$1"; printf 'unexpected\n'"#,
            "@echo off\r\necho invoked>\"%~1\"\r\necho unexpected\r\n",
        );
        let config = AsrSection {
            max_duration_ms: 1,
            ..config_with(
                asr,
                ffmpeg_stub_with_samples(dir.path(), 17),
                vec![marker.to_string_lossy().into_owned()],
            )
        };
        let input = dir.path().join("voice.ogg");
        fs::write(&input, b"bounded-compressed-input").expect("input");

        let error = transcribe_file(&config, &input, None)
            .await
            .expect_err("decoded audio exceeds one millisecond");

        assert_eq!(error, AsrError::TooLong);
        assert!(
            !marker.exists(),
            "sidecar must not run after duration rejection"
        );
    }

    #[test]
    fn wav_parser_enforces_complete_extents_and_exact_duration_boundary() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("boundary.wav");

        fs::write(&path, pcm_wav_bytes(32)).expect("exact fixture");
        assert!(!wav_exceeds_duration(&path, 1).expect("exact one millisecond"));
        fs::write(&path, pcm_wav_bytes(34)).expect("one-sample-over fixture");
        assert!(wav_exceeds_duration(&path, 1).expect("one sample over"));

        let mut valid_trailing = pcm_wav_bytes(32);
        valid_trailing.extend_from_slice(b"JUNK");
        valid_trailing.extend_from_slice(&3_u32.to_le_bytes());
        valid_trailing.extend_from_slice(b"abc\0");
        fix_riff_extent(&mut valid_trailing);
        fs::write(&path, &valid_trailing).expect("valid trailing chunk");
        assert!(!wav_exceeds_duration(&path, 1).expect("complete trailing chunk"));

        let mut trailing_garbage = valid_trailing.clone();
        trailing_garbage.push(0xff);
        fs::write(&path, trailing_garbage).expect("trailing garbage");
        assert_eq!(
            wav_exceeds_duration(&path, 1),
            Err(AsrError::UnsupportedCodec)
        );

        let mut truncated_header = pcm_wav_bytes(32);
        truncated_header.extend_from_slice(b"JUNK");
        fix_riff_extent(&mut truncated_header);
        fs::write(&path, truncated_header).expect("truncated chunk header");
        assert_eq!(
            wav_exceeds_duration(&path, 1),
            Err(AsrError::UnsupportedCodec)
        );

        let mut lying_data = pcm_wav_bytes(32);
        lying_data[40..44].copy_from_slice(&64_u32.to_le_bytes());
        fs::write(&path, lying_data).expect("lying data extent");
        assert_eq!(
            wav_exceeds_duration(&path, 1),
            Err(AsrError::UnsupportedCodec)
        );

        let mut multiple_data = pcm_wav_bytes(32);
        multiple_data.extend_from_slice(b"data");
        multiple_data.extend_from_slice(&0_u32.to_le_bytes());
        fix_riff_extent(&mut multiple_data);
        fs::write(&path, multiple_data).expect("multiple data chunks");
        assert_eq!(
            wav_exceeds_duration(&path, 1),
            Err(AsrError::UnsupportedCodec)
        );

        let mut lying_riff = pcm_wav_bytes(32);
        lying_riff[4..8].copy_from_slice(&999_u32.to_le_bytes());
        fs::write(&path, lying_riff).expect("lying RIFF extent");
        assert_eq!(
            wav_exceeds_duration(&path, 1),
            Err(AsrError::UnsupportedCodec)
        );

        let mut truncated = pcm_wav_bytes(32);
        truncated.pop();
        fs::write(&path, truncated).expect("truncated WAV");
        assert_eq!(
            wav_exceeds_duration(&path, 1),
            Err(AsrError::UnsupportedCodec)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bounded_decode_writer_never_crosses_the_hard_quota() {
        let _process_lock = lock_asr_process_tests();
        for (bytes, expected) in [(980_u64, Ok(())), (982, Err(AsrError::Oversize))] {
            let dir = tempdir().expect("tempdir");
            let output = dir.path().join("decoded.wav");
            create_private_file(&output, &[]).expect("private output");
            let program = stub(
                dir.path(),
                "single-write",
                &format!("dd if=/dev/zero bs={bytes} count=1 2>/dev/null"),
                "",
            );
            let mut command = Command::new(program);
            command
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            let file = open_no_follow_write(&output).expect("open bounded destination");
            let mut process = SupervisedProcess::spawn(&mut command, AsrError::UnsupportedCodec)
                .expect("spawn single writer");
            let turn = CancellationToken::new();
            let shutdown = CancellationToken::new();
            let result = write_bounded_decode(&mut process, file, 1_024, &turn, &shutdown)
                .await
                .map(|_| ());
            assert_eq!(result, expected);
            assert!(
                fs::metadata(&output).expect("bounded output").len() <= 1_024,
                "the bridge-owned file must never transiently or finally exceed quota"
            );
        }
    }

    #[tokio::test]
    async fn absolute_duration_cap_applies_even_to_unvalidated_configuration() {
        let dir = tempdir().expect("tempdir");
        let spawned = dir.path().join("must-not-spawn");
        let exploding = stub(
            dir.path(),
            "absolute-cap-explode",
            &format!("printf spawned > '{}'", spawned.display()),
            "@echo off\r\nexit /b 99\r\n",
        );
        let config = AsrSection {
            max_duration_ms: ASR_ABSOLUTE_MAX_DURATION_MS + 1,
            ..config_with(exploding.clone(), exploding, Vec::new())
        };
        let error = transcribe_file(
            &config,
            Path::new("voice.ogg"),
            Some(ASR_ABSOLUTE_MAX_DURATION_MS + 1),
        )
        .await
        .expect_err("absolute duration cap");
        assert_eq!(error, AsrError::TooLong);
        assert!(!spawned.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn decoded_expansion_is_killed_while_ffmpeg_is_still_running() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let ffmpeg_child = dir.path().join("ffmpeg-grandchild.pid");
        let ffmpeg = stub(
            dir.path(),
            "ffmpeg-expansion",
            &format!(
                r#"sleep 60 & child=$!; printf '%s' "$child" > "{}"; dd if=/dev/zero bs=1024 count=80 2>/dev/null; wait "$child""#,
                ffmpeg_child.display()
            ),
            "",
        );
        let sidecar_marker = dir.path().join("sidecar-must-not-run");
        let sidecar = stub(
            dir.path(),
            "sidecar-after-expansion",
            &format!("printf invoked > '{}'", sidecar_marker.display()),
            "",
        );
        let input = dir.path().join("compressed.ogg");
        fs::write(&input, b"tiny compressed input").expect("input");
        let config = AsrSection {
            max_duration_ms: 1,
            ..config_with(sidecar, ffmpeg, Vec::new())
        };

        let error = timeout(
            Duration::from_secs(5),
            transcribe_file(&config, &input, None),
        )
        .await
        .expect("decoded ceiling interrupts ffmpeg before its timeout")
        .expect_err("decoded expansion");

        assert_eq!(error, AsrError::Oversize);
        assert!(!sidecar_marker.exists());
        let pid = fs::read_to_string(&ffmpeg_child)
            .expect("grandchild pid")
            .parse::<u32>()
            .expect("pid");
        wait_for_process_exit(pid, "expanded ffmpeg process tree exits").await;
    }

    #[tokio::test]
    async fn oversized_sidecar_stdout_is_bounded_and_classified() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let transcript = "x".repeat(64);
        let asr = stub(
            dir.path(),
            "asr-oversize",
            &format!("printf '{transcript}'"),
            &format!("@echo off\r\necho {transcript}\r\n"),
        );
        let config = AsrSection {
            max_transcript_bytes: 16,
            ..config_with(asr, ffmpeg_stub(dir.path()), Vec::new())
        };
        let input = dir.path().join("voice.ogg");
        fs::write(&input, b"fake-opus").expect("input");

        let error = transcribe_file(&config, &input, None)
            .await
            .expect_err("stdout must stay bounded");

        assert_eq!(error, AsrError::TranscriptTooLarge);
        assert_eq!(error.code(), "transcript_too_large");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn cancelling_real_transcribe_future_removes_private_workspace() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let workspace_root = dir.path().join("workspaces");
        fs::create_dir(&workspace_root).expect("workspace root");
        let source = dir.path().join("decoded.pcm");
        fs::write(&source, vec![0_u8; 320]).expect("PCM source");
        let marker = dir.path().join("ffmpeg-started");
        let source_text = source.to_string_lossy();
        let marker_text = marker.to_string_lossy();
        let ffmpeg = stub(
            dir.path(),
            "ffmpeg-cancellable",
            &format!(
                r#"cat "{}"; sleep 60 & child=$!; printf '%s' "$child" > "{}"; wait "$child""#,
                source_text.replace('"', r#"\""#),
                marker_text.replace('"', r#"\""#),
            ),
            &format!(
                "@echo off\r\ntype \"{source_text}\"\r\necho ready>\"{marker_text}\"\r\nping -n 60 127.0.0.1 >nul\r\n"
            ),
        );
        let sidecar = stub(
            dir.path(),
            "sidecar-unused",
            "printf unexpected",
            "@echo off\r\necho unexpected\r\n",
        );
        let input = dir.path().join("voice.ogg");
        fs::write(&input, b"private compressed speech").expect("input");
        let config = config_with(sidecar, ffmpeg, Vec::new());
        let task_root = workspace_root.clone();
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            let shutdown = CancellationToken::new();
            transcribe_file_in(
                &config,
                &input,
                &task_root,
                config.max_duration_ms.min(ASR_ABSOLUTE_MAX_DURATION_MS),
                &task_cancellation,
                &shutdown,
            )
            .await
        });

        timeout(Duration::from_secs(5), async {
            while !marker.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("ffmpeg starts");
        let workspace = fs::read_dir(&workspace_root)
            .expect("read workspaces")
            .next()
            .expect("active private workspace")
            .expect("workspace entry")
            .path();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&workspace)
                    .expect("workspace metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o700,
                "decoded speech directory must be owner-only"
            );
            assert_eq!(
                fs::metadata(workspace.join("decoded.wav"))
                    .expect("decoded WAV metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600,
                "decoded speech file must be owner-only"
            );
        }

        #[cfg(unix)]
        let grandchild_pid = fs::read_to_string(&marker)
            .expect("ffmpeg grandchild marker")
            .parse::<u32>()
            .expect("ffmpeg grandchild pid");
        cancellation.cancel();
        assert_eq!(
            task.await.expect("transcription task joins"),
            Err(AsrError::Cancelled)
        );
        timeout(Duration::from_secs(3), async {
            loop {
                if fs::read_dir(&workspace_root)
                    .expect("read workspaces")
                    .next()
                    .is_none()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("cancelled workspace is removed after child termination");
        #[cfg(unix)]
        wait_for_process_exit(
            grandchild_pid,
            "non-exec ffmpeg grandchild exits before cleanup completes",
        )
        .await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn shutdown_token_kills_and_waits_for_non_exec_sidecar_tree() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let workspace_root = dir.path().join("workspaces");
        fs::create_dir(&workspace_root).expect("workspace root");
        let pid_marker = dir.path().join("sidecar-grandchild.pid");
        let sidecar = stub(
            dir.path(),
            "sidecar-shutdown-tree",
            r#"sleep 60 & child=$!; printf '%s' "$child" > "$1"; wait "$child""#,
            "",
        );
        let input = dir.path().join("voice.ogg");
        fs::write(&input, b"compressed speech").expect("input");
        let config = config_with(
            sidecar,
            ffmpeg_stub(dir.path()),
            vec![pid_marker.to_string_lossy().into_owned()],
        );
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            let turn = CancellationToken::new();
            transcribe_file_in(
                &config,
                &input,
                &workspace_root,
                config.max_duration_ms,
                &turn,
                &task_shutdown,
            )
            .await
        });
        timeout(Duration::from_secs(5), async {
            while !pid_marker.exists() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("sidecar starts");
        let pid = fs::read_to_string(&pid_marker)
            .expect("pid marker")
            .parse::<u32>()
            .expect("pid");

        shutdown.cancel();
        assert_eq!(task.await.expect("task joins"), Err(AsrError::Cancelled));
        wait_for_process_exit(pid, "shutdown waits for whole sidecar tree").await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_supervisor_kills_and_reaps_non_exec_grandchild() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let program = stub(
            dir.path(),
            "drop-process-tree",
            r#"sleep 60 & child=$!; printf '%s\n' "$child"; wait "$child""#,
            "",
        );
        let mut command = Command::new(program);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut process = SupervisedProcess::spawn(&mut command, AsrError::SidecarFailed)
            .expect("spawn process group");
        let pid = read_descendant_pid_handshake(&mut process).await;
        drop(process);
        wait_for_process_exit(pid, "drop kills whole process group").await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn normal_sidecar_success_kills_non_exec_descendants_before_return() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let pid_marker = dir.path().join("normal-success-grandchild.pid");
        let sidecar = stub(
            dir.path(),
            "sidecar-normal-tree",
            r#"sleep 60 & child=$!; printf '%s' "$child" > "$1"; printf 'bounded transcript\n'; exit 0"#,
            "",
        );
        let input = dir.path().join("voice.ogg");
        fs::write(&input, b"compressed speech").expect("input");
        let config = config_with(
            sidecar,
            ffmpeg_stub(dir.path()),
            vec![pid_marker.to_string_lossy().into_owned()],
        );
        let transcript = transcribe_file(&config, &input, None)
            .await
            .expect("normal sidecar result");
        assert_eq!(transcript, "bounded transcript");
        let pid = fs::read_to_string(&pid_marker)
            .expect("pid marker")
            .parse::<u32>()
            .expect("pid");
        assert!(
            !std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success()),
            "normal success must reap the non-exec descendant before returning"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn actual_supervisor_timeout_kills_non_exec_descendants() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let program = stub(
            dir.path(),
            "timeout-tree",
            r#"sleep 60 & child=$!; printf '%s\n' "$child"; wait "$child""#,
            "",
        );
        let mut command = Command::new(program);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut process = SupervisedProcess::spawn(&mut command, AsrError::SidecarFailed)
            .expect("spawn timeout tree");
        let pid = read_descendant_pid_handshake(&mut process).await;
        let turn = CancellationToken::new();
        let shutdown = CancellationToken::new();
        assert_eq!(
            process
                .wait_bounded(
                    Duration::from_millis(25),
                    &turn,
                    &shutdown,
                    None,
                    AsrError::SidecarFailed,
                )
                .await,
            Err(AsrError::SidecarFailed)
        );
        assert!(
            !std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success()),
            "timeout must reap the non-exec descendant before returning"
        );
    }

    #[test]
    fn stale_private_workspaces_are_swept_without_touching_other_entries() {
        let root = tempdir().expect("tempdir");
        for index in 0..(ASR_STALE_SCAN_LIMIT + 32) {
            fs::write(root.path().join(format!("unrelated-{index:04}")), b"keep")
                .expect("unrelated entry");
        }
        let mut stale_workspace = AsrWorkspace::new_in(root.path()).expect("stale workspace");
        fs::write(
            stale_workspace.path().join("decoded.wav"),
            b"private speech",
        )
        .expect("decoded speech");
        let stale = stale_workspace
            .directory
            .take()
            .expect("workspace directory")
            .keep();
        let unrelated = root.path().join("keep-me");
        fs::create_dir(&unrelated).expect("unrelated directory");

        let spoofed = TempDirBuilder::new()
            .prefix(ASR_TEMP_PREFIX)
            .tempdir_in(root.path())
            .expect("spoofed workspace")
            .keep();
        fs::write(spoofed.join("decoded.wav"), b"must survive without marker")
            .expect("spoofed content");

        sweep_complete_cycle(root.path(), Duration::ZERO, 16);

        assert!(!stale.exists());
        assert!(unrelated.exists());
        assert!(spoofed.exists(), "a prefix alone is not deletion authority");
    }

    #[test]
    fn large_root_rounds_hard_bound_reads_and_eventually_finish_the_cycle() {
        const ROUND_LIMIT: usize = 7;

        let root = tempdir().expect("tempdir");
        for index in 0..(ASR_STALE_SCAN_LIMIT + 73) {
            fs::write(root.path().join(format!("unrelated-{index:04}")), b"keep")
                .expect("unrelated entry");
        }
        let mut hostile = Vec::new();
        for index in 0..(ROUND_LIMIT * 5) {
            let path = root
                .path()
                .join(format!("{ASR_TEMP_PREFIX}hostile-{index:04}"));
            fs::create_dir(&path).expect("hostile workspace-shaped entry");
            hostile.push(path);
        }
        let mut workspace = AsrWorkspace::new_in(root.path()).expect("owned stale workspace");
        fs::write(workspace.path().join("decoded.wav"), b"private speech").expect("decoded speech");
        let stale = workspace.directory.take().expect("workspace").keep();

        let initial_entries = fs::read_dir(root.path())
            .expect("count root entries")
            .count();
        assert!(initial_entries > ASR_STALE_SCAN_LIMIT);
        let mut sweeper = StaleWorkspaceSweeper::new(root.path().to_owned());
        let mut completed = false;
        let mut rounds = 0_usize;
        while rounds <= initial_entries.saturating_add(32) {
            let round = sweeper
                .sweep_round(Duration::ZERO, ROUND_LIMIT)
                .expect("bounded stale sweep round");
            rounds += 1;
            assert!(round.read_attempts <= ROUND_LIMIT);
            assert!(round.metadata_attempts <= round.read_attempts);
            assert!(round.cleanup_attempts <= round.metadata_attempts);
            if !round.cycle_complete {
                assert_eq!(round.read_attempts, ROUND_LIMIT);
            }
            if round.cycle_complete {
                completed = true;
                break;
            }
        }

        assert!(completed, "retained iterator must reach directory EOF");
        assert!(rounds > 1, "a large root must span multiple bounded rounds");
        assert!(
            !stale.exists(),
            "one complete cycle must reach owned stale work"
        );
        assert!(hostile.iter().all(|path| path.exists()));
    }

    #[test]
    fn restart_resets_progress_without_granting_prefix_cleanup_authority() {
        let root = tempdir().expect("tempdir");
        for index in 0..32 {
            fs::write(root.path().join(format!("unrelated-{index:04}")), b"keep")
                .expect("unrelated entry");
        }
        let spoofed = root
            .path()
            .join(format!("{ASR_TEMP_PREFIX}spoofed-after-restart"));
        ensure_private_directory(&spoofed).expect("private spoofed workspace");
        fs::write(spoofed.join("decoded.wav"), b"must survive").expect("spoofed decoded file");
        let mut workspace = AsrWorkspace::new_in(root.path()).expect("owned stale workspace");
        fs::write(workspace.path().join("decoded.wav"), b"private speech").expect("decoded speech");
        let stale = workspace.directory.take().expect("workspace").keep();

        let mut before_restart = StaleWorkspaceSweeper::new(root.path().to_owned());
        let first = before_restart
            .sweep_round(Duration::ZERO, 3)
            .expect("bounded pre-restart round");
        assert_eq!(first.read_attempts, 3);
        assert!(!first.cycle_complete);
        drop(before_restart);

        sweep_complete_cycle(root.path(), Duration::ZERO, 5);

        assert!(
            !stale.exists(),
            "restart may reset progress but must remain live"
        );
        assert_eq!(
            fs::read(spoofed.join("decoded.wav")).expect("spoofed content survives"),
            b"must survive"
        );
    }

    #[test]
    fn stateful_sweep_reaches_stale_entries_beyond_hostile_prefixes() {
        let root = tempdir().expect("tempdir");
        let mut hostile = Vec::new();
        for index in 0..(ASR_STALE_SCAN_LIMIT + 80) {
            let path = root
                .path()
                .join(format!("{ASR_TEMP_PREFIX}aaaa-hostile-{index:04}"));
            fs::create_dir(&path).expect("hostile prefix directory");
            hostile.push(path);
        }
        let mut stale_workspace = AsrWorkspace::new_in(root.path()).expect("stale workspace");
        fs::write(
            stale_workspace.path().join("decoded.wav"),
            b"private speech",
        )
        .expect("decoded speech");
        let original = stale_workspace.directory.take().expect("directory").keep();
        let stale = root.path().join(format!("{ASR_TEMP_PREFIX}zzzz-owned"));
        fs::rename(original, &stale).expect("place stale entry beyond hostile names");

        let mut sweeper = StaleWorkspaceSweeper::new(root.path().to_owned());
        for _ in 0..4 {
            let round = sweeper
                .sweep_round(Duration::ZERO, 128)
                .expect("bounded stale sweep round");
            assert!(round.read_attempts <= 128);
            assert!(round.metadata_attempts <= round.read_attempts);
            assert!(round.cleanup_attempts <= round.metadata_attempts);
            if !stale.exists() {
                break;
            }
        }
        assert!(
            !stale.exists(),
            "bounded sweeps must advance past more than one scan window"
        );
        assert!(hostile.iter().all(|path| path.exists()));
    }

    #[cfg(unix)]
    #[test]
    fn stateful_sweep_advances_past_hostile_fresh_and_symlink_windows() {
        use std::os::unix::fs::symlink;

        let root = tempdir().expect("tempdir");
        let outside = root.path().join("outside-target");
        fs::write(&outside, b"outside").expect("outside fixture");
        let mut hostile = Vec::new();
        let mut fresh = Vec::new();
        let mut links = Vec::new();
        for index in 0..90 {
            let path = root
                .path()
                .join(format!("{ASR_TEMP_PREFIX}aaaa-hostile-{index:04}"));
            ensure_private_directory(&path).expect("private hostile prefix");
            hostile.push(path);

            let path = root
                .path()
                .join(format!("{ASR_TEMP_PREFIX}bbbb-fresh-{index:04}"));
            ensure_private_directory(&path).expect("private fresh workspace");
            create_private_file(&path.join(ASR_TEMP_MARKER), ASR_TEMP_MARKER_CONTENTS)
                .expect("fresh ownership marker");
            fresh.push(path);

            let path = root
                .path()
                .join(format!("{ASR_TEMP_PREFIX}cccc-symlink-{index:04}"));
            symlink(&outside, &path).expect("hostile symlink");
            links.push(path);
        }
        let mut workspace = AsrWorkspace::new_in(root.path()).expect("owned stale workspace");
        fs::write(workspace.path().join("decoded.wav"), b"private speech").expect("decoded speech");
        let original = workspace.directory.take().expect("workspace").keep();
        let stale = root.path().join(format!("{ASR_TEMP_PREFIX}zzzz-stale"));
        fs::rename(original, &stale).expect("ordered stale workspace");
        let touched = std::process::Command::new("touch")
            .args(["-t", "200001010000"])
            .arg(&stale)
            .status()
            .expect("set stale mtime");
        assert!(touched.success());

        let mut sweeper = StaleWorkspaceSweeper::new(root.path().to_owned());
        for _ in 0..6 {
            let round = sweeper
                .sweep_round(ASR_STALE_AGE, 64)
                .expect("bounded stale sweep round");
            assert!(round.read_attempts <= 64);
            assert!(round.metadata_attempts <= round.read_attempts);
            assert!(round.cleanup_attempts <= round.metadata_attempts);
            if !stale.exists() {
                break;
            }
        }

        assert!(!stale.exists(), "stateful rounds must reach the stale tail");
        assert!(hostile.iter().all(|path| path.exists()));
        assert!(fresh.iter().all(|path| path.exists()));
        assert!(links.iter().all(|path| path.is_symlink()));
        assert_eq!(fs::read(outside).expect("outside survives"), b"outside");
    }

    #[test]
    fn quarantine_prefix_without_marker_or_claim_is_not_cleanup_authority() {
        let root = tempdir().expect("tempdir");
        let spoofed = root.path().join(format!("{ASR_QUARANTINE_PREFIX}spoofed"));
        ensure_private_directory(&spoofed).expect("private spoofed quarantine");
        fs::write(spoofed.join("decoded.wav"), b"must survive").expect("spoofed decoded name");

        sweep_complete_cycle(root.path(), Duration::ZERO, 16);

        assert_eq!(
            fs::read(spoofed.join("decoded.wav")).expect("unproven content survives"),
            b"must survive"
        );
        assert!(!cleanup_claim_path(&spoofed).expect("claim path").exists());
    }

    #[test]
    fn unknown_extra_file_is_preserved_but_decoded_speech_is_erased() {
        let root = tempdir().expect("tempdir");
        let mut workspace = AsrWorkspace::new_in(root.path()).expect("workspace");
        fs::write(workspace.path().join("decoded.wav"), b"private speech").expect("decoded speech");
        fs::write(workspace.path().join("attacker-owned"), b"do not delete")
            .expect("unknown fixture");
        let original = workspace.directory.take().expect("directory").keep();

        assert!(cleanup_owned_workspace_once(&original).is_err());
        assert!(!original.exists(), "workspace is atomically quarantined");
        let quarantine = fs::read_dir(root.path())
            .expect("root entries")
            .flatten()
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(ASR_QUARANTINE_PREFIX))
            })
            .expect("quarantine remains retryable");
        assert!(!quarantine.join("decoded.wav").exists());
        assert_eq!(
            fs::read(quarantine.join("attacker-owned")).expect("unknown survives"),
            b"do not delete"
        );
        assert!(quarantine.join(ASR_TEMP_MARKER).exists());
        assert!(
            cleanup_claim_path(&quarantine)
                .expect("claim path")
                .exists()
        );
    }

    #[test]
    fn external_claim_keeps_post_marker_rmdir_failure_retryable() {
        let root = tempdir().expect("tempdir");
        let mut workspace = AsrWorkspace::new_in(root.path()).expect("workspace");
        fs::write(workspace.path().join("decoded.wav"), b"private speech").expect("decoded speech");
        let original = workspace.directory.take().expect("directory").keep();
        let quarantine = prepare_quarantine(&original).expect("quarantine");
        let claim = cleanup_claim_path(&quarantine).expect("claim");
        fs::remove_file(quarantine.join(ASR_TEMP_MARKER)).expect("simulate marker removal");
        fs::write(quarantine.join("late-entry"), b"preserve").expect("late entry");

        assert!(cleanup_quarantined_workspace_once(&quarantine).is_err());
        assert!(!quarantine.join("decoded.wav").exists());
        assert!(claim.exists(), "external proof survives failed rmdir");
        fs::remove_file(quarantine.join("late-entry")).expect("remove fixture");
        cleanup_quarantined_workspace_once(&quarantine).expect("retry succeeds from claim");
        assert!(!quarantine.exists());
        assert!(!claim.exists());
    }

    #[test]
    fn concurrent_drop_and_sweep_leave_no_decoded_speech() {
        let root = tempdir().expect("tempdir");
        let workspace = AsrWorkspace::new_in(root.path()).expect("workspace");
        fs::write(workspace.path().join("decoded.wav"), b"private speech").expect("decoded speech");
        let root_path = root.path().to_owned();
        let dropper = std::thread::spawn(move || drop(workspace));
        let mut sweeper = StaleWorkspaceSweeper::new(root_path.clone());
        for _ in 0..4 {
            let _ = sweeper
                .sweep_round(Duration::ZERO, ASR_STALE_SCAN_LIMIT)
                .expect("bounded racing stale sweep");
        }
        dropper.join().expect("drop thread");
        for entry in fs::read_dir(&root_path).expect("root entries").flatten() {
            if entry.path().is_dir() {
                assert!(
                    !entry.path().join("decoded.wav").exists(),
                    "racing cleanup paths must erase known decoded audio"
                );
            }
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_workspace_and_unicode_files_have_protected_private_dacls() {
        let root = tempdir().expect("tempdir");
        ensure_private_directory(root.path()).expect("secure root");
        let unicode_root = root.path().join("语音-🔒");
        ensure_private_directory(&unicode_root).expect("secure Unicode root");
        let workspace = AsrWorkspace::new_in(&unicode_root).expect("workspace");
        let decoded = workspace.path().join("decoded.wav");
        create_private_file(&decoded, b"private speech").expect("decoded fixture");
        windows_private_acl::apply_and_verify(root.path(), true).expect("root DACL");
        windows_private_acl::apply_and_verify(workspace.path(), true).expect("workspace DACL");
        windows_private_acl::apply_and_verify(&decoded, false).expect("decoded DACL");
        windows_private_acl::apply_and_verify(&workspace.path().join(ASR_TEMP_MARKER), false)
            .expect("marker DACL");
        for path in [&unicode_root, workspace.path(), decoded.as_path()] {
            let output = std::process::Command::new("powershell.exe")
                .args([
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    "if ((Get-Acl -LiteralPath $args[0]).AreAccessRulesProtected) { exit 0 } else { exit 7 }",
                ])
                .arg(path)
                .output()
                .expect("query protected DACL");
            assert!(output.status.success(), "DACL must have SE_DACL_PROTECTED");
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn windows_job_timeout_terminates_a_spawned_descendant() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let marker = dir.path().join("job-child.pid");
        let script = stub(
            dir.path(),
            "windows-job-tree",
            "",
            &format!(
                "@echo off\r\npowershell.exe -NoProfile -NonInteractive -Command \"$p=Start-Process ping.exe -ArgumentList '-t','127.0.0.1' -PassThru; Set-Content -LiteralPath '{}' -Value $p.Id; Wait-Process -Id $p.Id\"\r\n",
                marker.display()
            ),
        );
        let mut command = Command::new(script);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut process = SupervisedProcess::spawn(&mut command, AsrError::SidecarFailed)
            .expect("spawn Job Object tree");
        timeout(Duration::from_secs(5), async {
            while !marker.exists() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Windows descendant starts");
        let turn = CancellationToken::new();
        let shutdown = CancellationToken::new();
        assert_eq!(
            process
                .wait_bounded(
                    Duration::from_millis(25),
                    &turn,
                    &shutdown,
                    None,
                    AsrError::SidecarFailed,
                )
                .await,
            Err(AsrError::SidecarFailed)
        );
        let pid = fs::read_to_string(&marker).expect("pid");
        let status = std::process::Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "if (Get-Process -Id $args[0] -ErrorAction SilentlyContinue) { exit 9 } else { exit 0 }",
                pid.trim(),
            ])
            .status()
            .expect("query descendant");
        assert!(status.success(), "Job Object must terminate the descendant");
    }

    #[cfg(unix)]
    #[test]
    fn stale_sweep_never_follows_a_decoded_file_symlink() {
        use std::os::unix::fs::symlink;

        let root = tempdir().expect("tempdir");
        let outside = root.path().join("outside-private-audio");
        fs::write(&outside, b"must survive").expect("outside fixture");
        let mut workspace = AsrWorkspace::new_in(root.path()).expect("workspace");
        symlink(&outside, workspace.path().join("decoded.wav")).expect("decoded symlink");
        let workspace = workspace.directory.take().expect("directory").keep();

        sweep_complete_cycle(root.path(), Duration::ZERO, 16);

        assert!(
            !workspace.exists(),
            "workspace should be atomically quarantined"
        );
        assert_eq!(
            fs::read(&outside).expect("outside survives"),
            b"must survive"
        );
        let quarantine = fs::read_dir(root.path())
            .expect("root entries")
            .flatten()
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(ASR_QUARANTINE_PREFIX))
            })
            .expect("failed-closed quarantine");
        fs::remove_file(quarantine.join("decoded.wav")).expect("remove fixture symlink");
        cleanup_quarantined_workspace_once(&quarantine).expect("cleanup after fixture removal");
    }

    #[test]
    fn sherpa_json_envelope_is_parsed_and_bounded() {
        assert_eq!(
            parse_sidecar_transcript("diagnostic\n{\"text\":\" known transcript \"}\n", 32,)
                .expect("JSON transcript"),
            "known transcript"
        );
        assert_eq!(
            parse_sidecar_transcript("{\"text\":\"too long\"}", 4),
            Err(AsrError::TranscriptTooLarge)
        );
    }

    #[tokio::test]
    #[ignore = "requires ffmpeg + sherpa-onnx SenseVoice; run with LARK_ASR_SMOKE=1 -- --ignored"]
    async fn sensevoice_transcribes_real_feishu_like_ogg() {
        let _process_lock = lock_asr_process_tests();
        assert_eq!(
            std::env::var("LARK_ASR_SMOKE").ok().as_deref(),
            Some("1"),
            "set LARK_ASR_SMOKE=1"
        );
        let command = PathBuf::from(std::env::var("LARK_ASR_SIDECAR").expect("LARK_ASR_SIDECAR"));
        let ffmpeg =
            PathBuf::from(std::env::var("LARK_ASR_FFMPEG").unwrap_or_else(|_| "ffmpeg".to_owned()));
        let ogg = PathBuf::from(std::env::var("LARK_ASR_SAMPLE_OGG").expect("LARK_ASR_SAMPLE_OGG"));
        assert!(
            command.is_file(),
            "configured SenseVoice sidecar is missing"
        );
        assert!(ogg.is_file(), "configured SenseVoice sample is missing");
        let config = AsrSection {
            command: Some(command),
            args: Vec::new(),
            ffmpeg,
            ..AsrSection::default()
        };
        let text = transcribe_file(&config, &ogg, None)
            .await
            .expect("SenseVoice transcript");
        assert!(
            (text.contains("开放") || text.contains("开饭"))
                && text.contains("九点")
                && text.contains("下午五点"),
            "unexpected SenseVoice transcript (content redacted)"
        );
    }
}
