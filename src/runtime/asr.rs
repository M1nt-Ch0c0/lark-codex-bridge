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

/// Initializes and sweeps the bridge-owned private ASR root. The router calls
/// this before accepting reverse tool work so a storage-permission failure is
/// observed before any decoded speech can be written.
pub(crate) fn initialize_storage() -> Result<(), AsrError> {
    let root = private_root();
    ensure_private_directory(&root)?;
    sweep_stale_workspaces(&root, ASR_STALE_AGE, ASR_STALE_SCAN_LIMIT);
    Ok(())
}

/// Retries the bounded, bridge-owned stale workspace sweep.
pub(crate) fn sweep_stale_storage() {
    let root = private_root();
    if ensure_private_directory(&root).is_ok() {
        sweep_stale_workspaces(&root, ASR_STALE_AGE, ASR_STALE_SCAN_LIMIT);
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
    for attempt in 0..ASR_CLEANUP_ATTEMPTS {
        match cleanup_owned_workspace_once(&path) {
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

fn sweep_stale_workspaces(root: &Path, stale_age: Duration, scan_limit: usize) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    let now = SystemTime::now();
    let mut matching = 0_usize;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(ASR_TEMP_PREFIX) {
            continue;
        }
        if matching >= scan_limit {
            break;
        }
        matching += 1;
        let path = entry.path();
        let Ok(metadata) = fs::symlink_metadata(&path) else {
            continue;
        };
        if !metadata.file_type().is_dir()
            || metadata
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
        if !is_owned_stale_workspace(&path) {
            continue;
        }
        if cleanup_owned_workspace_once(&path).is_err() {
            tracing::warn!("stale private ASR workspace cleanup failed");
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
    let Ok(entries) = fs::read_dir(path) else {
        return false;
    };
    let mut count = 0_usize;
    for entry in entries.take(3) {
        let Ok(entry) = entry else {
            return false;
        };
        count += 1;
        let name = entry.file_name();
        if name != ASR_TEMP_MARKER && name != "decoded.wav" {
            return false;
        }
        let Ok(metadata) = fs::symlink_metadata(entry.path()) else {
            return false;
        };
        if !metadata.file_type().is_file() {
            return false;
        }
    }
    (1..=2).contains(&count)
}

fn cleanup_owned_workspace_once(path: &Path) -> std::io::Result<()> {
    if !is_owned_stale_workspace(path) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "not a bridge-owned ASR workspace",
        ));
    }
    for name in ["decoded.wav", ASR_TEMP_MARKER] {
        let candidate = path.join(name);
        match fs::symlink_metadata(&candidate) {
            Ok(_) => fs::remove_file(candidate)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    fs::remove_dir(path)
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
    match fs::create_dir(path) {
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
        .arg("wav")
        .arg("-fs")
        .arg(decoded_byte_limit.to_string())
        .arg(output)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut process = SupervisedProcess::spawn(&mut command, AsrError::UnsupportedCodec)?;
    let status = process
        .wait_bounded(
            ASR_FFMPEG_TIMEOUT,
            turn_cancellation,
            shutdown,
            Some((output, decoded_byte_limit)),
            AsrError::UnsupportedCodec,
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

fn decoded_byte_limit(max_duration_ms: u64) -> u64 {
    let pcm = ASR_DECODED_PCM_BYTES_PER_SECOND
        .saturating_mul(max_duration_ms)
        .saturating_add(999)
        / 1_000;
    pcm.saturating_add(64 * 1_024)
        .min(ASR_DECODED_WAV_MAX_BYTES)
}

fn wav_exceeds_duration(path: &Path, max_duration_ms: u64) -> Result<bool, AsrError> {
    const MAX_HEADER_BYTES: u64 = 64 * 1024;
    const MAX_CHUNKS: usize = 32;

    let mut file = File::open(path).map_err(|_| AsrError::UnsupportedCodec)?;
    let mut header = [0_u8; 12];
    file.read_exact(&mut header)
        .map_err(|_| AsrError::UnsupportedCodec)?;
    if &header[..4] != b"RIFF" || &header[8..] != b"WAVE" {
        return Err(AsrError::UnsupportedCodec);
    }

    let mut byte_rate = None;
    for _ in 0..MAX_CHUNKS {
        if file
            .stream_position()
            .map_err(|_| AsrError::UnsupportedCodec)?
            > MAX_HEADER_BYTES
        {
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
        match &chunk[..4] {
            b"fmt " => {
                if size < 16 {
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
                byte_rate = Some(rate);
                skip_chunk_remainder(&mut file, size, 16)?;
            }
            b"data" => {
                let rate = byte_rate.ok_or(AsrError::UnsupportedCodec)?;
                return Ok(
                    u128::from(size) * 1_000 > u128::from(rate) * u128::from(max_duration_ms)
                );
            }
            _ => skip_chunk_remainder(&mut file, size, 0)?,
        }
    }
    Err(AsrError::UnsupportedCodec)
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

    fn write_pcm_wav(path: &Path, samples: u32) {
        let data_bytes = samples.checked_mul(2).expect("bounded sample fixture");
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
        fs::write(path, wav).expect("write PCM WAV fixture");
    }

    fn ffmpeg_stub_with_samples(dir: &Path, samples: u32) -> PathBuf {
        let source = dir.join(format!("decoded-{}.wav", Uuid::new_v4().simple()));
        write_pcm_wav(&source, samples);
        let source = source.to_string_lossy();
        let unix = format!(
            r#"out=""; for arg in "$@"; do out=$arg; done; cp "{}" "$out""#,
            source.replace('"', r#"\""#)
        );
        let windows = format!(
            "@echo off\r\n:loop\r\nif \"%~2\"==\"\" (\r\ncopy /y \"{source}\" \"%~1\" >nul\r\nexit /b %errorlevel%\r\n)\r\nshift\r\ngoto loop\r\n"
        );
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
                r#"out=""; for arg in "$@"; do out=$arg; done; dd if=/dev/zero of="$out" bs=1024 count=80 2>/dev/null; sleep 60 & child=$!; printf '%s' "$child" > "{}"; wait "$child""#,
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
        timeout(Duration::from_secs(2), async {
            while std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("expanded ffmpeg process tree exits");
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
        let source = dir.path().join("decoded.wav");
        write_pcm_wav(&source, 160);
        let marker = dir.path().join("ffmpeg-started");
        let source_text = source.to_string_lossy();
        let marker_text = marker.to_string_lossy();
        let ffmpeg = stub(
            dir.path(),
            "ffmpeg-cancellable",
            &format!(
                r#"out=""; for arg in "$@"; do out=$arg; done; cp "{}" "$out"; sleep 60 & child=$!; printf '%s' "$child" > "{}"; wait "$child""#,
                source_text.replace('"', r#"\""#),
                marker_text.replace('"', r#"\""#),
            ),
            &format!(
                "@echo off\r\n:loop\r\nif \"%~2\"==\"\" (\r\ncopy /y \"{source_text}\" \"%~1\" >nul\r\necho ready>\"{marker_text}\"\r\nping -n 60 127.0.0.1 >nul\r\nexit /b 0\r\n)\r\nshift\r\ngoto loop\r\n"
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
        timeout(Duration::from_secs(2), async {
            while std::process::Command::new("kill")
                .args(["-0", &grandchild_pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("non-exec ffmpeg grandchild exits before cleanup completes");
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
        timeout(Duration::from_secs(2), async {
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
        .expect("shutdown waits for whole sidecar tree");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_supervisor_kills_and_reaps_non_exec_grandchild() {
        let _process_lock = lock_asr_process_tests();
        let dir = tempdir().expect("tempdir");
        let pid_marker = dir.path().join("drop-grandchild.pid");
        let program = stub(
            dir.path(),
            "drop-process-tree",
            &format!(
                r#"sleep 60 & child=$!; printf '%s' "$child" > '{}'; wait "$child""#,
                pid_marker.display()
            ),
            "",
        );
        let mut command = Command::new(program);
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let process = SupervisedProcess::spawn(&mut command, AsrError::SidecarFailed)
            .expect("spawn process group");
        timeout(Duration::from_secs(5), async {
            while !pid_marker.exists() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("grandchild starts");
        let pid = fs::read_to_string(&pid_marker)
            .expect("pid marker")
            .parse::<u32>()
            .expect("pid");
        drop(process);
        timeout(Duration::from_secs(2), async {
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
        .expect("drop kills whole process group");
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

        sweep_stale_workspaces(root.path(), Duration::ZERO, 16);

        assert!(!stale.exists());
        assert!(unrelated.exists());
        assert!(spoofed.exists(), "a prefix alone is not deletion authority");
    }

    #[cfg(windows)]
    #[test]
    fn windows_workspace_and_files_have_verified_private_dacls() {
        let root = tempdir().expect("tempdir");
        ensure_private_directory(root.path()).expect("secure root");
        let workspace = AsrWorkspace::new_in(root.path()).expect("workspace");
        let decoded = workspace.path().join("decoded.wav");
        create_private_file(&decoded, b"private speech").expect("decoded fixture");
        windows_private_acl::apply_and_verify(root.path(), true).expect("root DACL");
        windows_private_acl::apply_and_verify(workspace.path(), true).expect("workspace DACL");
        windows_private_acl::apply_and_verify(&decoded, false).expect("decoded DACL");
        windows_private_acl::apply_and_verify(&workspace.path().join(ASR_TEMP_MARKER), false)
            .expect("marker DACL");
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

        sweep_stale_workspaces(root.path(), Duration::ZERO, 16);

        assert!(workspace.exists(), "symlinked workspace must fail closed");
        assert_eq!(
            fs::read(&outside).expect("outside survives"),
            b"must survive"
        );
        fs::remove_file(workspace.join("decoded.wav")).expect("remove fixture symlink");
        fs::remove_file(workspace.join(ASR_TEMP_MARKER)).expect("remove fixture marker");
        fs::remove_dir(workspace).expect("remove fixture workspace");
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
        assert!(command.is_file(), "sidecar missing: {}", command.display());
        assert!(ogg.is_file(), "sample missing: {}", ogg.display());
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
            "unexpected SenseVoice transcript: {text:?}"
        );
    }
}
