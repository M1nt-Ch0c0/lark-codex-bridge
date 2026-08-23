//! Local speech-to-text sidecar used for Feishu/Lark audio parts.
//!
//! The bridge never links ONNX or Python ASR. A configured external command
//! receives a 16 kHz WAV path as its last argument and must print a transcript
//! on stdout. ffmpeg is used only to decode Feishu Opus/Ogg into that WAV.

use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime};

use tempfile::{Builder as TempDirBuilder, TempDir};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::time::timeout;

use crate::config::AsrSection;
use crate::lark::normalize::normalize_transcript;
use crate::limits::{ASR_FFMPEG_TIMEOUT, ASR_SIDECAR_TIMEOUT};

const ASR_TEMP_PREFIX: &str = "lark-codex-bridge-asr-";
const ASR_TEMP_MARKER: &str = ".lark-codex-bridge-asr-v1";
const ASR_TEMP_MARKER_CONTENTS: &[u8] = b"lark-codex-bridge private ASR workspace v1\n";
const ASR_STALE_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const ASR_STALE_SCAN_LIMIT: usize = 256;
const ASR_CLEANUP_ATTEMPTS: usize = 20;
const ASR_CLEANUP_RETRY: Duration = Duration::from_millis(50);
static ASR_STALE_SWEEP: OnceLock<()> = OnceLock::new();

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
    if duration_ms.is_some_and(|duration| duration > config.max_duration_ms) {
        return Err(AsrError::TooLong);
    }
    if !config.is_configured() {
        return Err(AsrError::SidecarMissing);
    }

    let temp_root = std::env::temp_dir();
    ASR_STALE_SWEEP.get_or_init(|| {
        sweep_stale_workspaces(&temp_root, ASR_STALE_AGE, ASR_STALE_SCAN_LIMIT);
    });
    transcribe_file_in(config, input, &temp_root).await
}

async fn transcribe_file_in(
    config: &AsrSection,
    input: &Path,
    temp_root: &Path,
) -> Result<String, AsrError> {
    let workspace = AsrWorkspace::new_in(temp_root)?;
    let wav_path = workspace.path().join("decoded.wav");
    let decode = decode_to_wav(&config.ffmpeg, input, &wav_path, config.max_duration_ms).await;
    let result = match decode {
        Ok(()) => {
            let command = config.command.as_ref().ok_or(AsrError::SidecarMissing)?;
            run_sidecar(config, command, &wav_path).await
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
        let directory = TempDirBuilder::new()
            .prefix(ASR_TEMP_PREFIX)
            .tempdir_in(root)
            .map_err(|_| AsrError::TemporaryStorage)?;
        fs::write(
            directory.path().join(ASR_TEMP_MARKER),
            ASR_TEMP_MARKER_CONTENTS,
        )
        .map_err(|_| AsrError::TemporaryStorage)?;
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
        } else if fs::remove_dir_all(path).is_err() {
            tracing::warn!("private ASR workspace cleanup could not be scheduled");
        }
    }
}

async fn cleanup_workspace(path: PathBuf) {
    for attempt in 0..ASR_CLEANUP_ATTEMPTS {
        match fs::remove_dir_all(&path) {
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
    for entry in entries.take(scan_limit).flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(ASR_TEMP_PREFIX) {
            continue;
        }
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
        let decoded = path.join("decoded.wav");
        if decoded.exists() && fs::remove_file(&decoded).is_err() {
            tracing::warn!("stale private ASR workspace cleanup failed");
            continue;
        }
        if fs::remove_file(path.join(ASR_TEMP_MARKER)).is_err() || fs::remove_dir(path).is_err() {
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
    let Ok(mut marker) = File::open(&marker_path) else {
        return false;
    };
    let mut contents = Vec::with_capacity(ASR_TEMP_MARKER_CONTENTS.len());
    if marker
        .by_ref()
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

async fn decode_to_wav(
    ffmpeg: &Path,
    input: &Path,
    output: &Path,
    max_duration_ms: u64,
) -> Result<(), AsrError> {
    let decode_limit_ms = max_duration_ms.saturating_add(1);
    let decode_limit = format!("{}.{:03}", decode_limit_ms / 1_000, decode_limit_ms % 1_000);
    let mut child = Command::new(ffmpeg)
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
        .arg(output)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| AsrError::UnsupportedCodec)?;
    let Ok(Ok(status)) = timeout(ASR_FFMPEG_TIMEOUT, child.wait()).await else {
        let _ = child.kill().await;
        return Err(AsrError::UnsupportedCodec);
    };
    if !status.success() || !output.is_file() {
        let _ = child.kill().await;
        return Err(AsrError::UnsupportedCodec);
    }
    if wav_exceeds_duration(output, max_duration_ms)? {
        Err(AsrError::TooLong)
    } else {
        Ok(())
    }
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
) -> Result<String, AsrError> {
    let mut process = Command::new(command);
    process
        .args(&config.args)
        .arg(wav_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = process.spawn().map_err(|_| AsrError::SidecarFailed)?;
    let mut stdout = child.stdout.take().ok_or(AsrError::SidecarFailed)?;
    let execution = async {
        let stdout_limit = config
            .max_transcript_bytes
            .saturating_mul(4)
            .saturating_add(4 * 1024);
        let bytes = read_bounded_stdout(&mut stdout, stdout_limit).await?;
        let status = child.wait().await.map_err(|_| AsrError::SidecarFailed)?;
        Ok::<_, AsrError>((bytes, status))
    };
    let (bytes, status) = match timeout(ASR_SIDECAR_TIMEOUT, execution).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(error);
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(AsrError::SidecarFailed);
        }
    };
    if !status.success() {
        return Err(AsrError::SidecarFailed);
    }
    let stdout = String::from_utf8_lossy(&bytes);
    parse_sidecar_transcript(&stdout, config.max_transcript_bytes)
}

fn parse_sidecar_transcript(stdout: &str, max_bytes: usize) -> Result<String, AsrError> {
    // sherpa-onnx SenseVoice commonly emits one JSON object with a `text`
    // field. Parsing that envelope in Rust lets the documented wrapper `exec`
    // the recognizer directly, so process cancellation cannot orphan a
    // descendant shell pipeline. Plain-text sidecars remain supported.
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

async fn read_bounded_stdout(
    stdout: &mut tokio::process::ChildStdout,
    max_bytes: usize,
) -> Result<Vec<u8>, AsrError> {
    let mut bytes = Vec::with_capacity(max_bytes.min(8 * 1024));
    let mut chunk = [0_u8; 8 * 1024];
    loop {
        let read = stdout
            .read(&mut chunk)
            .await
            .map_err(|_| AsrError::SidecarFailed)?;
        if read == 0 {
            return Ok(bytes);
        }
        if bytes.len().saturating_add(read) > max_bytes {
            return Err(AsrError::TranscriptTooLarge);
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs2::FileExt;
    use std::fs::{self, File, OpenOptions};
    use tempfile::tempdir;
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
                r#"out=""; for arg in "$@"; do out=$arg; done; cp "{}" "$out"; printf ready > "{}"; exec sleep 60"#,
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
        let task =
            tokio::spawn(async move { transcribe_file_in(&config, &input, &task_root).await });

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
                    & 0o077,
                0,
                "decoded speech directory must be owner-only"
            );
        }

        task.abort();
        let _ = task.await;
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
    }

    #[test]
    fn stale_private_workspaces_are_swept_without_touching_other_entries() {
        let root = tempdir().expect("tempdir");
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
