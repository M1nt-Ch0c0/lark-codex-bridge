//! Local speech-to-text sidecar used for Feishu/Lark audio parts.
//!
//! The bridge never links ONNX or Python ASR. A configured external command
//! receives a 16 kHz WAV path as its last argument and must print a transcript
//! on stdout. ffmpeg is used only to decode Feishu Opus/Ogg into that WAV.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::process::Command;
use tokio::time::timeout;
use uuid::Uuid;

use crate::config::AsrSection;
use crate::lark::normalize::normalize_transcript;
use crate::limits::{ASR_FFMPEG_TIMEOUT, ASR_SIDECAR_TIMEOUT};

/// Why local transcription could not produce text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AsrError {
    /// No sidecar command is configured.
    SidecarMissing,
    /// Declared duration exceeds [`AsrSection::max_duration_ms`].
    TooLong,
    /// ffmpeg could not decode the inbound audio.
    UnsupportedCodec,
    /// Sidecar could not be spawned or exited unsuccessfully.
    SidecarFailed,
    /// Sidecar succeeded but stdout was empty after trimming.
    EmptyTranscript,
    /// Downloaded audio exceeded the attachment byte cap.
    Oversize,
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
            Self::Oversize => "oversize",
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
            Self::Oversize => "audio is too large to transcribe",
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
    let command = config.command.as_ref().ok_or(AsrError::SidecarMissing)?;
    if command.as_os_str().is_empty() {
        return Err(AsrError::SidecarMissing);
    }

    let wav_path = unique_wav_path();
    let decode = decode_to_wav(&config.ffmpeg, input, &wav_path).await;
    let transcript = match decode {
        Ok(()) => run_sidecar(config, command, &wav_path).await,
        Err(error) => Err(error),
    };
    let _ = std::fs::remove_file(&wav_path);
    transcript
}

fn unique_wav_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "lark-codex-bridge-asr-{}.wav",
        Uuid::new_v4().simple()
    ))
}

async fn decode_to_wav(ffmpeg: &Path, input: &Path, output: &Path) -> Result<(), AsrError> {
    let mut child = Command::new(ffmpeg)
        .arg("-hide_banner")
        .arg("-nostdin")
        .arg("-y")
        .arg("-loglevel")
        .arg("error")
        .arg("-i")
        .arg(input)
        .arg("-ac")
        .arg("1")
        .arg("-ar")
        .arg("16000")
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
    if status.success() && output.is_file() {
        Ok(())
    } else {
        let _ = child.kill().await;
        Err(AsrError::UnsupportedCodec)
    }
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
    let Ok(Ok(output)) = timeout(ASR_SIDECAR_TIMEOUT, process.output()).await else {
        return Err(AsrError::SidecarFailed);
    };
    if !output.status.success() {
        return Err(AsrError::SidecarFailed);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    normalize_transcript(&stdout, config.max_transcript_bytes).ok_or(AsrError::EmptyTranscript)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

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
        let dir = tempdir().expect("tempdir");
        let marker = dir.path().join("marker.txt");
        let ffmpeg = stub(
            dir.path(),
            "ffmpeg",
            r#"out=""; for arg in "$@"; do out=$arg; done; : > "$out""#,
            "@echo off\r\n:loop\r\nif \"%~2\"==\"\" (\r\ntype nul > \"%~1\"\r\nexit /b 0\r\n)\r\nshift\r\ngoto loop\r\n",
        );
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
        let dir = tempdir().expect("tempdir");
        let ffmpeg = stub(
            dir.path(),
            "ffmpeg",
            r#"out=""; for arg in "$@"; do out=$arg; done; : > "$out""#,
            "@echo off\r\n:loop\r\nif \"%~2\"==\"\" (\r\ntype nul > \"%~1\"\r\nexit /b 0\r\n)\r\nshift\r\ngoto loop\r\n",
        );
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
        let dir = tempdir().expect("tempdir");
        let ffmpeg = stub(
            dir.path(),
            "ffmpeg",
            r#"out=""; for arg in "$@"; do out=$arg; done; : > "$out""#,
            "@echo off\r\n:loop\r\nif \"%~2\"==\"\" (\r\ntype nul > \"%~1\"\r\nexit /b 0\r\n)\r\nshift\r\ngoto loop\r\n",
        );
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
}
