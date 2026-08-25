use assert_cmd::cargo::cargo_bin_cmd;
use clap::Parser;
use lark_codex_bridge::cli::{Cli, LogFormat};
use predicates::prelude::*;

#[test]
fn help_describes_the_codex_command() {
    cargo_bin_cmd!("lark-codex-bridge")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("lark-codex-bridge"))
        .stdout(predicate::str::contains("run"))
        .stdout(predicate::str::contains("codex"));
}

#[test]
fn version_matches_the_package_version() {
    cargo_bin_cmd!("lark-codex-bridge")
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains("0.1.0-alpha.1"));
}

#[test]
fn probe_reports_a_missing_codex_binary_without_panicking() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let missing_binary = temp.path().join("missing-codex");

    cargo_bin_cmd!("lark-codex-bridge")
        .args(["codex", "probe", "--binary"])
        .arg(&missing_binary)
        .assert()
        .failure()
        .stderr(predicate::str::contains("unable to run Codex binary"))
        .stderr(predicate::str::contains("panicked").not());
}

#[test]
fn parsed_cli_debug_redacts_secrets_ids_and_absolute_paths() {
    let auth = Cli::try_parse_from([
        "lark-codex-bridge",
        "lark",
        "auth",
        "register",
        "--app-id",
        "sensitive-app-id",
        "--app-secret",
        "sensitive-app-secret",
        "--tenant",
        "feishu",
    ])
    .expect("parse auth command");
    let auth_debug = format!("{auth:?}");
    assert!(!auth_debug.contains("sensitive"));

    let codex = Cli::try_parse_from([
        "lark-codex-bridge",
        "codex",
        "probe",
        "--binary",
        "/sensitive/customer/codex",
    ])
    .expect("parse codex command");
    assert!(!format!("{codex:?}").contains("/sensitive/customer"));

    let run = Cli::try_parse_from([
        "lark-codex-bridge",
        "run",
        "--config",
        "/sensitive/customer/bridge.toml",
    ])
    .expect("parse run command");
    let run_debug = format!("{run:?}");
    assert!(run_debug.contains("config_configured"));
    assert!(!run_debug.contains("/sensitive/customer"));
}

#[test]
fn verbosity_and_log_format_are_global_cli_options() {
    let before = Cli::try_parse_from([
        "lark-codex-bridge",
        "-vv",
        "--log-format",
        "json",
        "codex",
        "probe",
    ])
    .expect("global options before subcommand");
    assert_eq!(before.verbose, 2);
    assert_eq!(before.log_format, LogFormat::Json);

    let after = Cli::try_parse_from([
        "lark-codex-bridge",
        "codex",
        "probe",
        "-v",
        "--log-format",
        "human",
    ])
    .expect("global options after subcommand");
    assert_eq!(after.verbose, 1);
    assert_eq!(after.log_format, LogFormat::Human);
}

#[test]
fn verbose_diagnostics_use_stderr_and_redact_configured_paths() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let secret_marker = "SECRET_APP_TOKEN_AND_PROMPT_CONTENT";
    let missing_binary = temp.path().join(secret_marker).join("missing-codex");
    let output = cargo_bin_cmd!("lark-codex-bridge")
        .env_remove("RUST_LOG")
        .args(["-vv", "codex", "probe", "--binary"])
        .arg(&missing_binary)
        .output()
        .expect("run verbose probe");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty(), "tracing must never use stdout");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Codex supervisor epoch starting"));
    assert!(stderr.contains("Codex supervisor degraded"));
    assert!(stderr.contains("error: unable to run Codex binary"));
    assert!(!stderr.contains(secret_marker));
    assert!(!stderr.contains(&*missing_binary.to_string_lossy()));
}

#[test]
fn rust_log_overrides_verbose_defaults() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let missing_binary = temp.path().join("missing-codex");
    let output = cargo_bin_cmd!("lark-codex-bridge")
        .env("RUST_LOG", "error")
        .args(["-vv", "codex", "probe", "--binary"])
        .arg(missing_binary)
        .output()
        .expect("run filtered probe");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("error: unable to run Codex binary"));
    assert!(!stderr.contains("terminal tracing initialized"));
    assert!(!stderr.contains("Codex supervisor epoch starting"));
    assert!(!stderr.contains("Codex supervisor degraded"));
}

#[test]
fn invalid_rust_log_is_actionable_and_does_not_echo_its_value() {
    let secret_filter = "[SECRET_FILTER_CONTENT";
    let output = cargo_bin_cmd!("lark-codex-bridge")
        .env("RUST_LOG", secret_filter)
        .args(["codex", "probe", "--binary", "missing-codex"])
        .output()
        .expect("run invalid filter probe");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid RUST_LOG filter"));
    assert!(stderr.contains("lark_codex_bridge=debug"));
    assert!(!stderr.contains(secret_filter));
}

#[test]
fn json_log_format_is_structured_and_still_stderr_only() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let missing_binary = temp.path().join("missing-codex");
    let output = cargo_bin_cmd!("lark-codex-bridge")
        .env_remove("RUST_LOG")
        .args(["-v", "--log-format", "json", "codex", "probe", "--binary"])
        .arg(missing_binary)
        .output()
        .expect("run JSON probe");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(r#""level":"INFO""#));
    assert!(stderr.contains(r#""message":"CLI command started""#));
    assert!(stderr.contains(r#""command":"codex""#));
}
