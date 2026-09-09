use assert_cmd::cargo::cargo_bin_cmd;
use clap::Parser;
use lark_codex_bridge::{
    cli::{Cli, CodexCommand, Command as CliCommand, LogFormat},
    runtime::adoption::{THREAD_ADOPTION_SUPPORTED_PLATFORMS, ThreadAdoptionGate},
};
use predicates::prelude::*;
use serde_json::json;

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
fn adoption_status_is_machine_readable_and_fail_closed() {
    let availability = ThreadAdoptionGate::managed_sidecar().availability();
    let external = ThreadAdoptionGate::external_endpoint().availability();
    let assertion = cargo_bin_cmd!("lark-codex-bridge")
        .args(["codex", "adoption-status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("thread/resume").not())
        .stdout(predicate::str::contains("CODEX_HOME").not());
    let report: serde_json::Value = serde_json::from_slice(&assertion.get_output().stdout)
        .expect("adoption status should be JSON");

    assert_eq!(
        report,
        json!({
            "available": availability.is_available(),
            "classification": availability.code(),
            "guidance": availability.guidance(),
            "releaseAuthority": availability.release_authority(),
            "managedBackends": ["protocol_sidecar"],
            "supportedPlatforms": THREAD_ADOPTION_SUPPORTED_PLATFORMS,
            "externalEndpoint": {
                "available": external.is_available(),
                "classification": external.code(),
                "guidance": external.guidance(),
            },
            "requiresExplicitHandoff": true,
            "sharedEndpointIssue": 8,
        })
    );
}

#[test]
fn adoption_status_does_not_spawn_codex_or_read_its_profile() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let empty_path = temp.path().join("empty-path");
    let poisoned_home = temp.path().join("poisoned-codex-home");
    let missing_home = temp.path().join("missing-codex-home");
    std::fs::create_dir(&empty_path).expect("empty PATH directory");
    std::fs::create_dir(&poisoned_home).expect("poisoned CODEX_HOME directory");
    std::fs::write(
        poisoned_home.join("config.toml"),
        b"this is deliberately invalid Codex configuration = [",
    )
    .expect("poisoned Codex configuration");
    assert!(!missing_home.exists());

    for codex_home in [&poisoned_home, &missing_home] {
        cargo_bin_cmd!("lark-codex-bridge")
            .env("CODEX_HOME", codex_home)
            .env("PATH", &empty_path)
            .args(["codex", "adoption-status"])
            .assert()
            .success()
            .stdout(predicate::str::contains(format!(
                "\"classification\":\"{}\"",
                ThreadAdoptionGate::managed_sidecar().availability().code()
            )))
            .stdout(predicate::str::contains(
                "\"classification\":\"unavailable_shared_external_endpoint\"",
            ));
    }
}

#[test]
fn probe_reports_a_missing_sidecar_entrypoint_without_panicking() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let missing_entrypoint = temp.path().join("missing-sidecar.cjs");

    cargo_bin_cmd!("lark-codex-bridge")
        .args(["codex", "probe", "--entrypoint"])
        .arg(&missing_entrypoint)
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "configured Codex protocol sidecar is invalid",
        ))
        .stderr(predicate::str::contains("panicked").not());
}

#[test]
fn run_and_probe_report_the_same_sidecar_spawn_failure_before_runtime_ready() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let missing_entrypoint = temp.path().join("missing-sidecar.cjs");
    let encoded_entrypoint = serde_json::to_string(&missing_entrypoint.to_string_lossy())
        .expect("encode missing sidecar path as a TOML string");
    let config = temp.path().join("config.toml");
    std::fs::write(
        &config,
        format!(
            "owners = [\"ou_owner_cli_fail_closed\"]\n\n[codex.backend]\nmode = \"protocol_sidecar\"\nsidecar_entrypoint = {encoded_entrypoint}\n"
        ),
    )
    .expect("write runtime config");
    let expected = "configured Codex protocol sidecar is invalid";

    let run = cargo_bin_cmd!("lark-codex-bridge")
        .env("LARK_APP_ID", "cli_app_fail_closed")
        .env("LARK_APP_SECRET", "test-secret")
        .env("LARK_TENANT", "feishu")
        .env_remove("RUST_LOG")
        .args(["-v", "run", "--config"])
        .arg(&config)
        .output()
        .expect("run bridge with missing Codex sidecar");
    let probe = cargo_bin_cmd!("lark-codex-bridge")
        .env_remove("RUST_LOG")
        .args(["-v", "codex", "probe", "--entrypoint"])
        .arg(&missing_entrypoint)
        .output()
        .expect("probe missing Codex sidecar");
    let run_stderr = String::from_utf8_lossy(&run.stderr);
    let probe_stderr = String::from_utf8_lossy(&probe.stderr);

    assert!(!run.status.success());
    assert!(!probe.status.success());
    assert!(run.stdout.is_empty());
    assert!(probe.stdout.is_empty());
    assert!(run_stderr.contains(expected));
    assert!(probe_stderr.contains(expected));
    assert!(!run_stderr.contains("bridge runtime ready"));
    assert!(!run_stderr.contains(&*missing_entrypoint.to_string_lossy()));
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
        "--entrypoint",
        "/sensitive/customer/sidecar.cjs",
        "--codex-binary",
        "/sensitive/customer/codex",
    ])
    .expect("parse codex command");
    assert!(!format!("{codex:?}").contains("/sensitive/customer"));

    let sidecar = Cli::try_parse_from([
        "lark-codex-bridge",
        "codex",
        "sidecar-probe",
        "--node-binary",
        "/sensitive/customer/node",
        "--entrypoint",
        "/sensitive/customer/sidecar.cjs",
        "--codex-binary",
        "/sensitive/customer/codex",
        "--codex-home",
        "/sensitive/customer/home",
        "--codex-argument",
        "sensitive-wrapper-argument",
    ])
    .expect("parse sidecar probe command");
    let sidecar_debug = format!("{sidecar:?}");
    assert!(!sidecar_debug.contains("/sensitive/customer"));
    assert!(!sidecar_debug.contains("sensitive-wrapper-argument"));

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
fn sidecar_probe_uses_the_pinned_package_unless_a_binary_is_explicit() {
    let pinned = Cli::try_parse_from(["lark-codex-bridge", "codex", "probe"])
        .expect("parse pinned sidecar probe");
    assert!(matches!(
        pinned.command,
        CliCommand::Codex {
            command: CodexCommand::Probe {
                codex_binary: None,
                ..
            }
        }
    ));
    let pinned = Cli::try_parse_from(["lark-codex-bridge", "codex", "sidecar-probe"])
        .expect("parse pinned sidecar probe alias");
    assert!(matches!(
        pinned.command,
        CliCommand::Codex {
            command: CodexCommand::SidecarProbe {
                codex_binary: None,
                ..
            }
        }
    ));

    let overridden = Cli::try_parse_from([
        "lark-codex-bridge",
        "codex",
        "sidecar-probe",
        "--codex-binary",
        "reviewed-codex",
    ])
    .expect("parse overridden sidecar probe");
    assert!(matches!(
        overridden.command,
        CliCommand::Codex {
            command: CodexCommand::SidecarProbe {
                codex_binary: Some(ref binary),
                ..
            }
        } if binary == std::path::Path::new("reviewed-codex")
    ));
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
        .args(["-vv", "codex", "probe", "--entrypoint"])
        .arg(&missing_binary)
        .output()
        .expect("run verbose probe");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty(), "tracing must never use stdout");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Codex supervisor epoch starting"));
    assert!(stderr.contains("Codex supervisor degraded"));
    assert!(stderr.contains("error: configured Codex protocol sidecar is invalid"));
    assert!(!stderr.contains(secret_marker));
    assert!(!stderr.contains(&*missing_binary.to_string_lossy()));
}

#[test]
fn rust_log_overrides_verbose_defaults() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let missing_binary = temp.path().join("missing-codex");
    let output = cargo_bin_cmd!("lark-codex-bridge")
        .env("RUST_LOG", "error")
        .args(["-vv", "codex", "probe", "--entrypoint"])
        .arg(missing_binary)
        .output()
        .expect("run filtered probe");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("error: configured Codex protocol sidecar is invalid"));
    assert!(!stderr.contains("terminal tracing initialized"));
    assert!(!stderr.contains("Codex supervisor epoch starting"));
    assert!(!stderr.contains("Codex supervisor degraded"));
}

#[test]
fn invalid_rust_log_is_actionable_and_does_not_echo_its_value() {
    let secret_filter = "[SECRET_FILTER_CONTENT";
    let output = cargo_bin_cmd!("lark-codex-bridge")
        .env("RUST_LOG", secret_filter)
        .args(["codex", "probe", "--entrypoint", "missing-sidecar.cjs"])
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
        .args([
            "-v",
            "--log-format",
            "json",
            "codex",
            "probe",
            "--entrypoint",
        ])
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
