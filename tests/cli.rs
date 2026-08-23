use assert_cmd::cargo::cargo_bin_cmd;
use clap::Parser;
use lark_codex_bridge::{cli::Cli, runtime::adoption::ThreadAdoptionGate};
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
    let availability = ThreadAdoptionGate.availability();
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
            .stdout(predicate::str::contains(
                "\"classification\":\"unavailable_no_reliable_writer_release\"",
            ));
    }
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
