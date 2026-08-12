use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;

#[test]
fn help_describes_the_codex_command() {
    cargo_bin_cmd!("lark-codex-bridge")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("lark-codex-bridge"))
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
