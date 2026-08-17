//! First-run onboarding coordinator.
//!
//! The reference project let an operator run one command, complete the
//! QR/device flow, and continue straight into a runnable bridge. This module
//! restores that experience for the Rust bridge while keeping the existing
//! fail-closed config and credentials stores untouched:
//!
//! 1. Onboarding runs only when the default credentials or the default runtime
//!    config are missing. An explicit --config path is never touched and an
//!    existing default config is never overwritten.
//! 2. The creator `open_id` comes from the trusted registration response
//!    (`bot_hint`) or the application-owner API, never from the bot identity.
//!    When neither is available the operator is prompted on stdin.
//! 3. A profile-managed workspace is created under the platform data directory
//!    (`XDG_DATA_HOME` or `~/.local/share` on Unix, `LOCALAPPDATA` on Windows)
//!    with profile-local database and attachment-cache paths.
//! 4. Credentials reuse the existing store; the generated config and owner-hint
//!    sidecar are written with private permissions (0600 files / 0700 dirs on
//!    Unix) and atomic same-directory replacement. Concurrent first runs are
//!    serialized by an advisory lock so retries stay idempotent.

use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use fs2::FileExt;
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::config::{BridgeConfig, PathsSection, WorkspacePolicy, default_config_path};
use crate::lark::api::LarkApi;
use crate::lark::config::{LarkEndpoints, TenantBrand};
use crate::lark::credentials::{CredentialStore, FileCredentialStore, LarkCredentials};
use crate::lark::error::LarkError;
use crate::lark::http::LarkHttp;
use crate::lark::register::{RegistrationFlow, RegistrationOutcome};
use crate::lark::token::TenantTokenProvider;
use crate::runtime::policy::PlatformRoots;

/// Resolved profile paths for first-run onboarding.
///
/// Path derivation is injectable (`from_dirs`) so tests can exercise the
/// coordinator without mutating the process environment.
#[derive(Clone, Debug)]
struct OnboardingPaths {
    config_path: PathBuf,
    profile_path: PathBuf,
    workspace_dir: PathBuf,
    database_path: PathBuf,
    attachment_cache_path: PathBuf,
    lock_path: PathBuf,
}

impl OnboardingPaths {
    fn discover() -> Result<Self> {
        let config_path = default_config_path()
            .map_err(|_| anyhow!("unable to locate the platform configuration directory"))?;
        let config_dir = config_path
            .parent()
            .map(Path::to_path_buf)
            .ok_or_else(|| anyhow!("unable to locate the platform configuration directory"))?;
        let data_dir = default_data_dir()?;
        Ok(Self::from_dirs(&config_dir, &data_dir))
    }

    fn from_dirs(config_dir: &Path, data_dir: &Path) -> Self {
        let state_dir = data_dir.join("state");
        Self {
            config_path: config_dir.join("config.toml"),
            profile_path: data_dir.join("profile.toml"),
            workspace_dir: data_dir.join("workspace"),
            database_path: state_dir.join("bridge.sqlite3"),
            attachment_cache_path: state_dir.join("attachments"),
            lock_path: data_dir.join("onboarding.lock"),
        }
    }
}

fn default_data_dir() -> Result<PathBuf> {
    #[cfg(windows)]
    {
        let base = env::var_os("LOCALAPPDATA")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("unable to locate the platform data directory"))?;
        Ok(PathBuf::from(base).join("lark-codex-bridge"))
    }
    #[cfg(not(windows))]
    {
        if let Some(xdg) = env::var_os("XDG_DATA_HOME") {
            if !xdg.is_empty() {
                return Ok(PathBuf::from(xdg).join("lark-codex-bridge"));
            }
        }
        let home = env::var_os("HOME")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("unable to locate the platform data directory"))?;
        Ok(PathBuf::from(home)
            .join(".local")
            .join("share")
            .join("lark-codex-bridge"))
    }
}

/// What the coordinator must do for one run, derived only from presence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OnboardingAction {
    /// Both the default config and credentials are present.
    Ready,
    /// Config exists but credentials are missing; register credentials only.
    RegisterCredentialsOnly,
    /// Credentials exist but config is missing; resolve owner then generate.
    GenerateConfig,
    /// Both missing; register credentials then generate config.
    RegisterAndGenerateConfig,
}

fn decide(config_exists: bool, creds_present: bool) -> OnboardingAction {
    match (config_exists, creds_present) {
        (true, true) => OnboardingAction::Ready,
        (true, false) => OnboardingAction::RegisterCredentialsOnly,
        (false, true) => OnboardingAction::GenerateConfig,
        (false, false) => OnboardingAction::RegisterAndGenerateConfig,
    }
}

/// Async capabilities the coordinator needs, injectable for tests.
trait OnboardingOps: Send + Sync {
    fn load_credentials(&self) -> Result<Option<LarkCredentials>>;
    fn save_credentials(&self, creds: &LarkCredentials) -> Result<()>;
    fn register(&self) -> BoxFuture<'_, Result<(LarkCredentials, Option<String>)>>;
    fn app_creator(&self, creds: &LarkCredentials) -> BoxFuture<'_, Result<Option<String>>>;
    fn prompt_owner(&self) -> BoxFuture<'_, Result<String>>;
    fn platform_roots(&self) -> Result<PlatformRoots>;
}

/// Production implementation backed by the real credential store, QR device
/// flow, application-owner API, and stdin.
struct ProductionOps;

impl OnboardingOps for ProductionOps {
    fn load_credentials(&self) -> Result<Option<LarkCredentials>> {
        crate::lark::credentials::load_credentials().context("unable to load stored credentials")
    }

    fn save_credentials(&self, creds: &LarkCredentials) -> Result<()> {
        FileCredentialStore::at_default()
            .context("unable to locate the credentials file directory")?
            .save(creds)
            .map_err(|_| anyhow!("unable to store the credentials"))
    }

    fn register(&self) -> BoxFuture<'_, Result<(LarkCredentials, Option<String>)>> {
        Box::pin(run_device_flow())
    }

    fn app_creator(&self, creds: &LarkCredentials) -> BoxFuture<'_, Result<Option<String>>> {
        let creds = creds.clone();
        Box::pin(async move {
            let Ok(http) = LarkHttp::new(LarkEndpoints::for_tenant(creds.tenant)) else {
                return Ok(None);
            };
            let tokens = TenantTokenProvider::new(http.clone(), creds.clone());
            let api = LarkApi::new(http, tokens);
            match api.app_creator_id(&creds.app_id).await {
                Ok(creator) if valid_owner_id(&creator) => Ok(Some(creator)),
                Err(error @ LarkError::PermanentAuth { .. }) => Err(error.into()),
                Ok(_) | Err(_) => Ok(None),
            }
        })
    }

    fn prompt_owner(&self) -> BoxFuture<'_, Result<String>> {
        Box::pin(async move { read_owner_from_stdin() })
    }

    fn platform_roots(&self) -> Result<PlatformRoots> {
        PlatformRoots::discover()
            .map_err(|_| anyhow!("unable to determine safe platform filesystem roots"))
    }
}

/// Resolves the config path for a foreground run: an explicit --config is
/// returned untouched; otherwise onboarding runs (if needed) against the
/// default config and None is returned so the runtime loads it.
///
/// # Errors
///
/// Returns a content-free classification when onboarding cannot complete.
pub(crate) async fn resolve_run_config(config: Option<PathBuf>) -> Result<Option<PathBuf>> {
    if let Some(explicit) = config {
        return Ok(Some(explicit));
    }
    let paths = OnboardingPaths::discover()?;
    onboard_if_needed(&paths, &ProductionOps).await?;
    Ok(None)
}

async fn onboard_if_needed<O: OnboardingOps>(paths: &OnboardingPaths, ops: &O) -> Result<()> {
    if decide(paths.config_path.exists(), ops.load_credentials()?.is_some())
        == OnboardingAction::Ready
    {
        return Ok(());
    }
    // Serialize concurrent first runs; the decision is re-evaluated under the
    // lock so a loser observes the winner's completed profile and exits.
    let _lock = acquire_lock(&paths.lock_path)?;
    onboard(paths, ops).await
}

async fn onboard<O: OnboardingOps>(paths: &OnboardingPaths, ops: &O) -> Result<()> {
    let mut creds = ops.load_credentials()?;
    let mut fresh_hint = None;
    match decide(paths.config_path.exists(), creds.is_some()) {
        OnboardingAction::Ready => return Ok(()),
        OnboardingAction::RegisterCredentialsOnly => {
            let (new_creds, _) = ops.register().await?;
            return ops.save_credentials(&new_creds);
        }
        OnboardingAction::RegisterAndGenerateConfig => {
            let (new_creds, hint) = ops.register().await?;
            ops.save_credentials(&new_creds)?;
            creds = Some(new_creds);
            fresh_hint = hint;
        }
        OnboardingAction::GenerateConfig => {}
    }
    let creds = creds.ok_or_else(|| anyhow!("Lark credentials are unavailable"))?;
    let hint = match fresh_hint {
        Some(hint) => {
            if valid_owner_id(&hint) {
                persist_owner_hint(paths, &hint)?;
                Some(hint)
            } else {
                None
            }
        }
        None => load_owner_hint(paths)?,
    };
    let owner = resolve_owner(&creds, hint.as_deref(), ops).await?;
    let roots = ops.platform_roots()?;
    generate_and_write_config_with_roots(paths, &owner, &roots)
}

async fn resolve_owner<O: OnboardingOps>(
    creds: &LarkCredentials,
    hint: Option<&str>,
    ops: &O,
) -> Result<String> {
    if let Some(owner) = hint {
        if valid_owner_id(owner) {
            return Ok(owner.to_owned());
        }
    }
    if let Some(creator) = ops.app_creator(creds).await? {
        if valid_owner_id(&creator) {
            return Ok(creator);
        }
    }
    eprintln!("unable to discover the application creator; please enter the owner open_id manually");
    ops.prompt_owner().await
}

async fn run_device_flow() -> Result<(LarkCredentials, Option<String>)> {
    // Begin always targets the Feishu accounts host; the flow itself switches
    // to the Lark accounts host when the authorizing tenant is Lark-branded.
    let http = LarkHttp::new(LarkEndpoints::for_tenant(TenantBrand::Feishu))
        .context("unable to build the Lark HTTP client")?;
    let mut flow = RegistrationFlow::new(http, None);
    let challenge = flow
        .begin()
        .await
        .context("unable to start app registration")?;
    eprintln!("Open this URL in a browser to authorize the bridge app:");
    eprintln!("{}", challenge.url);
    loop {
        sleep(flow.interval()).await;
        match flow.poll_once().await {
            Ok(RegistrationOutcome::Pending) => {}
            Ok(RegistrationOutcome::SlowDown { new_interval }) => {
                eprintln!("registration server asked to slow down; polling every {new_interval}s");
            }
            Ok(RegistrationOutcome::Credentials { creds, bot_hint }) => {
                return Ok((creds, bot_hint));
            }
            Err(error) => return Err(error).context("app registration failed"),
        }
    }
}

fn read_owner_from_stdin() -> Result<String> {
    eprintln!("enter the owner open_id for this bridge (the account that authorized the app):");
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .map_err(|_| anyhow!("unable to read the owner open_id"))?;
    let owner = line.trim();
    if !valid_owner_id(owner) {
        bail!("the owner open_id is invalid");
    }
    Ok(owner.to_owned())
}

/// Mirrors the owner validation in `BridgeConfig`: non-empty, no surrounding
/// whitespace, and no ASCII whitespace anywhere in the identifier.
fn valid_owner_id(owner: &str) -> bool {
    !owner.is_empty()
        && owner.trim() == owner
        && !owner.bytes().any(|byte| byte.is_ascii_whitespace())
}

fn generate_and_write_config_with_roots(
    paths: &OnboardingPaths,
    owner: &str,
    roots: &PlatformRoots,
) -> Result<()> {
    if paths.config_path.exists() {
        bail!("bridge configuration already exists");
    }
    create_private_dir(&paths.workspace_dir)?;
    let mut config = build_config(paths, owner);
    config
        .validate_with_platform_roots(roots)
        .map_err(|_| anyhow!("generated bridge configuration is invalid"))?;
    write_config_atomic(paths, &config)
}

fn build_config(paths: &OnboardingPaths, owner: &str) -> BridgeConfig {
    BridgeConfig {
        owners: vec![owner.to_owned()],
        default_workspace: Some(paths.workspace_dir.clone()),
        workspace: WorkspacePolicy {
            allow_roots: vec![paths.workspace_dir.clone()],
            ..WorkspacePolicy::default()
        },
        paths: PathsSection {
            database: paths.database_path.clone(),
            attachment_cache: paths.attachment_cache_path.clone(),
        },
        ..BridgeConfig::default()
    }
}

/// Minimal on-disk representation of a generated config. Everything else
/// (concurrency, codex, network access) is left to the validated defaults.
#[derive(Serialize)]
struct GeneratedConfig {
    owners: Vec<String>,
    default_workspace: PathBuf,
    workspace: GeneratedWorkspace,
    paths: GeneratedPaths,
}

#[derive(Serialize)]
struct GeneratedWorkspace {
    allow_roots: Vec<PathBuf>,
}

#[derive(Serialize)]
struct GeneratedPaths {
    database: PathBuf,
    attachment_cache: PathBuf,
}

fn write_config_atomic(paths: &OnboardingPaths, config: &BridgeConfig) -> Result<()> {
    let generated = GeneratedConfig {
        owners: config.owners.clone(),
        default_workspace: config
            .default_workspace
            .clone()
            .ok_or_else(|| anyhow!("generated bridge configuration is missing a default workspace"))?,
        workspace: GeneratedWorkspace {
            allow_roots: config.workspace.allow_roots.clone(),
        },
        paths: GeneratedPaths {
            database: config.paths.database.clone(),
            attachment_cache: config.paths.attachment_cache.clone(),
        },
    };
    let text = toml::to_string(&generated)
        .map_err(|_| anyhow!("unable to encode bridge configuration"))?;
    write_atomic(&paths.config_path, text.as_bytes())
}

/// The persisted creator hint; a private-permission sidecar, not the authority
/// for access control (the generated config carries the real owner list).
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileFile {
    owner_hint: String,
}

fn persist_owner_hint(paths: &OnboardingPaths, hint: &str) -> Result<()> {
    let text = toml::to_string(&ProfileFile {
        owner_hint: hint.to_owned(),
    })
    .map_err(|_| anyhow!("unable to encode the owner hint"))?;
    write_atomic(&paths.profile_path, text.as_bytes())
}

fn load_owner_hint(paths: &OnboardingPaths) -> Result<Option<String>> {
    let text = match fs::read_to_string(&paths.profile_path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(anyhow!("unable to read the stored owner hint")),
    };
    let file: ProfileFile =
        toml::from_str(&text).map_err(|_| anyhow!("the stored owner hint is malformed"))?;
    Ok(valid_owner_id(&file.owner_hint).then_some(file.owner_hint))
}

/// Acquires an advisory lock file serializing concurrent first runs. The lock
/// is released when the returned file drops, so a panic or cancellation cannot
/// strand a half-onboarded profile.
fn acquire_lock(path: &Path) -> Result<fs::File> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            create_private_dir(parent)?;
        }
    }
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|_| anyhow!("unable to open the onboarding lock"))?;
    file.lock_exclusive()
        .map_err(|_| anyhow!("unable to acquire the onboarding lock"))?;
    Ok(file)
}

/// Writes bytes to path atomically via a same-directory temp file plus rename,
/// with private permissions. A failed replace leaves no temp file. On Windows
/// the rename target must be removed first, so the replace is not atomic there
/// (matching the credentials store).
fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            create_private_dir(parent)?;
        }
    }
    let temp = temp_path_for(path);
    write_private_file(&temp, bytes)?;
    // Windows cannot rename over an existing file; remove the target first
    // there (non-atomic), while Unix gets a true atomic replace.
    #[cfg(windows)]
    if path.exists() {
        fs::remove_file(path).map_err(|_| anyhow!("unable to replace the file"))?;
    }
    if fs::rename(&temp, path).is_err() {
        let _ = fs::remove_file(&temp);
        return Err(anyhow!("unable to replace the file"));
    }
    Ok(())
}

fn temp_path_for(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map_or_else(|| OsString::from("file"), OsStr::to_os_string);
    name.push(".tmp");
    path.with_file_name(name)
}

#[cfg(unix)]
fn create_private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
        .map_err(|_| anyhow!("unable to create a private directory"))
}

#[cfg(not(unix))]
fn create_private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).map_err(|_| anyhow!("unable to create a private directory"))
}

#[cfg(unix)]
fn write_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| anyhow!("unable to write a private file"))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| anyhow!("unable to write a private file"))?;
    // mode only applies on creation; a pre-existing loose temp file must be
    // tightened explicitly.
    file.set_permissions(fs::Permissions::from_mode(0o600))
        .map_err(|_| anyhow!("unable to secure a private file"))
}

#[cfg(not(unix))]
fn write_private_file(path: &Path, bytes: &[u8]) -> Result<()> {
    fs::write(path, bytes).map_err(|_| anyhow!("unable to write a private file"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::SecretString;
    use tempfile::TempDir;

    fn dirs(base: &Path) -> OnboardingPaths {
        OnboardingPaths::from_dirs(&base.join("config"), &base.join("data"))
    }

    fn credentials_path(base: &Path) -> PathBuf {
        base.join("config").join("credentials.toml")
    }

    fn injected_roots(base: &Path) -> PlatformRoots {
        let home = base.join("home");
        let temp = base.join("temp");
        let system = base.join("system");
        let desktop = home.join("Desktop");
        let downloads = home.join("Downloads");
        for path in [&home, &temp, &system, &desktop, &downloads] {
            fs::create_dir_all(path).expect("injected root should be created");
        }
        PlatformRoots::new(&home, vec![temp], vec![system], vec![desktop, downloads])
            .expect("injected roots should canonicalize")
    }

    fn test_creds(app_id: &str) -> LarkCredentials {
        LarkCredentials::new(
            app_id.to_owned(),
            SecretString::from("secret"),
            TenantBrand::Feishu,
        )
    }

    #[test]
    fn decide_covers_all_onboarding_entry_points() {
        assert_eq!(decide(true, true), OnboardingAction::Ready);
        assert_eq!(decide(true, false), OnboardingAction::RegisterCredentialsOnly);
        assert_eq!(decide(false, true), OnboardingAction::GenerateConfig);
        assert_eq!(
            decide(false, false),
            OnboardingAction::RegisterAndGenerateConfig
        );
    }

    #[test]
    fn owner_validation_rejects_empty_and_whitespace() {
        assert!(valid_owner_id("ou_owner_123"));
        assert!(!valid_owner_id(""));
        assert!(!valid_owner_id(" ou_owner"));
        assert!(!valid_owner_id("ou_owner "));
        assert!(!valid_owner_id("ou owner"));
        assert!(!valid_owner_id("ou_owner\t"));
    }

    #[test]
    fn generated_config_passes_policy_with_data_dir_workspace() {
        let scratch = TempDir::new().expect("scratch dir");
        let roots = injected_roots(scratch.path());
        let paths = dirs(scratch.path());
        fs::create_dir_all(&paths.workspace_dir).expect("workspace should exist");

        let mut config = build_config(&paths, "ou_creator_123");
        config
            .validate_with_platform_roots(&roots)
            .expect("data-dir workspace must pass policy");

        assert_eq!(config.owners, vec!["ou_creator_123".to_owned()]);
        assert!(config.default_workspace.is_some());
        assert_eq!(config.workspace.allow_roots.len(), 1);
        assert!(config.paths.database.is_absolute());
        assert!(config.paths.attachment_cache.is_absolute());
    }

    #[test]
    fn generated_config_passes_policy_with_workspace_under_home() {
        let scratch = TempDir::new().expect("scratch dir");
        let roots = injected_roots(scratch.path());
        let paths = OnboardingPaths::from_dirs(
            &scratch.path().join("config"),
            &scratch.path().join("home").join("data"),
        );
        fs::create_dir_all(&paths.workspace_dir).expect("workspace should exist");

        let mut config = build_config(&paths, "ou_creator_123");
        config
            .validate_with_platform_roots(&roots)
            .expect("under-home workspace must pass policy");

        assert_eq!(config.owners, vec!["ou_creator_123".to_owned()]);
        assert!(config.default_workspace.is_some());
    }

    #[test]
    fn owner_hint_round_trips_without_partial_state() {
        let scratch = TempDir::new().expect("scratch dir");
        let paths = dirs(scratch.path());

        persist_owner_hint(&paths, "ou_creator_123").expect("hint should persist");
        assert_eq!(
            load_owner_hint(&paths).expect("hint should load"),
            Some("ou_creator_123".to_owned())
        );
        assert!(!temp_path_for(&paths.profile_path).exists());
    }

    #[cfg(unix)]
    #[test]
    fn owner_hint_file_has_private_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let scratch = TempDir::new().expect("scratch dir");
        let paths = dirs(scratch.path());
        persist_owner_hint(&paths, "ou_creator_123").expect("hint should persist");
        let mode = fs::metadata(&paths.profile_path)
            .expect("profile should exist")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn config_write_is_atomic_and_idempotent_guard() {
        let scratch = TempDir::new().expect("scratch dir");
        let roots = injected_roots(scratch.path());
        let paths = dirs(scratch.path());

        generate_and_write_config_with_roots(&paths, "ou_creator_123", &roots)
            .expect("config should generate");

        let text = fs::read_to_string(&paths.config_path).expect("config should exist");
        let parsed: BridgeConfig = toml::from_str(&text).expect("config should parse");
        assert_eq!(parsed.owners, vec!["ou_creator_123".to_owned()]);
        assert!(!temp_path_for(&paths.config_path).exists());

        // A second generation must refuse to overwrite the existing config.
        let error = generate_and_write_config_with_roots(&paths, "ou_other_456", &roots)
            .expect_err("existing config must not be overwritten");
        assert!(!format!("{error:#}").contains("ou_other_456"));
        assert_eq!(
            fs::read_to_string(&paths.config_path).expect("config unchanged"),
            text
        );
    }

    #[test]
    fn lock_excludes_a_second_acquire() {
        let scratch = TempDir::new().expect("scratch dir");
        let paths = dirs(scratch.path());
        let lock = acquire_lock(&paths.lock_path).expect("lock should acquire");

        let contender = paths.lock_path.clone();
        let held = std::thread::spawn(move || {
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(contender)
                .expect("contender file should open");
            file.try_lock_exclusive().is_err()
        })
        .join()
        .expect("contender thread should join");
        assert!(held, "the second acquire must be excluded");

        drop(lock);
    }

    struct FakeOps {
        store: FileCredentialStore,
        register: Option<(LarkCredentials, Option<String>)>,
        creator: Option<String>,
        creator_error: bool,
        prompt: String,
        roots: PlatformRoots,
    }

    impl OnboardingOps for FakeOps {
        fn load_credentials(&self) -> Result<Option<LarkCredentials>> {
            self.store.load().context("unable to load stored credentials")
        }

        fn save_credentials(&self, creds: &LarkCredentials) -> Result<()> {
            self.store
                .save(creds)
                .map_err(|_| anyhow!("unable to store the credentials"))
        }

        fn register(&self) -> BoxFuture<'_, Result<(LarkCredentials, Option<String>)>> {
            let out = self
                .register
                .clone()
                .ok_or_else(|| anyhow!("register should not be called"));
            Box::pin(async move { out })
        }

        fn app_creator(&self, _creds: &LarkCredentials) -> BoxFuture<'_, Result<Option<String>>> {
            let out = self.creator.clone();
            let error = self.creator_error;
            Box::pin(async move {
                if error {
                    Err(anyhow!("creator lookup failed permanently"))
                } else {
                    Ok(out)
                }
            })
        }

        fn prompt_owner(&self) -> BoxFuture<'_, Result<String>> {
            let out = self.prompt.clone();
            Box::pin(async move { Ok(out) })
        }

        fn platform_roots(&self) -> Result<PlatformRoots> {
            Ok(self.roots.clone())
        }
    }

    fn fake_ops(
        base: &Path,
        register: Option<&str>,
        creator: Option<&str>,
    ) -> FakeOps {
        FakeOps {
            store: FileCredentialStore::new(credentials_path(base)),
            register: register.map(|id| (test_creds("cli_new"), Some(id.to_owned()))),
            creator: creator.map(str::to_owned),
            creator_error: false,
            prompt: "ou_prompted".to_owned(),
            roots: injected_roots(base),
        }
    }

    #[tokio::test]
    async fn new_registration_onboards_creator_and_credentials() {
        let scratch = TempDir::new().expect("scratch dir");
        let paths = dirs(scratch.path());
        let ops = fake_ops(scratch.path(), Some("ou_creator_123"), None);

        onboard(&paths, &ops).await.expect("onboarding should succeed");

        let text = fs::read_to_string(&paths.config_path).expect("config should exist");
        let parsed: BridgeConfig = toml::from_str(&text).expect("config should parse");
        assert_eq!(parsed.owners, vec!["ou_creator_123".to_owned()]);
        assert!(ops
            .store
            .load()
            .expect("credentials should load")
            .is_some());
        assert_eq!(
            load_owner_hint(&paths).expect("hint should load"),
            Some("ou_creator_123".to_owned())
        );
    }

    #[tokio::test]
    async fn already_registered_app_uses_creator_api() {
        let scratch = TempDir::new().expect("scratch dir");
        let paths = dirs(scratch.path());
        let ops = fake_ops(scratch.path(), None, Some("ou_creator_api"));
        ops.store
            .save(&test_creds("cli_existing"))
            .expect("credentials should persist");

        onboard(&paths, &ops).await.expect("onboarding should succeed");

        let text = fs::read_to_string(&paths.config_path).expect("config should exist");
        let parsed: BridgeConfig = toml::from_str(&text).expect("config should parse");
        assert_eq!(parsed.owners, vec!["ou_creator_api".to_owned()]);
    }

    #[tokio::test]
    async fn idempotent_rerun_is_a_noop_when_both_exist() {
        let scratch = TempDir::new().expect("scratch dir");
        let roots = injected_roots(scratch.path());
        let paths = dirs(scratch.path());
        generate_and_write_config_with_roots(&paths, "ou_creator_123", &roots)
            .expect("config should generate");
        let before = fs::read_to_string(&paths.config_path).expect("config should exist");
        let ops = fake_ops(scratch.path(), None, None);
        ops.store
            .save(&test_creds("cli_existing"))
            .expect("credentials should persist");

        onboard(&paths, &ops).await.expect("rerun should be a noop");

        assert_eq!(
            fs::read_to_string(&paths.config_path).expect("config unchanged"),
            before
        );
    }

    #[tokio::test]
    async fn invalid_existing_config_is_never_overwritten() {
        let scratch = TempDir::new().expect("scratch dir");
        let paths = dirs(scratch.path());
        let ops = fake_ops(scratch.path(), None, Some("ou_creator_api"));
        ops.store
            .save(&test_creds("cli_existing"))
            .expect("credentials should persist");
        fs::write(&paths.config_path, "this is not valid toml").expect("invalid config written");
        let before = fs::read_to_string(&paths.config_path).expect("read invalid config");

        onboard(&paths, &ops)
            .await
            .expect("an existing config must never be overwritten");

        assert_eq!(
            fs::read_to_string(&paths.config_path).expect("config unchanged"),
            before
        );
    }

    #[tokio::test]
    async fn register_credentials_only_leaves_existing_config_untouched() {
        let scratch = TempDir::new().expect("scratch dir");
        let paths = dirs(scratch.path());
        fs::create_dir_all(scratch.path().join("config")).expect("config dir");
        fs::write(&paths.config_path, "owners = [\"ou_existing\"]\n").expect("config written");
        let before = fs::read_to_string(&paths.config_path).expect("read config");
        let ops = fake_ops(scratch.path(), Some("ou_unused"), None);

        onboard(&paths, &ops)
            .await
            .expect("credentials-only onboarding should succeed");

        assert_eq!(
            fs::read_to_string(&paths.config_path).expect("config unchanged"),
            before
        );
        assert!(ops
            .store
            .load()
            .expect("credentials should load")
            .is_some());
        assert_eq!(load_owner_hint(&paths).expect("hint should load"), None);
    }

    #[tokio::test]
    async fn resolve_owner_surfaces_definitive_creator_failure() {
        let scratch = TempDir::new().expect("scratch dir");
        let mut ops = fake_ops(scratch.path(), None, None);
        ops.creator_error = true;

        let error = resolve_owner(&test_creds("cli_existing"), None, &ops)
            .await
            .expect_err("a definitive creator failure must surface");

        assert!(format!("{error:#}").contains("creator lookup failed permanently"));
    }
}
