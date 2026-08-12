//! App credential storage with environment overrides and redacted secrets.
//!
//! The default on-disk location follows platform convention:
//!
//! - Linux/BSD: `$XDG_CONFIG_HOME/lark-codex-bridge/credentials.toml`,
//!   falling back to `~/.config/lark-codex-bridge/credentials.toml`;
//! - macOS: same `XDG_CONFIG_HOME`/`~/.config` rule (no Apple directories,
//!   matching the rest of this CLI's cross-platform behavior);
//! - Windows: `%APPDATA%\lark-codex-bridge\credentials.toml`.
//!
//! `LARK_CREDENTIALS_FILE` overrides the path. `LARK_APP_ID`,
//! `LARK_APP_SECRET`, and `LARK_TENANT` override the file entirely so tests
//! and the smoke gate never touch real state.

use std::env;
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

use super::config::TenantBrand;
use super::error::LarkError;

/// App credentials for one tenant.
///
/// `Debug` is manually implemented and never prints the secret.
#[derive(Clone)]
pub struct LarkCredentials {
    /// The app ID (`cli_...`).
    pub app_id: String,
    /// The app secret, held in a [`SecretString`] so it is redacted and
    /// zeroized on drop.
    pub app_secret: SecretString,
    /// The tenant the app belongs to.
    pub tenant: TenantBrand,
}

impl LarkCredentials {
    /// Builds credentials from parts.
    #[must_use]
    pub fn new(app_id: String, app_secret: SecretString, tenant: TenantBrand) -> Self {
        Self {
            app_id,
            app_secret,
            tenant,
        }
    }
}

impl fmt::Debug for LarkCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LarkCredentials")
            .field("app_id", &self.app_id)
            .field("app_secret", &"<redacted>")
            .field("tenant", &self.tenant)
            .finish()
    }
}

/// Persistence boundary for app credentials.
pub trait CredentialStore {
    /// Loads stored credentials, or `Ok(None)` when none exist.
    ///
    /// # Errors
    ///
    /// Returns an error when credentials exist but cannot be read or parsed.
    fn load(&self) -> Result<Option<LarkCredentials>, LarkError>;

    /// Persists credentials.
    ///
    /// # Errors
    ///
    /// Returns an error when the store cannot be written.
    fn save(&self, creds: &LarkCredentials) -> Result<(), LarkError>;
}

/// Reads credentials from `LARK_APP_ID` / `LARK_APP_SECRET` / `LARK_TENANT`.
///
/// All three variables must be set together; a partial set is an error so a
/// half-configured environment fails loudly instead of silently falling back
/// to the file store.
pub struct EnvCredentialsStore;

impl EnvCredentialsStore {
    /// Reads credentials from an arbitrary lookup function instead of the
    /// process environment (the store itself reads the environment).
    ///
    /// # Errors
    ///
    /// Returns an error when the override set is partial or malformed.
    pub fn from_lookup(
        mut get: impl FnMut(&str) -> Option<String>,
    ) -> Result<Option<LarkCredentials>, LarkError> {
        let app_id = get("LARK_APP_ID");
        let app_secret = get("LARK_APP_SECRET");
        let tenant = get("LARK_TENANT");
        match (app_id, app_secret, tenant) {
            (None, None, None) => Ok(None),
            (Some(app_id), Some(app_secret), Some(tenant)) => {
                if app_id.is_empty() || app_secret.is_empty() {
                    return Err(LarkError::protocol("empty LARK_APP_ID or LARK_APP_SECRET"));
                }
                let tenant = tenant
                    .parse::<TenantBrand>()
                    .map_err(|_| LarkError::protocol("LARK_TENANT must be feishu or lark"))?;
                Ok(Some(LarkCredentials::new(
                    app_id,
                    SecretString::from(app_secret),
                    tenant,
                )))
            }
            _ => Err(LarkError::protocol(
                "LARK_APP_ID, LARK_APP_SECRET, and LARK_TENANT must be set together",
            )),
        }
    }
}

impl CredentialStore for EnvCredentialsStore {
    fn load(&self) -> Result<Option<LarkCredentials>, LarkError> {
        Self::from_lookup(|key| env::var(key).ok())
    }

    fn save(&self, _creds: &LarkCredentials) -> Result<(), LarkError> {
        Err(LarkError::protocol("environment credentials are read-only"))
    }
}

#[derive(Serialize, Deserialize)]
struct CredentialsFile {
    app_id: String,
    app_secret: String,
    tenant: String,
}

/// TOML file-backed credential store.
///
/// Writes are atomic (temp file + rename) with `0600` permissions on Unix.
/// On Windows the rename target must be removed first, so the replace is not
/// atomic there; the file ACLs still restrict access to the owning user.
pub struct FileCredentialStore {
    path: PathBuf,
}

impl FileCredentialStore {
    /// Creates a store at an explicit path.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Creates a store at the platform default path (see module docs).
    ///
    /// # Errors
    ///
    /// Returns an error when no config directory can be determined.
    pub fn at_default() -> Result<Self, LarkError> {
        Ok(Self::new(default_credentials_path()?))
    }

    /// Returns the backing file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl CredentialStore for FileCredentialStore {
    fn load(&self) -> Result<Option<LarkCredentials>, LarkError> {
        let text = match fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(LarkError::retryable("reading the credentials file")),
        };
        let file: CredentialsFile =
            toml::from_str(&text).map_err(|_| LarkError::protocol("malformed credentials file"))?;
        if file.app_id.is_empty() || file.app_secret.is_empty() {
            return Err(LarkError::protocol("credentials file has empty fields"));
        }
        let tenant = file.tenant.parse::<TenantBrand>()?;
        Ok(Some(LarkCredentials::new(
            file.app_id,
            SecretString::from(file.app_secret),
            tenant,
        )))
    }

    fn save(&self, creds: &LarkCredentials) -> Result<(), LarkError> {
        let file = CredentialsFile {
            app_id: creds.app_id.clone(),
            app_secret: creds.app_secret.expose_secret().to_owned(),
            tenant: creds.tenant.as_str().to_owned(),
        };
        let text =
            toml::to_string(&file).map_err(|_| LarkError::protocol("encoding credentials"))?;
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)
                    .map_err(|_| LarkError::retryable("creating the credentials directory"))?;
            }
        }
        let temp = self.path.with_extension("toml.tmp");
        write_private_file(&temp, text.as_bytes())?;
        // Windows cannot rename over an existing file; remove the target
        // first there (non-atomic), while Unix gets a true atomic replace.
        #[cfg(windows)]
        if self.path.exists() {
            fs::remove_file(&self.path)
                .map_err(|_| LarkError::retryable("replacing the credentials file"))?;
        }
        fs::rename(&temp, &self.path)
            .map_err(|_| LarkError::retryable("replacing the credentials file"))?;
        Ok(())
    }
}

#[cfg(unix)]
fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), LarkError> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| LarkError::retryable("writing the credentials file"))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| LarkError::retryable("writing the credentials file"))
}

#[cfg(not(unix))]
fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), LarkError> {
    fs::write(path, bytes).map_err(|_| LarkError::retryable("writing the credentials file"))
}

fn default_credentials_path() -> Result<PathBuf, LarkError> {
    if let Some(path) = env::var_os("LARK_CREDENTIALS_FILE") {
        if !path.is_empty() {
            return Ok(PathBuf::from(path));
        }
    }
    #[cfg(windows)]
    {
        let base = env::var_os("APPDATA")
            .ok_or_else(|| LarkError::retryable("locating the APPDATA directory"))?;
        Ok(PathBuf::from(base)
            .join("lark-codex-bridge")
            .join("credentials.toml"))
    }
    #[cfg(not(windows))]
    {
        if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
            if !xdg.is_empty() {
                return Ok(PathBuf::from(xdg)
                    .join("lark-codex-bridge")
                    .join("credentials.toml"));
            }
        }
        let home = env::var_os("HOME")
            .ok_or_else(|| LarkError::retryable("locating the home directory"))?;
        Ok(PathBuf::from(home)
            .join(".config")
            .join("lark-codex-bridge")
            .join("credentials.toml"))
    }
}

/// Loads credentials, preferring the `LARK_*` environment overrides and
/// falling back to the default file store.
///
/// # Errors
///
/// Returns an error when a configured source holds invalid credentials.
pub fn load_credentials() -> Result<Option<LarkCredentials>, LarkError> {
    if let Some(creds) = EnvCredentialsStore.load()? {
        return Ok(Some(creds));
    }
    FileCredentialStore::at_default()?.load()
}
