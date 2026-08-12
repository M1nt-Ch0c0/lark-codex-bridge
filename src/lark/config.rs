//! Tenant brand selection and per-tenant endpoint resolution.
//!
//! Only the two official tenants are supported: Feishu (`feishu.cn`) and
//! Lark international (`larksuite.com`). No domain is ever hardcoded without
//! going through [`TenantBrand`].

use std::fmt;
use std::str::FromStr;

use url::Url;

use super::error::LarkError;

/// Supported Feishu/Lark tenant brands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantBrand {
    /// Feishu, hosted on `feishu.cn`.
    Feishu,
    /// Lark international, hosted on `larksuite.com`.
    Lark,
}

impl TenantBrand {
    /// Returns the stable lowercase wire/config spelling of the brand.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Feishu => "feishu",
            Self::Lark => "lark",
        }
    }
}

impl fmt::Display for TenantBrand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TenantBrand {
    type Err = LarkError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "feishu" => Ok(Self::Feishu),
            "lark" => Ok(Self::Lark),
            _ => Err(LarkError::protocol("unknown tenant brand")),
        }
    }
}

/// Base URLs for one tenant.
///
/// [`LarkEndpoints::for_tenant`] only ever constructs the two official
/// tenant domains; tests may point the public fields at a local stub.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LarkEndpoints {
    /// `OpenAPI` base, e.g. `https://open.feishu.cn`.
    pub open_base: Url,
    /// Accounts base, e.g. `https://accounts.feishu.cn`.
    pub accounts_base: Url,
}

impl LarkEndpoints {
    /// Returns the official endpoints for a tenant brand.
    ///
    /// # Panics
    ///
    /// Never; the hardcoded tenant URLs are valid. The `expect` only documents
    /// that invariant.
    #[must_use]
    pub fn for_tenant(tenant: TenantBrand) -> Self {
        let (open, accounts) = match tenant {
            TenantBrand::Feishu => ("https://open.feishu.cn", "https://accounts.feishu.cn"),
            TenantBrand::Lark => (
                "https://open.larksuite.com",
                "https://accounts.larksuite.com",
            ),
        };
        Self {
            open_base: Url::parse(open).expect("hardcoded tenant URL is valid"),
            accounts_base: Url::parse(accounts).expect("hardcoded tenant URL is valid"),
        }
    }

    pub(crate) fn open_url(&self, path: &str) -> Result<Url, LarkError> {
        self.open_base
            .join(path)
            .map_err(|_| LarkError::protocol("invalid OpenAPI path"))
    }

    pub(crate) fn accounts_url(&self, path: &str) -> Result<Url, LarkError> {
        self.accounts_base
            .join(path)
            .map_err(|_| LarkError::protocol("invalid accounts path"))
    }
}
