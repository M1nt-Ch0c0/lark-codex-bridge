//! Durable receipt-boundary intake types.

use sha2::{Digest, Sha256};

use crate::lark::credentials::LarkCredentials;

/// Opaque database namespace for one tenant brand and application ID.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TenantNamespace([u8; 32]);

impl TenantNamespace {
    /// Derives a stable, domain-separated namespace without retaining credentials.
    #[must_use]
    pub fn from_credentials(credentials: &LarkCredentials) -> Self {
        const DOMAIN: &[u8] = b"lark-codex-bridge/inbound-tenant/v1";
        let brand = credentials.tenant.as_str().as_bytes();
        let app_id = credentials.app_id.as_bytes();
        let mut hasher = Sha256::new();
        for value in [DOMAIN, brand, app_id] {
            hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
            hasher.update(value);
        }
        Self(hasher.finalize().into())
    }

    pub(crate) fn as_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut value = String::with_capacity(64);
        for byte in self.0 {
            value.push(char::from(HEX[usize::from(byte >> 4)]));
            value.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        value
    }
}

impl std::fmt::Debug for TenantNamespace {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let hex = self.as_hex();
        formatter
            .debug_tuple("TenantNamespace")
            .field(&format_args!("{}…", &hex[..8]))
            .finish()
    }
}
