//! Durable receipt-boundary intake types.

use std::fmt;
use std::sync::Arc;

use futures_util::FutureExt;
use sha2::{Digest, Sha256};

use crate::lark::bridge::{IntakeHook, IntakeVerdict, RetainedInbound};
use crate::lark::credentials::LarkCredentials;
use crate::lark::error::LarkError;
use crate::limits::{STORE_INBOUND_RECEIVED_MAX_BYTES, STORE_INBOUND_RECEIVED_MAX_ROWS};
use crate::store::{DedupOutcome, StoreError, StoreHandle};

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

/// Single-use durable intake state consumed by bridge startup.
pub struct IntakeRuntime {
    binding: TenantNamespace,
    recovery: Vec<RetainedInbound>,
    hook: IntakeHook,
}

impl IntakeRuntime {
    /// Constructs the narrow test/custom-intake seam while binding it to credentials.
    ///
    /// # Errors
    ///
    /// Returns a classified error when the supplied recovery set exceeds store bounds.
    pub fn try_from_parts(
        credentials: &LarkCredentials,
        recovery: Vec<RetainedInbound>,
        hook: IntakeHook,
    ) -> Result<Self, LarkError> {
        let recovery_bytes = recovery.iter().try_fold(0_usize, |total, retained| {
            total.checked_add(retained.retained_bytes())
        });
        let Some(recovery_bytes) = recovery_bytes else {
            return Err(LarkError::protocol(
                "startup inbound recovery bytes overflow",
            ));
        };
        if recovery.len() > usize::try_from(STORE_INBOUND_RECEIVED_MAX_ROWS).unwrap_or(usize::MAX) {
            return Err(LarkError::exhausted(
                "startup inbound recovery count exceeds the store bound",
                STORE_INBOUND_RECEIVED_MAX_ROWS,
            ));
        }
        if recovery_bytes > usize::try_from(STORE_INBOUND_RECEIVED_MAX_BYTES).unwrap_or(usize::MAX)
        {
            return Err(LarkError::exhausted(
                "startup inbound recovery bytes exceed the store bound",
                STORE_INBOUND_RECEIVED_MAX_BYTES,
            ));
        }
        Ok(Self {
            binding: TenantNamespace::from_credentials(credentials),
            recovery,
            hook,
        })
    }

    pub(crate) fn matches(&self, credentials: &LarkCredentials) -> bool {
        self.binding == TenantNamespace::from_credentials(credentials)
    }

    pub(crate) fn into_parts(self) -> (Vec<RetainedInbound>, IntakeHook) {
        (self.recovery, self.hook)
    }
}

impl fmt::Debug for IntakeRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IntakeRuntime")
            .field("binding", &self.binding)
            .field("recovery_count", &self.recovery.len())
            .field(
                "recovery_bytes",
                &self
                    .recovery
                    .iter()
                    .map(RetainedInbound::retained_bytes)
                    .sum::<usize>(),
            )
            .finish_non_exhaustive()
    }
}

/// Store-backed durable receipt-boundary preparation.
pub struct DurableIntake;

impl DurableIntake {
    /// Scans strict startup recovery and builds a credential-bound hook.
    ///
    /// # Errors
    ///
    /// Returns a content-free Lark classification for store or integrity failures.
    pub async fn prepare(
        store: StoreHandle,
        credentials: &LarkCredentials,
    ) -> Result<IntakeRuntime, LarkError> {
        let namespace = TenantNamespace::from_credentials(credentials);
        let recovery = store
            .recover_received(&namespace)
            .await
            .map_err(map_store_error)?;
        let hook_store = store;
        let hook_namespace = namespace;
        let hook: IntakeHook = Arc::new(move |event| {
            let store = hook_store.clone();
            let namespace = hook_namespace.clone();
            async move {
                match store
                    .register_inbound(&namespace, &event)
                    .await
                    .map_err(map_store_error)?
                {
                    DedupOutcome::New(retained) | DedupOutcome::ReplayReceived(retained) => {
                        Ok(IntakeVerdict::Enqueue(retained))
                    }
                    DedupOutcome::Duplicate { .. } => Ok(IntakeVerdict::DropDuplicate),
                }
            }
            .boxed()
        });
        IntakeRuntime::try_from_parts(credentials, recovery, hook)
    }
}

fn map_store_error(error: StoreError) -> LarkError {
    match error {
        StoreError::QueueFull
        | StoreError::Closed
        | StoreError::Io { .. }
        | StoreError::Sqlite { .. } => LarkError::retryable("persisting an inbound event"),
        StoreError::PayloadTooLarge { limit, .. } => {
            LarkError::exhausted("persisting an inbound event payload", limit)
        }
        StoreError::CapacityExceeded { .. } => LarkError::exhausted(
            "persisting the bounded inbound collection",
            STORE_INBOUND_RECEIVED_MAX_BYTES,
        ),
        StoreError::AlreadyOpen
        | StoreError::Migration { .. }
        | StoreError::NotFound { .. }
        | StoreError::InvalidTransition { .. }
        | StoreError::CorruptData { .. }
        | StoreError::InvalidPath { .. } => {
            LarkError::protocol("durable inbound store invariant failed")
        }
    }
}
