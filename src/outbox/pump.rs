//! Bounded outbox pump: claim, send, record receipts, bounded retry, explicit
//! `uncertain_delivery`, and graceful shutdown.
//!
//! The pump only drains rows while the Lark transport reports
//! [`TransportState::Connected`]; while disconnected, rows stay `pending` in
//! the store (design §13.2). On reconnect it resumes in deterministic `id`
//! order. A send whose outcome is unknown is recorded as
//! `uncertain_delivery` and never blindly re-sent.

#![allow(clippy::doc_markdown)]

use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::payload::OutboxOperation;
use crate::lark::api::LarkApi;
use crate::lark::error::LarkError;
use crate::lark::transport::TransportState;
use crate::limits::{
    OUTBOX_POLL_INTERVAL, OUTBOX_RETRY_BASE, OUTBOX_RETRY_MAX, STORE_OUTBOX_CLAIM_MAX_BATCH,
};
use crate::store::{OutboxDepth, OutboxRow, StoreError, StoreHandle, now_ms};

/// How one failed send must be handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryClass {
    /// The server explicitly rejected the request (an error code or HTTP
    /// error status was returned): safe to retry, bounded by the attempt cap.
    Retryable,
    /// The send outcome is unknown (no server response was received): never
    /// automatically re-sent.
    Uncertain,
    /// A definitive failure that retrying cannot fix (permanent auth, oversize
    /// body, or a corrupt payload): terminal `failed`.
    Permanent,
}

/// Classifies a send failure into the three-way delivery semantics.
///
/// The existing [`LarkError`] taxonomy carries a server `code` when the peer
/// responded (either a Lark envelope code or an HTTP status). A response
/// proves the send was *not* applied, so it is safe to retry; no response
/// means the request may have reached Lark before the connection dropped.
#[must_use]
pub fn classify_delivery(error: &LarkError) -> DeliveryClass {
    match error {
        LarkError::PermanentAuth { .. } | LarkError::Exhausted { .. } => DeliveryClass::Permanent,
        LarkError::Retryable { code: Some(_), .. } => DeliveryClass::Retryable,
        // No server response (transport failure/timeout), or a 200 whose
        // envelope could not be parsed (or a code-0 response missing its
        // message_id): the send may have been applied, so it is never
        // blindly re-sent.
        LarkError::Retryable { code: None, .. } | LarkError::ProtocolViolation { .. } => {
            DeliveryClass::Uncertain
        }
    }
}

/// Tunables for the outbox pump; defaults match the production limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboxPumpConfig {
    /// Base delay of the deterministic exponential backoff.
    pub retry_base: Duration,
    /// Upper bound of one retry delay.
    pub retry_max: Duration,
    /// Poll cadence for discovering newly enqueued rows while connected.
    pub poll_interval: Duration,
    /// Rows claimed per batch (clamped by the store).
    pub claim_batch: u32,
}

impl Default for OutboxPumpConfig {
    fn default() -> Self {
        Self {
            retry_base: OUTBOX_RETRY_BASE,
            retry_max: OUTBOX_RETRY_MAX,
            poll_interval: OUTBOX_POLL_INTERVAL,
            claim_batch: STORE_OUTBOX_CLAIM_MAX_BATCH,
        }
    }
}

/// Entry point for the durable outbox pump.
pub struct OutboxPump;

impl OutboxPump {
    /// Starts the pump actor over a shared store and Lark API client, gated by
    /// the given transport state subscription.
    #[must_use]
    pub fn spawn(
        store: StoreHandle,
        api: LarkApi,
        transport: watch::Receiver<TransportState>,
        config: OutboxPumpConfig,
    ) -> OutboxHandle {
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(store.clone(), api, transport, config, shutdown.clone()));
        OutboxHandle {
            store,
            shutdown,
            join: Some(task),
        }
    }
}

/// Handle to a running pump; also answers depth queries for `/status`.
pub struct OutboxHandle {
    store: StoreHandle,
    shutdown: CancellationToken,
    join: Option<JoinHandle<()>>,
}

impl OutboxHandle {
    /// Current queue depth snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn depth(&self) -> Result<OutboxDepth, StoreError> {
        self.store.outbox_depth().await
    }

    /// Requests shutdown and waits for the pump to exit. No row is left in a
    /// detached task: the in-flight send (if any) is awaited to completion
    /// first, bounded by the Lark HTTP timeout.
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }
}

async fn run(
    store: StoreHandle,
    api: LarkApi,
    mut transport: watch::Receiver<TransportState>,
    config: OutboxPumpConfig,
    shutdown: CancellationToken,
) {
    // Rows stranded in `sending` by a prior process are explicitly uncertain:
    // delivery may have reached Lark before that process died.
    if let Err(error) = store.recover_sending_outbox().await {
        tracing::warn!(error = %error, "outbox startup recovery failed");
    }
    loop {
        if !wait_until_connected(&mut transport, &shutdown).await {
            break;
        }
        let batch = match store.claim_outbox_batch(now_ms(), config.claim_batch).await {
            Ok(batch) => batch,
            Err(error) => {
                tracing::warn!(error = %error, "outbox claim failed");
                if !sleep_or_shutdown(config.poll_interval, &shutdown).await {
                    break;
                }
                continue;
            }
        };
        if batch.is_empty() {
            if !sleep_or_shutdown(config.poll_interval, &shutdown).await {
                break;
            }
            continue;
        }
        let mut cursor = 0;
        while cursor < batch.len() {
            if !is_connected(&transport) {
                // Disconnected after the claim but before any send: re-park the
                // un-sent tail without counting attempts, then wait to resume.
                for row in &batch[cursor..] {
                    if let Err(error) = store.release_outbox_claim(row.id).await {
                        tracing::warn!(error = %error, outbox_id = row.id, "outbox claim release failed");
                    }
                }
                break;
            }
            process_row(&store, &api, &batch[cursor], config).await;
            cursor += 1;
        }
    }
}

async fn process_row(
    store: &StoreHandle,
    api: &LarkApi,
    row: &OutboxRow,
    config: OutboxPumpConfig,
) {
    let operation = match OutboxOperation::decode(&row.payload_json) {
        Ok(operation) => operation,
        Err(error) => {
            tracing::warn!(error = %error, outbox_id = row.id, "outbox payload is undeliverable");
            let _ = store.fail_outbox_terminal(row.id).await;
            return;
        }
    };
    match send(api, &operation).await {
        Ok(message_id) => {
            if message_id.is_empty() {
                // The receipt contract (design §9) requires a non-empty
                // message_id; the API layer already enforces this, so treat a
                // theoretical empty receipt as an unknown outcome.
                if let Err(error) = store
                    .fail_outbox(row.id, row.attempts + 1, now_ms(), true)
                    .await
                {
                    tracing::warn!(error = %error, outbox_id = row.id, "outbox receipt failure");
                }
                return;
            }
            if let Err(error) = store.complete_outbox(row.id, &message_id).await {
                tracing::warn!(error = %error, outbox_id = row.id, "outbox receipt failed");
            }
        }
        Err(error) => {
            let class = classify_delivery(&error);
            record_failure(store, row, class, config, &error).await;
        }
    }
}

async fn send(api: &LarkApi, operation: &OutboxOperation) -> Result<String, LarkError> {
    match operation {
        OutboxOperation::ReplyText {
            message_id,
            thread_id: Some(_),
            text,
        } => api
            .reply_text_in_thread(message_id.as_str(), text.as_str())
            .await
            .map(|message| message.message_id),
        OutboxOperation::ReplyText {
            message_id,
            thread_id: None,
            text,
        } => api
            .reply_text(message_id.as_str(), text.as_str())
            .await
            .map(|message| message.message_id),
    }
}

async fn record_failure(
    store: &StoreHandle,
    row: &OutboxRow,
    class: DeliveryClass,
    config: OutboxPumpConfig,
    error: &LarkError,
) {
    let attempts = row.attempts.saturating_add(1);
    let result = match class {
        DeliveryClass::Retryable => {
            let next = now_ms().saturating_add(retry_delay_ms(attempts, config));
            store.fail_outbox(row.id, attempts, next, false).await
        }
        DeliveryClass::Uncertain => store.fail_outbox(row.id, attempts, now_ms(), true).await,
        DeliveryClass::Permanent => store.fail_outbox_terminal(row.id).await,
    };
    if let Err(store_error) = result {
        tracing::warn!(
            error = %error,
            store_error = %store_error,
            outbox_id = row.id,
            class = ?class,
            "outbox send failed and the receipt could not be recorded"
        );
    }
}

fn retry_delay_ms(attempts: u32, config: OutboxPumpConfig) -> i64 {
    let base = u64::try_from(config.retry_base.as_millis()).unwrap_or(u64::MAX);
    let exponent = attempts.saturating_sub(1).min(10);
    let scaled = base.saturating_mul(1_u64 << exponent);
    let capped = scaled.min(u64::try_from(config.retry_max.as_millis()).unwrap_or(u64::MAX));
    i64::try_from(capped).unwrap_or(i64::MAX)
}

async fn wait_until_connected(
    transport: &mut watch::Receiver<TransportState>,
    shutdown: &CancellationToken,
) -> bool {
    loop {
        if is_connected(transport) {
            return true;
        }
        let changed = tokio::select! {
            () = shutdown.cancelled() => return false,
            changed = transport.changed() => changed,
        };
        if changed.is_err() {
            // The transport actor is gone: no further transitions will arrive.
            // Park until shutdown; rows remain pending and are never sent.
            shutdown.cancelled().await;
            return false;
        }
    }
}

async fn sleep_or_shutdown(duration: Duration, shutdown: &CancellationToken) -> bool {
    tokio::select! {
        () = shutdown.cancelled() => false,
        () = tokio::time::sleep(duration) => true,
    }
}

fn is_connected(transport: &watch::Receiver<TransportState>) -> bool {
    matches!(*transport.borrow(), TransportState::Connected)
}
