//! Bounded outbox pump: claim, send, record receipts, bounded retry, explicit
//! `uncertain_delivery`, and graceful shutdown.
//!
//! The pump only drains rows while the Lark transport reports
//! [`TransportState::Connected`]; while disconnected, rows stay `pending` in
//! the store (design §13.2). On reconnect it resumes in deterministic `id`
//! order. A send whose outcome is unknown is recorded as
//! `uncertain_delivery` and never blindly re-sent.

#![allow(clippy::doc_markdown)]

use std::future::Future;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::payload::OutboxOperation;
use crate::lark::api::LarkApi;
use crate::lark::error::LarkError;
use crate::lark::transport::TransportState;
use crate::limits::{
    OUTBOX_POLL_INTERVAL, OUTBOX_RETRY_BASE, OUTBOX_RETRY_MAX, OUTBOX_SWEEP_BATCH,
    OUTBOX_SWEEP_INTERVAL, OUTBOX_TERMINAL_RETENTION_MS, STORE_OUTBOX_CLAIM_MAX_BATCH,
    STORE_RECEIPT_WRITE_ATTEMPTS,
};
use crate::store::{OutboxDepth, OutboxRow, OutboxState, StoreError, StoreHandle, now_ms};

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
        // A definitive peer rejection: the server responded with a non-success
        // status, so nothing was sent and a bounded retry is safe.
        LarkError::Retryable { code: Some(_), .. }
        | LarkError::ProtocolViolation { code: Some(_), .. } => DeliveryClass::Retryable,
        // No usable response (transport failure/timeout), or a 200 whose
        // envelope could not be parsed (or a code-0 response missing its
        // message_id): the send may have been applied, so it is never
        // blindly re-sent.
        LarkError::Retryable { code: None, .. }
        | LarkError::ProtocolViolation { code: None, .. } => DeliveryClass::Uncertain,
    }
}

/// Result of processing one claimed row, telling the batch loop whether it may
/// advance to the next row or must stop to preserve in-order delivery.
enum ProcessOutcome {
    /// The row reached a terminal state (delivered, terminal failure, or
    /// explicit uncertainty): no later retry can overtake it, so the batch
    /// may advance.
    Resolved,
    /// The row will be retried later. Its receipt transaction has already
    /// parked every later live row behind it, so the batch must stop.
    Deferred,
}

enum SendFailure {
    Delivery(LarkError),
    Store(StoreError),
    DependencyPending,
    DependencyPermanent,
    DependencyUncertain,
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
    match store.recover_sending_outbox().await {
        Ok(recovered) => {
            tracing::info!(
                recovered_uncertain = recovered,
                "outbox recovery scan complete"
            );
        }
        Err(error) => tracing::warn!(error = %error, "outbox startup recovery failed"),
    }
    let sweep_interval_ms = i64::try_from(OUTBOX_SWEEP_INTERVAL.as_millis()).unwrap_or(i64::MAX);
    let mut next_sweep_ms = 0_i64;
    loop {
        if shutdown.is_cancelled() {
            break;
        }
        if now_ms() >= next_sweep_ms {
            sweep_terminal_rows(&store).await;
            next_sweep_ms = now_ms().saturating_add(sweep_interval_ms);
        }
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
        tracing::debug!(claimed_rows = batch.len(), "outbox batch claimed");
        let mut cursor = 0;
        while cursor < batch.len() {
            if shutdown.is_cancelled() {
                // Shutdown landed after the claim but before this row: re-park
                // the un-sent tail without counting attempts and exit.
                release_tail(&store, &batch[cursor..]).await;
                break;
            }
            if !is_connected(&transport) {
                // Disconnected after the claim but before any send: re-park the
                // un-sent tail without counting attempts, then wait to resume.
                release_tail(&store, &batch[cursor..]).await;
                break;
            }
            let outcome = process_row(&store, &api, &batch[cursor], config, &shutdown).await;
            cursor += 1;
            if matches!(outcome, ProcessOutcome::Deferred) {
                // The retry receipt transaction already returned the claimed
                // tail to `pending` and deferred all later live rows. A second
                // write here would recreate the crash window it closes.
                break;
            }
        }
        if shutdown.is_cancelled() {
            break;
        }
    }
    tracing::info!("outbox pump stopped");
}

#[allow(clippy::too_many_lines)]
async fn process_row(
    store: &StoreHandle,
    api: &LarkApi,
    row: &OutboxRow,
    config: OutboxPumpConfig,
    shutdown: &CancellationToken,
) -> ProcessOutcome {
    let operation = match OutboxOperation::decode(&row.payload_json) {
        Ok(operation) => operation,
        Err(error) => {
            tracing::warn!(error = %error, "outbox payload is undeliverable");
            if let Err(store_error) = write_receipt(shutdown, config.poll_interval, || {
                store.fail_outbox_terminal(row.id)
            })
            .await
            {
                // The row stays `sending`; startup `recover_sending_outbox`
                // will mark it explicitly uncertain, so a transient store
                // failure can never silently drop an undeliverable payload.
                tracing::warn!(
                    error = %error,
                    store_error = %store_error,
                    "outbox payload is undeliverable and the terminal receipt could not be recorded"
                );
            }
            return ProcessOutcome::Resolved;
        }
    };
    let operation_kind = operation_kind(&operation);
    tracing::debug!(
        operation = operation_kind,
        attempt = row.attempts.saturating_add(1),
        "outbox row sending"
    );
    match send(store, api, row, &operation).await {
        Ok(message_id) => {
            if message_id.is_empty() {
                // The receipt contract (design §9) requires a non-empty
                // message_id; the API layer already enforces this, so treat a
                // theoretical empty receipt as an unknown outcome.
                if let Err(error) = write_receipt(shutdown, config.poll_interval, || {
                    store.fail_outbox(row.id, row.attempts + 1, now_ms(), true)
                })
                .await
                {
                    tracing::warn!(error = %error, "outbox receipt failure");
                }
                tracing::warn!(
                    operation = operation_kind,
                    attempt = row.attempts.saturating_add(1),
                    state = "uncertain",
                    "outbox delivery receipt was empty"
                );
                return ProcessOutcome::Resolved;
            }
            if let Err(error) = write_receipt(shutdown, config.poll_interval, || {
                store.complete_outbox(row.id, &message_id)
            })
            .await
            {
                tracing::warn!(error = %error, "outbox receipt failed");
            } else {
                tracing::info!(
                    operation = operation_kind,
                    attempt = row.attempts.saturating_add(1),
                    state = "sent",
                    "outbox row delivered"
                );
            }
            ProcessOutcome::Resolved
        }
        Err(SendFailure::Delivery(error)) => {
            let class = classify_delivery(&error);
            record_failure(store, row, class, config, &error, shutdown).await
        }
        Err(SendFailure::Store(store_error)) => {
            tracing::warn!(
                error = %store_error,
                "progress dependency lookup failed"
            );
            record_failure(
                store,
                row,
                DeliveryClass::Retryable,
                config,
                &LarkError::retryable("resolving a progress dependency"),
                shutdown,
            )
            .await
        }
        Err(SendFailure::DependencyPending) => {
            record_failure(
                store,
                row,
                DeliveryClass::Retryable,
                config,
                &LarkError::retryable("waiting for a progress dependency"),
                shutdown,
            )
            .await
        }
        Err(SendFailure::DependencyPermanent) => {
            record_failure(
                store,
                row,
                DeliveryClass::Permanent,
                config,
                &LarkError::exhausted("progress dependency is unavailable", 0),
                shutdown,
            )
            .await
        }
        Err(SendFailure::DependencyUncertain) => {
            record_failure(
                store,
                row,
                DeliveryClass::Uncertain,
                config,
                &LarkError::protocol("progress dependency delivery is uncertain"),
                shutdown,
            )
            .await
        }
    }
}

const fn operation_kind(operation: &OutboxOperation) -> &'static str {
    match operation {
        OutboxOperation::ReplyText { .. } => "reply_text",
        OutboxOperation::ReplyProgressCard { .. } => "reply_progress_card",
        OutboxOperation::UpdateProgressCard { .. } => "update_progress_card",
        OutboxOperation::FinalizeProgressCard { .. } => "finalize_progress_card",
    }
}

async fn send(
    store: &StoreHandle,
    api: &LarkApi,
    row: &OutboxRow,
    operation: &OutboxOperation,
) -> Result<String, SendFailure> {
    match operation {
        OutboxOperation::ReplyText {
            message_id,
            thread_id: Some(_),
            text,
        } => api
            .reply_text_in_thread(message_id.as_str(), text.as_str())
            .await
            .map(|message| message.message_id)
            .map_err(SendFailure::Delivery),
        OutboxOperation::ReplyText {
            message_id,
            thread_id: None,
            text,
        } => api
            .reply_text(message_id.as_str(), text.as_str())
            .await
            .map(|message| message.message_id)
            .map_err(SendFailure::Delivery),
        OutboxOperation::ReplyProgressCard {
            message_id,
            thread_id: Some(_),
            text,
        } => api
            .reply_card_in_thread(message_id, progress_card(text))
            .await
            .map(|message| message.message_id)
            .map_err(SendFailure::Delivery),
        OutboxOperation::ReplyProgressCard {
            message_id,
            thread_id: None,
            text,
        } => api
            .reply_card(message_id, progress_card(text))
            .await
            .map(|message| message.message_id)
            .map_err(SendFailure::Delivery),
        OutboxOperation::UpdateProgressCard { anchor_key, text } => {
            let anchor =
                progress_anchor(store, row, anchor_key, ProgressDependency::Update).await?;
            match anchor {
                ProgressAnchor::Delivered(message_id) => api
                    .update_card(&message_id, progress_card(text))
                    .await
                    .map(|()| message_id)
                    .map_err(SendFailure::Delivery),
                ProgressAnchor::Pending => Err(SendFailure::DependencyPending),
                ProgressAnchor::Failed => Err(SendFailure::DependencyPermanent),
                ProgressAnchor::Uncertain => Err(SendFailure::DependencyUncertain),
            }
        }
        OutboxOperation::FinalizeProgressCard {
            anchor_key,
            message_id,
            thread_id,
            text,
        } => {
            let anchor =
                progress_anchor(store, row, anchor_key, ProgressDependency::Finalize).await?;
            match anchor {
                ProgressAnchor::Delivered(receipt) => api
                    .update_card(&receipt, progress_card(text))
                    .await
                    .map(|()| receipt)
                    .map_err(SendFailure::Delivery),
                ProgressAnchor::Failed if thread_id.is_some() => api
                    .reply_text_in_thread(message_id, text)
                    .await
                    .map(|message| message.message_id)
                    .map_err(SendFailure::Delivery),
                ProgressAnchor::Failed => api
                    .reply_text(message_id, text)
                    .await
                    .map(|message| message.message_id)
                    .map_err(SendFailure::Delivery),
                ProgressAnchor::Pending => Err(SendFailure::DependencyPending),
                ProgressAnchor::Uncertain => Err(SendFailure::DependencyUncertain),
            }
        }
    }
}

enum ProgressAnchor {
    Delivered(String),
    Pending,
    Failed,
    Uncertain,
}

#[derive(Clone, Copy)]
enum ProgressDependency {
    Update,
    Finalize,
}

async fn progress_anchor(
    store: &StoreHandle,
    dependent: &OutboxRow,
    anchor_key: &str,
    dependency: ProgressDependency,
) -> Result<ProgressAnchor, SendFailure> {
    if !valid_progress_dependency(dependent, anchor_key, dependency) {
        return Err(SendFailure::DependencyPermanent);
    }
    let row = store
        .outbox_row_by_key(anchor_key)
        .await
        .map_err(SendFailure::Store)?
        .ok_or(SendFailure::DependencyPermanent)?;
    if row.id >= dependent.id || row.scope_key != dependent.scope_key || row.kind != "progress" {
        return Err(SendFailure::DependencyPermanent);
    }
    if !matches!(
        OutboxOperation::decode(&row.payload_json),
        Ok(OutboxOperation::ReplyProgressCard { .. })
    ) {
        return Err(SendFailure::DependencyPermanent);
    }
    match row.state {
        OutboxState::Sent => row
            .receipt_message_id
            .filter(|receipt| !receipt.is_empty())
            .map(ProgressAnchor::Delivered)
            .ok_or(SendFailure::DependencyPermanent),
        OutboxState::Pending | OutboxState::Sending => Ok(ProgressAnchor::Pending),
        OutboxState::Failed => Ok(ProgressAnchor::Failed),
        OutboxState::UncertainDelivery => Ok(ProgressAnchor::Uncertain),
    }
}

fn valid_progress_dependency(
    dependent: &OutboxRow,
    anchor_key: &str,
    dependency: ProgressDependency,
) -> bool {
    match dependency {
        ProgressDependency::Update => {
            if dependent.kind != "progress" {
                return false;
            }
            let Some(sequence) = dependent
                .idempotency_key
                .strip_prefix(anchor_key)
                .and_then(|suffix| suffix.strip_prefix(':'))
            else {
                return false;
            };
            let Ok(sequence_number) = sequence.parse::<u32>() else {
                return false;
            };
            sequence_number > 0 && sequence_number.to_string() == sequence
        }
        ProgressDependency::Finalize => {
            dependent.kind == "final" && dependent.idempotency_key == format!("{anchor_key}:final")
        }
    }
}

fn progress_card(text: &str) -> Value {
    json!({
        "schema": "2.0",
        "body": {
            "elements": [{
                "tag": "markdown",
                "content": text,
            }],
        },
    })
}

async fn record_failure(
    store: &StoreHandle,
    row: &OutboxRow,
    class: DeliveryClass,
    config: OutboxPumpConfig,
    error: &LarkError,
    shutdown: &CancellationToken,
) -> ProcessOutcome {
    let attempts = row.attempts.saturating_add(1);
    let retry_at = now_ms().saturating_add(retry_delay_ms(attempts, config));
    let result = match class {
        DeliveryClass::Retryable => write_receipt(shutdown, config.poll_interval, || {
            store.fail_outbox_and_defer_successors(row.id, attempts, retry_at)
        })
        .await
        .map(Some),
        DeliveryClass::Uncertain => write_receipt(shutdown, config.poll_interval, || {
            store.fail_outbox(row.id, attempts, now_ms(), true)
        })
        .await
        .map(|()| None),
        DeliveryClass::Permanent => write_receipt(shutdown, config.poll_interval, || {
            store.fail_outbox_terminal(row.id)
        })
        .await
        .map(|()| None),
    };
    let deferred = match result {
        Ok(Some(deferred)) => deferred,
        Ok(None) => false,
        Err(store_error) => {
            // The row stays `sending`; startup `recover_sending_outbox` will
            // mark it explicitly uncertain, so a transient store failure can
            // never silently drop an attempted send.
            tracing::warn!(
                error_kind = ?error.kind(),
                store_error = %store_error,
                class = ?class,
                "outbox send failed and the receipt could not be recorded"
            );
            return ProcessOutcome::Resolved;
        }
    };
    let retry_delay = if matches!(class, DeliveryClass::Retryable) {
        u64::try_from(retry_delay_ms(attempts, config)).unwrap_or(u64::MAX)
    } else {
        0
    };
    tracing::warn!(
        error_kind = ?error.kind(),
        class = ?class,
        attempts,
        retry_delay_ms = retry_delay,
        state = if matches!(class, DeliveryClass::Retryable) {
            "pending"
        } else if matches!(class, DeliveryClass::Uncertain) {
            "uncertain"
        } else {
            "failed"
        },
        "outbox delivery failed"
    );
    if deferred {
        ProcessOutcome::Deferred
    } else {
        ProcessOutcome::Resolved
    }
}

async fn release_tail(store: &StoreHandle, tail: &[OutboxRow]) {
    let mut failures = 0_usize;
    for row in tail {
        if let Err(error) = store.release_outbox_claim(row.id).await {
            failures = failures.saturating_add(1);
            tracing::warn!(error = %error, "outbox claim release failed");
        }
    }
    tracing::debug!(
        released_rows = tail.len().saturating_sub(failures),
        failures,
        "outbox claimed rows returned to pending"
    );
}

/// One bounded, cancellation-aware terminal sweep. Failures are logged and
/// dropped: a sweep that cannot run must never stall the send loop.
async fn sweep_terminal_rows(store: &StoreHandle) {
    let older_than_ms = now_ms().saturating_sub(OUTBOX_TERMINAL_RETENTION_MS);
    match store
        .sweep_terminal_outbox(older_than_ms, OUTBOX_SWEEP_BATCH)
        .await
    {
        Ok(0) => {}
        Ok(deleted) => tracing::debug!(deleted, "swept terminal outbox rows"),
        Err(error) => tracing::warn!(error = %error, "outbox terminal sweep failed"),
    }
}

/// Runs one receipt write up to [`STORE_RECEIPT_WRITE_ATTEMPTS`] times,
/// parking for `interval` between attempts. Returns the first success or the
/// last [`StoreError`] once the budget is exhausted (or shutdown interrupts a
/// backoff). Cancellation-aware, so no fixed sleep can outlive the pump.
async fn write_receipt<T, F, Fut>(
    shutdown: &CancellationToken,
    interval: Duration,
    mut write: F,
) -> Result<T, StoreError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StoreError>>,
{
    let mut last_error = StoreError::Closed;
    for attempt in 0..STORE_RECEIPT_WRITE_ATTEMPTS {
        match write().await {
            Ok(value) => return Ok(value),
            Err(error) => last_error = error,
        }
        let remaining = STORE_RECEIPT_WRITE_ATTEMPTS - attempt - 1;
        if remaining == 0 || !sleep_or_shutdown(interval, shutdown).await {
            break;
        }
    }
    Err(last_error)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn receipt_write_retries_a_transient_failure_then_succeeds() {
        let cap = usize::try_from(STORE_RECEIPT_WRITE_ATTEMPTS).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let result: Result<i32, StoreError> =
            write_receipt(&shutdown, Duration::from_millis(1), || {
                let calls = Arc::clone(&calls);
                async move {
                    if calls.fetch_add(1, Ordering::SeqCst) < cap - 1 {
                        Err(StoreError::QueueFull)
                    } else {
                        Ok(42_i32)
                    }
                }
            })
            .await;
        assert_eq!(result, Ok(42));
        assert_eq!(calls.load(Ordering::SeqCst), cap);
    }

    #[tokio::test]
    async fn receipt_write_gives_up_after_the_bounded_budget() {
        let cap = usize::try_from(STORE_RECEIPT_WRITE_ATTEMPTS).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let result: Result<(), StoreError> =
            write_receipt(&shutdown, Duration::from_millis(1), || {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(StoreError::QueueFull)
                }
            })
            .await;
        assert_eq!(result, Err(StoreError::QueueFull));
        assert_eq!(calls.load(Ordering::SeqCst), cap);
    }

    #[tokio::test]
    async fn receipt_write_aborts_the_backoff_on_shutdown() {
        let calls = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let result: Result<(), StoreError> =
            write_receipt(&shutdown, Duration::from_secs(60), || {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(StoreError::QueueFull)
                }
            })
            .await;
        assert_eq!(result, Err(StoreError::QueueFull));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a cancelled shutdown must abort before the next attempt"
        );
    }

    #[tokio::test]
    async fn sweep_terminal_rows_prunes_overage_terminals_only() {
        use crate::store::sqlite_error;

        let store = StoreHandle::open_in_memory().await.expect("store");
        store
            .run(|connection| {
                connection
                    .execute(
                        "INSERT INTO outbox
                         (idempotency_key, scope_key, kind, payload_json, payload_bytes,
                          state, attempts, next_retry_ms, receipt_message_id, created_ms, updated_ms)
                         VALUES
                         ('sent_old', 'im:oc', 'final', '{}', 2, 'sent', 1, 0, 'om_r', 1, 1),
                         ('pending_old', 'im:oc', 'final', '{}', 2, 'pending', 0, 0, NULL, 1, 1)",
                        [],
                    )
                    .map_err(|error| sqlite_error("seeding sweep rows", &error))?;
                Ok(())
            })
            .await
            .expect("seed rows");

        sweep_terminal_rows(&store).await;

        let depth = store.outbox_depth().await.expect("depth");
        assert_eq!(depth.pending, 1, "pending rows are never swept");
        let sent: i64 = store
            .run(|connection| {
                connection
                    .query_row(
                        "SELECT COUNT(*) FROM outbox WHERE state = 'sent'",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|error| sqlite_error("counting sent rows", &error))
            })
            .await
            .expect("sent count");
        assert_eq!(sent, 0, "the over-age terminal row is swept");
    }
}
