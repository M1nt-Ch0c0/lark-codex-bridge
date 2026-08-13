//! Wiring between the WebSocket transport, the normalizer, and the bounded
//! inbound event channel consumed by the next milestone's scope runtime.
//!
//! [`LarkBridge::start`] resolves the bot identity (`GET /bot/v3/info`, so
//! mention detection uses the real bot `open_id`), builds the normalizer, and
//! installs it as the transport's inbound handler. Every completed `event`
//! payload is normalized and pushed into a channel bounded by both count
//! ([`LARK_INBOUND_EVENT_CAPACITY`]) and raw-payload bytes
//! ([`LARK_INBOUND_EVENT_BYTE_BUDGET`]); the byte permit is parked inside the
//! queued item and released only when the receiver dequeues and drops it,
//! matching the transport/RPC permit pattern. A full channel fails the
//! handler, so the transport's receipt honestly reports `{code: 500}` instead
//! of silently dropping the event. Card-action payloads are acknowledged with
//! `{code: 200, data}` and logged as unsupported (IDs only) for this
//! milestone rather than routed.
//!
//! Redaction: handler return values and errors carry IDs, sizes, and
//! classified error kinds only — never message text or card content.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use serde_json::json;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use super::api::LarkApi;
use super::config::LarkEndpoints;
use super::credentials::LarkCredentials;
use super::error::LarkError;
use super::frame::MessageType;
use super::http::LarkHttp;
use super::normalize::{InboundEvent, NormalizeOutcome, Normalizer};
use super::token::TenantTokenProvider;
use super::transport::{InboundFrameHandler, LarkTransport, TransportConfig, TransportHandle};
use crate::limits::{LARK_INBOUND_EVENT_BYTE_BUDGET, LARK_INBOUND_EVENT_CAPACITY};
use crate::runtime::intake::IntakeRuntime;

/// Durable receipt-boundary hook invoked after normalization.
pub type IntakeHook = Arc<
    dyn Fn(Box<InboundEvent>) -> BoxFuture<'static, Result<IntakeVerdict, LarkError>> + Send + Sync,
>;

/// Durable intake decision for one normalized event.
#[derive(Debug)]
pub enum IntakeVerdict {
    /// Enqueue this canonical persisted event using its retained byte count.
    Enqueue(RetainedInbound),
    /// An accepted or terminal canonical row already exists; acknowledge only.
    DropDuplicate,
}

/// One canonical persisted inbound event and the exact retained blob size.
pub struct RetainedInbound {
    event: Box<InboundEvent>,
    retained_bytes: usize,
}

impl RetainedInbound {
    pub(crate) fn new(event: Box<InboundEvent>, retained_bytes: usize) -> Self {
        Self {
            event,
            retained_bytes,
        }
    }

    /// Borrows the canonical persisted event.
    #[must_use]
    pub fn event(&self) -> &InboundEvent {
        &self.event
    }

    /// Returns the exact persisted payload byte length.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// Consumes the retained value and returns its event.
    #[must_use]
    pub fn into_event(self) -> Box<InboundEvent> {
        self.event
    }
}

impl fmt::Debug for RetainedInbound {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedInbound")
            .field("retained_bytes", &self.retained_bytes)
            .finish_non_exhaustive()
    }
}

/// One normalized inbound event parked in the bounded channel.
///
/// The byte-budget permit is held inside the item until the receiver dequeues
/// and drops it, so a slow consumer back-pressures the handler (and therefore
/// the receipt) instead of growing memory.
pub struct QueuedInboundEvent {
    /// The normalized event.
    pub event: InboundEvent,
    /// Byte-budget permit sized by the raw event payload; held until the
    /// receiver dequeues and drops the item.
    pub permit: OwnedSemaphorePermit,
}

impl QueuedInboundEvent {
    /// Consumes the wrapper, releasing the byte permit, and returns the event.
    #[must_use]
    pub fn into_event(self) -> InboundEvent {
        self.event
    }
}

impl fmt::Debug for QueuedInboundEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueuedInboundEvent")
            .field("event_id", &self.event.event_id)
            .field("message_id", &self.event.message_id)
            .field("text_len", &self.event.text.len())
            .field("resource_count", &self.event.resources.len())
            .finish_non_exhaustive()
    }
}

/// Tunables for the bridge wiring; defaults match the production limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BridgeConfig {
    /// Transport-level tunables (pong timeout, handler timeout).
    pub transport: TransportConfig,
    /// Count bound of the inbound event channel.
    pub event_capacity: usize,
    /// Byte budget of the inbound event channel (raw payload bytes).
    pub event_byte_budget: usize,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            transport: TransportConfig::default(),
            event_capacity: LARK_INBOUND_EVENT_CAPACITY,
            event_byte_budget: LARK_INBOUND_EVENT_BYTE_BUDGET,
        }
    }
}

/// Entry point wiring credentials to a running transport plus the normalized
/// inbound event stream.
pub struct LarkBridge;

impl LarkBridge {
    /// Starts the bridge against the official endpoints of the credentials'
    /// tenant with production limits.
    ///
    /// # Errors
    ///
    /// Returns a classified [`LarkError`] when the token exchange or bot-info
    /// lookup fails, or when the bot-info response carries no `open_id`.
    pub async fn start(
        creds: LarkCredentials,
    ) -> Result<(TransportHandle, mpsc::Receiver<QueuedInboundEvent>), LarkError> {
        Self::start_with(
            LarkEndpoints::for_tenant(creds.tenant),
            creds,
            BridgeConfig::default(),
        )
        .await
    }

    /// Starts the bridge against explicit endpoints and tunables.
    ///
    /// Resolves the bot identity, builds the normalizer, installs the inbound
    /// handler, and spawns the transport actor. The returned receiver yields
    /// every normalized `im.message.receive_v1` event; card actions are
    /// acknowledged but not routed (see module docs).
    ///
    /// # Errors
    ///
    /// Same contract as [`LarkBridge::start`].
    pub async fn start_with(
        endpoints: LarkEndpoints,
        creds: LarkCredentials,
        config: BridgeConfig,
    ) -> Result<(TransportHandle, mpsc::Receiver<QueuedInboundEvent>), LarkError> {
        let http = LarkHttp::new(endpoints)?;
        let tokens = TenantTokenProvider::new(http.clone(), creds.clone());
        let api = LarkApi::new(http.clone(), tokens);
        let info = api.bot_info().await?;
        let bot_open_id = info
            .open_id
            .filter(|open_id| !open_id.is_empty())
            .ok_or_else(|| LarkError::protocol("bot info response missing open_id"))?;
        let normalizer = Arc::new(Normalizer::new(api, bot_open_id));

        let (tx, rx) = mpsc::channel(config.event_capacity);
        let budget = Arc::new(Semaphore::new(config.event_byte_budget));
        let handler: InboundFrameHandler = Arc::new(move |headers, payload: Bytes| {
            let normalizer = Arc::clone(&normalizer);
            let tx = tx.clone();
            let budget = Arc::clone(&budget);
            Box::pin(async move {
                if matches!(headers.ty(), Some(MessageType::Card)) {
                    tracing::info!(
                        message_id = headers.message_id().unwrap_or(""),
                        "lark card action is unsupported in this milestone; acknowledging"
                    );
                    return Ok(Some(json!({ "status": "unsupported" })));
                }
                let outcome = normalizer.normalize(&payload).await?;
                match outcome {
                    NormalizeOutcome::Ignored { reason } => {
                        tracing::debug!(
                            reason,
                            message_id = headers.message_id().unwrap_or(""),
                            "lark event ignored by the normalizer"
                        );
                        Ok(None)
                    }
                    NormalizeOutcome::Event { event, degradation } => {
                        if let Some(degradation) = degradation {
                            tracing::warn!(
                                ?degradation,
                                message_id = %event.message_id,
                                "lark event normalized with degradation"
                            );
                        }
                        let size = u32::try_from(payload.len()).unwrap_or(u32::MAX);
                        let permit = budget.clone().try_acquire_many_owned(size).map_err(|_| {
                            LarkError::exhausted(
                                "the inbound event byte budget is full",
                                config.event_byte_budget as u64,
                            )
                        })?;
                        tx.try_send(QueuedInboundEvent {
                            event: *event,
                            permit,
                        })
                        .map_err(|_| {
                            LarkError::exhausted(
                                "the inbound event channel is full",
                                config.event_capacity as u64,
                            )
                        })?;
                        Ok(None)
                    }
                }
            })
        });
        let handle = LarkTransport::start_with_config(http, creds, handler, config.transport);
        Ok((handle, rx))
    }

    /// Starts the bridge with a single-use durable receipt-boundary runtime.
    ///
    /// Startup recovery is completely preloaded before the WebSocket actor is
    /// spawned. Live ordering is normalize → durable hook → count permit →
    /// byte permit → send, with no await after the hook returns.
    ///
    /// # Errors
    ///
    /// Returns a classified error for credential binding, invalid limits,
    /// startup recovery overflow, HTTP setup, or bot identity failures.
    pub async fn start_with_runtime(
        endpoints: LarkEndpoints,
        creds: LarkCredentials,
        config: BridgeConfig,
        intake: IntakeRuntime,
    ) -> Result<(TransportHandle, mpsc::Receiver<QueuedInboundEvent>), LarkError> {
        if !intake.matches(&creds) {
            return Err(LarkError::protocol(
                "durable intake credential binding mismatch",
            ));
        }
        if config.event_capacity == 0 || config.event_byte_budget == 0 {
            return Err(LarkError::protocol(
                "durable inbound channel bounds must be non-zero",
            ));
        }
        if config.event_capacity > Semaphore::MAX_PERMITS
            || config.event_byte_budget > Semaphore::MAX_PERMITS
        {
            return Err(LarkError::protocol(
                "durable inbound channel bound exceeds Tokio semaphore limits",
            ));
        }
        let http = LarkHttp::new(endpoints)?;
        let tokens = TenantTokenProvider::new(http.clone(), creds.clone());
        let api = LarkApi::new(http.clone(), tokens);
        let info = api.bot_info().await?;
        let bot_open_id = info
            .open_id
            .filter(|open_id| !open_id.is_empty())
            .ok_or_else(|| LarkError::protocol("bot info response missing open_id"))?;
        let normalizer = Arc::new(Normalizer::new(api, bot_open_id));
        let (recovery, hook) = intake.into_parts();

        let recovery_count = recovery.len();
        let recovery_bytes = recovery.iter().try_fold(0_usize, |total, item| {
            total.checked_add(item.retained_bytes())
        });
        let Some(recovery_bytes) = recovery_bytes else {
            return Err(LarkError::protocol(
                "startup inbound recovery bytes overflow",
            ));
        };
        if recovery_count > config.event_capacity {
            return Err(LarkError::exhausted(
                "startup inbound recovery count exceeds the channel",
                u64::try_from(config.event_capacity).unwrap_or(u64::MAX),
            ));
        }
        if recovery_bytes > config.event_byte_budget {
            return Err(LarkError::exhausted(
                "startup inbound recovery bytes exceed the channel budget",
                u64::try_from(config.event_byte_budget).unwrap_or(u64::MAX),
            ));
        }

        let (tx, rx) = mpsc::channel(config.event_capacity);
        let budget = Arc::new(Semaphore::new(config.event_byte_budget));
        let mut preload = Vec::with_capacity(recovery_count);
        for retained in recovery {
            let size = u32::try_from(retained.retained_bytes())
                .map_err(|_| LarkError::protocol("startup inbound payload size overflow"))?;
            let reserve = tx.clone().try_reserve_owned().map_err(|_| {
                LarkError::protocol("startup inbound recovery count reservation failed")
            })?;
            let permit = budget.clone().try_acquire_many_owned(size).map_err(|_| {
                LarkError::protocol("startup inbound recovery byte reservation failed")
            })?;
            preload.push((reserve, retained, permit));
        }
        for (reserve, retained, permit) in preload {
            reserve.send(QueuedInboundEvent {
                event: *retained.into_event(),
                permit,
            });
        }

        let handler: InboundFrameHandler = Arc::new(move |headers, payload: Bytes| {
            let normalizer = Arc::clone(&normalizer);
            let hook = Arc::clone(&hook);
            let tx = tx.clone();
            let budget = Arc::clone(&budget);
            Box::pin(async move {
                if matches!(headers.ty(), Some(MessageType::Card)) {
                    tracing::info!(
                        message_id = headers.message_id().unwrap_or(""),
                        "lark card action is unsupported in this milestone; acknowledging"
                    );
                    return Ok(Some(json!({ "status": "unsupported" })));
                }
                let outcome = normalizer.normalize(&payload).await?;
                let NormalizeOutcome::Event { event, degradation } = outcome else {
                    return Ok(None);
                };
                if let Some(degradation) = degradation {
                    tracing::warn!(
                        ?degradation,
                        message_id = %event.message_id,
                        "lark event normalized with degradation"
                    );
                }
                let retained = match hook(event).await? {
                    IntakeVerdict::DropDuplicate => return Ok(None),
                    IntakeVerdict::Enqueue(retained) => retained,
                };
                let reserve = tx.try_reserve_owned().map_err(|_| {
                    LarkError::exhausted(
                        "the durable inbound event channel is full",
                        u64::try_from(config.event_capacity).unwrap_or(u64::MAX),
                    )
                })?;
                let size = u32::try_from(retained.retained_bytes())
                    .map_err(|_| LarkError::protocol("durable inbound payload size overflow"))?;
                let permit = budget.try_acquire_many_owned(size).map_err(|_| {
                    LarkError::exhausted(
                        "the durable inbound byte budget is full",
                        u64::try_from(config.event_byte_budget).unwrap_or(u64::MAX),
                    )
                })?;
                reserve.send(QueuedInboundEvent {
                    event: *retained.into_event(),
                    permit,
                });
                Ok(None)
            })
        });
        let handle = LarkTransport::start_with_config(http, creds, handler, config.transport);
        Ok((handle, rx))
    }
}
