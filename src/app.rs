//! Minimal application assembly shared with the durable outbound runtime.

use std::fmt;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{FutureExt, future::BoxFuture};
use tokio::sync::{mpsc, watch};

use crate::channel::native::{NativeChannel, NativeInboundSource};
use crate::channel::sidecar::{NodeSidecar, NodeSidecarConfig};
use crate::channel::{
    ChatMessageQuery, ConnectionState, ControlledMediaResolver, InboundRuntime, InboundSource,
    OutboundDelivery,
};
use crate::config::{ChannelSection, ChannelTransport};
use crate::lark::api::LarkApi;
use crate::lark::bridge::{BridgeConfig as LarkBridgeConfig, LarkBridge, QueuedInboundEvent};
use crate::lark::config::LarkEndpoints;
use crate::lark::credentials::{LarkCredentials, load_credentials};
use crate::lark::http::LarkHttp;
use crate::lark::token::TenantTokenProvider;
use crate::outbox::{OutboxPump, OutboxPumpConfig, OutboxReplySink};
use crate::runtime::attachments::{AttachmentCache, AttachmentLimits, ChannelResourceDownloader};
use crate::runtime::context::ContextRegistry;
use crate::runtime::intake::{DurableIntake, TenantNamespace};
use crate::runtime::policy::AccessPolicy;
use crate::runtime::router::{RouteAttemptError, RouteError, Router, RouterHandle, RouterSettings};
use crate::runtime::scope::DurableReplySink;
use crate::store::{StoreError, StoreHandle};
use crate::{codex::supervisor::AppServerSupervisor, config::BridgeConfig};

/// Static application startup, runtime, and shutdown failure categories.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AppError {
    /// Configuration loading or policy validation failed.
    #[error("bridge configuration is unavailable or invalid")]
    Config,
    /// Credentials could not be loaded or no source was configured.
    #[error("Lark credentials are unavailable or invalid")]
    Credentials,
    /// The durable store failed to start or stop.
    #[error("the durable store failed")]
    Store,
    /// Lark HTTP, identity, or transport startup failed.
    #[error("the Lark runtime failed")]
    Lark,
    /// The Codex app-server supervisor failed.
    #[error("the Codex supervisor failed")]
    Supervisor,
    /// The injected durable outbound runtime failed to start.
    #[error("the durable outbound runtime failed")]
    Outbound,
    /// The attachment cache failed to open or reconcile.
    #[error("the attachment cache failed")]
    Attachments,
    /// The scope router failed to start or stop.
    #[error("the scope router failed")]
    Router,
    /// The durable inbound producer disappeared without a shutdown request.
    #[error("the durable inbound stream closed unexpectedly")]
    InboundClosed,
}

/// Static failure classification for constructing the outbound runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("the durable outbound runtime could not start")]
pub struct OutboundStartError;

/// Factory boundary implemented by the durable outbox component.
pub trait OutboundFactory: Send + Sync {
    /// Builds the reply sink and starts its delivery pump.
    ///
    /// # Errors
    ///
    /// Returns a content-free classification if startup cannot complete.
    fn start(
        &self,
        store: StoreHandle,
        delivery: Arc<dyn OutboundDelivery>,
        transport: watch::Receiver<ConnectionState>,
    ) -> Result<OutboundRuntime, OutboundStartError>;
}

/// Production durable-outbox assembly used by the CLI.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProductionOutboundFactory;

impl OutboundFactory for ProductionOutboundFactory {
    fn start(
        &self,
        store: StoreHandle,
        delivery: Arc<dyn OutboundDelivery>,
        transport: watch::Receiver<ConnectionState>,
    ) -> Result<OutboundRuntime, OutboundStartError> {
        let sink: Arc<dyn DurableReplySink> = Arc::new(OutboxReplySink::new(store.clone()));
        let pump =
            OutboxPump::spawn_shared(store, delivery, transport, OutboxPumpConfig::default());
        Ok(OutboundRuntime::new(sink, async move {
            pump.shutdown().await;
        }))
    }
}

/// The two outbound capabilities needed by application assembly.
pub struct OutboundRuntime {
    sink: Arc<dyn DurableReplySink>,
    shutdown: BoxFuture<'static, ()>,
}

impl OutboundRuntime {
    /// Combines a durable sink with the future that orderly stops its pump.
    #[must_use]
    pub fn new<S>(sink: Arc<dyn DurableReplySink>, shutdown: S) -> Self
    where
        S: Future<Output = ()> + Send + 'static,
    {
        Self {
            sink,
            shutdown: shutdown.boxed(),
        }
    }

    /// Clones the reply sink for the scope router.
    #[must_use]
    pub fn sink(&self) -> Arc<dyn DurableReplySink> {
        Arc::clone(&self.sink)
    }

    /// Stops and joins the outbound delivery component.
    pub async fn shutdown(self) {
        self.shutdown.await;
    }
}

impl fmt::Debug for OutboundRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboundRuntime")
            .finish_non_exhaustive()
    }
}

/// Loads operator configuration and credentials, then runs until `shutdown`.
///
/// The concrete durable outbox is deliberately injected through
/// [`OutboundFactory`], keeping application assembly independent of its
/// implementation branch.
///
/// # Errors
///
/// Returns only content-free classifications for startup, runtime, or orderly
/// shutdown failures.
pub async fn run_with_outbound_until<F, S>(
    config_path: Option<&Path>,
    outbound_factory: &F,
    shutdown: S,
) -> Result<DriveSummary, AppError>
where
    F: OutboundFactory + ?Sized,
    S: Future<Output = ()>,
{
    let config = BridgeConfig::load(config_path).map_err(|_| AppError::Config)?;
    let credentials = load_credentials()
        .map_err(|_| AppError::Credentials)?
        .ok_or(AppError::Credentials)?;
    run_config_with_outbound_until(config, credentials, outbound_factory, shutdown).await
}

/// Runs the production bridge with the durable outbox until `shutdown`.
///
/// # Errors
///
/// Returns the same content-free startup/runtime classifications as
/// [`run_with_outbound_until`].
pub async fn run_until<S>(config_path: Option<&Path>, shutdown: S) -> Result<DriveSummary, AppError>
where
    S: Future<Output = ()>,
{
    run_with_outbound_until(config_path, &ProductionOutboundFactory, shutdown).await
}

/// Runs an already-loaded bridge configuration until `shutdown`.
///
/// This is the narrow assembly seam used by the eventual `run` CLI and by
/// integration tests that supply explicit credentials.
///
/// # Errors
///
/// Returns only content-free classifications. Components started before a
/// later startup failure are stopped before the error is returned.
#[allow(clippy::too_many_lines)]
pub async fn run_config_with_outbound_until<F, S>(
    config: BridgeConfig,
    credentials: LarkCredentials,
    outbound_factory: &F,
    shutdown: S,
) -> Result<DriveSummary, AppError>
where
    F: OutboundFactory + ?Sized,
    S: Future<Output = ()>,
{
    tracing::info!("bridge runtime starting");
    let policy = AccessPolicy::from_config(&config).map_err(|_| AppError::Config)?;
    let router_settings = RouterSettings::from_config(&config);
    let process_config = config.codex.process_config();
    let database_path = config.paths.database.clone();
    let attachment_cache_path = config.paths.attachment_cache.clone();
    let tenant = TenantNamespace::from_credentials(&credentials);
    let endpoints = LarkEndpoints::for_tenant(credentials.tenant);
    let http = LarkHttp::new(endpoints.clone()).map_err(|_| AppError::Lark)?;
    let tokens = TenantTokenProvider::new(http.clone(), credentials.clone());
    let api = LarkApi::new(http.clone(), tokens);
    let native = Arc::new(NativeChannel::new(api.clone()));

    let store = StoreHandle::open(&database_path)
        .await
        .map_err(|_| AppError::Store)?;
    tracing::debug!("durable store opened");
    let attachment_store = store.clone();
    let media: Arc<dyn ControlledMediaResolver> = native.clone();
    let attachment_downloader = Arc::new(ChannelResourceDownloader::new(media));
    let opened_attachment_cache = tokio::task::spawn_blocking(move || {
        AttachmentCache::open(
            &attachment_cache_path,
            attachment_store,
            attachment_downloader,
            AttachmentLimits::default(),
        )
    })
    .await;
    let Ok(Ok(attachment_cache)) = opened_attachment_cache else {
        stop_store_after_error(store).await;
        return Err(AppError::Attachments);
    };
    let attachment_cache = Arc::new(attachment_cache);
    if attachment_cache.reconcile().await.is_err() {
        drop(attachment_cache);
        stop_store_after_error(store).await;
        return Err(AppError::Attachments);
    }
    let context_registry = Arc::new(ContextRegistry::default());
    let Ok(InboundRuntime {
        source,
        events: inbound,
    }) = start_inbound(
        &config.channel,
        &credentials,
        &http,
        &api,
        Arc::clone(&native),
        &store,
    )
    .await
    else {
        stop_store_after_error(store).await;
        return Err(AppError::Lark);
    };
    let Ok(supervisor) = AppServerSupervisor::start(process_config).await else {
        source.shutdown().await;
        stop_store_after_error(store).await;
        return Err(AppError::Supervisor);
    };
    let delivery: Arc<dyn OutboundDelivery> = native;
    let Ok(outbound) = outbound_factory.start(store.clone(), delivery, source.subscribe_state())
    else {
        source.shutdown().await;
        stop_supervisor_after_error(supervisor).await;
        stop_store_after_error(store).await;
        return Err(AppError::Outbound);
    };
    let Ok(router) = Router::start_with_contexts(
        store.clone(),
        tenant,
        policy,
        router_settings,
        supervisor,
        outbound.sink(),
        Arc::clone(&attachment_cache),
        context_registry,
    )
    .await
    else {
        source.shutdown().await;
        outbound.shutdown().await;
        stop_store_after_error(store).await;
        return Err(AppError::Router);
    };
    tracing::info!("bridge runtime ready");

    let summary = drive_inbound(&router, inbound, shutdown).await;
    tracing::info!(
        exit = ?summary.exit,
        routed_events = summary.routed,
        route_failures = summary.route_failures,
        "bridge shutdown started"
    );

    // Stop producers first, then settle scope actors and their durable
    // projections before stopping the delivery pump and store writer.
    source.shutdown().await;
    let router_result = router.shutdown().await;
    outbound.shutdown().await;
    drop(attachment_cache);
    let store_result = store.shutdown().await;

    finish_run(summary, router_result, store_result)
}

fn finish_run(
    summary: DriveSummary,
    router_result: Result<(), RouteError>,
    store_result: Result<(), StoreError>,
) -> Result<DriveSummary, AppError> {
    router_result.map_err(|_| AppError::Router)?;
    store_result.map_err(|_| AppError::Store)?;
    tracing::info!(
        exit = ?summary.exit,
        routed_events = summary.routed,
        route_failures = summary.route_failures,
        "bridge runtime stopped"
    );
    match summary.exit {
        DriveExit::Shutdown => Ok(summary),
        DriveExit::InboundClosed => Err(AppError::InboundClosed),
        DriveExit::RouterFailed => Err(AppError::Router),
    }
}

async fn start_inbound(
    channel: &ChannelSection,
    credentials: &LarkCredentials,
    http: &LarkHttp,
    api: &LarkApi,
    native: Arc<NativeChannel>,
    store: &StoreHandle,
) -> Result<InboundRuntime, AppError> {
    let intake = DurableIntake::prepare(store.clone(), credentials)
        .await
        .map_err(|_| AppError::Lark)?;
    let bot_open_id = api
        .bot_info()
        .await
        .map_err(|_| AppError::Lark)?
        .open_id
        .filter(|open_id| !open_id.is_empty())
        .ok_or(AppError::Lark)?;
    let query: Arc<dyn ChatMessageQuery> = native;
    let normalizer = Arc::new(crate::lark::normalize::Normalizer::with_query(
        query,
        bot_open_id,
    ));
    let bridge_config = LarkBridgeConfig::default();
    let (event_handler, events) =
        LarkBridge::prepare_durable(credentials, bridge_config, intake, normalizer)
            .map_err(|_| AppError::Lark)?;
    let source: Box<dyn InboundSource> = match channel.transport {
        ChannelTransport::Native => {
            Box::new(NativeInboundSource::new(LarkBridge::start_prepared_native(
                http.clone(),
                credentials.clone(),
                bridge_config,
                event_handler,
            )))
        }
        ChannelTransport::NodeSidecar => {
            let sidecar_config = NodeSidecarConfig {
                node_binary: channel.node_binary.clone(),
                entrypoint: channel.sidecar_entrypoint.clone(),
                ..NodeSidecarConfig::default()
            };
            match NodeSidecar::start(
                sidecar_config,
                credentials.clone(),
                Arc::clone(&event_handler),
            )
            .await
            {
                Ok(sidecar) => Box::new(sidecar),
                Err(_) if channel.fallback_to_native => {
                    tracing::warn!("node sidecar startup failed; using configured native fallback");
                    Box::new(NativeInboundSource::new(LarkBridge::start_prepared_native(
                        http.clone(),
                        credentials.clone(),
                        bridge_config,
                        event_handler,
                    )))
                }
                Err(_) => return Err(AppError::Lark),
            }
        }
    };
    Ok(InboundRuntime { source, events })
}

async fn stop_supervisor_after_error(supervisor: crate::codex::supervisor::SupervisorHandle) {
    if supervisor.shutdown().await.is_err() {
        tracing::warn!("Codex supervisor cleanup failed after startup error");
    }
}

async fn stop_store_after_error(store: StoreHandle) {
    if store.shutdown().await.is_err() {
        tracing::warn!("durable store cleanup failed after startup error");
    }
}

/// Why the durable inbound driver stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DriveExit {
    /// The process received its external shutdown signal.
    Shutdown,
    /// Every durable inbound producer disappeared unexpectedly.
    InboundClosed,
    /// The router consumed an event and failed, or stopped accepting work.
    RouterFailed,
}

/// Bounded, content-free observations from one driver run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DriveSummary {
    /// The condition that stopped the driver.
    pub exit: DriveExit,
    /// Events accepted by the scope router.
    pub routed: u64,
    /// Events the router could not accept during this run.
    pub route_failures: u64,
}

enum EventRouteError {
    Retry {
        error: RouteError,
        event: Box<QueuedInboundEvent>,
    },
    Fatal(RouteError),
}

trait EventRouter: Send + Sync {
    fn route(&self, event: QueuedInboundEvent) -> BoxFuture<'_, Result<(), EventRouteError>>;
}

impl EventRouter for RouterHandle {
    fn route(&self, event: QueuedInboundEvent) -> BoxFuture<'_, Result<(), EventRouteError>> {
        async move {
            self.route_recoverable(event)
                .await
                .map_err(classify_route_failure)
        }
        .boxed()
    }
}

fn classify_route_failure(failure: RouteAttemptError) -> EventRouteError {
    let (error, event) = failure.into_parts();
    match (error, event) {
        (RouteError::Capacity, Some(event)) => EventRouteError::Retry {
            error: RouteError::Capacity,
            event,
        },
        (error, _) => EventRouteError::Fatal(error),
    }
}

const ROUTE_RETRY_BASE: Duration = Duration::from_millis(25);
const ROUTE_RETRY_MAX: Duration = Duration::from_secs(1);

fn route_retry_delay(attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(6);
    ROUTE_RETRY_BASE
        .saturating_mul(1_u32 << shift)
        .min(ROUTE_RETRY_MAX)
}

async fn drive_inbound<R, S>(
    router: &R,
    mut inbound: mpsc::Receiver<QueuedInboundEvent>,
    shutdown: S,
) -> DriveSummary
where
    R: EventRouter + ?Sized,
    S: Future<Output = ()>,
{
    tokio::pin!(shutdown);
    let mut routed_count = 0_u64;
    let mut route_failures = 0_u64;

    let exit = 'driver: loop {
        let event = tokio::select! {
            biased;
            () = &mut shutdown => break DriveExit::Shutdown,
            event = inbound.recv() => event,
        };
        let Some(event) = event else {
            break DriveExit::InboundClosed;
        };
        let mut event = event;
        let mut retry_attempt = 0_u32;
        loop {
            match router.route(event).await {
                Ok(()) => {
                    routed_count = routed_count.saturating_add(1);
                    break;
                }
                Err(EventRouteError::Retry {
                    error,
                    event: retry_event,
                }) => {
                    route_failures = route_failures.saturating_add(1);
                    retry_attempt = retry_attempt.saturating_add(1);
                    let delay = route_retry_delay(retry_attempt);
                    tracing::warn!(
                        error = %error,
                        retry_attempt,
                        delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                        "durable inbound routing will retry"
                    );
                    tokio::select! {
                        biased;
                        () = &mut shutdown => break 'driver DriveExit::Shutdown,
                        () = tokio::time::sleep(delay) => event = *retry_event,
                    }
                }
                Err(EventRouteError::Fatal(error)) => {
                    route_failures = route_failures.saturating_add(1);
                    tracing::error!(error = %error, "durable inbound router failed");
                    break 'driver DriveExit::RouterFailed;
                }
            }
        }
    };

    DriveSummary {
        exit,
        routed: routed_count,
        route_failures,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::pending;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use futures_util::{FutureExt, future::BoxFuture};
    use secrecy::SecretString;
    use tokio::sync::{Semaphore, mpsc, watch};

    use super::{
        DriveExit, EventRouteError, EventRouter, OutboundFactory, OutboundRuntime,
        ProductionOutboundFactory, drive_inbound,
    };
    use crate::channel::OutboundDelivery;
    use crate::channel::native::NativeChannel;
    use crate::lark::api::{ChatMode, LarkApi};
    use crate::lark::bridge::QueuedInboundEvent;
    use crate::lark::config::{LarkEndpoints, TenantBrand};
    use crate::lark::credentials::LarkCredentials;
    use crate::lark::http::LarkHttp;
    use crate::lark::normalize::{InboundEvent, ScopeKey};
    use crate::lark::token::TenantTokenProvider;
    use crate::lark::transport::TransportState;
    use crate::runtime::router::RouteError;
    use crate::runtime::scope::{DurableReplySink, ReplySinkError, TurnFinalization};
    use crate::store::{InboundRejectionKind, NewOutboxRow, StoreHandle};

    struct FakeRouter {
        event_ids: Mutex<Vec<String>>,
        failures: Mutex<VecDeque<RouteError>>,
    }

    impl EventRouter for FakeRouter {
        fn route(&self, event: QueuedInboundEvent) -> BoxFuture<'_, Result<(), EventRouteError>> {
            async move {
                let event_id = event.event.event_id.clone();
                self.event_ids.lock().expect("event ids").push(event_id);
                match self.failures.lock().expect("failures").pop_front() {
                    Some(RouteError::Capacity) => Err(EventRouteError::Retry {
                        error: RouteError::Capacity,
                        event: Box::new(event),
                    }),
                    Some(error) => Err(EventRouteError::Fatal(error)),
                    None => Ok(()),
                }
            }
            .boxed()
        }
    }

    async fn queued(event_id: &str) -> QueuedInboundEvent {
        let permit = Arc::new(Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("permit");
        QueuedInboundEvent::new(
            InboundEvent {
                event_id: event_id.to_owned(),
                message_id: format!("message-{event_id}"),
                chat_id: "chat-app-driver".to_owned(),
                sender_id: "owner-app-driver".to_owned(),
                chat_type: ChatMode::P2p,
                thread_id: None,
                root_id: None,
                reply_to_message_id: None,
                text: "hello".to_owned(),
                mentions_bot: false,
                mention_all: false,
                sender_is_human: true,
                mentions: Vec::new(),
                parts: Vec::new(),
                resources: Vec::new(),
                message_type: "text".to_owned(),
                create_time_ms: 1,
                scope: ScopeKey::Chat("chat-app-driver".to_owned()),
            },
            permit,
        )
    }

    #[tokio::test]
    async fn driver_routes_in_order_until_the_durable_channel_closes() {
        let router = FakeRouter {
            event_ids: Mutex::new(Vec::new()),
            failures: Mutex::new(VecDeque::new()),
        };
        let (sender, receiver) = mpsc::channel(2);
        sender.send(queued("first").await).await.expect("first");
        sender.send(queued("second").await).await.expect("second");
        drop(sender);

        let summary = drive_inbound(&router, receiver, pending::<()>()).await;

        assert_eq!(summary.exit, DriveExit::InboundClosed);
        assert_eq!(summary.routed, 2);
        assert_eq!(summary.route_failures, 0);
        assert_eq!(
            *router.event_ids.lock().expect("event ids"),
            vec!["first", "second"]
        );
    }

    #[tokio::test]
    async fn transient_route_failure_retries_the_same_event_before_later_work() {
        let router = FakeRouter {
            event_ids: Mutex::new(Vec::new()),
            failures: Mutex::new(VecDeque::from([RouteError::Capacity])),
        };
        let (sender, receiver) = mpsc::channel(2);
        sender.send(queued("first").await).await.expect("first");
        sender.send(queued("second").await).await.expect("second");
        drop(sender);

        let summary = drive_inbound(&router, receiver, pending::<()>()).await;

        assert_eq!(summary.routed, 2);
        assert_eq!(summary.route_failures, 1);
        assert_eq!(
            *router.event_ids.lock().expect("event ids"),
            vec!["first", "first", "second"]
        );
    }

    #[tokio::test]
    async fn closed_router_stops_the_driver_instead_of_parking_more_work() {
        let router = FakeRouter {
            event_ids: Mutex::new(Vec::new()),
            failures: Mutex::new(VecDeque::from([RouteError::Closed])),
        };
        let (sender, receiver) = mpsc::channel(2);
        sender.send(queued("first").await).await.expect("first");
        sender.send(queued("second").await).await.expect("second");

        let summary = drive_inbound(&router, receiver, pending::<()>()).await;

        assert_eq!(summary.exit, DriveExit::RouterFailed);
        assert_eq!(summary.routed, 0);
        assert_eq!(summary.route_failures, 1);
        assert_eq!(*router.event_ids.lock().expect("event ids"), vec!["first"]);
    }

    #[tokio::test]
    async fn shutdown_signal_stops_before_waiting_for_more_input() {
        let router = FakeRouter {
            event_ids: Mutex::new(Vec::new()),
            failures: Mutex::new(VecDeque::new()),
        };
        let (_sender, receiver) = mpsc::channel(1);

        let summary = drive_inbound(&router, receiver, async {}).await;

        assert_eq!(summary.exit, DriveExit::Shutdown);
        assert_eq!(summary.routed, 0);
        assert_eq!(summary.route_failures, 0);
    }

    struct NoopSink;

    impl DurableReplySink for NoopSink {
        fn rejection_notice(
            &self,
            _event: &InboundEvent,
            _reason: InboundRejectionKind,
        ) -> Result<NewOutboxRow, ReplySinkError> {
            Err(ReplySinkError::Unavailable)
        }

        fn finalize(
            &self,
            _turn: TurnFinalization,
        ) -> BoxFuture<'static, Result<(), ReplySinkError>> {
            async { Err(ReplySinkError::Unavailable) }.boxed()
        }
    }

    #[tokio::test]
    async fn outbound_runtime_exposes_only_the_sink_and_orderly_shutdown() {
        let stopped = Arc::new(AtomicBool::new(false));
        let task_stopped = Arc::clone(&stopped);
        let runtime = OutboundRuntime::new(Arc::new(NoopSink), async move {
            task_stopped.store(true, Ordering::SeqCst);
        });

        let _sink: Arc<dyn DurableReplySink> = runtime.sink();
        assert_eq!(format!("{runtime:?}"), "OutboundRuntime { .. }");
        runtime.shutdown().await;

        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn production_outbound_factory_starts_sink_and_joins_pump() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        let credentials = LarkCredentials::new(
            "cli_app_factory".to_owned(),
            SecretString::from("test-secret"),
            TenantBrand::Feishu,
        );
        let endpoints = LarkEndpoints::for_tenant(TenantBrand::Feishu);
        let http = LarkHttp::new(endpoints).expect("http");
        let tokens = TenantTokenProvider::new(http.clone(), credentials);
        let api = LarkApi::new(http, tokens);
        let (_transport, state) = watch::channel(TransportState::Connecting { attempt: 1 });

        let delivery: Arc<dyn OutboundDelivery> = Arc::new(NativeChannel::new(api));
        let runtime = ProductionOutboundFactory
            .start(store.clone(), delivery, state)
            .expect("factory start");
        let event = queued("factory").await;
        let row = runtime
            .sink()
            .rejection_notice(&event.event, InboundRejectionKind::Policy)
            .expect("production sink");
        assert_eq!(row.kind, "notice");
        runtime.shutdown().await;
        store.shutdown().await.expect("store shutdown");
    }
}
