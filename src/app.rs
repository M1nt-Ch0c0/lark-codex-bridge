//! Minimal application assembly shared with the durable outbound runtime.

use std::fmt;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use futures_util::{FutureExt, future::BoxFuture};
use tokio::sync::{mpsc, watch};

use crate::lark::api::LarkApi;
use crate::lark::bridge::{BridgeConfig as LarkBridgeConfig, LarkBridge, QueuedInboundEvent};
use crate::lark::config::LarkEndpoints;
use crate::lark::credentials::{LarkCredentials, load_credentials};
use crate::lark::http::LarkHttp;
use crate::lark::token::TenantTokenProvider;
use crate::lark::transport::TransportState;
use crate::runtime::intake::{DurableIntake, TenantNamespace};
use crate::runtime::policy::AccessPolicy;
use crate::runtime::router::{RouteError, Router, RouterHandle, RouterSettings};
use crate::runtime::scope::DurableReplySink;
use crate::store::StoreHandle;
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
        api: LarkApi,
        transport: watch::Receiver<TransportState>,
    ) -> Result<OutboundRuntime, OutboundStartError>;
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

/// Runs an already-loaded bridge configuration until `shutdown`.
///
/// This is the narrow assembly seam used by the eventual `run` CLI and by
/// integration tests that supply explicit credentials.
///
/// # Errors
///
/// Returns only content-free classifications. Components started before a
/// later startup failure are stopped before the error is returned.
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
    let policy = AccessPolicy::from_config(&config).map_err(|_| AppError::Config)?;
    let router_settings = RouterSettings::from_config(&config);
    let process_config = config.codex.process_config();
    let database_path = config.paths.database.clone();
    let tenant = TenantNamespace::from_credentials(&credentials);
    let endpoints = LarkEndpoints::for_tenant(credentials.tenant);
    let http = LarkHttp::new(endpoints.clone()).map_err(|_| AppError::Lark)?;
    let tokens = TenantTokenProvider::new(http.clone(), credentials.clone());
    let api = LarkApi::new(http, tokens);

    let store = StoreHandle::open(&database_path)
        .await
        .map_err(|_| AppError::Store)?;
    let Ok(intake) = DurableIntake::prepare(store.clone(), &credentials).await else {
        stop_store_after_error(store).await;
        return Err(AppError::Lark);
    };
    let Ok((transport, inbound)) =
        LarkBridge::start_with_runtime(endpoints, credentials, LarkBridgeConfig::default(), intake)
            .await
    else {
        stop_store_after_error(store).await;
        return Err(AppError::Lark);
    };
    let Ok(supervisor) = AppServerSupervisor::start(process_config).await else {
        transport.shutdown().await;
        stop_store_after_error(store).await;
        return Err(AppError::Supervisor);
    };
    let Ok(outbound) = outbound_factory.start(store.clone(), api, transport.subscribe_state())
    else {
        transport.shutdown().await;
        stop_supervisor_after_error(supervisor).await;
        stop_store_after_error(store).await;
        return Err(AppError::Outbound);
    };
    let Ok(router) = Router::start(
        store.clone(),
        tenant,
        policy,
        router_settings,
        supervisor,
        outbound.sink(),
    )
    .await
    else {
        transport.shutdown().await;
        outbound.shutdown().await;
        stop_store_after_error(store).await;
        return Err(AppError::Router);
    };

    let summary = drive_inbound(&router, inbound, shutdown).await;

    // Stop producers first, then settle scope actors and their durable
    // projections before stopping the delivery pump and store writer.
    transport.shutdown().await;
    let router_result = router.shutdown().await;
    outbound.shutdown().await;
    let store_result = store.shutdown().await;

    if router_result.is_err() {
        return Err(AppError::Router);
    }
    if store_result.is_err() {
        return Err(AppError::Store);
    }
    if summary.exit == DriveExit::InboundClosed {
        return Err(AppError::InboundClosed);
    }
    Ok(summary)
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

trait EventRouter: Send + Sync {
    fn route(&self, event: QueuedInboundEvent) -> BoxFuture<'_, Result<(), RouteError>>;
}

impl EventRouter for RouterHandle {
    fn route(&self, event: QueuedInboundEvent) -> BoxFuture<'_, Result<(), RouteError>> {
        async move { self.route(event).await }.boxed()
    }
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

    let exit = loop {
        let event = tokio::select! {
            biased;
            () = &mut shutdown => break DriveExit::Shutdown,
            event = inbound.recv() => event,
        };
        let Some(event) = event else {
            break DriveExit::InboundClosed;
        };
        match router.route(event).await {
            Ok(()) => routed_count = routed_count.saturating_add(1),
            Err(error) => {
                route_failures = route_failures.saturating_add(1);
                tracing::warn!(error = %error, "durable inbound routing failed");
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
    use std::future::pending;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use futures_util::{FutureExt, future::BoxFuture};
    use tokio::sync::{Semaphore, mpsc};

    use super::{DriveExit, EventRouter, OutboundRuntime, drive_inbound};
    use crate::lark::api::ChatMode;
    use crate::lark::bridge::QueuedInboundEvent;
    use crate::lark::normalize::{InboundEvent, ScopeKey};
    use crate::runtime::router::RouteError;
    use crate::runtime::scope::{DurableReplySink, ReplySinkError, TurnFinalization};
    use crate::store::{InboundRejectionKind, NewOutboxRow};

    struct FakeRouter {
        event_ids: Mutex<Vec<String>>,
        fail_event: Option<&'static str>,
    }

    impl EventRouter for FakeRouter {
        fn route(&self, event: QueuedInboundEvent) -> BoxFuture<'_, Result<(), RouteError>> {
            async move {
                let event_id = event.event.event_id;
                self.event_ids
                    .lock()
                    .expect("event ids")
                    .push(event_id.clone());
                if self.fail_event == Some(event_id.as_str()) {
                    Err(RouteError::Capacity)
                } else {
                    Ok(())
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
        QueuedInboundEvent {
            event: InboundEvent {
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
                resources: Vec::new(),
                message_type: "text".to_owned(),
                create_time_ms: 1,
                scope: ScopeKey::Chat("chat-app-driver".to_owned()),
            },
            permit,
        }
    }

    #[tokio::test]
    async fn driver_routes_in_order_until_the_durable_channel_closes() {
        let router = FakeRouter {
            event_ids: Mutex::new(Vec::new()),
            fail_event: None,
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
    async fn one_route_failure_does_not_stop_later_durable_work() {
        let router = FakeRouter {
            event_ids: Mutex::new(Vec::new()),
            fail_event: Some("first"),
        };
        let (sender, receiver) = mpsc::channel(2);
        sender.send(queued("first").await).await.expect("first");
        sender.send(queued("second").await).await.expect("second");
        drop(sender);

        let summary = drive_inbound(&router, receiver, pending::<()>()).await;

        assert_eq!(summary.routed, 1);
        assert_eq!(summary.route_failures, 1);
        assert_eq!(
            *router.event_ids.lock().expect("event ids"),
            vec!["first", "second"]
        );
    }

    #[tokio::test]
    async fn shutdown_signal_stops_before_waiting_for_more_input() {
        let router = FakeRouter {
            event_ids: Mutex::new(Vec::new()),
            fail_event: None,
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
}
