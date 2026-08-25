//! Durable, read-only reconciliation for operator-owned external Codex endpoints.
//!
//! This actor reconnects sockets, never processes. It resumes each managed thread before reading
//! bounded authoritative snapshots, folds terminal notifications by stable identifiers, and
//! commits only through a persisted connection-epoch fence. There is intentionally no write replay
//! or write-admission API in this module.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
    future::Future,
    time::Duration,
};

use serde::Serialize;
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    codex::{
        external::{EndpointLabel, ExternalCapabilityProfile, ExternalEndpointGate},
        external_transport::{
            ExternalReadEvent, ExternalReadOnlyConnection, ExternalTransportError,
        },
        rpc::ConnectionEpoch,
        types::{
            SortDirection, ThreadItem, ThreadItemsListParams, ThreadReadParams, ThreadResumeParams,
            ThreadTurnsListParams, Turn, TurnStatus,
        },
    },
    limits::{
        EXTERNAL_MANAGED_THREAD_CAPACITY, EXTERNAL_RECONCILE_ENDPOINT_BYTES,
        EXTERNAL_RECONCILE_ENTRY_CAPACITY, EXTERNAL_RECONCILE_EVENT_CAPACITY,
        EXTERNAL_RECONCILE_MAILBOX_BYTES, EXTERNAL_RECONCILE_PAGE_CAPACITY,
        EXTERNAL_RECONCILE_PAGE_SIZE, EXTERNAL_RECONCILE_THREAD_BYTES,
        EXTERNAL_RECONNECT_INITIAL_DELAY, EXTERNAL_RECONNECT_MAX_DELAY,
    },
    store::{
        ExternalApplyOutcome, ExternalEndpointState, ExternalFenceOutcome, ExternalItemTerminal,
        ExternalTerminalStatus, ExternalThreadSnapshot, ExternalThreadState, ExternalTurnTerminal,
        ExternalUncertaintyReason, StoreError, StoreHandle,
    },
};

const COMMAND_CAPACITY: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExternalRecoverySettings {
    pub request_timeout: Duration,
    pub reconnect_initial_delay: Duration,
    pub reconnect_max_delay: Duration,
}

impl Default for ExternalRecoverySettings {
    fn default() -> Self {
        Self {
            request_timeout: crate::limits::CONTROL_RPC_TIMEOUT,
            reconnect_initial_delay: EXTERNAL_RECONNECT_INITIAL_DELAY,
            reconnect_max_delay: EXTERNAL_RECONNECT_MAX_DELAY,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalRecoveryState {
    Connecting {
        epoch: u64,
    },
    Reconciling {
        epoch: u64,
    },
    Ready {
        epoch: u64,
    },
    Unavailable {
        epoch: u64,
        reason: ExternalUncertaintyReason,
    },
    Stopped,
}

impl ExternalRecoveryState {
    #[must_use]
    pub const fn ready_epoch(self) -> Option<u64> {
        match self {
            Self::Ready { epoch } => Some(epoch),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ExternalRecoveryError {
    #[error("external recovery requires the resume_shared capability profile")]
    UnsupportedProfile,
    #[error("external recovery settings are invalid")]
    InvalidSettings,
    #[error("external recovery durable state failed")]
    Store,
    #[error("external recovery actor is closed")]
    Closed,
    #[error("external recovery wait timed out")]
    Timeout,
}

enum RecoveryCommand {
    Wake,
    Reconnect {
        reason: ExternalUncertaintyReason,
        reply: oneshot::Sender<()>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// Handle for the socket-only recovery actor.
pub struct ExternalRecoveryCoordinator {
    store: StoreHandle,
    endpoint_label: EndpointLabel,
    commands: mpsc::Sender<RecoveryCommand>,
    state: watch::Receiver<ExternalRecoveryState>,
    cancellation: CancellationToken,
    actor: Option<JoinHandle<()>>,
}

impl fmt::Debug for ExternalRecoveryCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalRecoveryCoordinator")
            .field("endpoint_label", &self.endpoint_label)
            .field("state", &*self.state.borrow())
            .finish_non_exhaustive()
    }
}

impl ExternalRecoveryCoordinator {
    /// Starts socket reconciliation. The actor owns no process handle or server lifecycle API.
    ///
    /// # Errors
    ///
    /// Rejects non-resume gates and zero or inverted time bounds.
    pub fn start(
        gate: ExternalEndpointGate,
        store: StoreHandle,
        parent_cancellation: CancellationToken,
        settings: ExternalRecoverySettings,
    ) -> Result<Self, ExternalRecoveryError> {
        if gate.capability_profile() != ExternalCapabilityProfile::ResumeShared {
            return Err(ExternalRecoveryError::UnsupportedProfile);
        }
        if settings.request_timeout.is_zero()
            || settings.reconnect_initial_delay.is_zero()
            || settings.reconnect_max_delay < settings.reconnect_initial_delay
        {
            return Err(ExternalRecoveryError::InvalidSettings);
        }
        let endpoint_label = gate.endpoint_label().clone();
        let (commands, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (state_tx, state) = watch::channel(ExternalRecoveryState::Connecting { epoch: 0 });
        let cancellation = parent_cancellation.child_token();
        drop(parent_cancellation);
        let actor_cancel = cancellation.clone();
        let actor_store = store.clone();
        let actor_label = endpoint_label.clone();
        let actor = tokio::spawn(async move {
            run_recovery_actor(
                gate,
                actor_store,
                actor_label,
                command_rx,
                state_tx,
                actor_cancel,
                settings,
            )
            .await;
        });
        Ok(Self {
            store,
            endpoint_label,
            commands,
            state,
            cancellation,
            actor: Some(actor),
        })
    }

    #[must_use]
    pub fn state(&self) -> ExternalRecoveryState {
        *self.state.borrow()
    }

    #[must_use]
    pub fn subscribe_state(&self) -> watch::Receiver<ExternalRecoveryState> {
        self.state.clone()
    }

    /// Durably adopts a thread for read-only recovery and wakes the actor.
    ///
    /// # Errors
    ///
    /// Returns `Store` if registration cannot be persisted, or `Closed` if the actor has stopped.
    /// Registration is fenced by the actor's first durable epoch reservation, so a call that races
    /// actor startup can surface `Store` (a store not-found); callers should retry once the actor
    /// has published its first epoch.
    pub async fn manage_thread(&self, thread_id: &str) -> Result<(), ExternalRecoveryError> {
        self.store
            .register_external_thread(self.endpoint_label.as_str(), thread_id)
            .await
            .map_err(|_| ExternalRecoveryError::Store)?;
        self.commands
            .send(RecoveryCommand::Wake)
            .await
            .map_err(|_| ExternalRecoveryError::Closed)
    }

    /// Reads the durable terminal and uncertainty projection for one managed thread.
    ///
    /// # Errors
    ///
    /// Returns `Store` if the snapshot cannot be read.
    pub async fn thread_snapshot(
        &self,
        thread_id: &str,
    ) -> Result<Option<ExternalThreadSnapshot>, ExternalRecoveryError> {
        self.store
            .external_thread_snapshot(self.endpoint_label.as_str(), thread_id)
            .await
            .map_err(|_| ExternalRecoveryError::Store)
    }

    /// Requests a bridge-side socket reconnect. It cannot restart or signal the external server.
    ///
    /// # Errors
    ///
    /// Returns `Closed` if the recovery actor has stopped.
    pub async fn request_socket_reconnect(&self) -> Result<(), ExternalRecoveryError> {
        self.request_reconnect(ExternalUncertaintyReason::SocketDisconnect)
            .await
    }

    /// Records an operator-announced server restart and fences the current socket epoch.
    ///
    /// # Errors
    ///
    /// Returns `Closed` if the recovery actor has stopped.
    pub async fn note_operator_server_restart(&self) -> Result<(), ExternalRecoveryError> {
        self.request_reconnect(ExternalUncertaintyReason::ServerRestart)
            .await
    }

    async fn request_reconnect(
        &self,
        reason: ExternalUncertaintyReason,
    ) -> Result<(), ExternalRecoveryError> {
        let (reply, wait) = oneshot::channel();
        self.commands
            .send(RecoveryCommand::Reconnect { reason, reply })
            .await
            .map_err(|_| ExternalRecoveryError::Closed)?;
        wait.await.map_err(|_| ExternalRecoveryError::Closed)
    }

    /// Waits until reconciliation is ready on an epoch newer than `prior_epoch`.
    ///
    /// # Errors
    ///
    /// Returns `Timeout` at the supplied deadline, or `Closed` if the actor stops first.
    pub async fn wait_for_ready_after(
        &self,
        prior_epoch: u64,
        timeout: Duration,
    ) -> Result<u64, ExternalRecoveryError> {
        let mut state = self.state.clone();
        let wait = async move {
            loop {
                if let Some(epoch) = state.borrow_and_update().ready_epoch() {
                    if epoch > prior_epoch {
                        return Ok(epoch);
                    }
                }
                state
                    .changed()
                    .await
                    .map_err(|_| ExternalRecoveryError::Closed)?;
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| ExternalRecoveryError::Timeout)?
    }

    /// Cancels the actor and closes only its WebSocket.
    ///
    /// # Errors
    ///
    /// Returns `Closed` if the actor task fails while stopping.
    pub async fn shutdown(mut self) -> Result<(), ExternalRecoveryError> {
        let (reply, wait) = oneshot::channel();
        let _ = self
            .commands
            .send(RecoveryCommand::Shutdown { reply })
            .await;
        self.cancellation.cancel();
        if let Some(actor) = self.actor.take() {
            actor.await.map_err(|_| ExternalRecoveryError::Closed)?;
        }
        let _ = wait.await;
        Ok(())
    }
}

impl Drop for ExternalRecoveryCoordinator {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(actor) = self.actor.take() {
            actor.abort();
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn run_recovery_actor(
    gate: ExternalEndpointGate,
    store: StoreHandle,
    endpoint_label: EndpointLabel,
    mut commands: mpsc::Receiver<RecoveryCommand>,
    state: watch::Sender<ExternalRecoveryState>,
    cancellation: CancellationToken,
    settings: ExternalRecoverySettings,
) {
    let mut reconnect_reason = ExternalUncertaintyReason::BridgeRestart;
    let mut reconnect_delay = settings.reconnect_initial_delay;
    'outer: loop {
        if cancellation.is_cancelled() {
            break;
        }
        let Ok(reservation) = store
            .reserve_external_epoch(endpoint_label.as_str(), reconnect_reason)
            .await
        else {
            publish(
                &state,
                ExternalRecoveryState::Unavailable {
                    epoch: 0,
                    reason: ExternalUncertaintyReason::ProtocolViolation,
                },
            );
            if wait_backoff_or_command(
                &mut commands,
                &cancellation,
                reconnect_delay,
                &mut reconnect_reason,
            )
            .await
            {
                break;
            }
            reconnect_delay = next_delay(reconnect_delay, settings.reconnect_max_delay);
            continue;
        };
        let epoch = reservation.epoch;
        publish(&state, ExternalRecoveryState::Connecting { epoch });
        let connection = ExternalReadOnlyConnection::connect(
            &gate,
            ConnectionEpoch::new(epoch),
            cancellation.clone(),
        );
        let mut connection = tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            command = commands.recv() => {
                if handle_preconnect_command(command, &mut reconnect_reason) {
                    break;
                }
                continue;
            }
            result = connection => if let Ok(connection) = result {
                connection
            } else {
                let _ = store.mark_external_unavailable(
                    endpoint_label.as_str(),
                    epoch,
                    ExternalUncertaintyReason::SocketDisconnect,
                ).await;
                publish(&state, ExternalRecoveryState::Unavailable {
                    epoch,
                    reason: ExternalUncertaintyReason::SocketDisconnect,
                });
                if wait_backoff_or_command(
                    &mut commands,
                    &cancellation,
                    reconnect_delay,
                    &mut reconnect_reason,
                ).await {
                    break;
                }
                reconnect_delay = next_delay(reconnect_delay, settings.reconnect_max_delay);
                continue;
            },
        };
        reconnect_delay = settings.reconnect_initial_delay;
        publish(&state, ExternalRecoveryState::Reconciling { epoch });
        let _ = store
            .set_external_endpoint_state(
                endpoint_label.as_str(),
                epoch,
                ExternalEndpointState::Reconciling,
            )
            .await;
        match reconcile_epoch(
            &mut connection,
            &store,
            endpoint_label.as_str(),
            epoch,
            settings.request_timeout,
            &cancellation,
        )
        .await
        {
            Ok(()) => {
                let _ = store
                    .set_external_endpoint_state(
                        endpoint_label.as_str(),
                        epoch,
                        ExternalEndpointState::Ready,
                    )
                    .await;
                publish(&state, ExternalRecoveryState::Ready { epoch });
            }
            Err(failure) => {
                let reason = failure.reason();
                let _ = store
                    .mark_external_unavailable(endpoint_label.as_str(), epoch, reason)
                    .await;
                publish(&state, ExternalRecoveryState::Unavailable { epoch, reason });
                connection.abort();
                reconnect_reason = reason;
                if wait_backoff_or_command(
                    &mut commands,
                    &cancellation,
                    reconnect_delay,
                    &mut reconnect_reason,
                )
                .await
                {
                    break;
                }
                reconnect_delay = next_delay(reconnect_delay, settings.reconnect_max_delay);
                continue;
            }
        }

        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    let _ = connection.shutdown().await;
                    break 'outer;
                }
                command = commands.recv() => match command {
                    Some(RecoveryCommand::Wake) => {
                        reconnect_reason = ExternalUncertaintyReason::SocketDisconnect;
                        connection.abort();
                        continue 'outer;
                    }
                    Some(RecoveryCommand::Reconnect { reason, reply }) => {
                        reconnect_reason = reason;
                        let _ = store.mark_external_unavailable(
                            endpoint_label.as_str(), epoch, reason
                        ).await;
                        publish(&state, ExternalRecoveryState::Unavailable { epoch, reason });
                        connection.abort();
                        let _ = reply.send(());
                        continue 'outer;
                    }
                    Some(RecoveryCommand::Shutdown { reply }) => {
                        let _ = connection.shutdown().await;
                        let _ = reply.send(());
                        break 'outer;
                    }
                    None => break 'outer,
                },
                event = connection.recv_event() => match event {
                    Ok(Some(ExternalReadEvent::Closed(exit))) => {
                        reconnect_reason = closed_failure(exit).reason();
                        let _ = store.mark_external_unavailable(
                            endpoint_label.as_str(), epoch, reconnect_reason
                        ).await;
                        publish(&state, ExternalRecoveryState::Unavailable {
                            epoch,
                            reason: reconnect_reason,
                        });
                        connection.abort();
                        if wait_backoff_or_command(
                            &mut commands,
                            &cancellation,
                            reconnect_delay,
                            &mut reconnect_reason,
                        ).await {
                            break 'outer;
                        }
                        reconnect_delay = next_delay(
                            reconnect_delay,
                            settings.reconnect_max_delay,
                        );
                        continue 'outer;
                    }
                    Ok(None) => {
                        reconnect_reason = ExternalUncertaintyReason::SocketDisconnect;
                        let _ = store.mark_external_unavailable(
                            endpoint_label.as_str(), epoch, reconnect_reason
                        ).await;
                        publish(&state, ExternalRecoveryState::Unavailable {
                            epoch,
                            reason: reconnect_reason,
                        });
                        connection.abort();
                        if wait_backoff_or_command(
                            &mut commands,
                            &cancellation,
                            reconnect_delay,
                            &mut reconnect_reason,
                        ).await {
                            break 'outer;
                        }
                        reconnect_delay = next_delay(
                            reconnect_delay,
                            settings.reconnect_max_delay,
                        );
                        continue 'outer;
                    }
                    Ok(Some(event)) => {
                        if apply_live_event(&store, endpoint_label.as_str(), epoch, event).await.is_err() {
                            reconnect_reason = ExternalUncertaintyReason::ProtocolViolation;
                            let _ = store.mark_external_unavailable(
                                endpoint_label.as_str(), epoch, reconnect_reason
                            ).await;
                            publish(&state, ExternalRecoveryState::Unavailable {
                                epoch,
                                reason: reconnect_reason,
                            });
                            connection.abort();
                            if wait_backoff_or_command(
                                &mut commands,
                                &cancellation,
                                reconnect_delay,
                                &mut reconnect_reason,
                            ).await {
                                break 'outer;
                            }
                            reconnect_delay = next_delay(
                                reconnect_delay,
                                settings.reconnect_max_delay,
                            );
                            continue 'outer;
                        }
                    }
                    Err(_) => {
                        reconnect_reason = ExternalUncertaintyReason::ProtocolViolation;
                        let _ = store.mark_external_unavailable(
                            endpoint_label.as_str(), epoch, reconnect_reason
                        ).await;
                        publish(&state, ExternalRecoveryState::Unavailable {
                            epoch,
                            reason: reconnect_reason,
                        });
                        connection.abort();
                        if wait_backoff_or_command(
                            &mut commands,
                            &cancellation,
                            reconnect_delay,
                            &mut reconnect_reason,
                        ).await {
                            break 'outer;
                        }
                        reconnect_delay = next_delay(
                            reconnect_delay,
                            settings.reconnect_max_delay,
                        );
                        continue 'outer;
                    }
                }
            }
        }
    }
    let latest = store
        .external_endpoint_epoch(endpoint_label.as_str())
        .await
        .ok()
        .flatten();
    if let Some(latest) = latest {
        if latest.state != ExternalEndpointState::Unavailable {
            let _ = store
                .mark_external_unavailable(
                    endpoint_label.as_str(),
                    latest.epoch,
                    ExternalUncertaintyReason::BridgeRestart,
                )
                .await;
        }
        let _ = store
            .set_external_endpoint_state(
                endpoint_label.as_str(),
                latest.epoch,
                ExternalEndpointState::Stopped,
            )
            .await;
    }
    publish(&state, ExternalRecoveryState::Stopped);
}

#[derive(Clone, Copy, Debug)]
enum EpochFailure {
    ConnectionLost,
    RequestTimeout,
    ProtocolViolation,
    Store,
}

impl EpochFailure {
    const fn reason(self) -> ExternalUncertaintyReason {
        match self {
            Self::ConnectionLost => ExternalUncertaintyReason::SocketDisconnect,
            Self::RequestTimeout => ExternalUncertaintyReason::RequestTimeout,
            Self::ProtocolViolation | Self::Store => ExternalUncertaintyReason::ProtocolViolation,
        }
    }
}

async fn reconcile_epoch(
    connection: &mut ExternalReadOnlyConnection,
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    request_timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<(), EpochFailure> {
    let managed = store
        .external_managed_threads(endpoint_label)
        .await
        .map_err(|_| EpochFailure::Store)?;
    if managed.len() > EXTERNAL_MANAGED_THREAD_CAPACITY {
        return Err(EpochFailure::Store);
    }
    let managed_ids = managed
        .iter()
        .map(|thread| thread.thread_id.clone())
        .collect::<HashSet<_>>();
    let uncertain = managed
        .iter()
        .filter(|thread| thread.state == ExternalThreadState::Uncertain)
        .map(|thread| thread.thread_id.clone())
        .collect::<HashSet<_>>();
    let mut ready = HashSet::new();
    let mut mailboxes = ReconcileMailboxes::default();
    for thread in managed
        .iter()
        .filter(|thread| thread.state != ExternalThreadState::Uncertain)
    {
        match store
            .begin_external_reconciliation(endpoint_label, &thread.thread_id, epoch)
            .await
            .map_err(|_| EpochFailure::Store)?
        {
            ExternalFenceOutcome::Current => {}
            ExternalFenceOutcome::Stale => return Err(EpochFailure::ConnectionLost),
        }
        let result = reconcile_thread(
            connection,
            store,
            endpoint_label,
            epoch,
            &thread.thread_id,
            request_timeout,
            cancellation,
            &managed_ids,
            &uncertain,
            &ready,
            &mut mailboxes,
        )
        .await;
        match result {
            Ok(()) => {
                ready.insert(thread.thread_id.clone());
            }
            Err(ThreadFailure::Uncertain(reason)) => {
                store
                    .mark_external_thread_uncertain(
                        endpoint_label,
                        &thread.thread_id,
                        epoch,
                        reason,
                    )
                    .await
                    .map_err(|_| EpochFailure::Store)?;
                ready.insert(thread.thread_id.clone());
            }
            Err(ThreadFailure::Epoch(failure)) => return Err(failure),
        }
    }
    Ok(())
}

enum ThreadFailure {
    Uncertain(ExternalUncertaintyReason),
    Epoch(EpochFailure),
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn reconcile_thread(
    connection: &mut ExternalReadOnlyConnection,
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    thread_id: &str,
    request_timeout: Duration,
    cancellation: &CancellationToken,
    managed_ids: &HashSet<String>,
    uncertain: &HashSet<String>,
    ready: &HashSet<String>,
    mailboxes: &mut ReconcileMailboxes,
) -> Result<(), ThreadFailure> {
    let client = connection.client();
    let mut resume_params = ThreadResumeParams::new(thread_id);
    resume_params.overrides.exclude_turns = Some(true);
    let resume = await_request(
        connection,
        client.resume_thread_with_timeout(&resume_params, request_timeout),
        store,
        endpoint_label,
        epoch,
        cancellation,
        managed_ids,
        uncertain,
        ready,
        mailboxes,
    )
    .await
    .map_err(ThreadFailure::Epoch)?;
    let mut projection = TerminalProjection::default();
    add_response_bytes(&resume, mailboxes, &mut projection.bytes)?;
    if resume.thread.id != thread_id {
        return Err(ThreadFailure::Uncertain(
            ExternalUncertaintyReason::ProtocolViolation,
        ));
    }
    projection.add_thread(&resume.thread)?;

    let mut read_params = ThreadReadParams::new(thread_id);
    read_params.include_turns = Some(false);
    let read = await_request(
        connection,
        client.read_thread_with_timeout(&read_params, request_timeout),
        store,
        endpoint_label,
        epoch,
        cancellation,
        managed_ids,
        uncertain,
        ready,
        mailboxes,
    )
    .await
    .map_err(ThreadFailure::Epoch)?;
    add_response_bytes(&read, mailboxes, &mut projection.bytes)?;
    if read.thread.id != thread_id {
        return Err(ThreadFailure::Uncertain(
            ExternalUncertaintyReason::ProtocolViolation,
        ));
    }
    projection.add_thread(&read.thread)?;

    let mut cursor = None;
    let mut seen_cursors = HashSet::new();
    for page in 0..EXTERNAL_RECONCILE_PAGE_CAPACITY {
        let params = ThreadTurnsListParams {
            thread_id: thread_id.to_owned(),
            cursor: cursor.clone(),
            limit: Some(EXTERNAL_RECONCILE_PAGE_SIZE),
            sort_direction: Some(SortDirection::Ascending),
            items_view: None,
        };
        let response = await_request(
            connection,
            client.list_thread_turns_with_timeout(&params, request_timeout),
            store,
            endpoint_label,
            epoch,
            cancellation,
            managed_ids,
            uncertain,
            ready,
            mailboxes,
        )
        .await
        .map_err(ThreadFailure::Epoch)?;
        add_response_bytes(&response, mailboxes, &mut projection.bytes)?;
        for turn in &response.data {
            projection.add_turn(turn)?;
        }
        cursor = response.next_cursor;
        let Some(next) = cursor.as_ref() else {
            break;
        };
        if page + 1 == EXTERNAL_RECONCILE_PAGE_CAPACITY || !seen_cursors.insert(next.clone()) {
            return Err(ThreadFailure::Uncertain(
                ExternalUncertaintyReason::PageLimit,
            ));
        }
    }

    let turn_ids = projection.all_turns.iter().cloned().collect::<Vec<_>>();
    let mut item_page_count = 0_usize;
    for turn_id in turn_ids {
        let mut cursor = None;
        let mut seen_cursors = HashSet::new();
        loop {
            if item_page_count >= EXTERNAL_RECONCILE_PAGE_CAPACITY {
                return Err(ThreadFailure::Uncertain(
                    ExternalUncertaintyReason::PageLimit,
                ));
            }
            item_page_count = item_page_count.saturating_add(1);
            let params = ThreadItemsListParams {
                thread_id: thread_id.to_owned(),
                turn_id: Some(turn_id.clone()),
                cursor: cursor.clone(),
                limit: Some(EXTERNAL_RECONCILE_PAGE_SIZE),
                sort_direction: Some(SortDirection::Ascending),
            };
            let response = await_request(
                connection,
                client.list_thread_items_with_timeout(&params, request_timeout),
                store,
                endpoint_label,
                epoch,
                cancellation,
                managed_ids,
                uncertain,
                ready,
                mailboxes,
            )
            .await
            .map_err(ThreadFailure::Epoch)?;
            add_response_bytes(&response, mailboxes, &mut projection.bytes)?;
            for entry in &response.data {
                if entry.turn_id != turn_id {
                    return Err(ThreadFailure::Uncertain(
                        ExternalUncertaintyReason::ProtocolViolation,
                    ));
                }
                projection.add_listed_item(&entry.turn_id, &entry.item)?;
            }
            cursor = response.next_cursor;
            let Some(next) = cursor.as_ref() else {
                break;
            };
            if !seen_cursors.insert(next.clone()) {
                return Err(ThreadFailure::Uncertain(
                    ExternalUncertaintyReason::PageLimit,
                ));
            }
        }
    }
    if let Some(reason) = mailboxes.overflow.remove(thread_id) {
        return Err(ThreadFailure::Uncertain(reason));
    }
    for event in mailboxes.take(thread_id) {
        projection.add_event(event)?;
    }
    projection.fold_listed_items()?;
    let turns = projection
        .turns
        .into_iter()
        .map(|(turn_id, status)| ExternalTurnTerminal { turn_id, status })
        .collect();
    let items = projection
        .items
        .into_iter()
        .map(|(turn_id, item_id)| ExternalItemTerminal { turn_id, item_id })
        .collect();
    match store
        .apply_external_reconciliation(endpoint_label, thread_id, epoch, turns, items)
        .await
        .map_err(|_| ThreadFailure::Epoch(EpochFailure::Store))?
    {
        ExternalApplyOutcome::Applied { .. } => Ok(()),
        ExternalApplyOutcome::ConflictingTerminal => Err(ThreadFailure::Uncertain(
            ExternalUncertaintyReason::ConflictingTerminal,
        )),
        ExternalApplyOutcome::Stale => Err(ThreadFailure::Epoch(EpochFailure::ConnectionLost)),
    }
}

#[allow(clippy::too_many_arguments)]
async fn await_request<T, F>(
    connection: &mut ExternalReadOnlyConnection,
    request: F,
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    cancellation: &CancellationToken,
    managed_ids: &HashSet<String>,
    uncertain: &HashSet<String>,
    ready: &HashSet<String>,
    mailboxes: &mut ReconcileMailboxes,
) -> Result<T, EpochFailure>
where
    F: Future<Output = Result<T, ExternalTransportError>>,
{
    tokio::pin!(request);
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(EpochFailure::ConnectionLost),
            event = connection.recv_event() => {
                let event = event.map_err(|_| EpochFailure::ProtocolViolation)?
                    .ok_or(EpochFailure::ConnectionLost)?;
                if let ExternalReadEvent::Closed(exit) = event {
                    return Err(closed_failure(exit));
                }
                route_reconcile_event(
                    store,
                    endpoint_label,
                    epoch,
                    event,
                    managed_ids,
                    uncertain,
                    ready,
                    mailboxes,
                ).await?;
            }
            result = &mut request => return result.map_err(map_transport_failure),
        }
    }
}

fn map_transport_failure(error: ExternalTransportError) -> EpochFailure {
    match error {
        ExternalTransportError::RequestTimeout => EpochFailure::RequestTimeout,
        ExternalTransportError::ConnectionLost => EpochFailure::ConnectionLost,
        _ => EpochFailure::ProtocolViolation,
    }
}

#[allow(clippy::too_many_arguments)]
async fn route_reconcile_event(
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    event: ExternalReadEvent,
    managed_ids: &HashSet<String>,
    uncertain: &HashSet<String>,
    ready: &HashSet<String>,
    mailboxes: &mut ReconcileMailboxes,
) -> Result<(), EpochFailure> {
    if matches!(event, ExternalReadEvent::RemoteControlStatusChanged(_)) {
        return Ok(());
    }
    let thread_id = event_thread_id(&event)
        .ok_or(EpochFailure::ProtocolViolation)?
        .to_owned();
    if !managed_ids.contains(&thread_id) {
        return Err(EpochFailure::ProtocolViolation);
    }
    if uncertain.contains(&thread_id) {
        return Ok(());
    }
    if ready.contains(&thread_id) {
        apply_live_event(store, endpoint_label, epoch, event).await
    } else {
        mailboxes.push(&thread_id, event)
    }
}

async fn apply_live_event(
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    event: ExternalReadEvent,
) -> Result<(), EpochFailure> {
    if matches!(event, ExternalReadEvent::RemoteControlStatusChanged(_)) {
        return Ok(());
    }
    let Some(thread_id) = event_thread_id(&event).map(str::to_owned) else {
        return Err(EpochFailure::ConnectionLost);
    };
    let (turn, item) = terminal_from_event(event)?;
    if turn.is_none() && item.is_none() {
        return Ok(());
    }
    match store
        .record_external_terminal(endpoint_label, &thread_id, epoch, turn, item)
        .await
        .map_err(|_| EpochFailure::Store)?
    {
        ExternalApplyOutcome::Applied { .. } | ExternalApplyOutcome::ConflictingTerminal => Ok(()),
        ExternalApplyOutcome::Stale => Err(EpochFailure::ConnectionLost),
    }
}

fn terminal_from_event(
    event: ExternalReadEvent,
) -> Result<(Option<ExternalTurnTerminal>, Option<ExternalItemTerminal>), EpochFailure> {
    match event {
        ExternalReadEvent::RemoteControlStatusChanged(_)
        | ExternalReadEvent::ThreadGoalCleared(_)
        | ExternalReadEvent::ThreadStatusChanged(_) => Ok((None, None)),
        ExternalReadEvent::ItemCompleted(notification) => {
            let item_id = notification
                .item
                .id()
                .ok_or(EpochFailure::ProtocolViolation)?
                .to_owned();
            Ok((
                None,
                Some(ExternalItemTerminal {
                    turn_id: notification.turn_id,
                    item_id,
                }),
            ))
        }
        ExternalReadEvent::TurnCompleted(notification) => Ok((
            Some(ExternalTurnTerminal {
                turn_id: notification.turn.id,
                status: terminal_status(&notification.turn.status)?,
            }),
            None,
        )),
        ExternalReadEvent::Closed(_) => Err(EpochFailure::ConnectionLost),
    }
}

fn event_thread_id(event: &ExternalReadEvent) -> Option<&str> {
    match event {
        ExternalReadEvent::RemoteControlStatusChanged(_) | ExternalReadEvent::Closed(_) => None,
        ExternalReadEvent::ThreadGoalCleared(notification) => Some(&notification.thread_id),
        ExternalReadEvent::ThreadStatusChanged(notification) => Some(&notification.thread_id),
        ExternalReadEvent::ItemCompleted(notification) => Some(&notification.thread_id),
        ExternalReadEvent::TurnCompleted(notification) => Some(&notification.thread_id),
    }
}

#[derive(Default)]
struct ReconcileMailboxes {
    events: HashMap<String, Vec<ExternalReadEvent>>,
    thread_bytes: HashMap<String, usize>,
    endpoint_bytes: usize,
    overflow: HashMap<String, ExternalUncertaintyReason>,
}

impl ReconcileMailboxes {
    fn push(&mut self, thread_id: &str, event: ExternalReadEvent) -> Result<(), EpochFailure> {
        if self.overflow.contains_key(thread_id) {
            return Ok(());
        }
        let bytes = serde_json::to_vec(&EventWeight(&event))
            .map_err(|_| EpochFailure::ProtocolViolation)?
            .len();
        let events = self.events.entry(thread_id.to_owned()).or_default();
        let thread_bytes = self.thread_bytes.entry(thread_id.to_owned()).or_default();
        if events.len() >= EXTERNAL_RECONCILE_EVENT_CAPACITY
            || thread_bytes.saturating_add(bytes) > EXTERNAL_RECONCILE_MAILBOX_BYTES
            || self.endpoint_bytes.saturating_add(bytes) > EXTERNAL_RECONCILE_ENDPOINT_BYTES
        {
            self.overflow.insert(
                thread_id.to_owned(),
                ExternalUncertaintyReason::BufferOverflow,
            );
            return Ok(());
        }
        *thread_bytes = thread_bytes.saturating_add(bytes);
        self.endpoint_bytes = self.endpoint_bytes.saturating_add(bytes);
        events.push(event);
        Ok(())
    }

    fn take(&mut self, thread_id: &str) -> Vec<ExternalReadEvent> {
        let bytes = self.thread_bytes.remove(thread_id).unwrap_or(0);
        self.endpoint_bytes = self.endpoint_bytes.saturating_sub(bytes);
        self.events.remove(thread_id).unwrap_or_default()
    }
}

/// Serializes only for an exact bounded byte count; values are immediately discarded.
struct EventWeight<'a>(&'a ExternalReadEvent);

impl Serialize for EventWeight<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            ExternalReadEvent::RemoteControlStatusChanged(value) => value.serialize(serializer),
            ExternalReadEvent::ThreadGoalCleared(value) => value.serialize(serializer),
            ExternalReadEvent::ThreadStatusChanged(value) => value.serialize(serializer),
            ExternalReadEvent::ItemCompleted(value) => value.serialize(serializer),
            ExternalReadEvent::TurnCompleted(value) => value.serialize(serializer),
            ExternalReadEvent::Closed(_) => ().serialize(serializer),
        }
    }
}

#[derive(Default)]
struct TerminalProjection {
    turns: BTreeMap<String, ExternalTerminalStatus>,
    all_turns: BTreeSet<String>,
    items: BTreeSet<(String, String)>,
    listed_items: BTreeSet<(String, String)>,
    bytes: usize,
}

impl TerminalProjection {
    fn add_thread(&mut self, thread: &crate::codex::types::Thread) -> Result<(), ThreadFailure> {
        for turn in &thread.turns {
            self.add_turn(turn)?;
        }
        Ok(())
    }

    fn add_turn(&mut self, turn: &Turn) -> Result<(), ThreadFailure> {
        self.all_turns.insert(turn.id.clone());
        let status = match terminal_status(&turn.status) {
            Ok(status) => status,
            Err(_) if turn.status == TurnStatus::InProgress => return self.ensure_entry_bounds(),
            Err(_) => {
                return Err(ThreadFailure::Uncertain(
                    ExternalUncertaintyReason::ProtocolViolation,
                ));
            }
        };
        for item in &turn.items {
            self.add_item(&turn.id, item)?;
        }
        if let Some(prior) = self.turns.insert(turn.id.clone(), status) {
            if prior != status {
                return Err(ThreadFailure::Uncertain(
                    ExternalUncertaintyReason::ConflictingTerminal,
                ));
            }
        }
        self.ensure_entry_bounds()
    }

    fn add_item(&mut self, turn_id: &str, item: &ThreadItem) -> Result<(), ThreadFailure> {
        let item_id = item.id().ok_or(ThreadFailure::Uncertain(
            ExternalUncertaintyReason::ProtocolViolation,
        ))?;
        self.items.insert((turn_id.to_owned(), item_id.to_owned()));
        self.ensure_entry_bounds()
    }

    fn add_listed_item(&mut self, turn_id: &str, item: &ThreadItem) -> Result<(), ThreadFailure> {
        let item_id = item.id().ok_or(ThreadFailure::Uncertain(
            ExternalUncertaintyReason::ProtocolViolation,
        ))?;
        self.listed_items
            .insert((turn_id.to_owned(), item_id.to_owned()));
        self.ensure_entry_bounds()
    }

    fn fold_listed_items(&mut self) -> Result<(), ThreadFailure> {
        for (turn_id, item_id) in std::mem::take(&mut self.listed_items) {
            if self.turns.contains_key(&turn_id) {
                self.items.insert((turn_id, item_id));
            }
        }
        self.ensure_entry_bounds()
    }

    fn add_event(&mut self, event: ExternalReadEvent) -> Result<(), ThreadFailure> {
        let (turn, item) = terminal_from_event(event).map_err(ThreadFailure::Epoch)?;
        if let Some(turn) = turn {
            if let Some(prior) = self.turns.insert(turn.turn_id, turn.status) {
                if prior != turn.status {
                    return Err(ThreadFailure::Uncertain(
                        ExternalUncertaintyReason::ConflictingTerminal,
                    ));
                }
            }
        }
        if let Some(item) = item {
            self.items.insert((item.turn_id, item.item_id));
        }
        self.ensure_entry_bounds()
    }

    fn ensure_entry_bounds(&self) -> Result<(), ThreadFailure> {
        if self.turns.len() > EXTERNAL_RECONCILE_ENTRY_CAPACITY
            || self.all_turns.len() > EXTERNAL_RECONCILE_ENTRY_CAPACITY
            || self.items.len() > EXTERNAL_RECONCILE_ENTRY_CAPACITY
            || self.listed_items.len() > EXTERNAL_RECONCILE_ENTRY_CAPACITY
        {
            Err(ThreadFailure::Uncertain(
                ExternalUncertaintyReason::PageLimit,
            ))
        } else {
            Ok(())
        }
    }
}

fn terminal_status(status: &TurnStatus) -> Result<ExternalTerminalStatus, EpochFailure> {
    match status {
        TurnStatus::Completed => Ok(ExternalTerminalStatus::Completed),
        TurnStatus::Failed => Ok(ExternalTerminalStatus::Failed),
        TurnStatus::Interrupted => Ok(ExternalTerminalStatus::Interrupted),
        TurnStatus::InProgress | TurnStatus::Unknown(_) => Err(EpochFailure::ProtocolViolation),
    }
}

fn closed_failure(exit: crate::codex::transport::TransportExit) -> EpochFailure {
    match exit {
        crate::codex::transport::TransportExit::ProtocolViolation
        | crate::codex::transport::TransportExit::TaskFailed => EpochFailure::ProtocolViolation,
        _ => EpochFailure::ConnectionLost,
    }
}

fn add_response_bytes<T: Serialize>(
    response: &T,
    mailboxes: &mut ReconcileMailboxes,
    thread_bytes: &mut usize,
) -> Result<(), ThreadFailure> {
    let bytes = serde_json::to_vec(response)
        .map_err(|_| ThreadFailure::Epoch(EpochFailure::ProtocolViolation))?
        .len();
    *thread_bytes = thread_bytes.saturating_add(bytes);
    mailboxes.endpoint_bytes = mailboxes.endpoint_bytes.saturating_add(bytes);
    if *thread_bytes > EXTERNAL_RECONCILE_THREAD_BYTES
        || mailboxes.endpoint_bytes > EXTERNAL_RECONCILE_ENDPOINT_BYTES
    {
        Err(ThreadFailure::Uncertain(
            ExternalUncertaintyReason::BufferOverflow,
        ))
    } else {
        Ok(())
    }
}

fn publish(state: &watch::Sender<ExternalRecoveryState>, value: ExternalRecoveryState) {
    state.send_replace(value);
}

fn next_delay(current: Duration, maximum: Duration) -> Duration {
    current.saturating_mul(2).min(maximum)
}

async fn wait_backoff_or_command(
    commands: &mut mpsc::Receiver<RecoveryCommand>,
    cancellation: &CancellationToken,
    delay: Duration,
    reconnect_reason: &mut ExternalUncertaintyReason,
) -> bool {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => true,
        command = commands.recv() => handle_preconnect_command(command, reconnect_reason),
        () = tokio::time::sleep(delay) => false,
    }
}

fn handle_preconnect_command(
    command: Option<RecoveryCommand>,
    reconnect_reason: &mut ExternalUncertaintyReason,
) -> bool {
    match command {
        Some(RecoveryCommand::Wake) => false,
        Some(RecoveryCommand::Reconnect { reason, reply }) => {
            *reconnect_reason = reason;
            let _ = reply.send(());
            false
        }
        Some(RecoveryCommand::Shutdown { reply }) => {
            let _ = reply.send(());
            true
        }
        None => true,
    }
}

impl From<StoreError> for ExternalRecoveryError {
    fn from(_: StoreError) -> Self {
        Self::Store
    }
}
