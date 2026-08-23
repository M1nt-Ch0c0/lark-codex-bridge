//! Durable, exact-target mutation coordination for a shared external Codex endpoint.

use std::{
    collections::HashMap,
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    codex::{
        external::{EndpointLabel, ExternalCapabilityProfile, ExternalEndpointGate},
        external_recovery::reconcile_epoch,
        external_transport::{
            ExternalApprovalRequest, ExternalMutationClient, ExternalReadEvent,
            ExternalReadOnlyConnection, ExternalTransportError,
        },
        rpc::ConnectionEpoch,
        types::{
            CommandExecutionApprovalDecision, CommandExecutionRequestApprovalResult,
            FileChangeRequestApprovalResult, PermissionsRequestApprovalResult,
            SimpleApprovalDecision, SortDirection, ThreadItemsListParams, ThreadQueueAddParams,
            ThreadQueueListParams, ThreadQueueStartParams, ThreadReadParams, ThreadTurnsListParams,
            TurnInterruptParams, TurnStartParams, TurnStatus, TurnSteerParams,
        },
    },
    limits::{
        EXTERNAL_WRITE_SHUTDOWN_TIMEOUT, MAX_OUTBOUND_VALUE_WIRE_BYTES, ROUTING_ID_BYTE_LIMIT,
    },
    runtime::policy::AuthorizedLarkActor,
    store::{
        ExternalApprovalClaimOutcome, ExternalApprovalKind, ExternalApprovalReassignmentOutcome,
        ExternalApprovalReceiveOutcome, ExternalApprovalResolution, ExternalEndpointState,
        ExternalMutationKind, ExternalMutationResolution, ExternalPrepareOutcome,
        ExternalTransitionOutcome, NewExternalApprovalClaim, NewExternalMutationIntent,
        StoreHandle,
    },
};
use uuid::Uuid;

const COMMAND_CAPACITY: usize = 32;
const APPROVAL_CAPACITY: usize = 64;
const APPROVAL_DEADLINE_MAX: Duration = Duration::from_secs(5 * 60);
const APPROVAL_REMOTE_MARGIN: Duration = Duration::from_secs(5);

#[derive(Clone, Eq, PartialEq)]
pub struct ExternalWriteSettings {
    pub request_timeout: Duration,
    pub approval_timeout: Duration,
    pub client_actor: String,
    pub approval_actor: String,
    pub approval_reviewer: String,
    pub approval_recipient: AuthorizedLarkActor,
}

impl fmt::Debug for ExternalWriteSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalWriteSettings")
            .field("request_timeout", &self.request_timeout)
            .field("approval_timeout", &self.approval_timeout)
            .field("client_actor", &"[redacted]")
            .field("approval_actor", &"[redacted]")
            .field("approval_reviewer", &"[configured]")
            .field("approval_recipient", &self.approval_recipient)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ExternalWriteError {
    #[error("external writes require mutate_shared or queue_shared")]
    UnsupportedProfile,
    #[error("external write settings or parameters are invalid")]
    InvalidSettings,
    #[error("external write durable state failed")]
    Store,
    #[error("external write transport failed")]
    Transport,
    #[error("external thread is not authoritatively ready")]
    NotReady,
    #[error("external thread already has a writer")]
    Busy,
    #[error("external thread is fenced by uncertainty")]
    Uncertain,
    #[error("external mutation source does not own the exact target")]
    Unauthorized,
    #[error("external live state conflicts with the exact mutation target")]
    Conflict,
    #[error("external mutation result is ambiguous and was fenced")]
    Ambiguous,
    #[error("external write coordinator is closed")]
    Closed,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ExternalMutationApplied {
    pub result_id: Option<String>,
}

impl fmt::Debug for ExternalMutationApplied {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalMutationApplied")
            .field("has_result_id", &self.result_id.is_some())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalApprovalPromptKind {
    Command,
    FileChange,
    Permissions,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ExternalApprovalPrompt {
    pub approval_id: String,
    pub kind: ExternalApprovalPromptKind,
    pub deadline_ms: i64,
}

impl fmt::Debug for ExternalApprovalPrompt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalApprovalPrompt")
            .field("kind", &self.kind)
            .field("deadline_ms", &self.deadline_ms)
            .finish_non_exhaustive()
    }
}

pub enum ExternalApprovalDecision {
    Command(CommandExecutionRequestApprovalResult),
    FileChange(FileChangeRequestApprovalResult),
    Permissions(PermissionsRequestApprovalResult),
}

impl fmt::Debug for ExternalApprovalDecision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Command(_) => "ExternalApprovalDecision::Command([redacted])",
            Self::FileChange(_) => "ExternalApprovalDecision::FileChange([redacted])",
            Self::Permissions(_) => "ExternalApprovalDecision::Permissions([redacted])",
        })
    }
}

enum WriteCommand {
    Start {
        source: AuthorizedLarkActor,
        intent_id: String,
        params: TurnStartParams,
        reply: oneshot::Sender<Result<ExternalMutationApplied, ExternalWriteError>>,
    },
    Steer {
        source: AuthorizedLarkActor,
        intent_id: String,
        params: TurnSteerParams,
        reply: oneshot::Sender<Result<ExternalMutationApplied, ExternalWriteError>>,
    },
    Interrupt {
        source: AuthorizedLarkActor,
        intent_id: String,
        params: TurnInterruptParams,
        reply: oneshot::Sender<Result<ExternalMutationApplied, ExternalWriteError>>,
    },
    QueueAdd {
        source: AuthorizedLarkActor,
        intent_id: String,
        expected_turn_id: String,
        params: ThreadQueueAddParams,
        reply: oneshot::Sender<Result<ExternalMutationApplied, ExternalWriteError>>,
    },
    QueueStart {
        source: AuthorizedLarkActor,
        intent_id: String,
        params: ThreadQueueStartParams,
        reply: oneshot::Sender<Result<ExternalMutationApplied, ExternalWriteError>>,
    },
    ResolveApproval {
        actor: AuthorizedLarkActor,
        approval_id: String,
        decision: ExternalApprovalDecision,
        reply: oneshot::Sender<Result<(), ExternalWriteError>>,
    },
    ReassignApprovalActor {
        new_actor: String,
        reply: oneshot::Sender<Result<(), ExternalWriteError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct ExternalWriteCoordinator {
    endpoint_label: EndpointLabel,
    epoch: u64,
    commands: mpsc::Sender<WriteCommand>,
    cancellation: CancellationToken,
    actor: Option<JoinHandle<()>>,
    approvals: mpsc::Receiver<ExternalApprovalPrompt>,
}

impl fmt::Debug for ExternalWriteCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalWriteCoordinator")
            .field("endpoint_label", &self.endpoint_label)
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

impl ExternalWriteCoordinator {
    /// Connects one statically configured writer, reconciles first, then admits commands.
    ///
    /// # Errors
    ///
    /// Returns a content-free classification if configuration, admission, reconciliation, or the
    /// durable readiness transition fails.
    #[allow(clippy::too_many_lines)]
    pub async fn connect(
        gate: ExternalEndpointGate,
        store: StoreHandle,
        parent_cancellation: CancellationToken,
        settings: ExternalWriteSettings,
    ) -> Result<Self, ExternalWriteError> {
        if !matches!(
            gate.capability_profile(),
            ExternalCapabilityProfile::MutateShared | ExternalCapabilityProfile::QueueShared
        ) {
            return Err(ExternalWriteError::UnsupportedProfile);
        }
        validate_settings(&settings)?;
        let endpoint_label = gate.endpoint_label().clone();
        let reservation = store
            .reserve_external_epoch(
                endpoint_label.as_str(),
                crate::store::ExternalUncertaintyReason::BridgeRestart,
            )
            .await
            .map_err(|_| ExternalWriteError::Store)?;
        let epoch = reservation.epoch;
        let cancellation = parent_cancellation.child_token();
        drop(parent_cancellation);
        let Ok(mut connection) = ExternalReadOnlyConnection::connect(
            &gate,
            ConnectionEpoch::new(epoch),
            cancellation.clone(),
        )
        .await
        else {
            let _ = store
                .mark_external_unavailable(
                    endpoint_label.as_str(),
                    epoch,
                    crate::store::ExternalUncertaintyReason::SocketDisconnect,
                )
                .await;
            return Err(ExternalWriteError::Transport);
        };
        if store
            .set_external_endpoint_state(
                endpoint_label.as_str(),
                epoch,
                ExternalEndpointState::Reconciling,
            )
            .await
            .is_err()
        {
            let _ = store
                .mark_external_unavailable(
                    endpoint_label.as_str(),
                    epoch,
                    crate::store::ExternalUncertaintyReason::ProtocolViolation,
                )
                .await;
            connection.abort();
            return Err(ExternalWriteError::Store);
        }
        if let Err(failure) = reconcile_epoch(
            &mut connection,
            &store,
            endpoint_label.as_str(),
            epoch,
            settings.request_timeout,
            &cancellation,
        )
        .await
        {
            let _ = store
                .mark_external_unavailable(endpoint_label.as_str(), epoch, failure.reason())
                .await;
            connection.abort();
            return Err(ExternalWriteError::NotReady);
        }
        if store
            .set_external_endpoint_state(
                endpoint_label.as_str(),
                epoch,
                ExternalEndpointState::Ready,
            )
            .await
            .is_err()
        {
            let _ = store
                .mark_external_unavailable(
                    endpoint_label.as_str(),
                    epoch,
                    crate::store::ExternalUncertaintyReason::ProtocolViolation,
                )
                .await;
            connection.abort();
            return Err(ExternalWriteError::Store);
        }
        let (commands, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (approval_tx, approvals) = mpsc::channel(APPROVAL_CAPACITY);
        let actor_cancel = cancellation.clone();
        let actor_store = store.clone();
        let actor_label = endpoint_label.clone();
        let actor = tokio::spawn(async move {
            run_write_actor(
                connection,
                actor_store,
                actor_label,
                epoch,
                settings,
                command_rx,
                approval_tx,
                actor_cancel,
            )
            .await;
        });
        Ok(Self {
            endpoint_label,
            epoch,
            commands,
            cancellation,
            actor: Some(actor),
            approvals,
        })
    }

    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Starts one new turn only after proving the exact managed thread and queue are idle.
    ///
    /// # Errors
    ///
    /// Returns a static classification when authorization, fencing, live-state proof, or exact
    /// result correlation fails.
    pub async fn start_turn(
        &self,
        source: AuthorizedLarkActor,
        intent_id: impl Into<String>,
        params: TurnStartParams,
    ) -> Result<ExternalMutationApplied, ExternalWriteError> {
        self.send_command(|reply| WriteCommand::Start {
            source,
            intent_id: intent_id.into(),
            params,
            reply,
        })
        .await
    }

    /// Steers one exact bridge-owned active turn.
    ///
    /// # Errors
    ///
    /// Returns a static classification when authorization, fencing, live-state proof, or exact
    /// result correlation fails.
    pub async fn steer_turn(
        &self,
        source: AuthorizedLarkActor,
        intent_id: impl Into<String>,
        params: TurnSteerParams,
    ) -> Result<ExternalMutationApplied, ExternalWriteError> {
        self.send_command(|reply| WriteCommand::Steer {
            source,
            intent_id: intent_id.into(),
            params,
            reply,
        })
        .await
    }

    /// Interrupts one exact bridge-owned active turn.
    ///
    /// # Errors
    ///
    /// Returns a static classification when authorization, fencing, live-state proof, or the
    /// mutation transport fails.
    pub async fn interrupt_turn(
        &self,
        source: AuthorizedLarkActor,
        intent_id: impl Into<String>,
        params: TurnInterruptParams,
    ) -> Result<ExternalMutationApplied, ExternalWriteError> {
        self.send_command(|reply| WriteCommand::Interrupt {
            source,
            intent_id: intent_id.into(),
            params,
            reply,
        })
        .await
    }

    /// Queues input against one exact bridge-owned active turn.
    ///
    /// # Errors
    ///
    /// Returns a static classification when authorization, fencing, live-state proof, or exact
    /// queue correlation fails.
    pub async fn queue_input(
        &self,
        source: AuthorizedLarkActor,
        intent_id: impl Into<String>,
        expected_turn_id: impl Into<String>,
        params: ThreadQueueAddParams,
    ) -> Result<ExternalMutationApplied, ExternalWriteError> {
        self.send_command(|reply| WriteCommand::QueueAdd {
            source,
            intent_id: intent_id.into(),
            expected_turn_id: expected_turn_id.into(),
            params,
            reply,
        })
        .await
    }

    /// Starts one exact bridge-owned queued submission while its thread is authoritatively idle.
    ///
    /// # Errors
    ///
    /// Returns a static classification when authorization, fencing, live-state proof, or exact
    /// queue correlation fails.
    pub async fn start_queued(
        &self,
        source: AuthorizedLarkActor,
        intent_id: impl Into<String>,
        params: ThreadQueueStartParams,
    ) -> Result<ExternalMutationApplied, ExternalWriteError> {
        self.send_command(|reply| WriteCommand::QueueStart {
            source,
            intent_id: intent_id.into(),
            params,
            reply,
        })
        .await
    }

    /// Receives the next approval destined for the one statically configured recipient UI.
    pub async fn recv_approval(&mut self) -> Option<ExternalApprovalPrompt> {
        self.approvals.recv().await
    }

    /// Claims and responds to one approval as the exact configured Lark recipient.
    ///
    /// # Errors
    ///
    /// Returns a static classification for a stale, duplicate, mismatched, unauthorized, or
    /// transport-ambiguous response.
    pub async fn resolve_approval(
        &self,
        actor: AuthorizedLarkActor,
        approval_id: impl Into<String>,
        decision: ExternalApprovalDecision,
    ) -> Result<(), ExternalWriteError> {
        let (reply, wait) = oneshot::channel();
        self.commands
            .send(WriteCommand::ResolveApproval {
                actor,
                approval_id: approval_id.into(),
                decision,
                reply,
            })
            .await
            .map_err(|_| ExternalWriteError::Closed)?;
        wait.await.map_err(|_| ExternalWriteError::Closed)?
    }

    /// Atomically reassigns the static approval handler only after the entire endpoint is drained.
    ///
    /// A successful reassignment orderly-closes this coordinator. The caller must discard it and
    /// reconnect with settings naming the new actor before admitting another mutation.
    ///
    /// # Errors
    ///
    /// Returns `Busy` while any mutation, owned turn, approval, or uncertain claim remains; other
    /// failures retain the same content-free classifications as ordinary coordinator commands.
    pub async fn reassign_approval_actor(
        &self,
        new_actor: impl Into<String>,
    ) -> Result<(), ExternalWriteError> {
        let (reply, wait) = oneshot::channel();
        self.commands
            .send(WriteCommand::ReassignApprovalActor {
                new_actor: new_actor.into(),
                reply,
            })
            .await
            .map_err(|_| ExternalWriteError::Closed)?;
        wait.await.map_err(|_| ExternalWriteError::Closed)?
    }

    async fn send_command(
        &self,
        build: impl FnOnce(
            oneshot::Sender<Result<ExternalMutationApplied, ExternalWriteError>>,
        ) -> WriteCommand,
    ) -> Result<ExternalMutationApplied, ExternalWriteError> {
        let (reply, wait) = oneshot::channel();
        self.commands
            .send(build(reply))
            .await
            .map_err(|_| ExternalWriteError::Closed)?;
        wait.await.map_err(|_| ExternalWriteError::Closed)?
    }

    /// Drains the actor command path, closes only its socket, and marks the epoch stopped.
    ///
    /// # Errors
    ///
    /// Returns `Closed` if the actor cannot complete an orderly bounded shutdown.
    pub async fn shutdown(mut self) -> Result<(), ExternalWriteError> {
        let (reply, wait) = oneshot::channel();
        let sent = self.commands.send(WriteCommand::Shutdown { reply }).await;
        let orderly = if sent.is_ok() {
            matches!(
                tokio::time::timeout(EXTERNAL_WRITE_SHUTDOWN_TIMEOUT, wait).await,
                Ok(Ok(()))
            )
        } else {
            false
        };
        if !orderly {
            self.cancellation.cancel();
        }
        if let Some(actor) = self.actor.take() {
            actor.await.map_err(|_| ExternalWriteError::Closed)?;
        }
        if orderly {
            Ok(())
        } else {
            Err(ExternalWriteError::Closed)
        }
    }
}

impl Drop for ExternalWriteCoordinator {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(actor) = self.actor.take() {
            actor.abort();
        }
    }
}

struct PendingApproval {
    request_key: String,
    deadline: tokio::time::Instant,
    request: ExternalApprovalRequest,
    responded: bool,
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn run_write_actor(
    mut connection: ExternalReadOnlyConnection,
    store: StoreHandle,
    endpoint_label: EndpointLabel,
    epoch: u64,
    settings: ExternalWriteSettings,
    mut commands: mpsc::Receiver<WriteCommand>,
    approval_tx: mpsc::Sender<ExternalApprovalPrompt>,
    cancellation: CancellationToken,
) {
    let Ok(mutation) = connection.mutation_client() else {
        return;
    };
    let mut fatal = false;
    let mut approvals = HashMap::<String, PendingApproval>::new();
    let mut deadline_tick = tokio::time::interval(Duration::from_millis(100));
    deadline_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while !fatal {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => break,
            event = connection.recv_event() => {
                match event {
                    Ok(Some(ExternalReadEvent::ItemCompleted(notification))) => {
                        if let Some(item_id) = notification.item.id().map(str::to_owned) {
                            let _ = store.record_external_terminal(
                                endpoint_label.as_str(), &notification.thread_id, epoch, None,
                                Some(crate::store::ExternalItemTerminal {
                                    turn_id: notification.turn_id,
                                    item_id,
                                })
                            ).await;
                        }
                    }
                    Ok(Some(ExternalReadEvent::TurnCompleted(notification))) => {
                        let status = match notification.turn.status {
                            TurnStatus::Completed => Some(crate::store::ExternalTerminalStatus::Completed),
                            TurnStatus::Failed => Some(crate::store::ExternalTerminalStatus::Failed),
                            TurnStatus::Interrupted => Some(crate::store::ExternalTerminalStatus::Interrupted),
                            TurnStatus::InProgress | TurnStatus::Unknown(_) => None,
                        };
                        if let Some(status) = status {
                            let _ = store.record_external_terminal(
                                endpoint_label.as_str(), &notification.thread_id, epoch,
                                Some(crate::store::ExternalTurnTerminal {
                                    turn_id: notification.turn.id,
                                    status,
                                }), None
                            ).await;
                        }
                    }
                    Ok(Some(ExternalReadEvent::Approval(request))) => {
                        fatal = receive_approval(
                            request,
                            &mutation,
                            &store,
                            endpoint_label.as_str(),
                            epoch,
                            &settings,
                            &approval_tx,
                            &mut approvals,
                        ).await.is_err();
                    }
                    Ok(Some(ExternalReadEvent::ServerRequestResolved(notification))) => {
                        if let Some(request_key) = request_key_from_value(&notification.request_id) {
                            let resolved = store.resolve_external_approval_request(
                                endpoint_label.as_str(), &request_key, epoch
                            ).await;
                            if matches!(resolved, Ok(ExternalTransitionOutcome::Applied)) {
                                approvals.retain(|_, pending| pending.request_key != request_key);
                            } else {
                                fatal = true;
                            }
                        } else {
                            fatal = true;
                        }
                    }
                    Ok(Some(ExternalReadEvent::ThreadSettingsUpdated(notification))) => {
                        if notification.thread_settings.approvals_reviewer
                            != settings.approval_reviewer
                        {
                            fatal = true;
                        }
                    }
                    Ok(Some(ExternalReadEvent::Closed(_)) | None) | Err(_) => fatal = true,
                    Ok(Some(_)) => {}
                }
            }
            _ = deadline_tick.tick(), if !approvals.is_empty() => {
                fatal = deny_expired_approvals(
                    &mutation,
                    &store,
                    endpoint_label.as_str(),
                    epoch,
                    &settings,
                    &mut approvals,
                ).await.is_err();
            }
            command = commands.recv() => {
                let Some(command) = command else { break };
                match command {
                    WriteCommand::Shutdown { reply } => {
                        let _ = store.mark_external_unavailable(
                            endpoint_label.as_str(), epoch,
                            crate::store::ExternalUncertaintyReason::BridgeRestart,
                        ).await;
                        let _ = store.set_external_endpoint_state(
                            endpoint_label.as_str(), epoch, ExternalEndpointState::Stopped,
                        ).await;
                        let _ = connection.shutdown().await;
                        let _ = reply.send(());
                        return;
                    }
                    WriteCommand::ReassignApprovalActor { new_actor, reply } => {
                        let result = if new_actor == settings.client_actor
                            || new_actor == settings.approval_recipient.as_str()
                        {
                            Err(ExternalWriteError::InvalidSettings)
                        } else if approvals.is_empty() {
                            store.reassign_external_approval_actor(
                                endpoint_label.as_str(), epoch, &settings.approval_actor, &new_actor,
                            ).await.map_err(|_| ExternalWriteError::Store).and_then(|outcome| {
                                match outcome {
                                    ExternalApprovalReassignmentOutcome::Reassigned => Ok(()),
                                    ExternalApprovalReassignmentOutcome::NotDrained => {
                                        Err(ExternalWriteError::Busy)
                                    }
                                    ExternalApprovalReassignmentOutcome::Stale => {
                                        Err(ExternalWriteError::NotReady)
                                    }
                                }
                            })
                        } else {
                            Err(ExternalWriteError::Busy)
                        };
                        if result.is_ok() {
                            let _ = store.mark_external_unavailable(
                                endpoint_label.as_str(), epoch,
                                crate::store::ExternalUncertaintyReason::BridgeRestart,
                            ).await;
                            let _ = store.set_external_endpoint_state(
                                endpoint_label.as_str(), epoch, ExternalEndpointState::Stopped,
                            ).await;
                            let _ = connection.shutdown().await;
                            let _ = reply.send(Ok(()));
                            return;
                        }
                        let _ = reply.send(result);
                    }
                    WriteCommand::ResolveApproval { actor, approval_id, decision, reply } => {
                        let result = resolve_pending_approval(
                            &mutation,
                            &store,
                            endpoint_label.as_str(),
                            epoch,
                            &settings,
                            &mut approvals,
                            &actor,
                            &approval_id,
                            decision,
                        ).await;
                        fatal = matches!(result, Err(ExternalWriteError::Uncertain));
                        let _ = reply.send(result);
                    }
                    command => {
                        let (reply, result, is_fatal) = execute_command(
                            command,
                            &connection,
                            &mutation,
                            &store,
                            endpoint_label.as_str(),
                            epoch,
                            &settings,
                        ).await;
                        fatal = is_fatal;
                        let _ = reply.send(result);
                    }
                }
            }
        }
    }
    let _ = store
        .mark_external_unavailable(
            endpoint_label.as_str(),
            epoch,
            crate::store::ExternalUncertaintyReason::SocketDisconnect,
        )
        .await;
    connection.abort();
    while let Ok(command) = commands.try_recv() {
        if let Some(reply) = command_reply(command) {
            let _ = reply.send(Err(ExternalWriteError::Closed));
        }
    }
}

type MutationReply = oneshot::Sender<Result<ExternalMutationApplied, ExternalWriteError>>;

fn command_reply(command: WriteCommand) -> Option<MutationReply> {
    match command {
        WriteCommand::Start { reply, .. }
        | WriteCommand::Steer { reply, .. }
        | WriteCommand::Interrupt { reply, .. }
        | WriteCommand::QueueAdd { reply, .. }
        | WriteCommand::QueueStart { reply, .. } => Some(reply),
        WriteCommand::ResolveApproval { reply, .. }
        | WriteCommand::ReassignApprovalActor { reply, .. } => {
            let _ = reply.send(Err(ExternalWriteError::Closed));
            None
        }
        WriteCommand::Shutdown { .. } => None,
    }
}

#[allow(clippy::too_many_lines)]
async fn execute_command(
    command: WriteCommand,
    connection: &ExternalReadOnlyConnection,
    mutation: &ExternalMutationClient,
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    settings: &ExternalWriteSettings,
) -> (
    MutationReply,
    Result<ExternalMutationApplied, ExternalWriteError>,
    bool,
) {
    match command {
        WriteCommand::Start {
            source,
            intent_id,
            params,
            reply,
        } => {
            let result = execute_start(
                connection,
                mutation,
                store,
                endpoint_label,
                epoch,
                settings,
                &source,
                &intent_id,
                params,
            )
            .await;
            let fatal = is_fatal_result(&result);
            (reply, result, fatal)
        }
        WriteCommand::Steer {
            source,
            intent_id,
            params,
            reply,
        } => {
            let result = execute_steer(
                connection,
                mutation,
                store,
                endpoint_label,
                epoch,
                settings,
                &source,
                &intent_id,
                params,
            )
            .await;
            let fatal = is_fatal_result(&result);
            (reply, result, fatal)
        }
        WriteCommand::Interrupt {
            source,
            intent_id,
            params,
            reply,
        } => {
            let result = execute_interrupt(
                connection,
                mutation,
                store,
                endpoint_label,
                epoch,
                settings,
                &source,
                &intent_id,
                params,
            )
            .await;
            let fatal = is_fatal_result(&result);
            (reply, result, fatal)
        }
        WriteCommand::QueueAdd {
            source,
            intent_id,
            expected_turn_id,
            params,
            reply,
        } => {
            let result = execute_queue_add(
                connection,
                mutation,
                store,
                endpoint_label,
                epoch,
                settings,
                &source,
                &intent_id,
                &expected_turn_id,
                params,
            )
            .await;
            let fatal = is_fatal_result(&result);
            (reply, result, fatal)
        }
        WriteCommand::QueueStart {
            source,
            intent_id,
            params,
            reply,
        } => {
            let result = execute_queue_start(
                connection,
                mutation,
                store,
                endpoint_label,
                epoch,
                settings,
                &source,
                &intent_id,
                params,
            )
            .await;
            let fatal = is_fatal_result(&result);
            (reply, result, fatal)
        }
        WriteCommand::ResolveApproval { .. }
        | WriteCommand::ReassignApprovalActor { .. }
        | WriteCommand::Shutdown { .. } => {
            unreachable!("control commands are handled by the actor")
        }
    }
}

fn is_fatal_result(result: &Result<ExternalMutationApplied, ExternalWriteError>) -> bool {
    matches!(
        result,
        Err(ExternalWriteError::Uncertain | ExternalWriteError::Ambiguous)
    )
}

#[allow(clippy::too_many_arguments)]
async fn execute_start(
    connection: &ExternalReadOnlyConnection,
    mutation: &ExternalMutationClient,
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    settings: &ExternalWriteSettings,
    source: &AuthorizedLarkActor,
    intent_id: &str,
    params: TurnStartParams,
) -> Result<ExternalMutationApplied, ExternalWriteError> {
    if connection.report().capability_profile != ExternalCapabilityProfile::QueueShared {
        return Err(ExternalWriteError::UnsupportedProfile);
    }
    let client_message_id = params
        .client_user_message_id
        .as_deref()
        .ok_or(ExternalWriteError::InvalidSettings)?;
    validate_command(intent_id, &params)?;
    validate_static_approval(params.approvals_reviewer.as_deref(), settings)?;
    acquire(
        store,
        endpoint_label,
        epoch,
        source,
        settings,
        intent_id,
        &params.thread_id,
        ExternalMutationKind::TurnStart,
        None,
        Some(client_message_id),
    )
    .await?;
    if !thread_is_idle(connection, &params.thread_id, settings.request_timeout).await?
        || !queue_is_empty(mutation, &params.thread_id, settings.request_timeout).await?
    {
        reject(store, endpoint_label, &params.thread_id, intent_id, epoch).await?;
        return Err(ExternalWriteError::Conflict);
    }
    mark_sent(store, endpoint_label, &params.thread_id, intent_id, epoch).await?;
    let response = match mutation.start_turn(&params, settings.request_timeout).await {
        Ok(response) => response,
        Err(error) => {
            return resolve_transport_failure(
                store,
                endpoint_label,
                &params.thread_id,
                intent_id,
                epoch,
                error,
            )
            .await;
        }
    };
    let turn_id = response.turn.id;
    let status_matches = matches!(response.turn.status, TurnStatus::InProgress);
    let client_matches = turn_has_client_message(
        connection,
        &params.thread_id,
        &turn_id,
        client_message_id,
        settings.request_timeout,
    )
    .await?;
    if !status_matches || !client_matches {
        uncertain(store, endpoint_label, &params.thread_id, intent_id, epoch).await?;
        return Err(ExternalWriteError::Ambiguous);
    }
    apply(
        store,
        endpoint_label,
        &params.thread_id,
        intent_id,
        epoch,
        Some(&turn_id),
    )
    .await?;
    Ok(ExternalMutationApplied {
        result_id: Some(turn_id),
    })
}

#[allow(clippy::too_many_arguments)]
async fn execute_steer(
    connection: &ExternalReadOnlyConnection,
    mutation: &ExternalMutationClient,
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    settings: &ExternalWriteSettings,
    source: &AuthorizedLarkActor,
    intent_id: &str,
    params: TurnSteerParams,
) -> Result<ExternalMutationApplied, ExternalWriteError> {
    let message_id = params
        .client_user_message_id
        .as_deref()
        .ok_or(ExternalWriteError::InvalidSettings)?;
    validate_command(intent_id, &params)?;
    require_owned_active(
        connection,
        store,
        endpoint_label,
        source,
        settings,
        &params.thread_id,
        &params.expected_turn_id,
    )
    .await?;
    acquire(
        store,
        endpoint_label,
        epoch,
        source,
        settings,
        intent_id,
        &params.thread_id,
        ExternalMutationKind::TurnSteer,
        Some(&params.expected_turn_id),
        Some(message_id),
    )
    .await?;
    mark_sent(store, endpoint_label, &params.thread_id, intent_id, epoch).await?;
    let response = match mutation.steer_turn(&params, settings.request_timeout).await {
        Ok(response) => response,
        Err(error) => {
            return resolve_transport_failure(
                store,
                endpoint_label,
                &params.thread_id,
                intent_id,
                epoch,
                error,
            )
            .await;
        }
    };
    if response.turn_id != params.expected_turn_id
        || !turn_has_client_message(
            connection,
            &params.thread_id,
            &response.turn_id,
            message_id,
            settings.request_timeout,
        )
        .await?
    {
        uncertain(store, endpoint_label, &params.thread_id, intent_id, epoch).await?;
        return Err(ExternalWriteError::Ambiguous);
    }
    apply(
        store,
        endpoint_label,
        &params.thread_id,
        intent_id,
        epoch,
        Some(&response.turn_id),
    )
    .await?;
    Ok(ExternalMutationApplied {
        result_id: Some(response.turn_id),
    })
}

#[allow(clippy::too_many_arguments)]
async fn execute_interrupt(
    connection: &ExternalReadOnlyConnection,
    mutation: &ExternalMutationClient,
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    settings: &ExternalWriteSettings,
    source: &AuthorizedLarkActor,
    intent_id: &str,
    params: TurnInterruptParams,
) -> Result<ExternalMutationApplied, ExternalWriteError> {
    validate_command(intent_id, &params)?;
    require_owned_active(
        connection,
        store,
        endpoint_label,
        source,
        settings,
        &params.thread_id,
        &params.turn_id,
    )
    .await?;
    acquire(
        store,
        endpoint_label,
        epoch,
        source,
        settings,
        intent_id,
        &params.thread_id,
        ExternalMutationKind::TurnInterrupt,
        Some(&params.turn_id),
        None,
    )
    .await?;
    mark_sent(store, endpoint_label, &params.thread_id, intent_id, epoch).await?;
    if let Err(error) = mutation
        .interrupt_turn(&params, settings.request_timeout)
        .await
    {
        return resolve_transport_failure(
            store,
            endpoint_label,
            &params.thread_id,
            intent_id,
            epoch,
            error,
        )
        .await;
    }
    apply(
        store,
        endpoint_label,
        &params.thread_id,
        intent_id,
        epoch,
        Some(&params.turn_id),
    )
    .await?;
    Ok(ExternalMutationApplied {
        result_id: Some(params.turn_id),
    })
}

#[allow(clippy::too_many_arguments)]
async fn execute_queue_add(
    connection: &ExternalReadOnlyConnection,
    mutation: &ExternalMutationClient,
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    settings: &ExternalWriteSettings,
    source: &AuthorizedLarkActor,
    intent_id: &str,
    expected_turn_id: &str,
    params: ThreadQueueAddParams,
) -> Result<ExternalMutationApplied, ExternalWriteError> {
    validate_command(intent_id, &params)?;
    require_owned_active(
        connection,
        store,
        endpoint_label,
        source,
        settings,
        &params.thread_id,
        expected_turn_id,
    )
    .await?;
    acquire(
        store,
        endpoint_label,
        epoch,
        source,
        settings,
        intent_id,
        &params.thread_id,
        ExternalMutationKind::QueueAdd,
        Some(expected_turn_id),
        Some(&params.client_user_message_id),
    )
    .await?;
    mark_sent(store, endpoint_label, &params.thread_id, intent_id, epoch).await?;
    let response = match mutation
        .add_to_queue(&params, settings.request_timeout)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return resolve_transport_failure(
                store,
                endpoint_label,
                &params.thread_id,
                intent_id,
                epoch,
                error,
            )
            .await;
        }
    };
    if response.queued_submission.client_user_message_id != params.client_user_message_id {
        uncertain(store, endpoint_label, &params.thread_id, intent_id, epoch).await?;
        return Err(ExternalWriteError::Ambiguous);
    }
    let queued_id = response.queued_submission.id;
    apply(
        store,
        endpoint_label,
        &params.thread_id,
        intent_id,
        epoch,
        Some(&queued_id),
    )
    .await?;
    Ok(ExternalMutationApplied {
        result_id: Some(queued_id),
    })
}

#[allow(clippy::too_many_arguments)]
async fn execute_queue_start(
    connection: &ExternalReadOnlyConnection,
    mutation: &ExternalMutationClient,
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    settings: &ExternalWriteSettings,
    source: &AuthorizedLarkActor,
    intent_id: &str,
    params: ThreadQueueStartParams,
) -> Result<ExternalMutationApplied, ExternalWriteError> {
    validate_command(intent_id, &params)?;
    let queued_id = params
        .queued_submission_id
        .as_deref()
        .ok_or(ExternalWriteError::InvalidSettings)?;
    let owner = store
        .external_mutation_owner(endpoint_label, &params.thread_id, queued_id)
        .await
        .map_err(|_| ExternalWriteError::Store)?
        .ok_or(ExternalWriteError::Unauthorized)?;
    if owner.source_actor != source.as_str()
        || owner.client_actor != settings.client_actor
        || owner.approval_actor != settings.approval_actor
    {
        return Err(ExternalWriteError::Unauthorized);
    }
    acquire(
        store,
        endpoint_label,
        epoch,
        source,
        settings,
        intent_id,
        &params.thread_id,
        ExternalMutationKind::QueueStart,
        None,
        None,
    )
    .await?;
    if !thread_is_idle(connection, &params.thread_id, settings.request_timeout).await?
        || !queue_contains(
            mutation,
            &params.thread_id,
            queued_id,
            settings.request_timeout,
        )
        .await?
    {
        reject(store, endpoint_label, &params.thread_id, intent_id, epoch).await?;
        return Err(ExternalWriteError::Conflict);
    }
    mark_sent(store, endpoint_label, &params.thread_id, intent_id, epoch).await?;
    let response = match mutation
        .start_queued(&params, settings.request_timeout)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return resolve_transport_failure(
                store,
                endpoint_label,
                &params.thread_id,
                intent_id,
                epoch,
                error,
            )
            .await;
        }
    };
    if !matches!(response.turn.status, TurnStatus::InProgress) {
        uncertain(store, endpoint_label, &params.thread_id, intent_id, epoch).await?;
        return Err(ExternalWriteError::Ambiguous);
    }
    let turn_id = response.turn.id;
    apply(
        store,
        endpoint_label,
        &params.thread_id,
        intent_id,
        epoch,
        Some(&turn_id),
    )
    .await?;
    Ok(ExternalMutationApplied {
        result_id: Some(turn_id),
    })
}

#[allow(clippy::too_many_arguments)]
async fn receive_approval(
    mut request: ExternalApprovalRequest,
    mutation: &ExternalMutationClient,
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    settings: &ExternalWriteSettings,
    approval_tx: &mpsc::Sender<ExternalApprovalPrompt>,
    approvals: &mut HashMap<String, PendingApproval>,
) -> Result<(), ExternalWriteError> {
    if request.epoch().get() != epoch {
        let _ = mutation.abandon_approval(&mut request);
        return Err(ExternalWriteError::Uncertain);
    }
    let request_key = request_key_from_value(
        &serde_json::to_value(request.request_id()).map_err(|_| ExternalWriteError::Transport)?,
    )
    .ok_or(ExternalWriteError::Transport)?;
    let approval_id = Uuid::new_v4().simple().to_string();
    let duration = approval_duration(&request, settings);
    let deadline = tokio::time::Instant::now() + duration;
    let durable_duration = duration.max(Duration::from_secs(1));
    let deadline_ms = wall_now_ms()
        .saturating_add(i64::try_from(durable_duration.as_millis()).unwrap_or(i64::MAX));
    let kind = match &request {
        ExternalApprovalRequest::Command { .. } => ExternalApprovalKind::Command,
        ExternalApprovalRequest::FileChange { .. } => ExternalApprovalKind::FileChange,
        ExternalApprovalRequest::Permissions { .. } => ExternalApprovalKind::Permissions,
    };
    let receive = store
        .receive_external_approval(NewExternalApprovalClaim {
            endpoint_label: endpoint_label.to_owned(),
            thread_id: request.thread_id().to_owned(),
            approval_id: approval_id.clone(),
            request_key: request_key.clone(),
            epoch,
            turn_id: request.turn_id().to_owned(),
            item_id: request.item_id().to_owned(),
            kind,
            client_actor: settings.client_actor.clone(),
            approval_actor: settings.approval_actor.clone(),
            recipient_actor: settings.approval_recipient.as_str().to_owned(),
            deadline_ms,
        })
        .await
        .map_err(|_| ExternalWriteError::Store)?;
    if receive != ExternalApprovalReceiveOutcome::Received {
        let _ = mutation.abandon_approval(&mut request);
        return Err(match receive {
            ExternalApprovalReceiveOutcome::NotOwned => ExternalWriteError::Unauthorized,
            ExternalApprovalReceiveOutcome::NotReady
            | ExternalApprovalReceiveOutcome::StaleEpoch => ExternalWriteError::NotReady,
            ExternalApprovalReceiveOutcome::ThreadFenced => ExternalWriteError::Uncertain,
            ExternalApprovalReceiveOutcome::ApprovalHandlerMismatch => {
                ExternalWriteError::InvalidSettings
            }
            ExternalApprovalReceiveOutcome::Duplicate { .. }
            | ExternalApprovalReceiveOutcome::Received => ExternalWriteError::Ambiguous,
        });
    }
    let prompt = ExternalApprovalPrompt {
        approval_id: approval_id.clone(),
        kind: match kind {
            ExternalApprovalKind::Command => ExternalApprovalPromptKind::Command,
            ExternalApprovalKind::FileChange => ExternalApprovalPromptKind::FileChange,
            ExternalApprovalKind::Permissions => ExternalApprovalPromptKind::Permissions,
        },
        deadline_ms,
    };
    approvals.insert(
        approval_id.clone(),
        PendingApproval {
            request_key,
            deadline,
            request,
            responded: false,
        },
    );
    if approval_tx.try_send(prompt).is_err() {
        deny_one_approval(
            mutation,
            store,
            endpoint_label,
            epoch,
            settings,
            approvals,
            &approval_id,
        )
        .await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn resolve_pending_approval(
    mutation: &ExternalMutationClient,
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    settings: &ExternalWriteSettings,
    approvals: &mut HashMap<String, PendingApproval>,
    actor: &AuthorizedLarkActor,
    approval_id: &str,
    decision: ExternalApprovalDecision,
) -> Result<(), ExternalWriteError> {
    if actor != &settings.approval_recipient {
        return Err(ExternalWriteError::Unauthorized);
    }
    let pending = approvals
        .get_mut(approval_id)
        .ok_or(ExternalWriteError::Conflict)?;
    if pending.responded || !decision_matches(&pending.request, &decision) {
        return Err(ExternalWriteError::Conflict);
    }
    match store
        .claim_external_approval(
            endpoint_label,
            pending.request.thread_id(),
            approval_id,
            actor.as_str(),
            epoch,
        )
        .await
        .map_err(|_| ExternalWriteError::Store)?
    {
        ExternalApprovalClaimOutcome::Claimed => {}
        ExternalApprovalClaimOutcome::Unauthorized => return Err(ExternalWriteError::Unauthorized),
        ExternalApprovalClaimOutcome::Duplicate | ExternalApprovalClaimOutcome::Stale => {
            return Err(ExternalWriteError::Conflict);
        }
    }
    store
        .resolve_external_approval(
            endpoint_label,
            pending.request.thread_id(),
            approval_id,
            epoch,
            ExternalApprovalResolution::Responding,
        )
        .await
        .map_err(|_| ExternalWriteError::Store)?;
    if respond_approval(mutation, &mut pending.request, &decision)
        .await
        .is_err()
    {
        let _ = store
            .resolve_external_approval(
                endpoint_label,
                pending.request.thread_id(),
                approval_id,
                epoch,
                ExternalApprovalResolution::Uncertain,
            )
            .await;
        return Err(ExternalWriteError::Uncertain);
    }
    pending.responded = true;
    Ok(())
}

async fn deny_expired_approvals(
    mutation: &ExternalMutationClient,
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    settings: &ExternalWriteSettings,
    approvals: &mut HashMap<String, PendingApproval>,
) -> Result<(), ExternalWriteError> {
    let now = tokio::time::Instant::now();
    let expired = approvals
        .iter()
        .filter(|(_, pending)| !pending.responded && pending.deadline <= now)
        .map(|(approval_id, _)| approval_id.clone())
        .collect::<Vec<_>>();
    for approval_id in expired {
        deny_one_approval(
            mutation,
            store,
            endpoint_label,
            epoch,
            settings,
            approvals,
            &approval_id,
        )
        .await?;
    }
    Ok(())
}

async fn deny_one_approval(
    mutation: &ExternalMutationClient,
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    settings: &ExternalWriteSettings,
    approvals: &mut HashMap<String, PendingApproval>,
    approval_id: &str,
) -> Result<(), ExternalWriteError> {
    let Some(mut pending) = approvals.remove(approval_id) else {
        return Ok(());
    };
    match store
        .claim_external_approval(
            endpoint_label,
            pending.request.thread_id(),
            approval_id,
            settings.approval_recipient.as_str(),
            epoch,
        )
        .await
        .map_err(|_| ExternalWriteError::Store)?
    {
        ExternalApprovalClaimOutcome::Claimed => {}
        ExternalApprovalClaimOutcome::Duplicate | ExternalApprovalClaimOutcome::Stale => {
            return Ok(());
        }
        ExternalApprovalClaimOutcome::Unauthorized => {
            return Err(ExternalWriteError::Unauthorized);
        }
    }
    store
        .resolve_external_approval(
            endpoint_label,
            pending.request.thread_id(),
            approval_id,
            epoch,
            ExternalApprovalResolution::Responding,
        )
        .await
        .map_err(|_| ExternalWriteError::Store)?;
    let denial = denial_for(&pending.request);
    if respond_approval(mutation, &mut pending.request, &denial)
        .await
        .is_err()
    {
        let _ = store
            .resolve_external_approval(
                endpoint_label,
                pending.request.thread_id(),
                approval_id,
                epoch,
                ExternalApprovalResolution::Uncertain,
            )
            .await;
        return Err(ExternalWriteError::Uncertain);
    }
    store
        .resolve_external_approval(
            endpoint_label,
            pending.request.thread_id(),
            approval_id,
            epoch,
            ExternalApprovalResolution::Denied,
        )
        .await
        .map_err(|_| ExternalWriteError::Store)?;
    Ok(())
}

async fn respond_approval(
    mutation: &ExternalMutationClient,
    request: &mut ExternalApprovalRequest,
    decision: &ExternalApprovalDecision,
) -> Result<(), ExternalTransportError> {
    match decision {
        ExternalApprovalDecision::Command(result) => {
            mutation.respond_command_approval(request, result).await
        }
        ExternalApprovalDecision::FileChange(result) => {
            mutation.respond_file_change_approval(request, result).await
        }
        ExternalApprovalDecision::Permissions(result) => {
            mutation.respond_permissions_approval(request, result).await
        }
    }
}

fn decision_matches(
    request: &ExternalApprovalRequest,
    decision: &ExternalApprovalDecision,
) -> bool {
    matches!(
        (request, decision),
        (
            ExternalApprovalRequest::Command { .. },
            ExternalApprovalDecision::Command(_)
        ) | (
            ExternalApprovalRequest::FileChange { .. },
            ExternalApprovalDecision::FileChange(_)
        ) | (
            ExternalApprovalRequest::Permissions { .. },
            ExternalApprovalDecision::Permissions(_)
        )
    )
}

fn denial_for(request: &ExternalApprovalRequest) -> ExternalApprovalDecision {
    match request {
        ExternalApprovalRequest::Command { .. } => {
            ExternalApprovalDecision::Command(CommandExecutionRequestApprovalResult {
                decision: CommandExecutionApprovalDecision::Simple(SimpleApprovalDecision::Decline),
            })
        }
        ExternalApprovalRequest::FileChange { .. } => {
            ExternalApprovalDecision::FileChange(FileChangeRequestApprovalResult {
                decision: SimpleApprovalDecision::Decline,
            })
        }
        ExternalApprovalRequest::Permissions { .. } => {
            ExternalApprovalDecision::Permissions(PermissionsRequestApprovalResult {
                permissions: json!({}),
                scope: None,
                strict_auto_review: None,
            })
        }
    }
}

fn approval_duration(
    request: &ExternalApprovalRequest,
    settings: &ExternalWriteSettings,
) -> Duration {
    let local = settings.approval_timeout.min(APPROVAL_DEADLINE_MAX);
    let remote = request
        .auto_resolution_ms()
        .map(Duration::from_millis)
        .map_or(APPROVAL_DEADLINE_MAX, |duration| {
            duration.saturating_sub(APPROVAL_REMOTE_MARGIN)
        });
    local.min(remote).max(Duration::from_millis(1))
}

fn request_key_from_value(value: &Value) -> Option<String> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let encoded = serde_json::to_vec(value).ok()?;
    let digest = Sha256::digest(&encoded);
    let mut key = String::with_capacity(64);
    for byte in digest {
        key.push(char::from(HEX[usize::from(byte >> 4)]));
        key.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Some(key)
}

fn wall_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX / 2)
}

#[allow(clippy::too_many_arguments)]
async fn acquire(
    store: &StoreHandle,
    endpoint_label: &str,
    epoch: u64,
    source: &AuthorizedLarkActor,
    settings: &ExternalWriteSettings,
    intent_id: &str,
    thread_id: &str,
    kind: ExternalMutationKind,
    expected_turn_id: Option<&str>,
    client_message_id: Option<&str>,
) -> Result<(), ExternalWriteError> {
    let outcome = store
        .prepare_external_mutation(NewExternalMutationIntent {
            endpoint_label: endpoint_label.to_owned(),
            thread_id: thread_id.to_owned(),
            intent_id: intent_id.to_owned(),
            epoch,
            kind,
            expected_turn_id: expected_turn_id.map(str::to_owned),
            client_message_id: client_message_id.map(str::to_owned),
            source_actor: source.as_str().to_owned(),
            client_actor: settings.client_actor.clone(),
            approval_actor: settings.approval_actor.clone(),
        })
        .await
        .map_err(|_| ExternalWriteError::Store)?;
    match outcome {
        ExternalPrepareOutcome::Prepared => Ok(()),
        ExternalPrepareOutcome::Busy => Err(ExternalWriteError::Busy),
        ExternalPrepareOutcome::Uncertain => Err(ExternalWriteError::Uncertain),
        ExternalPrepareOutcome::NotReady | ExternalPrepareOutcome::StaleEpoch => {
            Err(ExternalWriteError::NotReady)
        }
        ExternalPrepareOutcome::ApprovalHandlerMismatch => Err(ExternalWriteError::InvalidSettings),
        ExternalPrepareOutcome::Duplicate(_) => Err(ExternalWriteError::Conflict),
    }
}

async fn require_owned_active(
    connection: &ExternalReadOnlyConnection,
    store: &StoreHandle,
    endpoint_label: &str,
    source: &AuthorizedLarkActor,
    settings: &ExternalWriteSettings,
    thread_id: &str,
    expected_turn_id: &str,
) -> Result<(), ExternalWriteError> {
    let active = active_turn(connection, thread_id, settings.request_timeout).await?;
    if active.as_deref() != Some(expected_turn_id) {
        return Err(ExternalWriteError::Conflict);
    }
    let owner = store
        .external_mutation_owner(endpoint_label, thread_id, expected_turn_id)
        .await
        .map_err(|_| ExternalWriteError::Store)?
        .ok_or(ExternalWriteError::Unauthorized)?;
    if owner.source_actor != source.as_str()
        || owner.client_actor != settings.client_actor
        || owner.approval_actor != settings.approval_actor
    {
        return Err(ExternalWriteError::Unauthorized);
    }
    Ok(())
}

async fn thread_is_idle(
    connection: &ExternalReadOnlyConnection,
    thread_id: &str,
    request_timeout: Duration,
) -> Result<bool, ExternalWriteError> {
    let mut params = ThreadReadParams::new(thread_id);
    params.include_turns = Some(false);
    let read = connection
        .client()
        .read_thread_with_timeout(&params, request_timeout)
        .await
        .map_err(map_preflight_error)?;
    Ok(read.thread.id == thread_id && read.thread.status["type"] == "idle")
}

async fn active_turn(
    connection: &ExternalReadOnlyConnection,
    thread_id: &str,
    request_timeout: Duration,
) -> Result<Option<String>, ExternalWriteError> {
    let mut read_params = ThreadReadParams::new(thread_id);
    read_params.include_turns = Some(false);
    let client = connection.client();
    let read = client
        .read_thread_with_timeout(&read_params, request_timeout)
        .await
        .map_err(map_preflight_error)?;
    if read.thread.id != thread_id || read.thread.status["type"] != "active" {
        return Ok(None);
    }
    let turns = client
        .list_thread_turns_with_timeout(
            &ThreadTurnsListParams {
                thread_id: thread_id.to_owned(),
                cursor: None,
                limit: Some(2),
                sort_direction: Some(SortDirection::Descending),
                items_view: None,
            },
            request_timeout,
        )
        .await
        .map_err(map_preflight_error)?;
    let active = turns
        .data
        .into_iter()
        .filter(|turn| matches!(turn.status, TurnStatus::InProgress))
        .collect::<Vec<_>>();
    if active.len() != 1 {
        return Err(ExternalWriteError::Conflict);
    }
    Ok(active.into_iter().next().map(|turn| turn.id))
}

async fn queue_is_empty(
    mutation: &ExternalMutationClient,
    thread_id: &str,
    request_timeout: Duration,
) -> Result<bool, ExternalWriteError> {
    Ok(mutation
        .list_queue(
            &ThreadQueueListParams {
                thread_id: thread_id.to_owned(),
                cursor: None,
                limit: Some(1),
            },
            request_timeout,
        )
        .await
        .map_err(map_preflight_error)?
        .data
        .is_empty())
}

async fn queue_contains(
    mutation: &ExternalMutationClient,
    thread_id: &str,
    queued_id: &str,
    request_timeout: Duration,
) -> Result<bool, ExternalWriteError> {
    let queue = mutation
        .list_queue(
            &ThreadQueueListParams {
                thread_id: thread_id.to_owned(),
                cursor: None,
                limit: Some(100),
            },
            request_timeout,
        )
        .await
        .map_err(map_preflight_error)?;
    Ok(queue
        .data
        .iter()
        .filter(|item| item.id == queued_id)
        .count()
        == 1)
}

async fn turn_has_client_message(
    connection: &ExternalReadOnlyConnection,
    thread_id: &str,
    turn_id: &str,
    client_message_id: &str,
    request_timeout: Duration,
) -> Result<bool, ExternalWriteError> {
    let deadline = tokio::time::Instant::now() + request_timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        let items = connection
            .client()
            .list_thread_items_with_timeout(
                &ThreadItemsListParams {
                    thread_id: thread_id.to_owned(),
                    turn_id: Some(turn_id.to_owned()),
                    cursor: None,
                    limit: Some(100),
                    sort_direction: Some(SortDirection::Ascending),
                },
                remaining,
            )
            .await
            .map_err(map_preflight_error)?;
        if items.data.iter().any(|entry| {
            matches!(
                &entry.item,
                crate::codex::types::ThreadItem::UserMessage {
                    client_id: Some(found),
                    ..
                } if found == client_message_id
            )
        }) {
            return Ok(true);
        }
        tokio::time::sleep(Duration::from_millis(10).min(remaining)).await;
    }
}

fn validate_static_approval(
    reviewer: Option<&str>,
    settings: &ExternalWriteSettings,
) -> Result<(), ExternalWriteError> {
    if reviewer == Some(settings.approval_reviewer.as_str()) {
        Ok(())
    } else {
        Err(ExternalWriteError::InvalidSettings)
    }
}

fn validate_command<T: Serialize>(intent_id: &str, params: &T) -> Result<(), ExternalWriteError> {
    if intent_id.is_empty()
        || intent_id.len() > ROUTING_ID_BYTE_LIMIT
        || serde_json::to_vec(params)
            .map_err(|_| ExternalWriteError::InvalidSettings)?
            .len()
            > MAX_OUTBOUND_VALUE_WIRE_BYTES
    {
        return Err(ExternalWriteError::InvalidSettings);
    }
    Ok(())
}

fn validate_settings(settings: &ExternalWriteSettings) -> Result<(), ExternalWriteError> {
    if settings.request_timeout.is_zero()
        || settings.approval_timeout.is_zero()
        || settings.client_actor.is_empty()
        || settings.client_actor.len() > 256
        || settings.approval_actor.is_empty()
        || settings.approval_actor.len() > 256
        || settings.client_actor == settings.approval_actor
        || settings.client_actor == settings.approval_recipient.as_str()
        || settings.approval_actor == settings.approval_recipient.as_str()
        || settings.approval_reviewer.is_empty()
        || settings.approval_reviewer.len() > 256
    {
        return Err(ExternalWriteError::InvalidSettings);
    }
    Ok(())
}

fn map_preflight_error(error: ExternalTransportError) -> ExternalWriteError {
    match error {
        ExternalTransportError::ServerRejected { .. } => ExternalWriteError::Conflict,
        ExternalTransportError::UnsupportedProfile => ExternalWriteError::UnsupportedProfile,
        ExternalTransportError::Admission(_)
        | ExternalTransportError::Rpc
        | ExternalTransportError::RequestTimeout
        | ExternalTransportError::ConnectionLost
        | ExternalTransportError::ProtocolViolation => ExternalWriteError::Uncertain,
    }
}

async fn mark_sent(
    store: &StoreHandle,
    endpoint: &str,
    thread: &str,
    intent: &str,
    epoch: u64,
) -> Result<(), ExternalWriteError> {
    match store
        .mark_external_mutation_sent(endpoint, thread, intent, epoch)
        .await
        .map_err(|_| ExternalWriteError::Store)?
    {
        ExternalTransitionOutcome::Applied => Ok(()),
        ExternalTransitionOutcome::Stale => Err(ExternalWriteError::NotReady),
    }
}

async fn apply(
    store: &StoreHandle,
    endpoint: &str,
    thread: &str,
    intent: &str,
    epoch: u64,
    result_id: Option<&str>,
) -> Result<(), ExternalWriteError> {
    resolve(
        store,
        endpoint,
        thread,
        intent,
        epoch,
        ExternalMutationResolution::Applied { result_id },
    )
    .await
}

async fn reject(
    store: &StoreHandle,
    endpoint: &str,
    thread: &str,
    intent: &str,
    epoch: u64,
) -> Result<(), ExternalWriteError> {
    resolve(
        store,
        endpoint,
        thread,
        intent,
        epoch,
        ExternalMutationResolution::Rejected,
    )
    .await
}

async fn uncertain(
    store: &StoreHandle,
    endpoint: &str,
    thread: &str,
    intent: &str,
    epoch: u64,
) -> Result<(), ExternalWriteError> {
    resolve(
        store,
        endpoint,
        thread,
        intent,
        epoch,
        ExternalMutationResolution::Uncertain,
    )
    .await
}

async fn resolve(
    store: &StoreHandle,
    endpoint: &str,
    thread: &str,
    intent: &str,
    epoch: u64,
    resolution: ExternalMutationResolution<'_>,
) -> Result<(), ExternalWriteError> {
    match store
        .resolve_external_mutation(endpoint, thread, intent, epoch, resolution)
        .await
        .map_err(|_| ExternalWriteError::Store)?
    {
        ExternalTransitionOutcome::Applied => Ok(()),
        ExternalTransitionOutcome::Stale => Err(ExternalWriteError::NotReady),
    }
}

async fn resolve_transport_failure(
    store: &StoreHandle,
    endpoint: &str,
    thread: &str,
    intent: &str,
    epoch: u64,
    error: ExternalTransportError,
) -> Result<ExternalMutationApplied, ExternalWriteError> {
    if matches!(error, ExternalTransportError::ServerRejected { .. }) {
        reject(store, endpoint, thread, intent, epoch).await?;
        return Err(ExternalWriteError::Conflict);
    }
    uncertain(store, endpoint, thread, intent, epoch).await?;
    Err(ExternalWriteError::Uncertain)
}
