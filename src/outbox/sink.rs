//! The [`DurableReplySink`] adapter over the durable outbox.
//!
//! Progress snapshots and terminal replies are encoded as deterministic rows
//! before the scope actor advances durable turn state.

#![allow(clippy::doc_markdown)]

use std::fmt;

use futures_util::future::BoxFuture;

use super::payload::OutboxOperation;
use crate::lark::normalize::InboundEvent;
use crate::render::{ProjectedReply, ReplyProjector};
use crate::runtime::scope::{
    DurableReplySink, ReplySinkError, TurnFinalization, TurnProgress, TurnSource,
};
use crate::store::{InboundRejectionKind, NewOutboxRow, StoreError, StoreHandle, TurnResolution};

/// Durable outbound boundary backed by the shared store.
pub struct OutboxReplySink {
    store: StoreHandle,
}

impl OutboxReplySink {
    /// Creates a sink enqueueing into the given store.
    #[must_use]
    pub fn new(store: StoreHandle) -> Self {
        Self { store }
    }
}

impl fmt::Debug for OutboxReplySink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboxReplySink")
            .finish_non_exhaustive()
    }
}

impl DurableReplySink for OutboxReplySink {
    fn rejection_notice(
        &self,
        event: &InboundEvent,
        reason: InboundRejectionKind,
    ) -> Result<NewOutboxRow, ReplySinkError> {
        let operation = OutboxOperation::ReplyText {
            message_id: event.message_id.clone(),
            thread_id: event.thread_id.clone(),
            text: rejection_text(reason).to_owned(),
        };
        let payload_json = operation.encode().map_err(|_| ReplySinkError::Invariant)?;
        Ok(NewOutboxRow {
            idempotency_key: format!("{}:notice:{}", event.event_id, rejection_key(reason)),
            scope_key: event.scope.to_string(),
            kind: "notice".to_owned(),
            payload_json,
            next_retry_ms: 0,
        })
    }

    fn progress(&self, progress: TurnProgress) -> BoxFuture<'static, Result<(), ReplySinkError>> {
        let store = self.store.clone();
        Box::pin(async move {
            let anchor_key = format!("{}:progress", progress.turn_row_id);
            let (idempotency_key, operation) = if progress.sequence == 0 {
                (
                    anchor_key.clone(),
                    OutboxOperation::ReplyProgressCard {
                        message_id: progress.source.message_id,
                        thread_id: progress.source.thread_id,
                        text: progress.text,
                    },
                )
            } else {
                (
                    format!("{}:{}", anchor_key, progress.sequence),
                    OutboxOperation::UpdateProgressCard {
                        anchor_key,
                        text: progress.text,
                    },
                )
            };
            let payload_json = operation.encode().map_err(|_| ReplySinkError::Invariant)?;
            store
                .enqueue_outbox(NewOutboxRow {
                    idempotency_key,
                    scope_key: progress.scope_key,
                    kind: "progress".to_owned(),
                    payload_json,
                    next_retry_ms: 0,
                })
                .await
                .map_err(|error| map_store_error(&error))?;
            Ok(())
        })
    }

    fn finalize(&self, turn: TurnFinalization) -> BoxFuture<'static, Result<(), ReplySinkError>> {
        let store = self.store.clone();
        Box::pin(async move {
            let projector = ReplyProjector::with_defaults();
            let rows = build_finalization_rows(&turn, &projector)?;
            // The whole final answer is enqueued in one transaction: a partial
            // final (some parts persisted, later parts rejected) must never be
            // sent while the turn stays unresolved.
            if let Err(error) = store.enqueue_outbox_batch(&rows).await {
                return Err(map_store_error(&error));
            }
            Ok(())
        })
    }

    fn finalize_projected(
        &self,
        turn: TurnFinalization,
        reply: ProjectedReply,
    ) -> BoxFuture<'static, Result<(), ReplySinkError>> {
        let store = self.store.clone();
        Box::pin(async move {
            let rows = build_projected_finalization_rows(&turn, reply)?;
            store
                .enqueue_outbox_batch(&rows)
                .await
                .map_err(|error| map_store_error(&error))?;
            Ok(())
        })
    }
}

fn build_projected_finalization_rows(
    turn: &TurnFinalization,
    reply: ProjectedReply,
) -> Result<Vec<NewOutboxRow>, ReplySinkError> {
    match turn.resolution {
        TurnResolution::Completed => match reply {
            ProjectedReply::Final { parts } => final_rows(turn, &parts),
            ProjectedReply::ProgressFinal { text } => progress_final_rows(turn, &text),
            ProjectedReply::Empty => Ok(Vec::new()),
        },
        TurnResolution::Failed => notice_rows(turn, FAILED_TEXT),
        TurnResolution::Interrupted => notice_rows(turn, INTERRUPTED_TEXT),
        TurnResolution::Uncertain => notice_rows(turn, UNCERTAIN_TEXT),
    }
}

/// Builds every deterministic outbox row for one turn finalization.
///
/// `Completed` turns project the standalone final answer for the last
/// originating source only (split into bounded parts). `Failed`/`Interrupted`/
/// `Uncertain` turns enqueue a deterministic, content-free notice so the user
/// is never left with a silently dropped turn.
fn build_finalization_rows(
    turn: &TurnFinalization,
    projector: &ReplyProjector,
) -> Result<Vec<NewOutboxRow>, ReplySinkError> {
    match turn.resolution {
        TurnResolution::Completed => {
            let Some(outcome) = turn.outcome.as_ref() else {
                // A completed resolution without an outcome is a closed
                // invariant violation; surface a deterministic notice rather
                // than silently dropping the reply.
                return notice_rows(turn, UNCERTAIN_TEXT);
            };
            match projector.project_final(outcome) {
                ProjectedReply::Final { parts } => final_rows(turn, &parts),
                ProjectedReply::ProgressFinal { .. } => Err(ReplySinkError::Invariant),
                ProjectedReply::Empty => Ok(Vec::new()),
            }
        }
        TurnResolution::Failed => notice_rows(turn, FAILED_TEXT),
        TurnResolution::Interrupted => notice_rows(turn, INTERRUPTED_TEXT),
        TurnResolution::Uncertain => notice_rows(turn, UNCERTAIN_TEXT),
    }
}

fn progress_final_rows(
    turn: &TurnFinalization,
    text: &str,
) -> Result<Vec<NewOutboxRow>, ReplySinkError> {
    let Some(source) = turn.sources.last() else {
        return Ok(Vec::new());
    };
    let anchor_key = format!("{}:progress", turn.turn_row_id);
    let operation = OutboxOperation::FinalizeProgressCard {
        anchor_key: anchor_key.clone(),
        message_id: source.message_id.clone(),
        thread_id: source.thread_id.clone(),
        text: text.to_owned(),
    };
    Ok(vec![NewOutboxRow {
        idempotency_key: format!("{anchor_key}:final"),
        scope_key: turn.scope_key.clone(),
        kind: "final".to_owned(),
        payload_json: operation.encode().map_err(|_| ReplySinkError::Invariant)?,
        next_retry_ms: 0,
    }])
}

fn final_rows(
    turn: &TurnFinalization,
    parts: &[String],
) -> Result<Vec<NewOutboxRow>, ReplySinkError> {
    // One terminal answer per turn, addressed to the last source only. The
    // reference implementation (feishu-claude-code-bridge @ e5d3ce5) replies
    // to the last message of each debounced batch (channel.ts
    // `replyTo: lastMsg.messageId`), so a turn must not fan one final out to
    // every source.
    let Some(source) = turn.sources.last() else {
        return Ok(Vec::new());
    };
    let mut rows = Vec::new();
    for (index, text) in parts.iter().enumerate() {
        let key = if index == 0 {
            format!("{}:final", turn.turn_row_id)
        } else {
            format!("{}:final:{}", turn.turn_row_id, index)
        };
        rows.push(NewOutboxRow {
            idempotency_key: key,
            scope_key: turn.scope_key.clone(),
            kind: "final".to_owned(),
            payload_json: encode_reply(source, text)?,
            next_retry_ms: 0,
        });
    }
    Ok(rows)
}

fn notice_rows(
    turn: &TurnFinalization,
    text: &'static str,
) -> Result<Vec<NewOutboxRow>, ReplySinkError> {
    // A single notice per turn, addressed to the last source only.
    let Some(source) = turn.sources.last() else {
        return Ok(Vec::new());
    };
    Ok(vec![NewOutboxRow {
        idempotency_key: format!("{}:notice", turn.turn_row_id),
        scope_key: turn.scope_key.clone(),
        kind: "notice".to_owned(),
        payload_json: encode_reply(source, text)?,
        next_retry_ms: 0,
    }])
}

fn encode_reply(source: &TurnSource, text: &str) -> Result<String, ReplySinkError> {
    OutboxOperation::ReplyText {
        message_id: source.message_id.clone(),
        thread_id: source.thread_id.clone(),
        text: text.to_owned(),
    }
    .encode()
    .map_err(|_| ReplySinkError::Invariant)
}

fn map_store_error(error: &StoreError) -> ReplySinkError {
    match error {
        StoreError::QueueFull | StoreError::CapacityExceeded { .. } => ReplySinkError::Capacity,
        StoreError::PayloadTooLarge { .. }
        | StoreError::CorruptData { .. }
        | StoreError::InvalidPath { .. }
        | StoreError::InvalidTransition { .. }
        | StoreError::NotFound { .. } => ReplySinkError::Invariant,
        StoreError::Io { .. }
        | StoreError::Sqlite { .. }
        | StoreError::Migration { .. }
        | StoreError::AlreadyOpen
        | StoreError::Closed => ReplySinkError::Unavailable,
    }
}

const FAILED_TEXT: &str = "任务执行失败";
const INTERRUPTED_TEXT: &str = "任务已中断";
const UNCERTAIN_TEXT: &str = "任务执行结果未知，请重新发起";

fn rejection_text(reason: InboundRejectionKind) -> &'static str {
    match reason {
        InboundRejectionKind::Overloaded => "当前处理负载过高，请稍后重试",
        InboundRejectionKind::NotOwner
        | InboundRejectionKind::NotSender
        | InboundRejectionKind::NotGroup
        | InboundRejectionKind::MissingMention
        | InboundRejectionKind::OwnerCommandRequired
        | InboundRejectionKind::Policy => "该消息未获授权处理",
        InboundRejectionKind::Stale => "消息已过期，未处理",
        InboundRejectionKind::Internal => "处理该消息时发生内部错误",
    }
}

fn rejection_key(reason: InboundRejectionKind) -> &'static str {
    reason.as_str()
}
