//! Reply projection: the transport-agnostic pure layer that turns a completed
//! Codex turn into a standalone final reply.
//!
//! The hard reply contracts from the design (§9) live here as testable
//! functions, never as comments:
//!
//! 1. The last agent message with `MessagePhase::FinalAnswer` (or the trailing
//!    agent message when no phase marker exists) is the standalone final
//!    answer, never mixed into a progress view.
//! 2. A final-only turn produces no progress at all.
//! 3. A clean-empty turn (no visible output, empty final) sends nothing.
//! 4. A progress-send failure never swallows the final: progress and final are
//!    independent projections, so dropping a progress output cannot remove the
//!    final output.
//! 5. Text already streamed into the progress view is not re-sent when the
//!    turn ends without an independent final (the progress message is
//!    finalized in place).
//! 6. A final reply counts as delivered only once Lark returns a non-empty
//!    `message_id` (enforced at the outbox boundary, not here).
//!
//! Real-time progress is not wired into the scope runtime in this task; the
//! streaming [`ReplyProjector::observe`] path is the deferred integration
//! seam. The final-only durable sink uses only [`ReplyProjector::project_final`].
//!
//! Wiring point (out of scope here, covered by the prior report): the scope
//! actor's `Running` phase drives [`ReplyProjector::observe`] from its
//! `ThreadSubscription`, and each `Progress` output is enqueued as a
//! `progress` outbox row before the terminal final is projected.
//!
//! Redaction: `Debug` implementations report counts and lengths only — never
//! agent text, and the email audit mask never appears in `Debug` output.

#![allow(clippy::doc_markdown)]

use std::fmt;
use std::time::{Duration, Instant};

use crate::codex::client::{AppServerEvent, TurnOutcome};
use crate::codex::types::{MessagePhase, ThreadItem};
use crate::limits::{
    REPLY_MAX_SPLITS, REPLY_MESSAGE_MAX_CHARS, REPLY_TRUNCATION_MARKER, REPLY_UPDATE_MIN_CHARS,
    REPLY_UPDATE_MIN_INTERVAL,
};

/// Tunables for one [`ReplyProjector`]; defaults match the production limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectorConfig {
    /// Maximum characters (Unicode scalar values) in one split part.
    pub max_chars: usize,
    /// Maximum split parts before deterministic truncation.
    pub max_splits: usize,
    /// Minimum interval between two progress upserts.
    pub min_interval: Duration,
    /// Minimum newly accumulated characters before the next progress upsert.
    pub min_chars: usize,
}

impl Default for ProjectorConfig {
    fn default() -> Self {
        Self {
            max_chars: REPLY_MESSAGE_MAX_CHARS,
            max_splits: REPLY_MAX_SPLITS,
            min_interval: REPLY_UPDATE_MIN_INTERVAL,
            min_chars: REPLY_UPDATE_MIN_CHARS,
        }
    }
}

/// One terminal projection result.
#[derive(Clone, PartialEq, Eq)]
pub enum ProjectedReply {
    /// Standalone final answer, masked and split into bounded parts.
    Final {
        /// Bounded, already-masked message parts, in deterministic order.
        parts: Vec<String>,
    },
    /// Clean-empty turn (no visible output): send nothing.
    Empty,
}

impl fmt::Debug for ProjectedReply {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Final { parts } => formatter
                .debug_struct("Final")
                .field("part_count", &parts.len())
                .field(
                    "total_chars",
                    &parts.iter().map(|part| part.chars().count()).sum::<usize>(),
                )
                .finish(),
            Self::Empty => formatter.write_str("Empty"),
        }
    }
}

/// One streaming projection output, returned by [`ReplyProjector::observe`].
#[derive(Clone, PartialEq, Eq)]
pub enum ProjectorOutput {
    /// A throttled progress update carrying the accumulated masked text.
    Progress {
        /// Masked progress text accumulated since the previous upsert.
        text: String,
    },
    /// Nothing to emit for this event.
    Nothing,
}

impl fmt::Debug for ProjectorOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Progress { text } => formatter
                .debug_struct("Progress")
                .field("text_chars", &text.chars().count())
                .finish(),
            Self::Nothing => formatter.write_str("Nothing"),
        }
    }
}

/// Pure, transport-agnostic reply projector.
///
/// The final-only durable sink calls [`ReplyProjector::project_final`] (which
/// never consults streaming state). The streaming [`observe`] path is the
/// deferred progress-integration seam that will later be driven from the scope
/// actor's `ThreadSubscription`.
pub struct ReplyProjector {
    config: ProjectorConfig,
    progress_buffer: String,
    last_progress: Option<Instant>,
    streamed: bool,
    /// Id of the item whose deltas are currently buffered. A single slot is
    /// enough and bounded (`O(1)`): Codex delivers one agent message's deltas
    /// to completion before the next item, so the slot resets when the item id
    /// changes.
    current_item_id: Option<String>,
    /// Delta text received so far for `current_item_id`, not yet emitted.
    /// Deltas are never emitted directly: an `AgentMessageDelta` carries no
    /// phase, so the item's role (final answer vs. progress) is only known when
    /// its `ItemCompleted` arrives.
    current_item_buffer: String,
    /// Id of the most recently completed progress item. A single slot is
    /// enough and bounded (`O(1)`): Codex delivers one agent message to
    /// completion before the next item, so a duplicate `ItemCompleted` for the
    /// same item is recognized and dropped. A later item overwrites the slot,
    /// matching the existing single-slot delta buffer; an out-of-order
    /// duplicate of an earlier item is rare and the slot keeps no unbounded
    /// per-item memory.
    last_completed_item_id: Option<String>,
}

impl ReplyProjector {
    /// Creates a projector with the given tunables.
    #[must_use]
    pub fn new(config: ProjectorConfig) -> Self {
        Self {
            config,
            progress_buffer: String::new(),
            last_progress: None,
            streamed: false,
            current_item_id: None,
            current_item_buffer: String::new(),
            last_completed_item_id: None,
        }
    }

    /// Creates a projector with production limits.
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(ProjectorConfig::default())
    }

    /// Feeds one streaming event, returning a throttled progress update when
    /// both the interval and the character thresholds have been met.
    ///
    /// A `FinalAnswer`-phase agent message is never progress (contract 2); it
    /// is reserved for the terminal projection. Because an
    /// `AgentMessageDelta` carries no phase, a delta is only buffered per item
    /// and **never** emitted on its own: its text becomes progress (for a
    /// non-final item) or is dropped (for a `FinalAnswer` item) only when the
    /// item's `ItemCompleted` arrives and reveals its phase.
    ///
    /// Deduplication: Codex emits `AgentMessageDelta` events as an item
    /// streams and then an `ItemCompleted` event carrying the same item's full
    /// text. The deltas are held in a single-item buffer; at `ItemCompleted`
    /// that buffer becomes the progress prefix and only the tail beyond the
    /// deltas is appended, so the same content is never counted twice. A
    /// single-item slot is enough and bounded (`O(1)`): Codex delivers one
    /// agent message's deltas to completion before the next item, so the slot
    /// resets automatically when the item id changes.
    #[must_use]
    pub fn observe(&mut self, event: &AppServerEvent, now: Instant) -> ProjectorOutput {
        match event {
            AppServerEvent::AgentMessageDelta { item_id, delta, .. } => {
                // No phase is available here: only accumulate into the current
                // item's buffer. Progress is emitted exclusively at completion.
                self.begin_or_continue_delta(item_id);
                self.current_item_buffer.push_str(delta);
                truncate_to_chars(&mut self.current_item_buffer, self.config.max_chars);
                ProjectorOutput::Nothing
            }
            AppServerEvent::ItemCompleted {
                item:
                    ThreadItem::AgentMessage {
                        id, text, phase, ..
                    },
                ..
            } => {
                if is_final_phase(phase.as_ref()) {
                    // The final answer's streamed text belongs to the
                    // terminal projection (contract 1/2): drop its buffer,
                    // never emit progress for it.
                    self.drop_item_buffer(id);
                    return ProjectorOutput::Nothing;
                }
                if self.last_completed_item_id.as_deref() == Some(id.as_str()) {
                    // A duplicate `ItemCompleted` for the item already emitted:
                    // its full text was appended once, and the single-item
                    // delta buffer is now cleared, so replaying it would
                    // re-append the whole text (`tail_beyond(text, 0)`).
                    return ProjectorOutput::Nothing;
                }
                self.last_completed_item_id = Some(id.to_owned());
                // A non-final item's whole text becomes progress. Move the
                // not-yet-emitted delta prefix into the progress buffer and
                // append only the tail the deltas did not cover.
                let delta_prefix = self.take_item_buffer(id);
                self.progress_buffer.push_str(&delta_prefix);
                self.progress_buffer
                    .push_str(tail_beyond(text, delta_prefix.len()));
                truncate_to_chars(&mut self.progress_buffer, self.config.max_chars);
                self.maybe_emit(now)
            }
            _ => ProjectorOutput::Nothing,
        }
    }

    /// Records the item id whose deltas are now streaming, resetting the
    /// single-item slot when the item id changes.
    fn begin_or_continue_delta(&mut self, item_id: &str) {
        if self.current_item_id.as_deref() != Some(item_id) {
            self.current_item_id = Some(item_id.to_owned());
            self.current_item_buffer.clear();
        }
    }

    /// Drops the buffered deltas of a `FinalAnswer` item: their content is the
    /// terminal answer and must never leak out as progress.
    fn drop_item_buffer(&mut self, item_id: &str) {
        if self.current_item_id.as_deref() == Some(item_id) {
            self.current_item_id = None;
            self.current_item_buffer.clear();
        }
    }

    /// Moves the buffered delta text for `item_id` out of the single-item slot,
    /// leaving the slot empty. Returns an empty string when the slot holds a
    /// different (or no) item.
    fn take_item_buffer(&mut self, item_id: &str) -> String {
        if self.current_item_id.as_deref() == Some(item_id) {
            self.current_item_id = None;
            std::mem::take(&mut self.current_item_buffer)
        } else {
            String::new()
        }
    }

    /// Emits the accumulated progress buffer once both the interval and
    /// character thresholds have been met; otherwise keeps the buffer for the
    /// next completion.
    fn maybe_emit(&mut self, now: Instant) -> ProjectorOutput {
        if self.progress_buffer.is_empty() {
            return ProjectorOutput::Nothing;
        }
        let elapsed = self
            .last_progress
            .map_or(Duration::MAX, |last| now.saturating_duration_since(last));
        if elapsed >= self.config.min_interval
            && self.progress_buffer.chars().count() >= self.config.min_chars
        {
            self.last_progress = Some(now);
            self.streamed = true;
            let text = email_mask(&self.progress_buffer);
            self.progress_buffer.clear();
            ProjectorOutput::Progress { text }
        } else {
            ProjectorOutput::Nothing
        }
    }

    /// Projects the terminal reply, honoring the "never repeat streamed text"
    /// contract: when a turn ends without an independent final answer, the
    /// content already emitted into the progress view is finalized in place
    /// (contract 5). Anything accumulated but not yet emitted — the progress
    /// buffer plus any un-completed item's delta buffer — is delivered as the
    /// final, so a short streaming answer below the progress threshold is
    /// never silently dropped.
    #[must_use]
    pub fn finish(&self, outcome: &TurnOutcome) -> ProjectedReply {
        let Some(extracted) = extract_final(outcome) else {
            return ProjectedReply::Empty;
        };
        if extracted.independent {
            // The standalone final answer never mixes in streamed progress
            // (contract 1/4): any residual progress buffer is dropped.
            return self.render_final(&extracted.text);
        }
        if self.streamed {
            // Some progress was emitted: only the not-yet-displayed content
            // remains to deliver as the final.
            let mut rest = self.progress_buffer.clone();
            rest.push_str(&self.current_item_buffer);
            return self.render_final(&rest);
        }
        self.render_final(&extracted.text)
    }

    /// Projects the standalone final answer without consulting streaming
    /// state. This is the final-only path used by the durable reply sink.
    #[must_use]
    pub fn project_final(&self, outcome: &TurnOutcome) -> ProjectedReply {
        let Some(extracted) = extract_final(outcome) else {
            return ProjectedReply::Empty;
        };
        self.render_final(&extracted.text)
    }

    /// Masks agent-generated text and splits it into bounded parts.
    #[must_use]
    pub fn mask_and_split(&self, text: &str) -> Vec<String> {
        split_text(
            &email_mask(text),
            self.config.max_chars,
            self.config.max_splits,
        )
    }

    fn render_final(&self, text: &str) -> ProjectedReply {
        let masked = email_mask(text);
        let trimmed = masked.trim();
        if trimmed.is_empty() {
            ProjectedReply::Empty
        } else {
            ProjectedReply::Final {
                parts: split_text(trimmed, self.config.max_chars, self.config.max_splits),
            }
        }
    }
}

impl fmt::Debug for ReplyProjector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplyProjector")
            .field("config", &self.config)
            .field("streamed", &self.streamed)
            .field("buffered_chars", &self.progress_buffer.chars().count())
            .field("has_current_item", &self.current_item_id.is_some())
            .field(
                "current_item_buffer_chars",
                &self.current_item_buffer.chars().count(),
            )
            .field(
                "has_last_completed_item",
                &self.last_completed_item_id.is_some(),
            )
            .finish_non_exhaustive()
    }
}

struct ExtractedFinal {
    text: String,
    independent: bool,
}

/// Extracts the standalone final answer: the last `FinalAnswer`-phase agent
/// message, or (when none exists) the trailing agent message. The
/// `independent` flag distinguishes a phase-marked final from the fallback.
fn extract_final(outcome: &TurnOutcome) -> Option<ExtractedFinal> {
    for item in outcome.completed_items.iter().rev() {
        if let ThreadItem::AgentMessage { text, phase, .. } = item {
            if is_final_phase(phase.as_ref()) {
                return Some(ExtractedFinal {
                    text: text.clone(),
                    independent: true,
                });
            }
        }
    }
    for item in outcome.completed_items.iter().rev() {
        if let ThreadItem::AgentMessage { text, .. } = item {
            return Some(ExtractedFinal {
                text: text.clone(),
                independent: false,
            });
        }
    }
    None
}

fn is_final_phase(phase: Option<&MessagePhase>) -> bool {
    matches!(phase, Some(MessagePhase::FinalAnswer))
}

/// Returns the portion of a completed item's `text` not already buffered as
/// deltas (`prefix_bytes`). A zero prefix means the whole text is the
/// deterministic fallback; a prefix covering the text (or one that is not a
/// UTF-8 boundary) appends nothing.
fn tail_beyond(text: &str, prefix_bytes: usize) -> &str {
    if prefix_bytes == 0 {
        text
    } else if prefix_bytes >= text.len() || !text.is_char_boundary(prefix_bytes) {
        ""
    } else {
        &text[prefix_bytes..]
    }
}

/// Replaces the `@` in plausible email addresses with `[at]`, leaving package
/// versions (`pkg@1.2.3`), scoped package names (`@scope/pkg`), and `@mention`
/// markers untouched. A masking decision is deterministic and depends only on
/// the characters immediately around each `@`.
#[must_use]
pub fn email_mask(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    for (index, _) in text.match_indices('@') {
        out.push_str(&text[cursor..index]);
        if is_email_at(text, index) {
            out.push_str("[at]");
        } else {
            out.push('@');
        }
        cursor = index + 1;
    }
    out.push_str(&text[cursor..]);
    out
}

/// Whether the `@` at byte `index` is the separator of an email address: a
/// non-empty, valid local character on the left and a right-hand token whose
/// first character is not a digit (so `pkg@1.2.3` version ranges are never
/// masked) and whose last-dot segment is a pure alphabetic domain label of
/// 2..=24 letters (so `pkg@v1.2.3`-style tokens and other dot-separated
/// identifiers stay untouched).
fn is_email_at(text: &str, index: usize) -> bool {
    let Some(left) = text[..index].chars().next_back() else {
        return false;
    };
    if !(left.is_ascii_alphanumeric() || matches!(left, '.' | '_' | '%' | '+' | '-')) {
        return false;
    }
    let right = &text[index + 1..];
    let token: String = right.chars().take_while(|c| !c.is_whitespace()).collect();
    let Some(first) = token.chars().next() else {
        return false;
    };
    if first.is_ascii_digit() {
        return false;
    }
    let Some(last_dot) = token.rfind('.') else {
        return false;
    };
    let label = &token[last_dot + 1..];
    let len = label.chars().count();
    (2..=24).contains(&len) && label.chars().all(|c| c.is_ascii_alphabetic())
}

/// Deterministically splits `text` into at most `max_splits` parts of at most
/// `max_chars` characters each (Unicode scalar values). Any remainder beyond
/// the budget is truncated with an explicit marker instead of growing the part
/// count without bound.
///
/// # Panics
///
/// Panics if `max_chars` or `max_splits` is zero.
#[must_use]
pub fn split_text(text: &str, max_chars: usize, max_splits: usize) -> Vec<String> {
    assert!(max_chars > 0, "max_chars must be non-zero");
    assert!(max_splits > 0, "max_splits must be non-zero");
    let mut parts = Vec::new();
    let mut rest = text;
    while parts.len() < max_splits {
        if rest.chars().count() <= max_chars {
            parts.push(rest.to_owned());
            return parts;
        }
        let (head, tail) = split_at_chars(rest, max_chars);
        parts.push(head.to_owned());
        rest = tail;
    }
    if !rest.is_empty() {
        let last = parts
            .pop()
            .expect("max_splits is non-zero so at least one part exists");
        parts.push(truncate_with_marker(&last, max_chars));
    }
    parts
}

/// Splits `text` after exactly `count` characters, on a UTF-8 boundary.
fn split_at_chars(text: &str, count: usize) -> (&str, &str) {
    let byte = text
        .char_indices()
        .nth(count)
        .map_or(text.len(), |(index, _)| index);
    (&text[..byte], &text[byte..])
}

/// Truncates `text` to `max_chars` characters, replacing the tail with the
/// deterministic truncation marker.
fn truncate_with_marker(text: &str, max_chars: usize) -> String {
    let marker_chars = REPLY_TRUNCATION_MARKER.chars().count();
    let budget = max_chars.saturating_sub(marker_chars);
    let head: String = text.chars().take(budget).collect();
    format!("{head}{REPLY_TRUNCATION_MARKER}")
}

/// Truncates a buffer to at most `max_chars` characters in place.
fn truncate_to_chars(buffer: &mut String, max_chars: usize) {
    let excess = buffer.chars().count().saturating_sub(max_chars);
    if excess == 0 {
        return;
    }
    let keep = buffer.chars().count() - excess;
    let byte = buffer
        .char_indices()
        .nth(keep)
        .map_or(buffer.len(), |(index, _)| index);
    buffer.truncate(byte);
}
