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
//! The scope actor drives [`ReplyProjector::observe`] from its live thread
//! subscription. Each accepted progress snapshot becomes a durable card
//! create/update row, and fallback terminal content finalizes that card in
//! place.
//!
//! Redaction: `Debug` implementations report counts and lengths only — never
//! agent text, and the email audit mask never appears in `Debug` output.

#![allow(clippy::doc_markdown)]

use std::fmt;
use std::time::{Duration, Instant};

use crate::codex::client::{AppServerEvent, TurnOutcome};
use crate::codex::types::{MessagePhase, ThreadItem};
use crate::limits::{
    LARK_MAX_SEND_BODY_BYTES, REPLY_MAX_SPLITS, REPLY_MESSAGE_MAX_CHARS, REPLY_TRUNCATION_MARKER,
    REPLY_UPDATE_MIN_CHARS, REPLY_UPDATE_MIN_INTERVAL,
};

mod markdown;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MeasuredProjectionWork {
    email_mask: usize,
    markdown_inline: usize,
}

#[cfg(test)]
std::thread_local! {
    static MEASURED_PROJECTION_WORK: std::cell::Cell<MeasuredProjectionWork> =
        const { std::cell::Cell::new(MeasuredProjectionWork { email_mask: 0, markdown_inline: 0 }) };
}

#[cfg(test)]
fn record_email_mask_work(units: usize) {
    MEASURED_PROJECTION_WORK.with(|work| {
        let mut measured = work.get();
        measured.email_mask = measured.email_mask.saturating_add(units);
        work.set(measured);
    });
}

#[cfg(test)]
fn record_markdown_inline_work(units: usize) {
    MEASURED_PROJECTION_WORK.with(|work| {
        let mut measured = work.get();
        measured.markdown_inline = measured.markdown_inline.saturating_add(units);
        work.set(measured);
    });
}

#[cfg(test)]
fn take_measured_projection_work() -> MeasuredProjectionWork {
    MEASURED_PROJECTION_WORK.with(|work| work.replace(MeasuredProjectionWork::default()))
}

pub use markdown::{
    card_markdown_element_wire_len, render_lark_markdown, split_lark_markdown,
    stabilize_streaming_markdown,
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
    /// Standalone final answer, projected as Lark `post` Markdown and split
    /// into bounded, independently valid parts.
    Final {
        /// Bounded, already-masked message parts, in deterministic order.
        parts: Vec<String>,
    },
    /// A turn that already created a progress card is finalized by updating
    /// that card in place with the complete fallback answer.
    ProgressFinal {
        /// Bounded, already-masked final card text.
        text: String,
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
            Self::ProgressFinal { text } => formatter
                .debug_struct("ProgressFinal")
                .field("text_chars", &text.chars().count())
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
/// The scope runtime owns one projector per live turn. The final-only helper
/// [`ReplyProjector::project_final`] remains available for recovery and tests
/// that have no live streaming state.
pub struct ReplyProjector {
    config: ProjectorConfig,
    progress_buffer: String,
    last_progress: Option<Instant>,
    emitted_progress: u32,
    streamed_content: String,
    /// Snapshot taken immediately before the latest progress emission. The
    /// scope actor either persists that emission or calls `restore_progress`
    /// before feeding another event. Keeping the prior durable state separate
    /// from the bounded text buffer means truncating a later chunk can never
    /// erase knowledge that an earlier progress card already exists.
    progress_checkpoint: Option<ProgressCheckpoint>,
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
            emitted_progress: 0,
            streamed_content: String::new(),
            progress_checkpoint: None,
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
                if !is_progress_phase(phase.as_ref()) {
                    // Only an explicitly commentary-phase item is safe to
                    // expose as progress. A phase-less agent message is the
                    // protocol fallback final when no explicit final exists,
                    // so treating `None` as progress could both leak and then
                    // swallow the terminal answer.
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
            let text = email_mask(&self.progress_buffer);
            self.progress_checkpoint = Some(ProgressCheckpoint {
                streamed_content: self.streamed_content.clone(),
                emitted_progress: self.emitted_progress,
                last_progress: self.last_progress,
                emitted_text: text.clone(),
            });
            self.last_progress = Some(now);
            self.emitted_progress = self.emitted_progress.saturating_add(1);
            self.streamed_content.push_str(&text);
            truncate_to_chars(&mut self.streamed_content, self.config.max_chars);
            self.progress_buffer.clear();
            ProjectorOutput::Progress { text }
        } else {
            ProjectorOutput::Nothing
        }
    }

    /// Restores the most recent progress chunk after the durable sink rejects
    /// it. The actor calls this immediately on enqueue failure, before feeding
    /// another event, so the terminal projection can still deliver the text.
    pub fn restore_progress(&mut self, text: &str) {
        let Some(checkpoint) = self.progress_checkpoint.take() else {
            return;
        };
        if checkpoint.emitted_text != text {
            // Fail closed on a stale/mismatched acknowledgement. Retain the
            // checkpoint and current emitted state rather than corrupting an
            // unrelated progress emission.
            self.progress_checkpoint = Some(checkpoint);
            return;
        }
        self.streamed_content = checkpoint.streamed_content;
        self.emitted_progress = checkpoint.emitted_progress;
        self.last_progress = checkpoint.last_progress;
        let mut restored = String::with_capacity(text.len() + self.progress_buffer.len());
        restored.push_str(text);
        restored.push_str(&self.progress_buffer);
        truncate_to_chars(&mut restored, self.config.max_chars);
        self.progress_buffer = restored;
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
            if self.emitted_progress > 0 {
                return self.render_progress_snapshot();
            }
            return ProjectedReply::Empty;
        };
        if extracted.independent {
            // The standalone final answer never mixes in streamed progress
            // (contract 1/4): any residual progress buffer is dropped.
            return self.render_final(&extracted.text);
        }
        if self.emitted_progress > 0 {
            // The complete fallback is applied to the existing progress card
            // in place. This neither repeats already displayed text in a new
            // message nor loses residual text that missed a progress update.
            return self.render_progress_snapshot();
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

    /// Masks agent-generated text and splits it into bounded plain-text parts.
    ///
    /// This compatibility helper is not used for standalone final delivery;
    /// final replies go through [`render_lark_markdown`] and
    /// [`split_lark_markdown`].
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
            // Rendering strips raw HTML, which can expose an address the
            // pre-render mask could not recognize (`user@<b>example.com</b>`).
            // Re-mask the rendered Markdown so the exposure never ships.
            let markdown = email_mask(&render_lark_markdown(trimmed));
            if markdown.is_empty() {
                return ProjectedReply::Empty;
            }
            ProjectedReply::Final {
                parts: split_lark_markdown(
                    &markdown,
                    self.config.max_chars,
                    LARK_MAX_SEND_BODY_BYTES,
                    self.config.max_splits,
                ),
            }
        }
    }

    fn render_progress_final(&self, text: &str) -> ProjectedReply {
        let mut masked = email_mask(text.trim());
        truncate_to_chars(&mut masked, self.config.max_chars);
        if masked.is_empty() {
            ProjectedReply::Empty
        } else {
            ProjectedReply::ProgressFinal { text: masked }
        }
    }

    fn render_progress_snapshot(&self) -> ProjectedReply {
        let mut complete = self.streamed_content.clone();
        complete.push_str(&email_mask(&self.progress_buffer));
        complete.push_str(&email_mask(&self.current_item_buffer));
        self.render_progress_final(&complete)
    }
}

/// Bounded rollback state for the one progress enqueue that may be in flight.
struct ProgressCheckpoint {
    streamed_content: String,
    emitted_progress: u32,
    last_progress: Option<Instant>,
    emitted_text: String,
}

impl fmt::Debug for ReplyProjector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReplyProjector")
            .field("config", &self.config)
            .field("streamed", &(self.emitted_progress > 0))
            .field("buffered_chars", &self.progress_buffer.chars().count())
            .field("streamed_chars", &self.streamed_content.chars().count())
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
        if let ThreadItem::AgentMessage { text, phase, .. } = item {
            return Some(ExtractedFinal {
                text: text.clone(),
                // A phase-less item is the protocol's standalone-final
                // fallback and is deliberately never emitted as progress.
                independent: phase.is_none(),
            });
        }
    }
    None
}

fn is_final_phase(phase: Option<&MessagePhase>) -> bool {
    matches!(phase, Some(MessagePhase::FinalAnswer))
}

fn is_progress_phase(phase: Option<&MessagePhase>) -> bool {
    matches!(phase, Some(MessagePhase::Commentary))
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
/// the characters in the same whitespace-delimited token.
#[must_use]
pub fn email_mask(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut token = EmailToken::default();
    #[cfg(test)]
    let mut measured_work = 0_usize;

    for (index, character) in text.char_indices() {
        #[cfg(test)]
        {
            measured_work = measured_work.saturating_add(character.len_utf8());
        }
        if character.is_whitespace() {
            let emitted_work = token.emit(text, index, &mut output);
            #[cfg(test)]
            {
                measured_work = measured_work.saturating_add(emitted_work);
            }
            #[cfg(not(test))]
            let _ = emitted_work;
            output.push(character);
            #[cfg(test)]
            {
                measured_work = measured_work.saturating_add(character.len_utf8());
            }
            token.reset(index + character.len_utf8());
        } else {
            token.observe(index, character);
        }
    }
    let emitted_work = token.emit(text, text.len(), &mut output);
    #[cfg(test)]
    {
        measured_work = measured_work.saturating_add(emitted_work);
        record_email_mask_work(measured_work);
    }
    #[cfg(not(test))]
    let _ = emitted_work;
    output
}

#[derive(Debug, Default)]
struct EmailToken {
    start: usize,
    last_dot: Option<usize>,
    terminal_label_chars: usize,
    terminal_label_is_ascii_alpha: bool,
    previous_is_local: bool,
    candidates: Vec<usize>,
}

impl EmailToken {
    fn observe(&mut self, index: usize, character: char) {
        if character == '@' && self.previous_is_local {
            self.candidates.push(index);
        }

        if character == '.' {
            self.last_dot = Some(index);
            self.terminal_label_chars = 0;
            self.terminal_label_is_ascii_alpha = true;
        } else if self.last_dot.is_some() {
            self.terminal_label_chars = self.terminal_label_chars.saturating_add(1);
            self.terminal_label_is_ascii_alpha &= character.is_ascii_alphabetic();
        }

        self.previous_is_local =
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '%' | '+' | '-');
    }

    /// Emits one already-classified token without rescanning any candidate's
    /// suffix. The terminal label is shared by every `@` before the last dot,
    /// exactly matching the former per-candidate predicate.
    fn emit(&self, text: &str, end: usize, output: &mut String) -> usize {
        let output_start = output.len();
        let terminal_label_is_domain =
            self.terminal_label_is_ascii_alpha && (2..=24).contains(&self.terminal_label_chars);
        let mut cursor = self.start;
        for &candidate in &self.candidates {
            output.push_str(&text[cursor..candidate]);
            let right_starts_with_digit = text[candidate + 1..end]
                .chars()
                .next()
                .is_some_and(|character| character.is_ascii_digit());
            if terminal_label_is_domain
                && self.last_dot.is_some_and(|last_dot| candidate < last_dot)
                && !right_starts_with_digit
            {
                output.push_str("[at]");
            } else {
                output.push('@');
            }
            cursor = candidate + 1;
        }
        output.push_str(&text[cursor..end]);

        output
            .len()
            .saturating_sub(output_start)
            .saturating_add(self.candidates.len())
    }

    fn reset(&mut self, start: usize) {
        self.start = start;
        self.last_dot = None;
        self.terminal_label_chars = 0;
        self.terminal_label_is_ascii_alpha = false;
        self.previous_is_local = false;
        self.candidates.clear();
    }
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

#[cfg(test)]
mod tests {
    use super::{ProjectedReply, ReplyProjector, take_measured_projection_work};
    use crate::codex::client::{ThreadId, TurnId, TurnOutcome};
    use crate::codex::types::{MessagePhase, ThreadItem, TurnStatus};
    use crate::lark::api::post_markdown_reply_body_len;
    use crate::limits::{
        LARK_MAX_SEND_BODY_BYTES, REPLY_MAX_SPLITS, REPLY_MESSAGE_MAX_CHARS,
        REPLY_TRUNCATION_MARKER, THREAD_PROJECTION_BYTE_BUDGET,
    };
    use serde_json::Map;

    #[test]
    fn actual_final_projection_has_tight_linear_mask_and_markdown_work() {
        let target_bytes = THREAD_PROJECTION_BYTE_BUDGET - 4 * 1024;
        let pair_count = (target_bytes / 2 - "x.com ".len()) / 2;
        let mut text = String::with_capacity(target_bytes);
        text.push_str(&"a@".repeat(pair_count));
        text.push_str("x.com ");
        text.push_str(&"[".repeat(target_bytes - text.len()));
        assert_eq!(text.len(), target_bytes);

        let outcome = TurnOutcome {
            thread_id: ThreadId::from("thread_linear_projection"),
            turn_id: TurnId::from("turn_linear_projection"),
            status: TurnStatus::Completed,
            error: None,
            completed_items: vec![ThreadItem::AgentMessage {
                id: "item_linear_projection".to_owned(),
                text,
                phase: Some(MessagePhase::FinalAnswer),
                memory_citation: None,
                extra: Map::new(),
            }],
            token_usage: None,
        };

        let _ = take_measured_projection_work();
        let projected = ReplyProjector::with_defaults().project_final(&outcome);
        let measured = take_measured_projection_work();

        // The pre-render mask pays one classification visit per source byte,
        // one output-byte write, and exactly one constant-time decision per
        // `@` candidate. Every qualifying one-byte `@` expands to the
        // four-byte `[at]` marker. The post-render remask then revisits the
        // masked text — three bytes longer per pair — and finds no `@`.
        assert_eq!(
            measured.email_mask,
            target_bytes
                .saturating_mul(4)
                .saturating_add(pair_count.saturating_mul(10)),
        );
        assert!(
            measured.markdown_inline <= target_bytes.saturating_mul(7),
            "{} source bytes consumed {} Markdown work units",
            target_bytes,
            measured.markdown_inline,
        );
        assert!(
            measured.email_mask.saturating_add(measured.markdown_inline)
                <= target_bytes.saturating_mul(14),
            "{target_bytes} source bytes consumed {measured:?}",
        );

        let ProjectedReply::Final { parts } = projected else {
            panic!("the completed answer must project to bounded final parts");
        };
        assert_eq!(parts.len(), REPLY_MAX_SPLITS);
        assert!(
            parts
                .iter()
                .all(|part| part.chars().count() == REPLY_MESSAGE_MAX_CHARS)
        );
        assert!(
            parts.iter().all(|part| {
                post_markdown_reply_body_len(part, true) <= LARK_MAX_SEND_BODY_BYTES
            })
        );
        assert!(
            parts
                .last()
                .is_some_and(|part| part.ends_with(REPLY_TRUNCATION_MARKER))
        );
        assert!(parts.iter().all(|part| !part.contains('@')));
    }
}
