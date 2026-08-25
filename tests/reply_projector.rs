//! Pure-logic tests for the reply projector's hard contracts (design §9).
//!
//! No network, no store, no Codex: every input is a scripted `AppServerEvent`
//! or `TurnOutcome` constructed in memory.

use std::time::{Duration, Instant};

use lark_codex_bridge::codex::client::{AppServerEvent, ThreadId, TurnId, TurnOutcome};
use lark_codex_bridge::codex::types::{MessagePhase, ThreadItem, TurnStatus};
use lark_codex_bridge::render::{
    ProjectedReply, ProjectorConfig, ProjectorOutput, ReplyProjector, email_mask, split_text,
};
use serde_json::Map;

fn agent(id: &str, text: &str, phase: Option<MessagePhase>) -> ThreadItem {
    ThreadItem::AgentMessage {
        id: id.to_owned(),
        text: text.to_owned(),
        phase,
        memory_citation: None,
        extra: Map::new(),
    }
}

fn outcome(items: Vec<ThreadItem>, status: TurnStatus) -> TurnOutcome {
    TurnOutcome {
        thread_id: ThreadId::from("thread_1"),
        turn_id: TurnId::from("turn_1"),
        status,
        error: None,
        completed_items: items,
        token_usage: None,
    }
}

fn delta(text: &str) -> AppServerEvent {
    AppServerEvent::AgentMessageDelta {
        turn_id: TurnId::from("turn_1"),
        item_id: "item_1".to_owned(),
        delta: text.to_owned(),
    }
}

fn delta_for(item_id: &str, text: &str) -> AppServerEvent {
    AppServerEvent::AgentMessageDelta {
        turn_id: TurnId::from("turn_1"),
        item_id: item_id.to_owned(),
        delta: text.to_owned(),
    }
}

fn completed(item_id: &str, text: &str, phase: Option<MessagePhase>) -> AppServerEvent {
    AppServerEvent::ItemCompleted {
        turn_id: TurnId::from("turn_1"),
        item: agent(item_id, text, phase),
    }
}

#[test]
fn standalone_final_is_the_final_answer() {
    let projector = ReplyProjector::with_defaults();
    let turn = outcome(
        vec![
            agent("a1", "thinking out loud", Some(MessagePhase::Commentary)),
            agent(
                "a2",
                "the actual answer user@example.com",
                Some(MessagePhase::FinalAnswer),
            ),
        ],
        TurnStatus::Completed,
    );

    match projector.project_final(&turn) {
        ProjectedReply::Final { parts } => {
            assert_eq!(parts, vec!["the actual answer user[at]example.com"]);
        }
        ProjectedReply::ProgressFinal { .. } => panic!("no progress card exists"),
        ProjectedReply::Empty => panic!("expected a standalone final"),
    }
}

#[test]
fn final_render_remasks_addresses_exposed_by_html_stripping() {
    let projector = ReplyProjector::with_defaults();
    // The pre-render mask cannot see through raw HTML: `<b>` breaks the
    // terminal label and the dotted comment moves the last dot. Rendering
    // strips both, exposing clean addresses that the post-render mask must
    // still catch.
    let turn = outcome(
        vec![agent(
            "a1",
            "contact user@<b>example.com</b> or admin@<!--x.y-->example.org",
            Some(MessagePhase::FinalAnswer),
        )],
        TurnStatus::Completed,
    );

    match projector.project_final(&turn) {
        ProjectedReply::Final { parts } => {
            assert_eq!(
                parts,
                vec!["contact user[at]example.com or admin[at]example.org"]
            );
        }
        ProjectedReply::ProgressFinal { .. } => panic!("no progress card exists"),
        ProjectedReply::Empty => panic!("expected a standalone final"),
    }
}

#[test]
fn final_only_turn_produces_no_progress() {
    let mut projector = ReplyProjector::with_defaults();
    let now = Instant::now();

    // A completed FinalAnswer agent message is the final, never progress.
    let completed = AppServerEvent::ItemCompleted {
        turn_id: TurnId::from("turn_1"),
        item: agent("a1", "the answer", Some(MessagePhase::FinalAnswer)),
    };
    assert!(matches!(
        projector.observe(&completed, now),
        ProjectorOutput::Nothing
    ));

    let turn = outcome(
        vec![agent("a1", "the answer", Some(MessagePhase::FinalAnswer))],
        TurnStatus::Completed,
    );
    match projector.finish(&turn) {
        ProjectedReply::Final { parts } => assert_eq!(parts, vec!["the answer"]),
        ProjectedReply::ProgressFinal { .. } => panic!("no progress card exists"),
        ProjectedReply::Empty => panic!("expected a final"),
    }
}

#[test]
fn clean_empty_turn_sends_nothing() {
    let projector = ReplyProjector::with_defaults();

    let no_items = outcome(vec![], TurnStatus::Completed);
    assert_eq!(projector.project_final(&no_items), ProjectedReply::Empty);

    let empty_agent = outcome(
        vec![agent("a1", "   ", Some(MessagePhase::FinalAnswer))],
        TurnStatus::Completed,
    );
    assert_eq!(projector.project_final(&empty_agent), ProjectedReply::Empty);
}

#[test]
fn progress_failure_does_not_swallow_the_final() {
    let mut projector = ReplyProjector::with_defaults();
    let now = Instant::now();

    // Streaming commentary produces progress; the final is projected
    // independently, so a consumer may drop the progress output without ever
    // losing the terminal reply.
    let _progress = projector.observe(&delta("streamed commentary text"), now);
    let turn = outcome(
        vec![
            agent(
                "a1",
                "streamed commentary text",
                Some(MessagePhase::Commentary),
            ),
            agent("a2", "independent final", Some(MessagePhase::FinalAnswer)),
        ],
        TurnStatus::Completed,
    );

    match projector.finish(&turn) {
        ProjectedReply::Final { parts } => assert_eq!(parts, vec!["independent final"]),
        ProjectedReply::ProgressFinal { .. } => panic!("the final is independent"),
        ProjectedReply::Empty => panic!("the final must survive progress loss"),
    }
}

#[test]
fn rejected_progress_is_restored_to_the_standalone_final() {
    let mut projector = ReplyProjector::new(eager_config());
    let now = Instant::now();
    let text = match projector.observe(
        &completed("progress", "must survive", Some(MessagePhase::Commentary)),
        now,
    ) {
        ProjectorOutput::Progress { text } => text,
        ProjectorOutput::Nothing => panic!("progress should be emitted"),
    };
    projector.restore_progress(&text);

    let turn = outcome(
        vec![agent(
            "progress",
            "must survive",
            Some(MessagePhase::Commentary),
        )],
        TurnStatus::Completed,
    );
    assert_eq!(
        projector.finish(&turn),
        ProjectedReply::Final {
            parts: vec!["must survive".to_owned()]
        }
    );
}

#[test]
fn rejected_truncated_update_preserves_an_existing_durable_progress_card() {
    let mut projector = ReplyProjector::new(ProjectorConfig {
        max_chars: 5,
        max_splits: 2,
        min_interval: Duration::ZERO,
        min_chars: 1,
    });
    let now = Instant::now();

    let first = projector.observe(
        &completed("first", "12345", Some(MessagePhase::Commentary)),
        now,
    );
    assert!(matches!(first, ProjectorOutput::Progress { .. }));
    // The first output is considered durably enqueued (no restore call).

    let second = projector.observe(
        &completed("second", "67890", Some(MessagePhase::Commentary)),
        now,
    );
    let rejected = match second {
        ProjectorOutput::Progress { text } => text,
        ProjectorOutput::Nothing => panic!("second progress should be emitted"),
    };
    // Appending the second chunk reaches the text cap, so the bounded streamed
    // buffer does not end with this chunk. Rollback must nevertheless restore
    // the exact pre-emission state, including the durable-card fact.
    projector.restore_progress(&rejected);

    let turn = outcome(
        vec![
            agent("first", "12345", Some(MessagePhase::Commentary)),
            agent("second", "67890", Some(MessagePhase::Commentary)),
        ],
        TurnStatus::Completed,
    );
    assert!(
        matches!(
            projector.finish(&turn),
            ProjectedReply::ProgressFinal { .. }
        ),
        "a failed later update must still finalize the existing progress card"
    );
}

fn low_chars_config() -> ProjectorConfig {
    ProjectorConfig {
        max_chars: 4000,
        max_splits: 8,
        min_interval: Duration::from_millis(1500),
        min_chars: 3,
    }
}

/// Emits immediately once the character threshold is crossed, so dedup tests
/// can observe every buffered accumulation without advancing the clock.
fn eager_config() -> ProjectorConfig {
    ProjectorConfig {
        max_chars: 4000,
        max_splits: 8,
        min_interval: Duration::ZERO,
        min_chars: 3,
    }
}

#[test]
fn short_streamed_answer_below_threshold_is_delivered_as_final() {
    // A short streaming answer that never crossed the progress emit threshold
    // must not be silently dropped: nothing was actually shown, so the whole
    // trailing message is delivered as the final.
    let mut projector = ReplyProjector::new(low_chars_config());
    let now = Instant::now();
    assert!(matches!(
        projector.observe(&delta("ab"), now),
        ProjectorOutput::Nothing
    ));
    let turn = outcome(
        vec![agent("a1", "ab", Some(MessagePhase::Commentary))],
        TurnStatus::Completed,
    );
    match projector.finish(&turn) {
        ProjectedReply::Final { parts } => assert_eq!(parts, vec!["ab"]),
        ProjectedReply::ProgressFinal { .. } => panic!("no progress card exists"),
        ProjectedReply::Empty => panic!("the unstreamed short answer must be delivered"),
    }
}

#[test]
fn final_answer_delta_never_emits_progress_and_buffer_is_dropped() {
    // An AgentMessageDelta carries no phase, so a FinalAnswer item's delta must
    // not leak out as progress while it streams; once the item completes as
    // FinalAnswer, its buffered delta is dropped (the content belongs to the
    // terminal projection, never the progress view — contract 2).
    let mut projector = ReplyProjector::new(eager_config());
    let now = Instant::now();

    assert!(matches!(
        projector.observe(&delta_for("a1", "the final answer"), now),
        ProjectorOutput::Nothing,
    ));
    assert!(matches!(
        projector.observe(
            &completed("a1", "the final answer", Some(MessagePhase::FinalAnswer)),
            now
        ),
        ProjectorOutput::Nothing,
    ));

    let turn = outcome(
        vec![agent(
            "a1",
            "the final answer",
            Some(MessagePhase::FinalAnswer),
        )],
        TurnStatus::Completed,
    );
    match projector.finish(&turn) {
        ProjectedReply::Final { parts } => assert_eq!(parts, vec!["the final answer"]),
        ProjectedReply::ProgressFinal { .. } => panic!("no progress card exists"),
        ProjectedReply::Empty => panic!("expected the standalone final"),
    }
}

#[test]
fn phase_less_fallback_final_never_emits_progress() {
    let mut projector = ReplyProjector::new(eager_config());
    let now = Instant::now();

    assert!(matches!(
        projector.observe(&delta_for("fallback", "fallback answer"), now),
        ProjectorOutput::Nothing,
    ));
    assert!(matches!(
        projector.observe(&completed("fallback", "fallback answer", None), now),
        ProjectorOutput::Nothing,
    ));

    let turn = outcome(
        vec![agent("fallback", "fallback answer", None)],
        TurnStatus::Completed,
    );
    match projector.finish(&turn) {
        ProjectedReply::Final { parts } => assert_eq!(parts, vec!["fallback answer"]),
        ProjectedReply::ProgressFinal { .. } => panic!("phase-less final is standalone"),
        ProjectedReply::Empty => panic!("the fallback final must remain terminal output"),
    }
}

#[test]
fn emitted_content_is_not_resent_as_final() {
    // Once Progress is actually emitted (at completion), the shown content is
    // finalized in place (contract 5) and must not be re-sent as the final.
    let mut projector = ReplyProjector::new(low_chars_config());
    let now = Instant::now();
    match projector.observe(&completed("a1", "abc", Some(MessagePhase::Commentary)), now) {
        ProjectorOutput::Progress { .. } => {}
        ProjectorOutput::Nothing => panic!("the completed item must emit"),
    }
    let turn = outcome(
        vec![agent("a1", "abc", Some(MessagePhase::Commentary))],
        TurnStatus::Completed,
    );
    assert_eq!(
        projector.finish(&turn),
        ProjectedReply::ProgressFinal {
            text: "abc".to_owned()
        }
    );
}

#[test]
fn unshown_buffered_text_after_emit_is_delivered_as_final() {
    // The final card update contains both shown and buffered content, while
    // creating no standalone duplicate message.
    let mut projector = ReplyProjector::new(low_chars_config());
    let now = Instant::now();
    assert!(matches!(
        projector.observe(&completed("a1", "abc", Some(MessagePhase::Commentary)), now),
        ProjectorOutput::Progress { .. }
    ));
    assert!(matches!(
        projector.observe(&completed("a2", "de", Some(MessagePhase::Commentary)), now),
        ProjectorOutput::Nothing,
    ));
    let turn = outcome(
        vec![
            agent("a1", "abc", Some(MessagePhase::Commentary)),
            agent("a2", "de", Some(MessagePhase::Commentary)),
        ],
        TurnStatus::Completed,
    );
    match projector.finish(&turn) {
        ProjectedReply::ProgressFinal { text } => assert_eq!(text, "abcde"),
        ProjectedReply::Final { .. } => panic!("the existing card must be finalized in place"),
        ProjectedReply::Empty => panic!("the unshown remainder must be delivered"),
    }
}

#[test]
fn unshown_residual_delta_after_emit_is_delivered_as_final() {
    // A later item that streamed deltas but never completed is un-shown
    // residual content: after an earlier item emitted progress, the residual
    // delta buffer is delivered as the final instead of being re-sent or lost.
    let mut projector = ReplyProjector::new(low_chars_config());
    let now = Instant::now();
    assert!(matches!(
        projector.observe(&delta_for("a1", "abc"), now),
        ProjectorOutput::Nothing,
    ));
    assert!(matches!(
        projector.observe(&completed("a1", "abc", Some(MessagePhase::Commentary)), now),
        ProjectorOutput::Progress { .. }
    ));
    assert!(matches!(
        projector.observe(&delta_for("a2", "de"), now),
        ProjectorOutput::Nothing,
    ));

    let turn = outcome(
        vec![agent("a1", "abc", Some(MessagePhase::Commentary))],
        TurnStatus::Completed,
    );
    match projector.finish(&turn) {
        ProjectedReply::ProgressFinal { text } => assert_eq!(text, "abcde"),
        ProjectedReply::Final { .. } => panic!("the existing card must be finalized in place"),
        ProjectedReply::Empty => panic!("the un-shown residual must be delivered"),
    }
}

#[test]
fn project_final_path_ignores_streaming_state() {
    // The final-only durable path never consults streaming state: the trailing
    // agent message is always delivered whole, regardless of prior progress.
    let mut projector = ReplyProjector::new(low_chars_config());
    let now = Instant::now();
    let _ = projector.observe(&delta("abc"), now);
    let turn = outcome(
        vec![agent("a1", "abcde", Some(MessagePhase::Commentary))],
        TurnStatus::Completed,
    );
    match projector.project_final(&turn) {
        ProjectedReply::Final { parts } => assert_eq!(parts, vec!["abcde"]),
        ProjectedReply::ProgressFinal { .. } => panic!("project_final ignores progress"),
        ProjectedReply::Empty => panic!("project_final must deliver the whole trailing message"),
    }
}

#[test]
fn email_mask_masks_only_plausible_emails() {
    assert_eq!(email_mask("user@example.com"), "user[at]example.com");
    assert_eq!(
        email_mask("a.b+tag@sub.domain.com"),
        "a.b+tag[at]sub.domain.com"
    );
    assert_eq!(
        email_mask("ping user@example.com done"),
        "ping user[at]example.com done"
    );

    // Package versions and scoped package names stay untouched.
    assert_eq!(email_mask("serde@1.0.229"), "serde@1.0.229");
    assert_eq!(email_mask("lodash@^4.17.21"), "lodash@^4.17.21");
    assert_eq!(email_mask("@types/node"), "@types/node");

    // Mentions and non-domain @ usages stay untouched.
    assert_eq!(email_mask("hi @everyone"), "hi @everyone");
    assert_eq!(email_mask("hello@world"), "hello@world");
    assert_eq!(email_mask("trailing@"), "trailing@");
}

#[test]
fn email_mask_leaves_version_like_tokens_and_mentions_untouched() {
    // Version ranges (`pkg@1.0.229-beta`, `pkg@v1.2.3`) must not be masked:
    // the right-hand token starts with a digit, or the last-dot segment is not
    // a pure alphabetic domain label.
    assert_eq!(email_mask("serde@1.0.229-beta"), "serde@1.0.229-beta");
    assert_eq!(email_mask("foo@v1.2.3"), "foo@v1.2.3");

    // Mentions and underscore-suffixed usernames stay untouched.
    assert_eq!(email_mask("@mention"), "@mention");
    assert_eq!(email_mask("@_user_1"), "@_user_1");

    // A real address is still masked; an already-masked one has no '@' left.
    assert_eq!(email_mask("user@example.com"), "user[at]example.com");
    assert_eq!(email_mask("user[at]example.com"), "user[at]example.com");
}

#[test]
fn single_pass_email_mask_matches_the_legacy_predicate_corpus() {
    fn legacy_is_email_at(text: &str, index: usize) -> bool {
        let Some(left) = text[..index].chars().next_back() else {
            return false;
        };
        if !(left.is_ascii_alphanumeric() || matches!(left, '.' | '_' | '%' | '+' | '-')) {
            return false;
        }
        let token: String = text[index + 1..]
            .chars()
            .take_while(|character| !character.is_whitespace())
            .collect();
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
        let length = label.chars().count();
        (2..=24).contains(&length)
            && label
                .chars()
                .all(|character| character.is_ascii_alphabetic())
    }

    fn legacy_email_mask(text: &str) -> String {
        let mut output = String::with_capacity(text.len());
        let mut cursor = 0;
        for (index, _) in text.match_indices('@') {
            output.push_str(&text[cursor..index]);
            if legacy_is_email_at(text, index) {
                output.push_str("[at]");
            } else {
                output.push('@');
            }
            cursor = index + 1;
        }
        output.push_str(&text[cursor..]);
        output
    }

    let alphabet = [
        'a', 'Z', '1', '@', '.', ' ', '\t', '-', '_', '%', '+', '/', 'é', ',', ' ',
    ];
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    for case in 0..20_000 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let length = usize::from(state.to_le_bytes()[0] % 64).saturating_add(case % 3);
        let mut sample = String::new();
        for _ in 0..length {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            sample.push(alphabet[usize::from(state.to_le_bytes()[0]) % alphabet.len()]);
        }
        assert_eq!(
            email_mask(&sample),
            legacy_email_mask(&sample),
            "{sample:?}"
        );
    }

    let pairs = 64 * 1024;
    let dense = format!("{}x.com", "a@".repeat(pairs));
    let masked = email_mask(&dense);
    assert_eq!(masked.matches("[at]").count(), pairs);
    assert!(!masked.contains('@'));
    assert!(masked.ends_with("x.com"));
}

#[test]
fn split_text_is_bounded_and_deterministic() {
    assert_eq!(split_text("abcdef", 3, 5), vec!["abc", "def"]);

    let long = "a".repeat(45);
    let parts = split_text(&long, 20, 2);
    assert_eq!(parts.len(), 2, "split count must respect max_splits");
    assert_eq!(parts[0], "a".repeat(20));
    assert_eq!(
        parts[1],
        format!("{}…[truncated]", "a".repeat(8)),
        "the remainder must be truncated with an explicit marker"
    );
    assert!(parts[1].chars().count() <= 20);

    // Deterministic: the same input always yields the same parts.
    assert_eq!(split_text(&long, 20, 2), parts);
}

#[test]
fn observe_throttles_by_interval_and_char_count() {
    let config = ProjectorConfig {
        max_chars: 4000,
        max_splits: 8,
        min_interval: Duration::from_millis(1500),
        min_chars: 5,
    };
    let mut projector = ReplyProjector::new(config);
    let t0 = Instant::now();

    // Below the character threshold: no progress yet.
    assert!(matches!(
        projector.observe(&completed("a", "ab", Some(MessagePhase::Commentary)), t0),
        ProjectorOutput::Nothing
    ));
    // Crossing the character threshold emits the accumulated text.
    match projector.observe(&completed("b", "cde", Some(MessagePhase::Commentary)), t0) {
        ProjectorOutput::Progress { text } => assert_eq!(text, "abcde"),
        ProjectorOutput::Nothing => panic!("crossing the threshold must emit"),
    }

    // Inside the interval: further text is not emitted.
    let t1 = t0 + Duration::from_secs(1);
    assert!(matches!(
        projector.observe(&completed("c", "xyz", Some(MessagePhase::Commentary)), t1),
        ProjectorOutput::Nothing
    ));

    // After the interval, new text is emitted again.
    let t2 = t1 + Duration::from_millis(600);
    match projector.observe(&completed("d", "more", Some(MessagePhase::Commentary)), t2) {
        ProjectorOutput::Progress { text } => {
            assert!(text.contains("xyz") || text.contains("more"));
        }
        ProjectorOutput::Nothing => panic!("expected a progress update after the interval"),
    }
}

#[test]
fn same_item_delta_then_completed_is_emitted_once_at_completion() {
    // Codex streams an item as deltas and then completes it with the same full
    // text. Deltas are never emitted on their own; the completed event emits
    // the merged text exactly once.
    let mut projector = ReplyProjector::new(eager_config());
    let now = Instant::now();

    assert!(matches!(
        projector.observe(&delta_for("item_1", "hello"), now),
        ProjectorOutput::Nothing,
    ));
    match projector.observe(
        &completed("item_1", "hello", Some(MessagePhase::Commentary)),
        now,
    ) {
        ProjectorOutput::Progress { text } => assert_eq!(text, "hello"),
        ProjectorOutput::Nothing => panic!("the completed item must emit once"),
    }

    let turn = outcome(
        vec![agent("item_1", "hello", Some(MessagePhase::Commentary))],
        TurnStatus::Completed,
    );
    assert_eq!(
        projector.finish(&turn),
        ProjectedReply::ProgressFinal {
            text: "hello".to_owned()
        },
        "the existing card is finalized in place without a duplicate message"
    );
}

#[test]
fn completed_without_prior_delta_is_counted_in_full() {
    // An item that arrives only as `ItemCompleted` (never streamed) is the
    // deterministic fallback: the whole text is counted once.
    let mut projector = ReplyProjector::new(eager_config());
    let now = Instant::now();

    match projector.observe(
        &completed("item_9", "full text", Some(MessagePhase::Commentary)),
        now,
    ) {
        ProjectorOutput::Progress { text } => assert_eq!(text, "full text"),
        ProjectorOutput::Nothing => panic!("a delta-less completed item must be counted whole"),
    }
}

#[test]
fn completed_appends_only_the_delta_uncovered_tail() {
    // Deltas may stream a prefix and the completed item then carries a longer
    // text: only the tail beyond the received delta bytes is appended, never
    // the whole (already-seen) text again.
    let mut projector = ReplyProjector::new(eager_config());
    let now = Instant::now();

    assert!(matches!(
        projector.observe(&delta_for("item_1", "he"), now),
        ProjectorOutput::Nothing,
    ));
    match projector.observe(
        &completed("item_1", "hello", Some(MessagePhase::Commentary)),
        now,
    ) {
        ProjectorOutput::Progress { text } => assert_eq!(text, "hello"),
        ProjectorOutput::Nothing => panic!("the merged prefix + tail must emit"),
    }
}

#[test]
fn multiple_items_stream_and_complete_without_duplication() {
    // Sequential items with mixed delta/completed arrival keep their own text:
    // each is counted once, and the single streaming slot resets per item id.
    let mut projector = ReplyProjector::new(eager_config());
    let now = Instant::now();

    // Item a: deltas accumulate without emitting; completion emits once.
    assert!(matches!(
        projector.observe(&delta_for("a", "ab"), now),
        ProjectorOutput::Nothing,
    ));
    assert!(matches!(
        projector.observe(&delta_for("a", "c"), now),
        ProjectorOutput::Nothing,
    ));
    match projector.observe(&completed("a", "abc", Some(MessagePhase::Commentary)), now) {
        ProjectorOutput::Progress { text } => assert_eq!(text, "abc"),
        ProjectorOutput::Nothing => panic!("item a must emit once at completion"),
    }
    match projector.observe(
        &completed("b", "world", Some(MessagePhase::Commentary)),
        now,
    ) {
        ProjectorOutput::Progress { text } => assert_eq!(text, "world"),
        ProjectorOutput::Nothing => panic!("item b must emit"),
    }
    assert!(matches!(
        projector.observe(&delta_for("c", "xy"), now),
        ProjectorOutput::Nothing,
    ));
    match projector.observe(&completed("c", "xyz", Some(MessagePhase::Commentary)), now) {
        ProjectorOutput::Progress { text } => assert_eq!(text, "xyz"),
        ProjectorOutput::Nothing => panic!("item c must emit the merged tail"),
    }
}

#[test]
fn interleaved_item_deltas_and_completions_do_not_duplicate() {
    // Even when two items' deltas interleave (violating the single-item
    // assumption), each item's completion emits its full text exactly once.
    let mut projector = ReplyProjector::new(eager_config());
    let now = Instant::now();

    assert!(matches!(
        projector.observe(&delta_for("a", "ab"), now),
        ProjectorOutput::Nothing,
    ));
    assert!(matches!(
        projector.observe(&delta_for("b", "xy"), now),
        ProjectorOutput::Nothing,
    ));
    match projector.observe(&completed("a", "abc", Some(MessagePhase::Commentary)), now) {
        ProjectorOutput::Progress { text } => assert_eq!(text, "abc"),
        ProjectorOutput::Nothing => panic!("item a must emit once"),
    }
    match projector.observe(&completed("b", "xyz", Some(MessagePhase::Commentary)), now) {
        ProjectorOutput::Progress { text } => assert_eq!(text, "xyz"),
        ProjectorOutput::Nothing => panic!("item b must emit once"),
    }
}

#[test]
fn duplicate_item_completed_is_not_replayed() {
    // A duplicate `ItemCompleted` for an item already emitted must be a no-op:
    // the single-item delta buffer is already cleared, so re-appending the full
    // text would double-count the same content.
    let mut projector = ReplyProjector::new(eager_config());
    let now = Instant::now();

    match projector.observe(
        &completed("item_1", "hello", Some(MessagePhase::Commentary)),
        now,
    ) {
        ProjectorOutput::Progress { text } => assert_eq!(text, "hello"),
        ProjectorOutput::Nothing => panic!("the first completed must emit once"),
    }
    assert!(matches!(
        projector.observe(
            &completed("item_1", "hello", Some(MessagePhase::Commentary)),
            now
        ),
        ProjectorOutput::Nothing
    ));
}

#[test]
fn distinct_items_still_emit_independently_after_dedup() {
    // The single dedup slot overwrites per item, so a genuinely new item must
    // still emit even after a prior item completed.
    let mut projector = ReplyProjector::new(eager_config());
    let now = Instant::now();

    match projector.observe(&completed("a", "abc", Some(MessagePhase::Commentary)), now) {
        ProjectorOutput::Progress { text } => assert_eq!(text, "abc"),
        ProjectorOutput::Nothing => panic!("item a must emit"),
    }
    match projector.observe(&completed("b", "def", Some(MessagePhase::Commentary)), now) {
        ProjectorOutput::Progress { text } => assert_eq!(text, "def"),
        ProjectorOutput::Nothing => panic!("item b must emit"),
    }
}

#[test]
fn duplicate_final_item_completed_stays_dropped() {
    // A repeated `ItemCompleted` for a FinalAnswer item is still dropped, never
    // replayed as progress; the terminal projection is unaffected.
    let mut projector = ReplyProjector::new(eager_config());
    let now = Instant::now();
    for _ in 0..2 {
        assert!(matches!(
            projector.observe(
                &completed("a1", "final", Some(MessagePhase::FinalAnswer)),
                now
            ),
            ProjectorOutput::Nothing
        ));
    }
    let turn = outcome(
        vec![agent("a1", "final", Some(MessagePhase::FinalAnswer))],
        TurnStatus::Completed,
    );
    assert_eq!(
        projector.finish(&turn),
        ProjectedReply::Final {
            parts: vec!["final".to_owned()]
        },
        "the final must still be delivered"
    );
}

#[test]
fn debug_output_never_leaks_agent_text() {
    let projector = ReplyProjector::with_defaults();
    let rendered = format!("{projector:?}");
    assert!(rendered.contains("buffered_chars"));

    let reply = ProjectedReply::Final {
        parts: vec!["secret answer body".to_owned()],
    };
    let rendered = format!("{reply:?}");
    assert!(!rendered.contains("secret answer body"));
    assert!(rendered.contains("part_count"));
}
