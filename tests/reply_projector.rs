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
        ProjectedReply::Empty => panic!("the final must survive progress loss"),
    }
}

fn low_chars_config() -> ProjectorConfig {
    ProjectorConfig {
        max_chars: 4000,
        max_splits: 8,
        min_interval: Duration::from_millis(1500),
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
        ProjectedReply::Empty => panic!("the unstreamed short answer must be delivered"),
    }
}

#[test]
fn streamed_emitted_content_is_not_resent_as_final() {
    // Once Progress is actually emitted, the shown content is finalized in
    // place (contract 5) and must not be re-sent as the final.
    let mut projector = ReplyProjector::new(low_chars_config());
    let now = Instant::now();
    assert!(matches!(
        projector.observe(&delta("abc"), now),
        ProjectorOutput::Progress { .. }
    ));
    let turn = outcome(
        vec![agent("a1", "abc", Some(MessagePhase::Commentary))],
        TurnStatus::Completed,
    );
    assert_eq!(projector.finish(&turn), ProjectedReply::Empty);
}

#[test]
fn partial_stream_emits_only_the_unshown_remainder_as_final() {
    // After one Progress emission, the not-yet-displayed buffer tail is the
    // only content left to deliver as the final.
    let mut projector = ReplyProjector::new(low_chars_config());
    let now = Instant::now();
    assert!(matches!(
        projector.observe(&delta("abc"), now),
        ProjectorOutput::Progress { .. }
    ));
    assert!(matches!(
        projector.observe(&delta("de"), now),
        ProjectorOutput::Nothing
    ));
    let turn = outcome(
        vec![agent("a1", "abcde", Some(MessagePhase::Commentary))],
        TurnStatus::Completed,
    );
    match projector.finish(&turn) {
        ProjectedReply::Final { parts } => assert_eq!(parts, vec!["de"]),
        ProjectedReply::Empty => panic!("the unstreamed remainder must be delivered"),
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
        projector.observe(&delta("ab"), t0),
        ProjectorOutput::Nothing
    ));
    // Crossing the character threshold emits the accumulated text.
    assert!(matches!(
        projector.observe(&delta("cde"), t0),
        ProjectorOutput::Progress { .. }
    ));

    // Inside the interval: further text is not emitted.
    let t1 = t0 + Duration::from_secs(1);
    assert!(matches!(
        projector.observe(&delta("xyz"), t1),
        ProjectorOutput::Nothing
    ));

    // After the interval, new text is emitted again.
    let t2 = t1 + Duration::from_millis(600);
    match projector.observe(&delta("more"), t2) {
        ProjectorOutput::Progress { text } => {
            assert!(text.contains("xyz") || text.contains("more"));
        }
        ProjectorOutput::Nothing => panic!("expected a progress update after the interval"),
    }
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
