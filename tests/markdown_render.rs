//! Deterministic Lark Markdown projection and wire-aware splitting.

use lark_codex_bridge::lark::api::post_markdown_reply_body_len;
use lark_codex_bridge::render::{
    render_lark_markdown, split_lark_markdown, stabilize_streaming_markdown,
};

#[test]
fn supported_subset_is_preserved_and_headings_are_safe() {
    let source = concat!(
        "# Result\n\n",
        "Paragraph with **bold**, *italic*, ~~deleted~~, `code`, and ",
        "[a link](https://example.com).\n\n",
        "- first\n* second\n1. ordered\n\n",
        "> quoted\n\n",
        "```rust\nfn main() {}\n```\n",
    );

    assert_eq!(
        render_lark_markdown(source),
        concat!(
            "**Result**\n\n",
            "Paragraph with **bold**, *italic*, ~~deleted~~, `code`, and ",
            "[a link](https://example.com).\n\n",
            "- first\n- second\n1. ordered\n\n",
            "> quoted\n\n",
            "```rust\nfn main() {}\n```",
        )
    );
}

#[test]
fn unsupported_blocks_degrade_deterministically_without_raw_controls() {
    let source = concat!(
        "| Name | State |\n| --- | :---: |\n| bridge | ready |\n\n",
        "<section><b>Visible HTML</b></section>\n",
        "- [ ] pending\n  - [x] nested done\n",
        ">> deeply quoted[^note]\n",
        "[^note]: footnote body\n",
        "<!-- hidden control -->after\u{0}\n\n\nend",
    );

    let expected = concat!(
        "```text\n",
        "| Name | State |\n| --- | :---: |\n| bridge | ready |\n",
        "```\n\n",
        "Visible HTML\n",
        "- ☐ pending\n- ☑ nested done\n",
        "> deeply quoted(footnote note)\n",
        "Footnote note: footnote body\n",
        "after\n\nend",
    );
    let first = render_lark_markdown(source);
    assert_eq!(first, expected);
    assert_eq!(render_lark_markdown(source), first, "projection is stable");
    assert!(!first.contains('<'));
    assert!(!first.contains("[^"));
    assert!(!first.contains("[ ]"));
    assert!(!first.contains('\0'));
}

#[test]
fn malformed_fences_are_repaired_but_streaming_source_can_continue() {
    let source = "before\n```rust unsafe options\nlet answer = 42;";
    assert_eq!(
        render_lark_markdown(source),
        "before\n```text\nlet answer = 42;\n```"
    );
    assert_eq!(
        stabilize_streaming_markdown(source),
        "before\n```text\nlet answer = 42;\n```"
    );
    assert_eq!(
        stabilize_streaming_markdown("```rust\nlet answer = 42;\n```"),
        "```rust\nlet answer = 42;\n```"
    );
}

#[test]
fn carrier_sanitizers_neutralize_controls_without_rewriting_code_or_links() {
    let source = concat!(
        "> <section>quoted <at id=\"ou_attacker\">name</at></section>\n",
        "outside <at user_id=\"ou_attacker\">mention</at> ",
        "`<at id=\"literal\">code</at> [^inside]` ",
        "[literal](https://example.com/a_(b)) [^outside]",
        "\u{202e}\u{2066}\u{200b}",
    );
    let post = render_lark_markdown(source);
    assert_eq!(
        post,
        concat!(
            "> quoted name\n",
            "outside mention `<at id=\"literal\">code</at> [^inside]` ",
            "[literal](https://example.com/a_(b)) (footnote outside)",
        )
    );
    let card = stabilize_streaming_markdown(source);
    assert!(card.contains("`<at id=\"literal\">code</at> [^inside]`"));
    assert!(card.contains("[footnote outside]"));
    assert!(!card.contains("ou_attacker"));
    for control in ['\u{202e}', '\u{2066}', '\u{200b}'] {
        assert!(!post.contains(control));
        assert!(!card.contains(control));
    }
}

#[test]
fn unsupported_images_references_and_tilde_fences_have_explicit_degradation() {
    let source = concat!(
        "![diagram](https://example.com/diagram.png)\n",
        "![logo][asset]\n",
        "[guide][docs]\n",
        "[docs]: https://example.com/docs\n",
        "~~~rust\n",
        "let literal = \"```\";\n",
        "~~~\n",
    );
    assert_eq!(
        render_lark_markdown(source),
        concat!(
            "Image: diagram (https://example.com/diagram.png)\n",
            "Image: logo (reference asset)\n",
            "guide (reference docs)\n",
            "Reference docs: https://example.com/docs\n",
            "````rust\n",
            "let literal = \"```\";\n",
            "````",
        )
    );
}

#[test]
fn malformed_inline_and_html_constructs_degrade_without_cross_line_swallowing() {
    let source = concat!(
        "> <div class=\"unfinished\"\n",
        "> still quoted\n",
        "[broken](https://example.com\n",
        "<https://example.com/path>\n",
    );
    assert_eq!(
        render_lark_markdown(source),
        concat!(
            "> ‹div class=\"unfinished\"\n",
            "> still quoted\n",
            "[broken] (invalid link target)\n",
            "[https://example.com/path](https://example.com/path)",
        )
    );
}

#[test]
fn wire_split_is_utf8_and_fence_safe_after_json_escaping() {
    let source = format!(
        "intro with a quote: \"\n```rust\n{}\n```\noutro",
        "界".repeat(240)
    );
    let rendered = render_lark_markdown(&source);
    let parts = split_lark_markdown(&rendered, 80, 260, 16);

    assert!(parts.len() > 2, "fixture must exercise repeated splitting");
    for part in &parts {
        assert!(part.chars().count() <= 80);
        assert!(post_markdown_reply_body_len(part, true) <= 260);
        assert!(
            fences_are_balanced(part),
            "part is not standalone-safe: {part:?}"
        );
    }
    assert!(!parts.join("").contains('\u{fffd}'), "UTF-8 was preserved");
}

#[test]
fn bounded_split_truncates_with_a_closed_fence() {
    let rendered = render_lark_markdown(&format!("```text\n{}", "payload ".repeat(200)));
    let parts = split_lark_markdown(&rendered, 100, 320, 2);

    assert_eq!(parts.len(), 2);
    assert!(parts[1].ends_with("…[truncated]"));
    assert!(parts.iter().all(|part| fences_are_balanced(part)));
    assert!(
        parts
            .iter()
            .all(|part| post_markdown_reply_body_len(part, true) <= 320)
    );
}

fn fences_are_balanced(markdown: &str) -> bool {
    let mut marker: Option<char> = None;
    for line in markdown.lines() {
        let trimmed = line.trim();
        let Some(candidate) = trimmed.chars().next() else {
            continue;
        };
        if !matches!(candidate, '`' | '~') {
            continue;
        }
        let run = trimmed
            .chars()
            .take_while(|character| *character == candidate)
            .count();
        if run < 3 {
            continue;
        }
        if marker == Some(candidate) {
            marker = None;
        } else if marker.is_none() {
            marker = Some(candidate);
        }
    }
    marker.is_none()
}
