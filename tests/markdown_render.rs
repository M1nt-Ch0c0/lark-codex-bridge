//! Deterministic Lark Markdown projection and wire-aware splitting.

use lark_codex_bridge::lark::api::post_markdown_reply_body_len;
use lark_codex_bridge::limits::LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES;
use lark_codex_bridge::render::{
    card_markdown_element_wire_len, render_lark_markdown, split_lark_markdown,
    stabilize_streaming_markdown,
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
            "```rust\n",
            "let literal = \"```\";\n",
            "```",
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
            "[broken] (invalid link target) https://example.com\n",
            "[https://example.com/path](https://example.com/path)",
        )
    );
}

#[test]
fn nested_quote_and_list_containers_keep_fences_and_tables_structural() {
    let source = concat!(
        "> - ```rust\n",
        ">   let answer = 42;\n",
        ">   ```\n",
        "> - | name | state |\n",
        ">   | --- | --- |\n",
        ">   | bridge | ready |\n",
        "- ~~~text\n",
        "  plain\n",
        "  ~~~\n",
    );
    assert_eq!(
        render_lark_markdown(source),
        concat!(
            "> ```rust\n",
            "> let answer = 42;\n",
            "> ```\n",
            "> ```text\n",
            "> | name | state |\n",
            "> | --- | --- |\n",
            "> | bridge | ready |\n",
            "> ```\n",
            "```text\n",
            "plain\n",
            "```",
        )
    );
}

#[test]
fn fenced_code_keeps_literal_container_markers_and_false_closers() {
    let top_level = concat!(
        "```text\n",
        "> literal quote\n",
        "- literal dash\n",
        "* literal star\n",
        "+ literal plus\n",
        "1. literal ordered\n",
        "- ```\n",
        "after false closer\n",
        "```\n",
    );
    let expected_top_level = top_level.trim_end();

    let nested = concat!(
        "> - ```text\n",
        ">   > literal nested quote\n",
        ">   - literal nested dash\n",
        ">   * literal nested star\n",
        ">   + literal nested plus\n",
        ">   1. literal nested ordered\n",
        ">   - ```\n",
        ">   after nested false closer\n",
        ">   ```\n",
    );
    let expected_nested = concat!(
        "> ```text\n",
        "> > literal nested quote\n",
        "> - literal nested dash\n",
        "> * literal nested star\n",
        "> + literal nested plus\n",
        "> 1. literal nested ordered\n",
        "> - ```\n",
        "> after nested false closer\n",
        "> ```",
    );

    let mismatched_closers = concat!(
        "> - ```text\n",
        "```\n",
        "> ```\n",
        ">   still code\n",
        ">   ```\n",
    );
    let expected_mismatched = concat!(
        "> ~~~text\n",
        "> ```\n",
        "> ```\n",
        "> still code\n",
        "> ~~~",
    );

    for rendered in [
        render_lark_markdown(top_level),
        stabilize_streaming_markdown(top_level),
    ] {
        assert_eq!(rendered, expected_top_level);
        assert!(rendered.contains("- ```\nafter false closer\n```"));
    }
    for rendered in [
        render_lark_markdown(nested),
        stabilize_streaming_markdown(nested),
    ] {
        assert_eq!(rendered, expected_nested);
        assert!(rendered.contains("> - ```\n> after nested false closer\n> ```"));
    }
    for rendered in [
        render_lark_markdown(mismatched_closers),
        stabilize_streaming_markdown(mismatched_closers),
    ] {
        assert_eq!(rendered, expected_mismatched);
        assert!(rendered.contains("> ```\n> ```\n> still code"));
    }
}

#[test]
fn encoded_tags_and_format_controls_are_inert_outside_code() {
    let source = concat!(
        "&lt;at id=\"ou_entity\"&gt;entity name&lt;/at&gt; ",
        "&#x3c;at user_id=\"ou_numeric\"&#x3e;numeric name&#60;/at&#62; ",
        "&LT;at id=\"ou_upper\"&GT;upper name&LT;/at&GT; ",
        "&#x202e;&rlm;&#8203;&NoBreak;&InvisibleTimes; ",
        "`&lt;at id=\"literal\"&gt;code&lt;/at&gt; &#x202e;`",
    );
    let rendered = render_lark_markdown(source);
    assert_eq!(
        rendered,
        concat!(
            "entity name numeric name upper name  ",
            "`&lt;at id=\"literal\"&gt;code&lt;/at&gt; &#x202e;`",
        )
    );
    assert!(!rendered.contains("ou_entity"));
    assert!(!rendered.contains("ou_numeric"));
    assert!(!rendered.contains("ou_upper"));
    assert!(!rendered.contains('\u{202e}'));
    assert!(!rendered.contains('\u{200f}'));
    assert!(!rendered.contains('\u{200b}'));
}

#[test]
fn entity_derived_characters_cannot_reconstruct_markdown_in_either_carrier() {
    let source = concat!(
        "&#42;&#42;bold&#42;&#42; &#95;italic&#95; &#126;&#126;strike&#126;&#126; ",
        "&#96;code&#96;\n",
        "&#96;&#96;&#96;rust&NewLine;let answer = 42;\n",
        "&#45;&#32;item\n",
        "&#49;&#46;&#32;ordered\n",
        "&#62;&#32;quote\n",
        "&#35;&#32;heading\n",
        "&#91;script&#93;&#40;javascript&#58;alert&#40;1&#41;&#41;\n",
        "&#33;&#91;image&#93;&#40;data&#58;text/plain,hello&#41;\n",
        "plain&NewLine;&#45;&#32;not-a-new-list\n",
        "&#124; a &#124; b &#124;&NewLine;&#124;&#45;&#45;&#45;&#124;&#45;&#45;&#45;&#124;\n",
        "- [&#120;] entity task letter\n",
        "&#91;docs&#93;&#58;&#32;https://example.com\n",
    );
    let expected = concat!(
        "＊＊bold＊＊ ＿italic＿ ～～strike～～ ｀code｀\n",
        "｀｀｀rust␤let answer = 42;\n",
        "－␠item\n",
        "１．␠ordered\n",
        "›␠quote\n",
        "＃␠heading\n",
        "［script］（javascript：alert（1））\n",
        "！［image］（data：text/plain,hello）\n",
        "plain␤－␠not-a-new-list\n",
        "｜ a ｜ b ｜␤｜－－－｜－－－｜\n",
        "- [ｘ] entity task letter\n",
        "［docs］：␠https://example.com",
    );
    for rendered in [
        render_lark_markdown(source),
        stabilize_streaming_markdown(source),
    ] {
        assert_eq!(rendered, expected);
        assert!(!rendered.contains("](javascript:"));
        assert!(!rendered.contains("](data:"));
        assert!(!rendered.contains("**"));
        assert!(!rendered.contains("```"));
        assert!(!rendered.contains("!["));
        assert!(!rendered.lines().any(|line| line.starts_with("> ")));
        assert!(fences_are_balanced(&rendered));
    }

    // Removing an encoded HTML wrapper can expose raw fence bytes. The
    // second structural pass must recognize and close that fence rather than
    // returning a post/Card2 payload with a dangling delimiter.
    let exposed = "&lt;b&gt;```text\npayload";
    for rendered in [
        render_lark_markdown(exposed),
        stabilize_streaming_markdown(exposed),
    ] {
        assert_eq!(rendered, "```text\npayload\n```");
        assert!(fences_are_balanced(&rendered));
    }
}

#[test]
fn carriers_apply_explicit_safe_url_schemes_and_keep_malformed_tails() {
    let source = concat!(
        "[secure](https://example.com/a_(b)) ",
        "[plain](http://example.com) ",
        "[mail](mailto:user@example.com) ",
        "[script](javascript:alert(1)) ",
        "[bidi](https://example.com/%E2%80%AEtxt) ",
        "<http://example.com/plain> ",
        "![bad](data:image/png;base64,AAAA)\n",
        "[broken](https://example.com trailing words\n",
        "![broken](https://example.com/image.png trailing image words",
    );
    let post = render_lark_markdown(source);
    assert!(post.contains("[secure](https://example.com/a_(b))"));
    assert!(post.contains("[plain](http://example.com)"));
    assert!(post.contains("[mail](mailto:user@example.com)"));
    assert!(post.contains("script (unsafe link target)"));
    assert!(post.contains("bidi (unsafe link target)"));
    assert!(post.contains("[http://example.com/plain](http://example.com/plain)"));
    assert!(post.contains("Image: bad (unsafe image target)"));
    assert!(post.contains("https://example.com trailing words"));
    assert!(post.contains("https://example.com/image.png trailing image words"));

    let card = stabilize_streaming_markdown(source);
    assert!(card.contains("[secure](https://example.com/a_(b))"));
    assert!(card.contains("plain (unsafe link target)"));
    assert!(card.contains("mail (unsafe link target)"));
    assert!(card.contains("http://example.com/plain (unsafe link target)"));
    assert!(!card.contains("[plain](http://"));
    assert!(!card.contains("[mail](mailto:"));
}

#[test]
fn escaped_closing_delimiters_use_backslash_parity_in_both_carriers() {
    let source = concat!(
        r"[odd\]](javascript:alert(1))",
        "\n",
        r"[even\\](javascript:alert(2))",
        "\n",
        r"[odd_three\\\]](data:text/plain,owned)",
        "\n",
        r"[even_four\\\\](data:text/plain,owned)",
        "\n",
        r"![odd\]](data:text/plain,owned)",
        "\n",
        r"![even\\](data:text/plain,owned)",
        "\n",
        r"![odd_three\\\]](javascript:alert(3))",
        "\n",
        r"![even_four\\\\](javascript:alert(4))",
        "\n",
        r"[target_odd](javascript:owned\)) trailing",
        "\n",
        r"[target_even](javascript:owned\\) trailing",
        "\n",
        r"![target_odd](data:text/plain,owned\)) trailing",
        "\n",
        r"![target_even](data:text/plain,owned\\) trailing",
    );
    let expected = concat!(
        r"odd\] (unsafe link target)",
        "\n",
        r"even\\ (unsafe link target)",
        "\n",
        r"odd_three\\\] (unsafe link target)",
        "\n",
        r"even_four\\\\ (unsafe link target)",
        "\n",
        r"Image: odd\] (unsafe image target)",
        "\n",
        r"Image: even\\ (unsafe image target)",
        "\n",
        r"Image: odd_three\\\] (unsafe image target)",
        "\n",
        r"Image: even_four\\\\ (unsafe image target)",
        "\n",
        "target_odd (unsafe link target) trailing",
        "\n",
        "target_even (unsafe link target) trailing",
        "\n",
        "Image: target_odd (unsafe image target) trailing",
        "\n",
        "Image: target_even (unsafe image target) trailing",
    );

    for rendered in [
        render_lark_markdown(source),
        stabilize_streaming_markdown(source),
    ] {
        assert_eq!(rendered, expected);
        assert!(!rendered.contains("](javascript:"));
        assert!(!rendered.contains("](data:"));
        assert!(!rendered.contains("javascript:"));
        assert!(!rendered.contains("data:text/plain"));
    }
}

#[test]
fn pathological_fence_delimiters_are_atomic_and_bounded() {
    let delimiter = "`".repeat(40_000);
    let source = format!("{delimiter}\nbody\n{delimiter}");
    let rendered = render_lark_markdown(&source);
    assert_eq!(rendered, "```\nbody\n```");

    let hostile_content = format!("~~~text\n{}\nbody\n~~~", "`".repeat(40_000));
    let card = stabilize_streaming_markdown(&hostile_content);
    assert!(card_markdown_element_wire_len(&card) <= LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES);
    assert!(card.ends_with("…[truncated]"));
    assert!(fences_are_balanced(&card));
    let opening = card.lines().next().expect("opening fence");
    assert!(opening.starts_with("~~~text"));
    assert!(
        opening
            .chars()
            .take_while(|character| *character == '~')
            .count()
            <= 64
    );
    let parts = split_lark_markdown(&render_lark_markdown(&hostile_content), 512, 1_024, 100);
    assert!(parts.len() > 2);
    assert!(parts.iter().all(|part| fences_are_balanced(part)));
}

#[test]
fn card2_element_cap_is_exact_after_sanitizing_and_json_escaping() {
    let overhead = card_markdown_element_wire_len("");
    let exact = "a".repeat(LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES - overhead);
    let at_limit = stabilize_streaming_markdown(&exact);
    assert_eq!(
        at_limit.len(),
        exact.len(),
        "projected len {}, exact len {}, projected wire {}, empty wire {}",
        at_limit.len(),
        exact.len(),
        card_markdown_element_wire_len(&at_limit),
        card_markdown_element_wire_len("")
    );
    assert_eq!(at_limit, exact);
    assert_eq!(
        card_markdown_element_wire_len(&at_limit),
        LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES
    );

    let over = stabilize_streaming_markdown(&format!("{exact}a"));
    assert!(over.ends_with("…[truncated]"));
    assert!(card_markdown_element_wire_len(&over) <= LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES);

    let expansion = format!(
        "```text\n{}\n```",
        "界\"\\\n".repeat(LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES)
    );
    let bounded = stabilize_streaming_markdown(&expansion);
    assert!(bounded.ends_with("…[truncated]"));
    assert!(fences_are_balanced(&bounded));
    assert!(bounded.is_char_boundary(bounded.len()));
    assert!(bounded.len() <= LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES);
    assert!(card_markdown_element_wire_len(&bounded) <= LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES);

    let controls = format!(
        "{}visible",
        "&lt;at id=\"ou_attacker\"&gt;&lt;/at&gt;".repeat(2_000)
    );
    let sanitized_first = stabilize_streaming_markdown(&controls);
    assert_eq!(sanitized_first, "visible");
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
