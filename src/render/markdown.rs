//! Deterministic projection from model Markdown to Lark's supported subset.
//!
//! This is deliberately not a CommonMark implementation. It preserves the
//! subset accepted by Lark `post` elements, makes unsupported block syntax
//! readable, and closes fenced code blocks before any snapshot is sent.

use crate::lark::api::post_markdown_reply_body_len;
use crate::limits::{LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES, REPLY_TRUNCATION_MARKER};

const MAX_SAFE_FENCE_MARKER_CHARS: usize = 64;
// One allowance is shared by the inline fragment and every recursive label.
// All suffix scans debit it, so even an unrecognized hostile construct can do
// only constant work per admitted source byte before linear inert degradation.
const INLINE_WORK_PER_INPUT_BYTE: usize = 64;
const INLINE_WORK_BASE: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct InlineWorkExhausted;

#[derive(Debug)]
struct InlineWorkBudget {
    remaining: usize,
    spent: usize,
}

impl InlineWorkBudget {
    fn new(input_bytes: usize) -> Self {
        Self {
            remaining: input_bytes
                .saturating_mul(INLINE_WORK_PER_INPUT_BYTE)
                .saturating_add(INLINE_WORK_BASE),
            spent: 0,
        }
    }

    fn charge(&mut self, units: usize) -> Result<(), InlineWorkExhausted> {
        if units > self.remaining {
            self.spent = self.spent.saturating_add(self.remaining);
            self.remaining = 0;
            return Err(InlineWorkExhausted);
        }
        self.remaining -= units;
        self.spent = self.spent.saturating_add(units);
        Ok(())
    }

    #[cfg(test)]
    fn spent(&self) -> usize {
        self.spent
    }
}

#[derive(Debug, Default)]
struct UnescapedBracketSearch {
    scanned_to: usize,
    cached: Option<usize>,
    preceding_backslashes: usize,
}

impl UnescapedBracketSearch {
    fn find(
        &mut self,
        text: &str,
        start: usize,
        budget: &mut InlineWorkBudget,
    ) -> Result<Option<usize>, InlineWorkExhausted> {
        if let Some(cached) = self.cached {
            if cached >= start {
                return Ok(Some(cached));
            }
            self.cached = None;
        }

        // Callers advance through `text`; this cursor never retreats. An
        // unmatched `[` therefore cannot rescan an already-inspected suffix.
        // Keeping the full preceding run also implements Markdown's odd/even
        // backslash rule instead of treating every immediately preceded `]`
        // as escaped.
        let bytes = text.as_bytes();
        while self.scanned_to < bytes.len() {
            budget.charge(1)?;
            let cursor = self.scanned_to;
            let byte = bytes[cursor];
            self.scanned_to += 1;
            if byte == b'\\' {
                self.preceding_backslashes = self.preceding_backslashes.saturating_add(1);
                continue;
            }

            let escaped = self.preceding_backslashes % 2 == 1;
            self.preceding_backslashes = 0;
            if cursor >= start && byte == b']' && !escaped {
                self.cached = Some(cursor);
                return Ok(Some(cursor));
            }
        }
        Ok(None)
    }
}

/// Converts model-produced Markdown into the deterministic subset carried by
/// a Lark `post` element with `tag=md`.
///
/// Preserved syntax: paragraphs, unordered and ordered lists, block quotes,
/// inline code, fenced code, bold, italic, strikethrough, and inline links.
/// Headings become bold paragraphs. Tables become `text` fenced blocks. Raw
/// HTML is removed, task markers become Unicode checkboxes, footnotes become
/// labeled plain text, deep block/list nesting is flattened, consecutive blank
/// lines are collapsed, control characters are removed, and an unclosed fence
/// is repaired at end of input.
#[must_use]
pub fn render_lark_markdown(input: &str) -> String {
    project_markdown(input, MarkdownCarrier::Post)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MarkdownCarrier {
    Post,
    Card2,
}

fn project_markdown(input: &str, carrier: MarkdownCarrier) -> String {
    let sanitized = sanitize_text(input);
    let projected = project_markdown_once(&sanitized, carrier);

    // Inline sanitization can remove an HTML wrapper immediately before raw
    // Markdown delimiters. Re-run the complete block parser over the emitted
    // representation so anything exposed by that removal is validated and,
    // in particular, no newly exposed fence can remain unclosed. The first
    // pass has already consumed every decodable entity outside code into an
    // inert, visible representation, so this converges in one additional pass
    // while code spans/blocks remain data. In particular, the second pass can
    // never reinterpret entity-derived delimiters, ASCII identifiers, or
    // whitespace as Markdown structure.
    project_markdown_once(&projected, carrier)
}

fn project_markdown_once(input: &str, carrier: MarkdownCarrier) -> String {
    let lines: Vec<&str> = input.lines().collect();
    let mut rendered = Vec::new();
    let mut index = 0;

    while index < lines.len() {
        let line = parse_container_line(lines[index]);
        if let Some((source_fence, context)) = parse_fence_opening(lines[index]) {
            let mut end = index + 1;
            while end < lines.len() {
                let (candidate, complete_context) = context.content_body(lines[end]);
                if complete_context && is_closing_fence(candidate, &source_fence) {
                    break;
                }
                end += 1;
            }
            let content = lines[index + 1..end]
                .iter()
                .map(|content| context.content_body(content).0.to_owned())
                .collect::<Vec<_>>();
            let (canonical, content) = canonical_fence(content, source_fence.language);
            push_line(
                &mut rendered,
                quote_line(context.quoted, canonical.opening_line()),
            );
            for content_line in content {
                push_line(&mut rendered, quote_line(context.quoted, content_line));
            }
            push_line(
                &mut rendered,
                quote_line(context.quoted, canonical.closing_line()),
            );
            index = if end < lines.len() { end + 1 } else { end };
            continue;
        }

        if index + 1 < lines.len() && contains_table_pipe(line.body) && {
            let delimiter = parse_container_line(lines[index + 1]);
            delimiter.quoted == line.quoted && is_table_delimiter(delimiter.body)
        } {
            let mut table = Vec::new();
            while index < lines.len() {
                let candidate = parse_container_line(lines[index]);
                if candidate.quoted != line.quoted || !contains_table_pipe(candidate.body) {
                    break;
                }
                table.push(sanitize_inline(candidate.body, carrier).trim().to_owned());
                index += 1;
            }
            let (fence, table) = canonical_fence(table, "text".to_owned());
            push_line(&mut rendered, quote_line(line.quoted, fence.opening_line()));
            for table_line in table {
                push_line(&mut rendered, quote_line(line.quoted, table_line));
            }
            push_line(&mut rendered, quote_line(line.quoted, fence.closing_line()));
            continue;
        }

        push_line(&mut rendered, normalize_line(lines[index], carrier));
        index += 1;
    }

    while rendered.last().is_some_and(String::is_empty) {
        rendered.pop();
    }
    rendered.join("\n")
}

/// Returns a safe Card 2.0 Markdown snapshot without changing the accumulated
/// source. Each call repairs only the emitted copy, so a later delta can still
/// complete the original fence naturally.
#[must_use]
pub fn stabilize_streaming_markdown(input: &str) -> String {
    let projected = project_markdown(input, MarkdownCarrier::Card2);
    bound_card_markdown(&projected)
}

/// Returns the exact serialized byte length of one Card 2.0 Markdown element.
/// This is the inner card element object Lark parses after decoding the outer
/// `content` string, including JSON escaping and the `tag`/`content` keys.
#[must_use]
pub fn card_markdown_element_wire_len(markdown: &str) -> usize {
    serde_json::to_vec(&serde_json::json!({
        "tag": "markdown",
        "content": markdown,
    }))
    .map_or(usize::MAX, |wire| wire.len())
}

/// Splits already-rendered Lark Markdown into independently valid post parts.
///
/// Every part is bounded by both Unicode scalar count and the exact serialized
/// Lark reply-body byte count (using the larger in-thread shape). When a split
/// falls inside a fenced block, the current part is closed and the next part is
/// reopened with the same fence and language. A remainder beyond `max_splits`
/// is replaced by the deterministic truncation marker.
///
/// # Panics
///
/// Panics when a limit is zero or too small to hold even one safe character or
/// the truncation marker. Production limits are sized far above that overhead.
#[must_use]
pub fn split_lark_markdown(
    text: &str,
    max_chars: usize,
    max_wire_bytes: usize,
    max_splits: usize,
) -> Vec<String> {
    assert!(max_chars > 0, "max_chars must be non-zero");
    assert!(max_wire_bytes > 0, "max_wire_bytes must be non-zero");
    assert!(max_splits > 0, "max_splits must be non-zero");
    if text.is_empty() {
        return Vec::new();
    }

    let mut parts = Vec::new();
    let mut rest = text.to_owned();
    while parts.len() < max_splits {
        if fits(&rest, max_chars, max_wire_bytes) {
            parts.push(rest);
            return parts;
        }

        if parts.len() + 1 == max_splits {
            parts.push(truncated_part(&rest, max_chars, max_wire_bytes));
            return parts;
        }

        let (part, tail) = split_one(&rest, max_chars, max_wire_bytes);
        assert_ne!(tail, rest, "Markdown split must make progress");
        parts.push(part);
        rest = tail;
    }
    parts
}

fn split_one(text: &str, max_chars: usize, max_wire_bytes: usize) -> (String, String) {
    let upper = text.chars().count().min(max_chars);
    for count in (1..=upper).rev() {
        let byte = preferred_boundary(text, count);
        if byte == 0 || byte >= text.len() {
            continue;
        }
        let prefix = &text[..byte];
        let suffix = &text[byte..];
        let (part, tail) = close_and_reopen(prefix, suffix);
        if tail != text && fits(&part, max_chars, max_wire_bytes) {
            return (part, tail);
        }
    }
    panic!("Markdown wire limit is too small for a safe split")
}

fn truncated_part(text: &str, max_chars: usize, max_wire_bytes: usize) -> String {
    let upper = text.chars().count().min(max_chars);
    for count in (0..=upper).rev() {
        let byte = if count == 0 {
            0
        } else {
            preferred_boundary(text, count)
        };
        let prefix = &text[..byte];
        let mut candidate = close_fence(prefix);
        if !candidate.is_empty() && !candidate.ends_with('\n') {
            candidate.push('\n');
        }
        candidate.push_str(REPLY_TRUNCATION_MARKER);
        if fits(&candidate, max_chars, max_wire_bytes) {
            return candidate;
        }
    }
    panic!("Markdown wire limit is too small for the truncation marker")
}

fn fits(text: &str, max_chars: usize, max_wire_bytes: usize) -> bool {
    text.chars().count() <= max_chars && post_markdown_reply_body_len(text, true) <= max_wire_bytes
}

fn preferred_boundary(text: &str, count: usize) -> usize {
    let exact = char_boundary(text, count);
    let prefix = &text[..exact];
    let minimum = count / 2;
    let preferred = prefix
        .rfind('\n')
        .map(|newline| newline + 1)
        .filter(|byte| prefix[..*byte].chars().count() >= minimum)
        .unwrap_or(exact);
    atomic_fence_boundary(text, preferred)
}

fn char_boundary(text: &str, count: usize) -> usize {
    text.char_indices()
        .nth(count)
        .map_or(text.len(), |(index, _)| index)
}

fn close_and_reopen(prefix: &str, suffix: &str) -> (String, String) {
    let Some(open) = open_fence_at_end(prefix) else {
        return (prefix.to_owned(), suffix.to_owned());
    };
    let mut part = prefix.to_owned();
    if !part.ends_with('\n') {
        part.push('\n');
    }
    part.push_str(&open.closing_line());

    let mut tail = open.opening_line();
    tail.push('\n');
    tail.push_str(suffix.strip_prefix('\n').unwrap_or(suffix));
    (part, tail)
}

fn bound_card_markdown(text: &str) -> String {
    if card_markdown_fits(text) {
        return text.to_owned();
    }

    let overhead = card_markdown_element_wire_len("").saturating_sub(2);
    let suffix = format!("\n{REPLY_TRUNCATION_MARKER}");
    let reserve = json_string_content_len(&suffix)
        .saturating_add(MAX_SAFE_FENCE_MARKER_CHARS)
        .saturating_add(4); // optional quote prefix plus a closing newline
    let prefix_budget = LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES
        .saturating_sub(overhead)
        .saturating_sub(reserve);
    let mut encoded = 0_usize;
    let mut boundary = 0_usize;
    for (index, character) in text.char_indices() {
        let next = encoded.saturating_add(json_character_len(character));
        if next > prefix_budget {
            break;
        }
        encoded = next;
        boundary = index + character.len_utf8();
    }
    boundary = atomic_fence_boundary(text, boundary);

    loop {
        let mut candidate = close_fence(&text[..boundary]);
        if !candidate.is_empty() && !candidate.ends_with('\n') {
            candidate.push('\n');
        }
        candidate.push_str(REPLY_TRUNCATION_MARKER);
        if card_markdown_fits(&candidate) {
            return candidate;
        }
        let Some((previous, _)) = text[..boundary].char_indices().next_back() else {
            panic!("Card Markdown limit cannot hold the truncation marker");
        };
        boundary = atomic_fence_boundary(text, previous);
    }
}

fn card_markdown_fits(text: &str) -> bool {
    text.len() <= LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES
        && card_markdown_element_wire_len(text) <= LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES
}

fn json_string_content_len(text: &str) -> usize {
    text.chars().map(json_character_len).sum()
}

fn json_character_len(character: char) -> usize {
    match character {
        '"' | '\\' | '\u{0008}' | '\t' | '\n' | '\u{000c}' | '\r' => 2,
        '\u{0000}'..='\u{001f}' => 6,
        _ => character.len_utf8(),
    }
}

fn atomic_fence_boundary(text: &str, boundary: usize) -> usize {
    if boundary == 0 || boundary >= text.len() {
        return boundary;
    }
    let line_start = text[..boundary].rfind('\n').map_or(0, |index| index + 1);
    let line_end = text[boundary..]
        .find('\n')
        .map_or(text.len(), |offset| boundary + offset);
    if boundary == line_start || boundary == line_end {
        return boundary;
    }
    let line = &text[line_start..line_end];
    let active = open_fence_at_end(&text[..line_start]);
    let delimiter_is_atomic = active.as_ref().map_or_else(
        || parse_fence_opening(line).is_some(),
        |open| {
            let (body, complete_context) = open.context.content_body(line);
            complete_context && is_closing_fence(body, &open.fence)
        },
    );
    if delimiter_is_atomic {
        line_start
    } else {
        boundary
    }
}

fn close_fence(prefix: &str) -> String {
    let Some(open) = open_fence_at_end(prefix) else {
        return prefix.to_owned();
    };
    let mut closed = prefix.to_owned();
    if !closed.ends_with('\n') {
        closed.push('\n');
    }
    closed.push_str(&open.closing_line());
    closed
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Fence {
    marker: char,
    length: usize,
    language: String,
}

struct OpenFence {
    fence: Fence,
    context: FenceContext,
}

impl OpenFence {
    fn opening_line(&self) -> String {
        quote_line(self.context.quoted, self.fence.opening_line())
    }

    fn closing_line(&self) -> String {
        quote_line(self.context.quoted, self.fence.closing_line())
    }
}

#[derive(Clone, Debug)]
struct FenceContext {
    containers: Vec<FenceContainer>,
    quoted: bool,
}

#[derive(Clone, Copy, Debug)]
enum FenceContainer {
    Quote,
    Indent(usize),
}

impl FenceContext {
    /// Removes only the containers captured by the opening fence and reports
    /// whether every captured container matched. In particular, a body line
    /// beginning with `>`, `-`, `*`, `+`, or an ordered-list marker is never
    /// reparsed as a new generic container.
    fn content_body<'a>(&self, line: &'a str) -> (&'a str, bool) {
        let mut body = line;
        for container in &self.containers {
            match *container {
                FenceContainer::Quote => {
                    let candidate = body.trim_start_matches([' ', '\t']);
                    let Some(rest) = candidate.strip_prefix('>') else {
                        return (body, false);
                    };
                    body = rest
                        .strip_prefix(' ')
                        .or_else(|| rest.strip_prefix('\t'))
                        .unwrap_or(rest);
                }
                FenceContainer::Indent(width) => {
                    let Some(rest) = strip_container_indent(body, width) else {
                        return (body, false);
                    };
                    body = rest;
                }
            }
        }
        (body, true)
    }
}

fn strip_container_indent(line: &str, required: usize) -> Option<&str> {
    let mut columns = 0_usize;
    let mut bytes = 0_usize;
    for character in line.chars() {
        match character {
            ' ' => columns = columns.saturating_add(1),
            '\t' => columns = columns.saturating_add(4 - columns % 4),
            _ => break,
        }
        bytes = bytes.saturating_add(character.len_utf8());
        if columns >= required {
            return line.get(bytes..);
        }
    }
    None
}

fn parse_fence_opening(line: &str) -> Option<(Fence, FenceContext)> {
    let mut body = line.trim_start();
    let mut containers = Vec::new();
    let mut quoted = false;
    loop {
        if let Some(rest) = body.strip_prefix('>') {
            quoted = true;
            containers.push(FenceContainer::Quote);
            body = rest.trim_start();
            continue;
        }
        if let Some((width, rest)) = ["- ", "* ", "+ "]
            .iter()
            .find_map(|prefix| body.strip_prefix(prefix).map(|rest| (prefix.len(), rest)))
        {
            containers.push(FenceContainer::Indent(width));
            body = rest.trim_start();
            continue;
        }
        let digits = body.chars().take_while(char::is_ascii_digit).count();
        if digits > 0 {
            let rest = &body[digits..];
            if let Some(rest) = rest.strip_prefix(". ").or_else(|| rest.strip_prefix(") ")) {
                containers.push(FenceContainer::Indent(digits.saturating_add(2)));
                body = rest.trim_start();
                continue;
            }
        }
        break;
    }
    parse_opening_fence(body).map(|fence| (fence, FenceContext { containers, quoted }))
}

#[derive(Clone, Copy)]
enum ListContainer<'a> {
    Unordered,
    Ordered(&'a str),
}

#[derive(Clone, Copy)]
struct ContainerLine<'a> {
    quoted: bool,
    list: Option<ListContainer<'a>>,
    body: &'a str,
}

fn parse_container_line(line: &str) -> ContainerLine<'_> {
    let mut body = line.trim_start();
    let mut quoted = false;
    let mut list = None;
    loop {
        if let Some(rest) = body.strip_prefix('>') {
            quoted = true;
            body = rest.trim_start();
            continue;
        }
        if let Some(rest) = ["- ", "* ", "+ "]
            .iter()
            .find_map(|prefix| body.strip_prefix(prefix))
        {
            list.get_or_insert(ListContainer::Unordered);
            body = rest.trim_start();
            continue;
        }
        let digits = body.chars().take_while(char::is_ascii_digit).count();
        if digits > 0 {
            let number = &body[..digits];
            let rest = &body[digits..];
            if let Some(rest) = rest.strip_prefix(". ").or_else(|| rest.strip_prefix(") ")) {
                list.get_or_insert(ListContainer::Ordered(number));
                body = rest.trim_start();
                continue;
            }
        }
        break;
    }
    ContainerLine { quoted, list, body }
}

fn quote_line(quoted: bool, line: String) -> String {
    if !quoted {
        return line;
    }
    if line.is_empty() {
        ">".to_owned()
    } else {
        format!("> {line}")
    }
}

fn canonical_fence(mut content: Vec<String>, language: String) -> (Fence, Vec<String>) {
    let backticks = required_fence_length(&content, '`');
    let tildes = required_fence_length(&content, '~');
    let (marker, length) = if backticks <= tildes {
        ('`', backticks)
    } else {
        ('~', tildes)
    };
    if length <= MAX_SAFE_FENCE_MARKER_CHARS {
        return (
            Fence {
                marker,
                length,
                language,
            },
            content,
        );
    }

    // Pathological content can contain delimiter-only lines longer than the
    // complete Card2 element budget. Prefixing only such lines keeps them
    // readable while guaranteeing one small, atomic fence delimiter.
    for line in &mut content {
        if delimiter_only_run(line, '`') >= 3 {
            line.insert_str(0, "│ ");
        }
    }
    (
        Fence {
            marker: '`',
            length: 3,
            language,
        },
        content,
    )
}

fn required_fence_length(content: &[String], marker: char) -> usize {
    content
        .iter()
        .map(|line| delimiter_only_run(line, marker))
        .max()
        .unwrap_or(0)
        .saturating_add(1)
        .max(3)
}

fn delimiter_only_run(line: &str, marker: char) -> usize {
    let trimmed = line.trim();
    let run = trimmed
        .chars()
        .take_while(|character| *character == marker)
        .count();
    if run >= 3 && trimmed.chars().skip(run).all(char::is_whitespace) {
        run
    } else {
        0
    }
}

impl Fence {
    fn opening_line(&self) -> String {
        format!(
            "{}{}",
            self.marker.to_string().repeat(self.length),
            self.language
        )
    }

    fn closing_line(&self) -> String {
        self.marker.to_string().repeat(self.length)
    }
}

fn parse_opening_fence(line: &str) -> Option<Fence> {
    let trimmed = line.trim_start();
    let marker = trimmed.chars().next()?;
    if !matches!(marker, '`' | '~') {
        return None;
    }
    let length = trimmed
        .chars()
        .take_while(|character| *character == marker)
        .count();
    if length < 3 {
        return None;
    }
    let rest = &trimmed[marker.len_utf8() * length..];
    let info = rest.trim();
    let language = if info.is_empty() {
        String::new()
    } else if info.len() <= 32
        && !info.chars().any(char::is_whitespace)
        && info.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '+' | '#' | '.')
        })
    {
        info.to_owned()
    } else {
        // Lark interprets the bytes after a fence as an active language/info
        // selector. Never concatenate or partially retain malformed input.
        "text".to_owned()
    };
    Some(Fence {
        marker,
        length,
        language,
    })
}

fn is_closing_fence(line: &str, open: &Fence) -> bool {
    let trimmed = line.trim();
    let count = trimmed
        .chars()
        .take_while(|character| *character == open.marker)
        .count();
    count >= open.length && trimmed.chars().skip(count).all(char::is_whitespace)
}

fn open_fence_at_end(text: &str) -> Option<OpenFence> {
    let mut open: Option<OpenFence> = None;
    for line in text.lines() {
        if let Some(current) = open.as_ref() {
            let (body, complete_context) = current.context.content_body(line);
            if complete_context && is_closing_fence(body, &current.fence) {
                open = None;
            }
        } else if let Some((fence, context)) = parse_fence_opening(line) {
            open = Some(OpenFence { fence, context });
        }
    }
    open
}

fn normalize_line(line: &str, carrier: MarkdownCarrier) -> String {
    let parsed = parse_container_line(line);
    let body = parsed.body.trim();
    if body.is_empty() {
        return String::new();
    }

    let normalized = if let Some((identifier, text)) = footnote_definition(body) {
        format!("Footnote {identifier}: {}", sanitize_inline(text, carrier))
    } else if let Some((identifier, target)) = reference_definition(body) {
        format!(
            "Reference {identifier}: {}",
            sanitize_inline(target, carrier)
        )
    } else if let Some(heading) = heading_text(body) {
        format!("**{}**", sanitize_inline(heading, carrier))
    } else if let Some((checked, text)) = task_state(body).filter(|_| parsed.list.is_some()) {
        let marker = if checked { '☑' } else { '☐' };
        format!("- {marker} {}", sanitize_inline(text, carrier))
    } else if let Some(list) = parsed.list {
        match list {
            ListContainer::Unordered => format!("- {}", sanitize_inline(body, carrier)),
            ListContainer::Ordered(number) => {
                format!("{number}. {}", sanitize_inline(body, carrier))
            }
        }
    } else {
        sanitize_inline(body, carrier)
    };

    quote_line(parsed.quoted, normalized)
}

fn heading_text(line: &str) -> Option<&str> {
    let count = line
        .chars()
        .take_while(|character| *character == '#')
        .count();
    if !(1..=6).contains(&count) {
        return None;
    }
    line.get(count..)?
        .strip_prefix(char::is_whitespace)
        .map(str::trim)
}

fn task_state(line: &str) -> Option<(bool, &str)> {
    let rest = line.strip_prefix('[')?;
    let mut chars = rest.chars();
    let state = chars.next()?;
    if !matches!(state, ' ' | 'x' | 'X') || chars.next()? != ']' {
        return None;
    }
    let text = chars.as_str().strip_prefix(char::is_whitespace)?.trim();
    Some((matches!(state, 'x' | 'X'), text))
}

fn footnote_definition(line: &str) -> Option<(&str, &str)> {
    let rest = line.strip_prefix("[^")?;
    let end = rest.find("]:")?;
    let identifier = &rest[..end];
    if identifier.is_empty() || !identifier.chars().all(is_footnote_identifier) {
        return None;
    }
    Some((identifier, rest[end + 2..].trim()))
}

fn reference_definition(line: &str) -> Option<(&str, &str)> {
    let rest = line.strip_prefix('[')?;
    if rest.starts_with('^') {
        return None;
    }
    let end = rest.find("]:")?;
    let identifier = &rest[..end];
    if identifier.is_empty() || !identifier.chars().all(is_footnote_identifier) {
        return None;
    }
    Some((identifier, rest[end + 2..].trim()))
}

fn is_footnote_identifier(character: char) -> bool {
    character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
}

fn contains_table_pipe(line: &str) -> bool {
    line.chars().filter(|character| *character == '|').count() >= 1
}

fn is_table_delimiter(line: &str) -> bool {
    let core = line.trim().trim_matches('|');
    let cells: Vec<&str> = core.split('|').collect();
    cells.len() >= 2
        && cells.iter().all(|cell| {
            let cell = cell.trim().trim_matches(':').trim();
            cell.len() >= 3 && cell.chars().all(|character| character == '-')
        })
}

fn sanitize_text(input: &str) -> String {
    input
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .chars()
        .filter(|character| {
            matches!(character, '\n' | '\t')
                || (!character.is_control()
                    && *character != '\u{7f}'
                    && !is_unicode_format_control(*character))
        })
        .collect()
}

fn is_unicode_format_control(character: char) -> bool {
    matches!(
        character,
        '\u{00ad}'
            | '\u{061c}'
            | '\u{070f}'
            | '\u{0890}'..='\u{0891}'
            | '\u{08e2}'
            | '\u{180e}'
            | '\u{200b}'..='\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{206f}'
            | '\u{feff}'
            | '\u{fff9}'..='\u{fffb}'
            | '\u{1bca0}'..='\u{1bca3}'
            | '\u{1d173}'..='\u{1d17a}'
            | '\u{e0001}'
            | '\u{e0020}'..='\u{e007f}'
    )
}

fn sanitize_inline(text: &str, carrier: MarkdownCarrier) -> String {
    let mut budget = InlineWorkBudget::new(text.len());
    let rendered = sanitize_inline_bounded(text, carrier, &mut budget);
    #[cfg(test)]
    super::record_markdown_inline_work(
        budget
            .spent()
            .saturating_add(usize::from(rendered.is_err()).saturating_mul(text.len())),
    );
    rendered.unwrap_or_else(|InlineWorkExhausted| {
        // A hostile construct can otherwise make every source delimiter scan
        // the same suffix. Degrade the whole inline fragment in one pass so a
        // partially parsed prefix cannot activate syntax around the fallback.
        inert_inline_text(text)
    })
}

fn sanitize_inline_bounded(
    text: &str,
    carrier: MarkdownCarrier,
    budget: &mut InlineWorkBudget,
) -> Result<String, InlineWorkExhausted> {
    let mut output = String::with_capacity(text.len());
    let mut cursor = 0;
    let mut brackets = UnescapedBracketSearch::default();
    while cursor < text.len() {
        budget.charge(1)?;
        let rest = &text[cursor..];

        if rest.starts_with('`') {
            let run = count_byte_run(text, cursor, b'`', budget)?;
            if let Some(end) = find_code_span_end(text, cursor + run, run, budget)? {
                output.push_str(&text[cursor..end]);
                cursor = end;
                continue;
            }
        }

        if rest.starts_with("![") {
            if let Some((rendered, consumed)) =
                degrade_image(text, cursor, carrier, budget, &mut brackets)?
            {
                output.push_str(&rendered);
                cursor += consumed;
                continue;
            }
        }

        if rest.starts_with("[^") {
            if let Some(end) = brackets.find(text, cursor + 2, budget)? {
                let identifier = &text[cursor + 2..end];
                budget.charge(identifier.len())?;
                if !identifier.is_empty() && identifier.chars().all(is_footnote_identifier) {
                    match carrier {
                        MarkdownCarrier::Post => output.push_str("(footnote "),
                        MarkdownCarrier::Card2 => output.push_str("[footnote "),
                    }
                    output.push_str(identifier);
                    output.push(match carrier {
                        MarkdownCarrier::Post => ')',
                        MarkdownCarrier::Card2 => ']',
                    });
                    cursor = end + 1;
                    continue;
                }
            }
        }

        if rest.starts_with('[') {
            if let Some((rendered, consumed)) =
                preserve_or_degrade_link(text, cursor, carrier, budget, &mut brackets)?
            {
                output.push_str(&rendered);
                cursor += consumed;
                continue;
            }
        }

        if rest.starts_with("<!--") {
            if let Some(end) = find_bytes(text, cursor + 4, b"-->", budget)? {
                cursor = end + 3;
            } else {
                output.push('‹');
                output.push_str("!--");
                cursor += 4;
            }
            continue;
        }

        if rest.starts_with('&') {
            if let Some((rendered, consumed)) = sanitize_entity(rest, carrier, budget)? {
                output.push_str(&rendered);
                cursor += consumed;
                continue;
            }
        }

        if rest.starts_with('<') {
            if let Some(end) = find_byte(text, cursor + 1, b'>', budget)? {
                let inner = &text[cursor + 1..end];
                if safe_link_target(inner, carrier, budget)? {
                    output.push('[');
                    output.push_str(inner);
                    output.push_str("](");
                    output.push_str(inner);
                    output.push(')');
                } else if looks_like_url_target(inner, budget)? {
                    output.push_str(&sanitize_inline_bounded(inner, carrier, budget)?);
                    output.push_str(" (unsafe link target)");
                } else if !looks_like_html_tag(inner) {
                    // Not an HTML tag, but raw angle syntax is outside both
                    // Lark carriers' supported subset. Keep it readable using
                    // inert Unicode brackets.
                    output.push('‹');
                    output.push_str(inner);
                    output.push('›');
                }
                cursor = end + 1;
                continue;
            }
            // A malformed tag must not swallow subsequent quoted lines or be
            // allowed to become active when a later stream delta arrives.
            output.push('‹');
            cursor += 1;
            continue;
        }

        let character = rest.chars().next().expect("cursor is below line length");
        output.push(character);
        cursor += character.len_utf8();
    }
    Ok(output)
}

fn count_byte_run(
    text: &str,
    start: usize,
    needle: u8,
    budget: &mut InlineWorkBudget,
) -> Result<usize, InlineWorkExhausted> {
    let mut cursor = start;
    while text.as_bytes().get(cursor) == Some(&needle) {
        budget.charge(1)?;
        cursor += 1;
    }
    Ok(cursor - start)
}

fn find_code_span_end(
    text: &str,
    mut cursor: usize,
    run: usize,
    budget: &mut InlineWorkBudget,
) -> Result<Option<usize>, InlineWorkExhausted> {
    while cursor < text.len() {
        budget.charge(1)?;
        let rest = &text[cursor..];
        if rest.starts_with('`') {
            let candidate = count_byte_run(text, cursor, b'`', budget)?;
            if candidate == run {
                return Ok(Some(cursor + run));
            }
            cursor += candidate;
        } else {
            cursor += rest
                .chars()
                .next()
                .expect("cursor is below line length")
                .len_utf8();
        }
    }
    Ok(None)
}

fn degrade_image(
    text: &str,
    opening: usize,
    carrier: MarkdownCarrier,
    budget: &mut InlineWorkBudget,
    brackets: &mut UnescapedBracketSearch,
) -> Result<Option<(String, usize)>, InlineWorkExhausted> {
    let Some(label_end) = brackets.find(text, opening + 2, budget)? else {
        return Ok(None);
    };
    let alt = sanitize_inline_bounded(&text[opening + 2..label_end], carrier, budget)?;
    let after = label_end + 1;
    let mut output = if alt.is_empty() {
        "Image".to_owned()
    } else {
        format!("Image: {alt}")
    };
    if text.as_bytes().get(after) == Some(&b'(') {
        if let Some(end) = find_closing_paren(text, after, budget)? {
            let target = text[after + 1..end].trim();
            if safe_link_target(target, carrier, budget)? {
                output.push_str(" (");
                output.push_str(target);
                output.push(')');
            } else if !target.is_empty() {
                output.push_str(" (unsafe image target)");
            }
            return Ok(Some((output, end + 1 - opening)));
        }
        output.push_str(" (invalid image target) ");
        // Consume only the syntactic opener. The malformed target and every
        // following byte remain visible instead of being swallowed to EOL.
        return Ok(Some((output, after + 1 - opening)));
    }
    if text.as_bytes().get(after) == Some(&b'[') {
        if let Some(reference_end) = brackets.find(text, after + 1, budget)? {
            let identifier = &text[after + 1..reference_end];
            output.push_str(" (reference ");
            output.push_str(&sanitize_inline_bounded(
                if identifier.is_empty() {
                    &text[opening + 2..label_end]
                } else {
                    identifier
                },
                carrier,
                budget,
            )?);
            output.push(')');
            return Ok(Some((output, reference_end + 1 - opening)));
        }
    }
    Ok(Some((output, after - opening)))
}

fn preserve_or_degrade_link(
    text: &str,
    opening: usize,
    carrier: MarkdownCarrier,
    budget: &mut InlineWorkBudget,
    brackets: &mut UnescapedBracketSearch,
) -> Result<Option<(String, usize)>, InlineWorkExhausted> {
    let Some(label_end) = brackets.find(text, opening + 1, budget)? else {
        return Ok(None);
    };
    let label = sanitize_inline_bounded(&text[opening + 1..label_end], carrier, budget)?;
    let after = label_end + 1;
    if text.as_bytes().get(after) == Some(&b'(') {
        if let Some(end) = find_closing_paren(text, after, budget)? {
            let target = &text[after + 1..end];
            if safe_link_target(target, carrier, budget)? {
                return Ok(Some((format!("[{label}]({target})"), end + 1 - opening)));
            }
            // Keep the rejected target visible as inert text, matching the
            // autolink degradation. Sanitizing decodes any entity into its
            // full-width form, so the text can never reactivate as link
            // syntax in the second pass.
            return Ok(Some((
                format!(
                    "{label} ({}) (unsafe link target)",
                    sanitize_inline_bounded(target, carrier, budget)?
                ),
                end + 1 - opening,
            )));
        }
        // Preserve all bytes after the malformed opener as inert/readable
        // trailing text. Consuming the whole remainder would silently delete
        // unrelated output after a missing `)`.
        return Ok(Some((
            format!("[{label}] (invalid link target) "),
            after + 1 - opening,
        )));
    }
    if text.as_bytes().get(after) == Some(&b'[') {
        if let Some(reference_end) = brackets.find(text, after + 1, budget)? {
            let identifier = &text[after + 1..reference_end];
            let identifier = if identifier.is_empty() {
                &text[opening + 1..label_end]
            } else {
                identifier
            };
            return Ok(Some((
                format!(
                    "{label} (reference {})",
                    sanitize_inline_bounded(identifier, carrier, budget)?
                ),
                reference_end + 1 - opening,
            )));
        }
    }
    Ok(Some((format!("[{label}]"), after - opening)))
}

fn find_byte(
    text: &str,
    start: usize,
    needle: u8,
    budget: &mut InlineWorkBudget,
) -> Result<Option<usize>, InlineWorkExhausted> {
    let bytes = text.as_bytes();
    let mut cursor = start;
    while cursor < bytes.len() {
        budget.charge(1)?;
        if bytes[cursor] == needle {
            return Ok(Some(cursor));
        }
        cursor += 1;
    }
    Ok(None)
}

fn find_bytes(
    text: &str,
    start: usize,
    needle: &[u8],
    budget: &mut InlineWorkBudget,
) -> Result<Option<usize>, InlineWorkExhausted> {
    let bytes = text.as_bytes();
    let mut cursor = start;
    while cursor.saturating_add(needle.len()) <= bytes.len() {
        budget.charge(1)?;
        if bytes[cursor..].starts_with(needle) {
            return Ok(Some(cursor));
        }
        cursor += 1;
    }
    Ok(None)
}

fn find_closing_paren(
    text: &str,
    opening: usize,
    budget: &mut InlineWorkBudget,
) -> Result<Option<usize>, InlineWorkExhausted> {
    let bytes = text.as_bytes();
    let mut depth = 0_u32;
    let mut cursor = opening;
    let mut preceding_backslashes = 0_usize;
    while cursor < bytes.len() {
        budget.charge(1)?;
        let byte = bytes[cursor];
        cursor += 1;
        if byte == b'\\' {
            preceding_backslashes = preceding_backslashes.saturating_add(1);
            continue;
        }

        let escaped = preceding_backslashes % 2 == 1;
        preceding_backslashes = 0;
        if !escaped {
            if byte == b'(' {
                depth = depth.saturating_add(1);
            } else if byte == b')' {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Ok(Some(cursor - 1));
                }
            }
        }
    }
    Ok(None)
}

fn safe_link_target(
    target: &str,
    carrier: MarkdownCarrier,
    budget: &mut InlineWorkBudget,
) -> Result<bool, InlineWorkExhausted> {
    // This pays for trimming, character validation, percent decoding and the
    // URL parser before any of those complete passes are attempted.
    budget.charge(target.len().saturating_mul(8).saturating_add(16))?;
    let target = target.trim();
    if target.is_empty()
        || target.chars().any(|character| {
            character.is_control()
                || is_unicode_format_control(character)
                || matches!(character, '<' | '>' | '"' | '\'' | '\\')
                || character.is_whitespace()
        })
        || decoded_url_has_unsafe_controls(target)
        || has_decodable_entity(target, budget)?
    {
        return Ok(false);
    }
    let Ok(url) = url::Url::parse(target) else {
        return Ok(false);
    };
    Ok(match carrier {
        MarkdownCarrier::Post => matches!(url.scheme(), "https" | "http" | "mailto"),
        MarkdownCarrier::Card2 => url.scheme() == "https",
    })
}

/// A bare `&` is legitimate query syntax, but a sequence this pipeline can
/// decode (`&amp;`, `&#41;`, …) must stay rejected: an active link emits its
/// target verbatim, and a later decode — the second projection pass or the
/// Lark renderer — could turn the entity back into a delimiter byte that was
/// never validated.
fn has_decodable_entity(
    target: &str,
    budget: &mut InlineWorkBudget,
) -> Result<bool, InlineWorkExhausted> {
    let bytes = target.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() {
        budget.charge(1)?;
        if bytes[cursor] == b'&' && decode_entity(&target[cursor..], budget)?.is_some() {
            return Ok(true);
        }
        cursor += 1;
    }
    Ok(false)
}

fn looks_like_url_target(
    target: &str,
    budget: &mut InlineWorkBudget,
) -> Result<bool, InlineWorkExhausted> {
    budget.charge(target.len().saturating_mul(2).saturating_add(4))?;
    let Some((scheme, _)) = target.split_once(':') else {
        return Ok(false);
    };
    Ok(!scheme.is_empty()
        && scheme.chars().enumerate().all(|(index, character)| {
            character.is_ascii_alphabetic()
                || (index > 0 && matches!(character, '+' | '-' | '.' | '0'..='9'))
        }))
}

fn decoded_url_has_unsafe_controls(target: &str) -> bool {
    let bytes = target.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut cursor = 0;
    while cursor < bytes.len() {
        if bytes[cursor] == b'%' {
            let Some(hex) = bytes.get(cursor + 1..cursor + 3) else {
                return true;
            };
            let Ok(hex) = std::str::from_utf8(hex) else {
                return true;
            };
            let Ok(byte) = u8::from_str_radix(hex, 16) else {
                return true;
            };
            decoded.push(byte);
            cursor += 3;
        } else {
            decoded.push(bytes[cursor]);
            cursor += 1;
        }
    }
    let Ok(decoded) = std::str::from_utf8(&decoded) else {
        return true;
    };
    decoded.chars().any(|character| {
        character.is_control() || is_unicode_format_control(character) || character == '\u{7f}'
    })
}

fn sanitize_entity(
    rest: &str,
    carrier: MarkdownCarrier,
    budget: &mut InlineWorkBudget,
) -> Result<Option<(String, usize)>, InlineWorkExhausted> {
    let Some((decoded, consumed)) = decode_entity(rest, budget)? else {
        return Ok(None);
    };
    if decoded == '<' {
        if let Some((inner, end)) = encoded_tag(rest, consumed, budget)? {
            if looks_like_html_tag(&inner) {
                return Ok(Some((String::new(), end)));
            }
            return Ok(Some((
                format!("‹{}›", sanitize_inline_bounded(&inner, carrier, budget)?),
                end,
            )));
        }
        return Ok(Some(("‹".to_owned(), consumed)));
    }
    if is_unicode_format_control(decoded) || (decoded.is_control() && !decoded.is_whitespace()) {
        return Ok(Some((String::new(), consumed)));
    }
    Ok(Some((inert_entity_text(decoded), consumed)))
}

fn inert_entity_text(decoded: char) -> String {
    // A decoded entity is untrusted syntax, not source Markdown. Preserve a
    // readable representation without ever emitting an ASCII byte that could
    // complete a fence, emphasis run, list/task marker, link/image/reference,
    // HTML control, or escape in the second pass or in either Lark carrier.
    // Visible control pictures also prevent entity whitespace from exposing a
    // raw delimiter at the start of a structural line.
    let mut output = String::new();
    push_inert_character(&mut output, decoded);
    output
}

fn inert_inline_text(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    for character in text.chars() {
        push_inert_character(&mut output, character);
    }
    output
}

fn push_inert_character(output: &mut String, character: char) {
    let inert = match character {
        '\n' | '\r' | '\u{0085}' | '\u{2028}' | '\u{2029}' => '␤',
        '\t' => '␉',
        character if character.is_whitespace() => '␠',
        '<' => '‹',
        '>' => '›',
        character if character.is_ascii_graphic() => char::from_u32(
            u32::from(character)
                .saturating_sub(u32::from('!'))
                .saturating_add(u32::from('！')),
        )
        .expect("ASCII graphic characters have full-width forms"),
        character => character,
    };
    output.push(inert);
}

fn encoded_tag(
    rest: &str,
    opening_len: usize,
    budget: &mut InlineWorkBudget,
) -> Result<Option<(String, usize)>, InlineWorkExhausted> {
    let mut cursor = opening_len;
    while cursor < rest.len() {
        budget.charge(1)?;
        if rest.as_bytes()[cursor] == b'>' {
            return Ok(Some((rest[opening_len..cursor].to_owned(), cursor + 1)));
        }
        if rest.as_bytes()[cursor] == b'&' {
            if let Some(('>', consumed)) = decode_entity(&rest[cursor..], budget)? {
                return Ok(Some((
                    rest[opening_len..cursor].to_owned(),
                    cursor + consumed,
                )));
            }
        }
        cursor += rest[cursor..]
            .chars()
            .next()
            .expect("cursor is below entity-tag length")
            .len_utf8();
    }
    Ok(None)
}

fn decode_entity(
    rest: &str,
    budget: &mut InlineWorkBudget,
) -> Result<Option<(char, usize)>, InlineWorkExhausted> {
    if rest.as_bytes().first() != Some(&b'&') {
        return Ok(None);
    }
    let mut end = 1;
    while end < rest.len() && end <= 32 {
        budget.charge(1)?;
        if rest.as_bytes()[end] == b';' {
            break;
        }
        end += 1;
    }
    if end >= rest.len() || end > 32 || rest.as_bytes()[end] != b';' {
        return Ok(None);
    }
    let body = &rest[1..end];
    let decoded = match body {
        "lt" | "LT" => '<',
        "gt" | "GT" => '>',
        "amp" | "AMP" => '&',
        "quot" | "QUOT" => '"',
        "apos" => '\'',
        "shy" => '\u{00ad}',
        "lrm" => '\u{200e}',
        "rlm" => '\u{200f}',
        "zwnj" => '\u{200c}',
        "zwj" => '\u{200d}',
        "ZeroWidthSpace"
        | "NegativeMediumSpace"
        | "NegativeThickSpace"
        | "NegativeThinSpace"
        | "NegativeVeryThinSpace" => '\u{200b}',
        "NoBreak" => '\u{2060}',
        "ApplyFunction" => '\u{2061}',
        "InvisibleTimes" => '\u{2062}',
        "InvisibleComma" => '\u{2063}',
        "Tab" => '\t',
        "NewLine" => '\n',
        numeric if numeric.starts_with("#x") || numeric.starts_with("#X") => {
            let Ok(numeric) = u32::from_str_radix(&numeric[2..], 16) else {
                return Ok(None);
            };
            let Some(decoded) = char::from_u32(numeric) else {
                return Ok(None);
            };
            decoded
        }
        numeric if numeric.starts_with('#') => {
            let Ok(numeric) = numeric[1..].parse::<u32>() else {
                return Ok(None);
            };
            let Ok(decoded) = numeric.try_into() else {
                return Ok(None);
            };
            decoded
        }
        _ => return Ok(None),
    };
    Ok(Some((decoded, end + 1)))
}

fn looks_like_html_tag(inner: &str) -> bool {
    let mut chars = inner.chars();
    let Some(mut first) = chars.next() else {
        return false;
    };
    if matches!(first, '/' | '!' | '?') {
        first = chars.next().unwrap_or('>');
    }
    first.is_ascii_alphabetic()
}

fn push_line(lines: &mut Vec<String>, line: String) {
    if line.is_empty() && lines.last().is_some_and(String::is_empty) {
        return;
    }
    lines.push(line);
}

#[cfg(test)]
mod tests {
    use super::{
        InlineWorkBudget, InlineWorkExhausted, MarkdownCarrier, render_lark_markdown,
        sanitize_inline, sanitize_inline_bounded, stabilize_streaming_markdown,
    };
    use crate::limits::{LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES, THREAD_PROJECTION_BYTE_BUDGET};

    #[test]
    fn multi_mib_unmatched_brackets_have_deterministic_linear_work() {
        let source = "[".repeat(THREAD_PROJECTION_BYTE_BUDGET);
        let mut budget = InlineWorkBudget::new(source.len());
        let rendered = sanitize_inline_bounded(&source, MarkdownCarrier::Post, &mut budget)
            .expect("the monotonic delimiter scan stays within its linear allowance");

        assert_eq!(rendered, source);
        assert!(
            budget.spent() <= source.len().saturating_mul(2),
            "{} bytes consumed {} deterministic work units",
            source.len(),
            budget.spent(),
        );

        assert_eq!(render_lark_markdown(&source), source);
        let card = stabilize_streaming_markdown(&source);
        assert!(
            super::card_markdown_element_wire_len(&card) <= LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES
        );
        assert!(card.ends_with("…[truncated]"));
    }

    #[test]
    fn forward_scan_budget_exhaustion_is_deterministic_and_inert() {
        let source = "<!--".repeat(32 * 1024);
        let mut budget = InlineWorkBudget::new(source.len());
        assert_eq!(
            sanitize_inline_bounded(&source, MarkdownCarrier::Post, &mut budget),
            Err(InlineWorkExhausted),
        );
        assert_eq!(
            budget.spent(),
            source
                .len()
                .saturating_mul(super::INLINE_WORK_PER_INPUT_BYTE)
                .saturating_add(super::INLINE_WORK_BASE),
        );

        let rendered = sanitize_inline(&source, MarkdownCarrier::Post);
        assert!(!rendered.contains('<'));
        assert!(!rendered.contains('>'));
        assert!(!rendered.contains("<!--"));
        assert!(rendered.starts_with("‹！－－"));
    }
}
