//! Deterministic projection from model Markdown to Lark's supported subset.
//!
//! This is deliberately not a CommonMark implementation. It preserves the
//! subset accepted by Lark `post` elements, makes unsupported block syntax
//! readable, and closes fenced code blocks before any snapshot is sent.

use crate::lark::api::post_markdown_reply_body_len;
use crate::limits::REPLY_TRUNCATION_MARKER;

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
    let sanitized = sanitize_text(input);
    let lines: Vec<&str> = sanitized.lines().collect();
    let mut rendered = Vec::new();
    let mut fence: Option<Fence> = None;
    let mut html = HtmlState::default();
    let mut index = 0;

    while index < lines.len() {
        let line = lines[index];
        if let Some(open) = fence.as_ref() {
            if is_closing_fence(line, open) {
                push_line(&mut rendered, open.closing_line());
                fence = None;
            } else {
                push_line(&mut rendered, line.to_owned());
            }
            index += 1;
            continue;
        }

        if let Some(open) = parse_opening_fence(line) {
            push_line(&mut rendered, open.opening_line());
            fence = Some(open);
            index += 1;
            continue;
        }

        if index + 1 < lines.len()
            && contains_table_pipe(line)
            && is_table_delimiter(lines[index + 1])
        {
            let mut table = Vec::new();
            while index < lines.len() && contains_table_pipe(lines[index]) {
                table.push(strip_html(lines[index], &mut html).trim().to_owned());
                index += 1;
            }
            let marker_len = table
                .iter()
                .map(|line| longest_run(line, '`'))
                .max()
                .unwrap_or(0)
                .saturating_add(1)
                .max(3);
            let marker = "`".repeat(marker_len);
            push_line(&mut rendered, format!("{marker}text"));
            for table_line in table {
                push_line(&mut rendered, table_line);
            }
            push_line(&mut rendered, marker);
            continue;
        }

        let without_html = strip_html(line, &mut html);
        push_line(&mut rendered, normalize_line(&without_html));
        index += 1;
    }

    if let Some(open) = fence {
        push_line(&mut rendered, open.closing_line());
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
    let mut stable = sanitize_text(input);
    if let Some(open) = open_fence_at_end(&stable) {
        if !stable.ends_with('\n') {
            stable.push('\n');
        }
        stable.push_str(&open.closing_line());
    }
    stable
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
    prefix
        .rfind('\n')
        .map(|newline| newline + 1)
        .filter(|byte| prefix[..*byte].chars().count() >= minimum)
        .unwrap_or(exact)
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
    let raw_language = rest.split_whitespace().next().unwrap_or_default();
    let language: String = raw_language
        .chars()
        .filter(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '+' | '#' | '.')
        })
        .take(32)
        .collect();
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

fn open_fence_at_end(text: &str) -> Option<Fence> {
    let mut open: Option<Fence> = None;
    for line in text.lines() {
        if let Some(current) = open.as_ref() {
            if is_closing_fence(line, current) {
                open = None;
            }
        } else {
            open = parse_opening_fence(line);
        }
    }
    open
}

fn normalize_line(line: &str) -> String {
    let mut body = line.trim();
    if body.is_empty() {
        return String::new();
    }

    let quoted = body.starts_with('>');
    if quoted {
        while let Some(rest) = body.strip_prefix('>') {
            body = rest.trim_start();
        }
    }

    let normalized = if let Some((identifier, text)) = footnote_definition(body) {
        format!(
            "Footnote {identifier}: {}",
            replace_footnote_references(text)
        )
    } else if let Some(heading) = heading_text(body) {
        format!("**{}**", replace_footnote_references(heading))
    } else if let Some((checked, text)) = task_item(body) {
        let marker = if checked { '☑' } else { '☐' };
        format!("- {marker} {}", replace_footnote_references(text))
    } else if let Some(text) = unordered_item(body) {
        format!("- {}", replace_footnote_references(text))
    } else if let Some((number, text)) = ordered_item(body) {
        format!("{number}. {}", replace_footnote_references(text))
    } else {
        replace_footnote_references(body)
    };

    if quoted {
        format!("> {normalized}")
    } else {
        normalized
    }
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

fn task_item(line: &str) -> Option<(bool, &str)> {
    for prefix in ["- [", "* [", "+ ["] {
        if let Some(rest) = line.strip_prefix(prefix) {
            let mut chars = rest.chars();
            let state = chars.next()?;
            if !matches!(state, ' ' | 'x' | 'X') || chars.next()? != ']' {
                continue;
            }
            let text = chars.as_str().strip_prefix(char::is_whitespace)?.trim();
            return Some((matches!(state, 'x' | 'X'), text));
        }
    }
    None
}

fn unordered_item(line: &str) -> Option<&str> {
    ["- ", "* ", "+ "]
        .iter()
        .find_map(|prefix| line.strip_prefix(prefix))
        .map(str::trim)
}

fn ordered_item(line: &str) -> Option<(&str, &str)> {
    let digits = line.chars().take_while(char::is_ascii_digit).count();
    if digits == 0 {
        return None;
    }
    let number = &line[..digits];
    let rest = &line[digits..];
    let rest = rest
        .strip_prefix(". ")
        .or_else(|| rest.strip_prefix(") "))?;
    Some((number, rest.trim()))
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

fn replace_footnote_references(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("[^") {
        output.push_str(&rest[..start]);
        let candidate = &rest[start + 2..];
        let Some(end) = candidate.find(']') else {
            output.push_str(&rest[start..]);
            return output;
        };
        let identifier = &candidate[..end];
        if identifier.is_empty() || !identifier.chars().all(is_footnote_identifier) {
            output.push_str(&rest[start..=start + 2 + end]);
        } else {
            output.push_str("(footnote ");
            output.push_str(identifier);
            output.push(')');
        }
        rest = &candidate[end + 1..];
    }
    output.push_str(rest);
    output
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

fn longest_run(text: &str, needle: char) -> usize {
    let mut longest = 0;
    let mut current = 0;
    for character in text.chars() {
        if character == needle {
            current += 1;
            longest = longest.max(current);
        } else {
            current = 0;
        }
    }
    longest
}

fn sanitize_text(input: &str) -> String {
    input
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .chars()
        .filter(|character| {
            matches!(character, '\n' | '\t') || (!character.is_control() && *character != '\u{7f}')
        })
        .collect()
}

#[derive(Default)]
struct HtmlState {
    comment: bool,
    tag: bool,
}

fn strip_html(line: &str, state: &mut HtmlState) -> String {
    let mut output = String::with_capacity(line.len());
    let mut cursor = 0;
    while cursor < line.len() {
        let rest = &line[cursor..];
        if state.comment {
            if let Some(end) = rest.find("-->") {
                state.comment = false;
                cursor += end + 3;
            } else {
                break;
            }
            continue;
        }
        if state.tag {
            if let Some(end) = rest.find('>') {
                state.tag = false;
                cursor += end + 1;
                push_space(&mut output);
            } else {
                break;
            }
            continue;
        }
        if rest.starts_with("<!--") {
            state.comment = true;
            cursor += 4;
            continue;
        }
        if rest.starts_with('<') && looks_like_html_tag(rest) {
            state.tag = true;
            cursor += 1;
            continue;
        }
        let character = rest.chars().next().expect("cursor is below line length");
        output.push(character);
        cursor += character.len_utf8();
    }
    output.trim().to_owned()
}

fn looks_like_html_tag(rest: &str) -> bool {
    if rest.starts_with("<http://") || rest.starts_with("<https://") {
        return false;
    }
    let mut chars = rest[1..].chars();
    let Some(mut first) = chars.next() else {
        return false;
    };
    if matches!(first, '/' | '!' | '?') {
        first = chars.next().unwrap_or('>');
    }
    first.is_ascii_alphabetic()
}

fn push_space(output: &mut String) {
    if !output.is_empty() && !output.ends_with(char::is_whitespace) {
        output.push(' ');
    }
}

fn push_line(lines: &mut Vec<String>, line: String) {
    if line.is_empty() && lines.last().is_some_and(String::is_empty) {
        return;
    }
    lines.push(line);
}
