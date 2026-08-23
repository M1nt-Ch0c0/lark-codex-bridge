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
    project_markdown(input, MarkdownCarrier::Post)
}

#[derive(Clone, Copy)]
enum MarkdownCarrier {
    Post,
    Card2,
}

fn project_markdown(input: &str, carrier: MarkdownCarrier) -> String {
    let sanitized = sanitize_text(input);
    let lines: Vec<&str> = sanitized.lines().collect();
    let mut rendered = Vec::new();
    let mut index = 0;

    while index < lines.len() {
        let line = lines[index];
        if let Some(source_fence) = parse_opening_fence(line) {
            let mut end = index + 1;
            while end < lines.len() && !is_closing_fence(lines[end], &source_fence) {
                end += 1;
            }
            let marker_len = lines[index + 1..end]
                .iter()
                .map(|content| longest_run(content, '`'))
                .max()
                .unwrap_or(0)
                .saturating_add(1)
                .max(3);
            let canonical = Fence {
                marker: '`',
                length: marker_len,
                language: source_fence.language,
            };
            push_line(&mut rendered, canonical.opening_line());
            for content in &lines[index + 1..end] {
                push_line(&mut rendered, (*content).to_owned());
            }
            push_line(&mut rendered, canonical.closing_line());
            index = if end < lines.len() { end + 1 } else { end };
            continue;
        }

        if index + 1 < lines.len()
            && contains_table_pipe(line)
            && is_table_delimiter(lines[index + 1])
        {
            let mut table = Vec::new();
            while index < lines.len() && contains_table_pipe(lines[index]) {
                table.push(sanitize_inline(lines[index], carrier).trim().to_owned());
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

        push_line(&mut rendered, normalize_line(line, carrier));
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
    project_markdown(input, MarkdownCarrier::Card2)
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

fn normalize_line(line: &str, carrier: MarkdownCarrier) -> String {
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
        format!("Footnote {identifier}: {}", sanitize_inline(text, carrier))
    } else if let Some((identifier, target)) = reference_definition(body) {
        format!(
            "Reference {identifier}: {}",
            sanitize_inline(target, carrier)
        )
    } else if let Some(heading) = heading_text(body) {
        format!("**{}**", sanitize_inline(heading, carrier))
    } else if let Some((checked, text)) = task_item(body) {
        let marker = if checked { '☑' } else { '☐' };
        format!("- {marker} {}", sanitize_inline(text, carrier))
    } else if let Some(text) = unordered_item(body) {
        format!("- {}", sanitize_inline(text, carrier))
    } else if let Some((number, text)) = ordered_item(body) {
        format!("{number}. {}", sanitize_inline(text, carrier))
    } else {
        sanitize_inline(body, carrier)
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
    let mut output = String::with_capacity(text.len());
    let mut cursor = 0;
    while cursor < text.len() {
        let rest = &text[cursor..];

        if rest.starts_with('`') {
            let run = rest.bytes().take_while(|byte| *byte == b'`').count();
            if let Some(end) = find_code_span_end(text, cursor + run, run) {
                output.push_str(&text[cursor..end]);
                cursor = end;
                continue;
            }
        }

        if rest.starts_with("![") {
            if let Some((rendered, consumed)) = degrade_image(rest, carrier) {
                output.push_str(&rendered);
                cursor += consumed;
                continue;
            }
        }

        if rest.starts_with("[^") {
            if let Some(end) = find_unescaped(rest, 2, b']') {
                let identifier = &rest[2..end];
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
                    cursor += end + 1;
                    continue;
                }
            }
        }

        if rest.starts_with('[') {
            if let Some((rendered, consumed)) = preserve_or_degrade_link(rest, carrier) {
                output.push_str(&rendered);
                cursor += consumed;
                continue;
            }
        }

        if rest.starts_with("<!--") {
            if let Some(end) = rest.find("-->") {
                cursor += end + 3;
            } else {
                output.push('‹');
                output.push_str("!--");
                cursor += 4;
            }
            continue;
        }

        if rest.starts_with('<') {
            if let Some(end) = rest.find('>') {
                let inner = &rest[1..end];
                if is_http_autolink(inner) {
                    output.push('[');
                    output.push_str(inner);
                    output.push_str("](");
                    output.push_str(inner);
                    output.push(')');
                } else if !looks_like_html_tag(inner) {
                    // Not an HTML tag, but raw angle syntax is outside both
                    // Lark carriers' supported subset. Keep it readable using
                    // inert Unicode brackets.
                    output.push('‹');
                    output.push_str(inner);
                    output.push('›');
                }
                cursor += end + 1;
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
    output
}

fn find_code_span_end(text: &str, mut cursor: usize, run: usize) -> Option<usize> {
    while cursor < text.len() {
        let rest = &text[cursor..];
        if rest.starts_with('`') {
            let candidate = rest.bytes().take_while(|byte| *byte == b'`').count();
            if candidate == run {
                return Some(cursor + run);
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
    None
}

fn degrade_image(rest: &str, carrier: MarkdownCarrier) -> Option<(String, usize)> {
    let label_end = find_unescaped(rest, 2, b']')?;
    let alt = sanitize_inline(&rest[2..label_end], carrier);
    let after = label_end + 1;
    let mut output = if alt.is_empty() {
        "Image".to_owned()
    } else {
        format!("Image: {alt}")
    };
    if rest.as_bytes().get(after) == Some(&b'(') {
        if let Some(end) = find_closing_paren(rest, after) {
            let target = rest[after + 1..end].trim();
            if !target.is_empty() {
                output.push_str(" (");
                output.push_str(target);
                output.push(')');
            }
            return Some((output, end + 1));
        }
        output.push_str(" (invalid image target)");
        return Some((output, rest.len()));
    }
    if rest.as_bytes().get(after) == Some(&b'[') {
        if let Some(reference_end) = find_unescaped(rest, after + 1, b']') {
            let identifier = &rest[after + 1..reference_end];
            output.push_str(" (reference ");
            output.push_str(if identifier.is_empty() {
                &rest[2..label_end]
            } else {
                identifier
            });
            output.push(')');
            return Some((output, reference_end + 1));
        }
    }
    Some((output, after))
}

fn preserve_or_degrade_link(rest: &str, carrier: MarkdownCarrier) -> Option<(String, usize)> {
    let label_end = find_unescaped(rest, 1, b']')?;
    let label = sanitize_inline(&rest[1..label_end], carrier);
    let after = label_end + 1;
    if rest.as_bytes().get(after) == Some(&b'(') {
        if let Some(end) = find_closing_paren(rest, after) {
            let target = &rest[after + 1..end];
            return Some((format!("[{label}]({target})"), end + 1));
        }
        return Some((format!("[{label}] (invalid link target)"), rest.len()));
    }
    if rest.as_bytes().get(after) == Some(&b'[') {
        if let Some(reference_end) = find_unescaped(rest, after + 1, b']') {
            let identifier = &rest[after + 1..reference_end];
            let identifier = if identifier.is_empty() {
                &rest[1..label_end]
            } else {
                identifier
            };
            return Some((
                format!("{label} (reference {identifier})"),
                reference_end + 1,
            ));
        }
    }
    Some((format!("[{label}]"), after))
}

fn find_unescaped(text: &str, start: usize, needle: u8) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut cursor = start;
    while cursor < bytes.len() {
        if bytes[cursor] == needle && (cursor == 0 || bytes[cursor - 1] != b'\\') {
            return Some(cursor);
        }
        cursor += 1;
    }
    None
}

fn find_closing_paren(text: &str, opening: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0_u32;
    let mut cursor = opening;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'\\' => cursor = cursor.saturating_add(1),
            b'(' => depth = depth.saturating_add(1),
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(cursor);
                }
            }
            _ => {}
        }
        cursor += 1;
    }
    None
}

fn is_http_autolink(inner: &str) -> bool {
    (inner.starts_with("https://") || inner.starts_with("http://"))
        && !inner.chars().any(char::is_whitespace)
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
