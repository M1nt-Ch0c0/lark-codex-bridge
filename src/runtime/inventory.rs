//! Bounded, redacted inventory of Codex MCP servers, skills, and sessions.
//!
//! Names and one-line skill summaries only. Commands, arguments, URLs, tokens,
//! and absolute home paths never enter the snapshot.

use std::fs;
use std::path::{Path, PathBuf};

use crate::limits::INVENTORY_MAX_ENTRIES;
use crate::store::{ThreadRow, ThreadStatus};

/// One skill entry safe to render on a Feishu card.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkillEntry {
    pub name: String,
    pub summary: Option<String>,
}

/// One session row already stored for the current scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionEntry {
    pub short_id: String,
    pub status: &'static str,
    pub current: bool,
}

/// Redacted inventory used by `/info`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InventorySnapshot {
    pub workspace: Option<String>,
    pub mcp: Vec<String>,
    pub skills: Vec<SkillEntry>,
    pub sessions: Vec<SessionEntry>,
}

/// Collects MCP and skill names from the local Codex home and workspace.
#[must_use]
pub fn collect_inventory(
    configured_codex_home: Option<&Path>,
    workspace: Option<&Path>,
    threads: &[ThreadRow],
    current_thread: Option<&str>,
) -> InventorySnapshot {
    let workspace_display = workspace.map(display_path);
    let codex_home = resolve_codex_home(configured_codex_home);
    let mcp = codex_home
        .as_deref()
        .map(list_mcp_servers)
        .unwrap_or_default();
    let mut skills = Vec::new();
    if let Some(home) = codex_home.as_deref() {
        push_skills(&mut skills, &home.join("skills"));
    }
    if let Some(cwd) = workspace {
        push_skills(&mut skills, &cwd.join(".codex").join("skills"));
        push_skills(&mut skills, &cwd.join(".agents").join("skills"));
        push_skills(&mut skills, &cwd.join("skills"));
    }
    skills.truncate(INVENTORY_MAX_ENTRIES);
    InventorySnapshot {
        workspace: workspace_display,
        mcp,
        skills,
        sessions: session_entries(threads, current_thread),
    }
}

fn resolve_codex_home(configured: Option<&Path>) -> Option<PathBuf> {
    if let Some(path) = configured.filter(|path| path.is_absolute()) {
        return Some(path.to_path_buf());
    }
    if let Ok(home) = std::env::var("CODEX_HOME") {
        let path = PathBuf::from(home);
        if path.is_absolute() {
            return Some(path);
        }
    }
    std::env::var("HOME")
        .ok()
        .map(|home| PathBuf::from(home).join(".codex"))
}

fn list_mcp_servers(codex_home: &Path) -> Vec<String> {
    let path = codex_home.join("config.toml");
    let Ok(bytes) = fs::read(&path) else {
        return Vec::new();
    };
    if bytes.len() > 256 * 1024 {
        return Vec::new();
    }
    let Ok(text) = String::from_utf8(bytes) else {
        return Vec::new();
    };
    let Ok(value) = toml::from_str::<toml::Value>(&text) else {
        return Vec::new();
    };
    let Some(table) = value.get("mcp_servers").and_then(toml::Value::as_table) else {
        return Vec::new();
    };
    let mut names = table
        .keys()
        .filter(|name| is_safe_label(name))
        .cloned()
        .collect::<Vec<_>>();
    names.sort();
    names.truncate(INVENTORY_MAX_ENTRIES);
    names
}

fn push_skills(skills: &mut Vec<SkillEntry>, dir: &Path) {
    if skills.len() >= INVENTORY_MAX_ENTRIES {
        return;
    }
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut names = Vec::new();
    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        let path = entry.path();
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_symlink() || !file_type.is_dir() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !is_safe_label(name) {
            continue;
        }
        names.push((name.to_owned(), path.join("SKILL.md")));
    }
    names.sort_by(|left, right| left.0.cmp(&right.0));
    for (name, skill_path) in names {
        if skills.len() >= INVENTORY_MAX_ENTRIES {
            break;
        }
        if skills.iter().any(|skill| skill.name == name) {
            continue;
        }
        skills.push(SkillEntry {
            summary: skill_summary(&skill_path),
            name,
        });
    }
}

fn skill_summary(path: &Path) -> Option<String> {
    let Ok(bytes) = fs::read(path) else {
        return None;
    };
    let text = String::from_utf8_lossy(&bytes[..bytes.len().min(2048)]);
    if let Some(front) = text.strip_prefix("---") {
        let body = front.splitn(2, "\n---").next().unwrap_or("");
        for line in body.lines() {
            let Some(value) = line.trim().strip_prefix("description:") else {
                continue;
            };
            let value = value.trim().trim_matches('"').trim_matches('\'');
            if !value.is_empty() {
                return Some(truncate_label(value, 80));
            }
        }
    }
    text.lines().find_map(|line| {
        line.trim()
            .strip_prefix('#')
            .map(str::trim)
            .filter(|title| !title.is_empty())
            .map(|title| truncate_label(title, 80))
    })
}

fn session_entries(threads: &[ThreadRow], current_thread: Option<&str>) -> Vec<SessionEntry> {
    threads
        .iter()
        .take(8)
        .map(|row| SessionEntry {
            short_id: short_id(&row.codex_thread_id),
            status: match row.status {
                ThreadStatus::Active => "进行中",
                ThreadStatus::Archived => "已归档",
            },
            current: current_thread == Some(row.codex_thread_id.as_str()),
        })
        .collect()
}

fn display_path(path: &Path) -> String {
    let raw = path.to_string_lossy();
    if let Ok(home) = std::env::var("HOME") {
        if let Some(rest) = raw.strip_prefix(&home) {
            return format!("~{rest}");
        }
    }
    let truncated: String = raw.chars().take(96).collect();
    if raw.chars().count() > 96 {
        format!("{truncated}…")
    } else {
        truncated
    }
}

fn short_id(id: &str) -> String {
    let take: String = id.chars().take(8).collect();
    if id.chars().count() > 8 {
        format!("{take}…")
    } else {
        take
    }
}

fn is_safe_label(name: &str) -> bool {
    if name.is_empty() || name == "." || name == ".." || name.chars().count() > 64 {
        return false;
    }
    name.chars().all(|ch| {
        !ch.is_control()
            && ch != '/'
            && ch != '\\'
            && (ch.is_alphanumeric() || matches!(ch, '-' | '_' | '.'))
    })
}

fn truncate_label(value: &str, max_chars: usize) -> String {
    let cleaned: String = value
        .chars()
        .filter(|ch| !ch.is_control() && *ch != '`' && *ch != '<' && *ch != '>')
        .collect();
    let mut text: String = cleaned.chars().take(max_chars).collect();
    if cleaned.chars().count() > max_chars {
        text.push('…');
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("lcb-inventory-{unique}"));
        fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[test]
    fn reads_mcp_names_and_skill_headings_without_commands() {
        let home = temp_dir();
        fs::write(
            home.join("config.toml"),
            "[mcp_servers.github]\ncommand = \"npx\"\nargs = [\"--token\", \"secret\"]\n\n[mcp_servers.docs]\ncommand = \"uvx\"\n",
        )
        .expect("config");
        let skill_dir = home.join("skills").join("review");
        fs::create_dir_all(&skill_dir).expect("skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            "# Code review\n\nDo not leak this.\n",
        )
        .expect("skill");

        let snapshot = collect_inventory(Some(&home), None, &[], None);
        assert!(snapshot.mcp.contains(&"github".to_owned()));
        assert!(snapshot.mcp.contains(&"docs".to_owned()));
        assert_eq!(snapshot.skills.len(), 1);
        assert_eq!(snapshot.skills[0].name, "review");
        assert_eq!(snapshot.skills[0].summary.as_deref(), Some("Code review"));
        let debug = format!("{snapshot:?}");
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("npx"));
        let _ = fs::remove_dir_all(home);
    }

    #[test]
    fn redacts_home_prefix_in_workspace() {
        let home = std::env::var("HOME").unwrap_or_else(|_| "/home/tester".to_owned());
        let path = PathBuf::from(&home).join("src").join("bridge");
        let snapshot = collect_inventory(None, Some(&path), &[], None);
        assert_eq!(snapshot.workspace.as_deref(), Some("~/src/bridge"));
    }

    #[test]
    fn keeps_unicode_skill_names_and_sorts_mcp() {
        let home = temp_dir();
        fs::write(
            home.join("config.toml"),
            "[mcp_servers.zeta]\ncommand = \"npx\"\n\n[mcp_servers.alpha]\ncommand = \"uvx\"\n",
        )
        .expect("config");
        let skill_dir = home.join("skills").join("会议纪要");
        fs::create_dir_all(&skill_dir).expect("skill dir");
        fs::write(
            skill_dir.join("SKILL.md"),
            "---\ndescription: \"整理一周会议\"\n---\n# ignored\n",
        )
        .expect("skill");

        let snapshot = collect_inventory(Some(&home), None, &[], None);
        assert_eq!(snapshot.mcp, vec!["alpha".to_owned(), "zeta".to_owned()]);
        assert_eq!(snapshot.skills.len(), 1);
        assert_eq!(snapshot.skills[0].name, "会议纪要");
        assert_eq!(snapshot.skills[0].summary.as_deref(), Some("整理一周会议"));
        let _ = fs::remove_dir_all(home);
    }
}
