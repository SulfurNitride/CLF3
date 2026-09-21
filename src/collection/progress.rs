//! Small, credential-free progress messages shared by CLI workers and hosts.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerEvent {
    Progress {
        phase: String,
        completed: u64,
        total: u64,
        item: String,
    },
    Completed {
        report_path: std::path::PathBuf,
    },
    Failed {
        message: String,
    },
    Cancelled,
}

pub type Progress<'a> = dyn Fn(WorkerEvent) + Sync + 'a;

pub fn report(progress: &Progress<'_>, phase: &str, completed: usize, total: usize, item: &str) {
    progress(WorkerEvent::Progress {
        phase: phase.into(),
        completed: completed as u64,
        total: total as u64,
        item: item.chars().take(300).collect(),
    });
}

/// Request/URL text must not become UI logs, including URLs inside error chains.
pub fn safe_message(message: &str) -> String {
    static URL: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)(?:https?|nxm)://[^\s]+").unwrap());
    URL.replace_all(message, "[source URL]")
        .chars()
        .filter(|c| !c.is_control() || *c == '\n')
        .take(1500)
        .collect()
}
