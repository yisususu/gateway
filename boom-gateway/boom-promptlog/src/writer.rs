use crate::config::PromptLogConfig;
use crate::entry::PromptLogEntry;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

/// Handle to the background prompt log writer.
///
/// Clone-safe handle that checks `should_capture()` against the live config.
/// The actual file I/O happens in a background tokio task.
#[derive(Clone)]
pub struct PromptLogWriter {
    config: Arc<ArcSwap<PromptLogConfig>>,
    sender: mpsc::UnboundedSender<PromptLogEntry>,
}

impl PromptLogWriter {
    /// Spawn the background writer task and return (writer_handle, sender).
    ///
    /// Boom-main keeps the writer in AppState and passes the sender to route handlers.
    /// The sender can also be cloned freely.
    pub fn spawn(config: PromptLogConfig) -> Self {
        let config = Arc::new(ArcSwap::from_pointee(config));
        let (sender, receiver) = mpsc::unbounded_channel();

        let config_clone = config.clone();
        tokio::spawn(async move {
            background_writer(receiver, config_clone).await;
        });

        Self { config, sender }
    }

    /// Check if this key/team should be captured.
    /// Call this BEFORE cloning the request body to avoid unnecessary work.
    pub fn should_capture(&self, key_hash: &str, team_id: Option<&str>) -> bool {
        self.config.load().should_capture(key_hash, team_id)
    }

    /// Get a clone of the sender for passing to stream wrappers.
    pub fn sender(&self) -> mpsc::UnboundedSender<PromptLogEntry> {
        self.sender.clone()
    }

    /// Send an entry to the background writer (non-blocking, fire-and-forget).
    pub fn send(&self, entry: PromptLogEntry) {
        if let Err(e) = self.sender.send(entry) {
            tracing::warn!("Prompt log channel closed, dropping entry: {}", e.0.request_id);
        }
    }

    /// Update config at runtime (hot-reload).
    pub fn update_config(&self, new_config: PromptLogConfig) {
        self.config.store(Arc::new(new_config));
    }

    /// Read a snapshot of the current config.
    pub fn config(&self) -> PromptLogConfig {
        self.config.load().as_ref().clone()
    }
}

/// State for an open log file.
struct OpenFile {
    file: tokio::fs::File,
    size: u64,
    sequence: u64,
}

/// Background writer loop.
async fn background_writer(
    mut receiver: mpsc::UnboundedReceiver<PromptLogEntry>,
    config: Arc<ArcSwap<PromptLogConfig>>,
) {
    // key_hash → open file state
    let mut open_files: HashMap<String, OpenFile> = HashMap::new();

    while let Some(entry) = receiver.recv().await {
        let cfg = config.load();
        let base_dir = PathBuf::from(&cfg.dir);
        let max_bytes = cfg.max_file_size_mb * 1024 * 1024;
        drop(cfg); // release config guard

        let key_dir = base_dir.join(&entry.key_hash);

        // Ensure directory exists.
        if let Err(e) = tokio::fs::create_dir_all(&key_dir).await {
            tracing::error!("Failed to create prompt log dir {:?}: {}", key_dir, e);
            continue;
        }

        // Serialize entry to a single JSON line.
        let json_line = match serde_json::to_string(&entry) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to serialize prompt log entry: {}", e);
                continue;
            }
        };
        let line_bytes = json_line.len() as u64;

        // Get or create open file for this key_hash.
        let of = match open_files.entry(entry.key_hash.clone()) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                // Scan directory for existing files to find max sequence.
                let seq = find_max_sequence(&key_dir).await + 1;
                let path = key_dir.join(format!("log_{:06}.jsonl", seq));
                match tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .await
                {
                    Ok(file) => {
                        // Get current file size for append mode.
                        let size = match tokio::fs::metadata(&path).await {
                            Ok(m) => m.len(),
                            Err(_) => 0,
                        };
                        e.insert(OpenFile { file, size, sequence: seq })
                    }
                    Err(err) => {
                        tracing::error!("Failed to open prompt log file {:?}: {}", path, err);
                        continue;
                    }
                }
            }
        };

        // Check if writing this line would exceed max file size.
        // If current file is non-empty and would overflow, rotate to a new file.
        if of.size > 0 && of.size + line_bytes > max_bytes {
            // Close current file by dropping it.
            let new_seq = of.sequence + 1;
            let path = key_dir.join(format!("log_{:06}.jsonl", new_seq));
            match tokio::fs::File::create(&path).await {
                Ok(file) => {
                    tracing::info!(
                        path = %path.display(),
                        key_hash = %entry.key_hash,
                        "Rotated prompt log file"
                    );
                    *of = OpenFile { file, size: 0, sequence: new_seq };
                }
                Err(err) => {
                    tracing::error!("Failed to create new prompt log file {:?}: {}", path, err);
                    continue;
                }
            }
        }

        // Write the line.
        if let Err(e) = of.file.write_all(json_line.as_bytes()).await {
            tracing::error!("Failed to write prompt log entry: {}", e);
        }
        if let Err(e) = of.file.write_all(b"\n").await {
            tracing::error!("Failed to write prompt log newline: {}", e);
        }
        of.size += line_bytes + 1; // +1 for newline
    }

    tracing::info!("Prompt log writer channel closed, exiting background task");
}

/// Scan a directory for existing log_*.jsonl files and return the max sequence number.
async fn find_max_sequence(dir: &std::path::Path) -> u64 {
    let mut max_seq: u64 = 0;
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(_) => return 0,
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(seq_str) = name_str
            .strip_prefix("log_")
            .and_then(|s| s.strip_suffix(".jsonl"))
        {
            if let Ok(seq) = seq_str.parse::<u64>() {
                max_seq = max_seq.max(seq);
            }
        }
    }
    max_seq
}
