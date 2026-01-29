//! Stock Game folder creation.
//!
//! The Stock Game folder is a copy of the original game installation.
//! This keeps the original game files clean while allowing root mods
//! (like SKSE, ENB) to be installed to the Stock Game copy.

use anyhow::{Context, Result};
use std::path::Path;
use walkdir::WalkDir;

/// Progress information for the copy operation.
#[derive(Debug, Clone)]
pub struct CopyProgress {
    /// Number of files copied so far.
    pub files_copied: u64,
    /// Total number of files to copy.
    pub total_files: u64,
    /// Bytes copied so far.
    pub bytes_copied: u64,
    /// Total bytes to copy.
    pub total_bytes: u64,
    /// Current file being copied.
    pub current_file: String,
}

/// Creates a Stock Game folder by copying the original game installation.
///
/// # Arguments
/// * `game_path` - Path to the original game installation
/// * `stock_game_path` - Path where the Stock Game copy will be created
/// * `progress_callback` - Optional callback for copy progress
///
/// # Returns
/// The total number of files copied.
pub fn create_stock_game<F>(
    game_path: &Path,
    stock_game_path: &Path,
    progress_callback: Option<F>,
) -> Result<u64>
where
    F: Fn(CopyProgress),
{
    // Validate source exists
    if !game_path.exists() {
        anyhow::bail!("Game path does not exist: {}", game_path.display());
    }

    // Create destination directory
    std::fs::create_dir_all(stock_game_path)
        .with_context(|| format!("Failed to create Stock Game directory: {}", stock_game_path.display()))?;

    // Count total files and bytes first
    let (total_files, total_bytes) = count_files_and_bytes(game_path)?;

    tracing::info!(
        "Copying {} files ({:.2} GB) to Stock Game folder",
        total_files,
        total_bytes as f64 / 1_073_741_824.0
    );

    let mut files_copied: u64 = 0;
    let mut bytes_copied: u64 = 0;

    // Walk the source directory and copy files
    for entry in WalkDir::new(game_path).follow_links(false) {
        let entry = entry.with_context(|| "Failed to read directory entry")?;
        let source_path = entry.path();

        // Calculate relative path
        let relative_path = source_path
            .strip_prefix(game_path)
            .with_context(|| "Failed to calculate relative path")?;

        let dest_path = stock_game_path.join(relative_path);

        if entry.file_type().is_dir() {
            // Create directory
            std::fs::create_dir_all(&dest_path)
                .with_context(|| format!("Failed to create directory: {}", dest_path.display()))?;
        } else if entry.file_type().is_file() {
            // Copy file
            let file_size = entry.metadata()
                .map(|m| m.len())
                .unwrap_or(0);

            // Report progress before copying
            if let Some(ref callback) = progress_callback {
                callback(CopyProgress {
                    files_copied,
                    total_files,
                    bytes_copied,
                    total_bytes,
                    current_file: relative_path.display().to_string(),
                });
            }

            // Ensure parent directory exists
            if let Some(parent) = dest_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            std::fs::copy(source_path, &dest_path)
                .with_context(|| format!("Failed to copy file: {}", source_path.display()))?;

            files_copied += 1;
            bytes_copied += file_size;
        }
        // Skip symlinks for now - could add symlink handling if needed
    }

    // Final progress report
    if let Some(ref callback) = progress_callback {
        callback(CopyProgress {
            files_copied,
            total_files,
            bytes_copied,
            total_bytes,
            current_file: "Complete".to_string(),
        });
    }

    tracing::info!("Stock Game folder created: {} files copied", files_copied);
    Ok(files_copied)
}

/// Counts the total number of files and bytes in a directory.
fn count_files_and_bytes(path: &Path) -> Result<(u64, u64)> {
    let mut file_count: u64 = 0;
    let mut byte_count: u64 = 0;

    for entry in WalkDir::new(path).follow_links(false) {
        let entry = entry?;
        if entry.file_type().is_file() {
            file_count += 1;
            byte_count += entry.metadata().map(|m| m.len()).unwrap_or(0);
        }
    }

    Ok((file_count, byte_count))
}

/// Verifies that a Stock Game folder is valid by checking for essential files.
pub fn verify_stock_game(stock_game_path: &Path, game_exe: &str) -> Result<()> {
    let exe_path = stock_game_path.join(game_exe);
    if !exe_path.exists() {
        anyhow::bail!(
            "Stock Game verification failed: {} not found",
            game_exe
        );
    }

    let data_path = stock_game_path.join("Data");
    if !data_path.exists() {
        anyhow::bail!("Stock Game verification failed: Data folder not found");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_stock_game() {
        // Create a mock game directory
        let source_dir = TempDir::new().unwrap();
        let source_path = source_dir.path();

        // Create some test files
        std::fs::create_dir_all(source_path.join("Data")).unwrap();
        std::fs::write(source_path.join("GameExe.exe"), b"mock exe").unwrap();
        std::fs::write(source_path.join("Data/plugin.esp"), b"mock plugin").unwrap();

        // Create Stock Game copy
        let dest_dir = TempDir::new().unwrap();
        let dest_path = dest_dir.path().join("Stock Game");

        let files_copied = create_stock_game::<fn(CopyProgress)>(
            source_path,
            &dest_path,
            None,
        ).unwrap();

        assert_eq!(files_copied, 2);
        assert!(dest_path.join("GameExe.exe").exists());
        assert!(dest_path.join("Data/plugin.esp").exists());
    }

    #[test]
    fn test_verify_stock_game() {
        let dir = TempDir::new().unwrap();
        let path = dir.path();

        // Should fail - no exe
        assert!(verify_stock_game(path, "Game.exe").is_err());

        // Add exe but no Data
        std::fs::write(path.join("Game.exe"), b"").unwrap();
        assert!(verify_stock_game(path, "Game.exe").is_err());

        // Add Data folder
        std::fs::create_dir(path.join("Data")).unwrap();
        assert!(verify_stock_game(path, "Game.exe").is_ok());
    }
}
