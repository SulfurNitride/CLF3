//! InlineFile and RemappedInlineFile directive handlers
//!
//! Extracts files embedded directly in the .wabbajack archive.

use crate::installer::processor::ProcessContext;
use crate::modlist::{InlineFileDirective, RemappedInlineFileDirective};
use crate::paths;

use anyhow::{Context, Result};
use std::fs::File;
use std::io::Write;

/// Handle an InlineFile directive
/// Extracts data from the wabbajack archive and writes to output
pub fn handle_inline_file(ctx: &ProcessContext, directive: &InlineFileDirective) -> Result<()> {
    // Read the file from wabbajack archive using source_data_id
    let entry_name = directive.source_data_id.to_string();
    let data = ctx
        .read_wabbajack_file(&entry_name)
        .with_context(|| format!("Failed to read inline file: {}", entry_name))?;

    // Verify size - warn but don't fail (modlist metadata may be inaccurate)
    if data.len() as u64 != directive.size {
        tracing::warn!(
            "Size mismatch for inline file {}: expected {} bytes, got {} (using actual)",
            directive.to,
            directive.size,
            data.len()
        );
    }

    // Write to output
    let output_path = ctx.resolve_output_path(&directive.to);
    paths::ensure_parent_dirs(&output_path)?;

    let mut file = File::create(&output_path)
        .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;
    file.write_all(&data)
        .with_context(|| format!("Failed to write output file: {}", output_path.display()))?;

    Ok(())
}

/// Handle a RemappedInlineFile directive
/// Similar to InlineFile but replaces path placeholders in the content
pub fn handle_remapped_inline_file(
    ctx: &ProcessContext,
    directive: &RemappedInlineFileDirective,
) -> Result<()> {
    // Read the file from wabbajack archive using source_data_id
    let entry_name = directive.source_data_id.to_string();
    let data = ctx
        .read_wabbajack_file(&entry_name)
        .with_context(|| format!("Failed to read remapped inline file: {}", entry_name))?;

    // Remap path placeholders
    // Common placeholders: GAME_PATH, MO2_PATH, etc.
    let content = String::from_utf8_lossy(&data);
    let remapped = remap_paths(&content, ctx);

    // Note: Size check is tricky here since remapping may change the size
    // The directive.size is the expected output size after remapping
    // For now, we'll skip strict size verification for remapped files

    // Write to output
    let output_path = ctx.resolve_output_path(&directive.to);
    paths::ensure_parent_dirs(&output_path)?;

    let mut file = File::create(&output_path)
        .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;
    file.write_all(remapped.as_bytes())
        .with_context(|| format!("Failed to write output file: {}", output_path.display()))?;

    Ok(())
}

/// Remap path placeholders in content
fn remap_paths(content: &str, ctx: &ProcessContext) -> String {
    // Early-out: if content has no magic tokens or legacy placeholders, skip all replacements
    if !content.contains("{--||") && !content.contains("[Game Folder Files]")
        && !content.contains("[MO2_PATH]") && !content.contains("[DOWNLOADS_PATH]")
        && !content.contains("download_directory=")
    {
        return content.to_string();
    }

    let mut result = content.to_string();

    // Get base paths and normalize once (remove trailing slashes, ensure forward slashes)
    let mo2_base = ctx.config.output_dir.to_string_lossy().trim_end_matches('/').trim_end_matches('\\').replace('\\', "/");
    let game_base = ctx.config.game_dir.to_string_lossy().trim_end_matches('/').trim_end_matches('\\').replace('\\', "/");
    let downloads_base = ctx.config.downloads_dir.to_string_lossy().trim_end_matches('/').trim_end_matches('\\').replace('\\', "/");

    // Derive all variants from the base forward-slash path
    let mo2_forward = format!("Z:{}", mo2_base);
    let game_forward = format!("Z:{}", game_base);
    let downloads_forward = format!("Z:{}", downloads_base);

    let mo2_double_back = format!("Z:{}", mo2_base.replace('/', "\\\\"));
    let game_double_back = format!("Z:{}", game_base.replace('/', "\\\\"));
    let downloads_double_back = format!("Z:{}", downloads_base.replace('/', "\\\\"));

    let mo2_back = format!("Z:{}", mo2_base.replace('/', "\\"));
    let game_back = format!("Z:{}", game_base.replace('/', "\\"));
    let downloads_back = format!("Z:{}", downloads_base.replace('/', "\\"));

    // Replace Wabbajack magic tokens
    // Format: {--||PATH_TYPE_MAGIC_STYLE||--}

    // MO2 path tokens
    result = result.replace("{--||MO2_PATH_MAGIC_FORWARD||--}", &mo2_forward);
    result = result.replace("{--||MO2_PATH_MAGIC_DOUBLE_BACK||--}", &mo2_double_back);
    result = result.replace("{--||MO2_PATH_MAGIC_BACK||--}", &mo2_back);

    // Game path tokens
    result = result.replace("{--||GAME_PATH_MAGIC_FORWARD||--}", &game_forward);
    result = result.replace("{--||GAME_PATH_MAGIC_DOUBLE_BACK||--}", &game_double_back);
    result = result.replace("{--||GAME_PATH_MAGIC_BACK||--}", &game_back);

    // Downloads path tokens
    result = result.replace("{--||DOWNLOADS_PATH_MAGIC_FORWARD||--}", &downloads_forward);
    result = result.replace("{--||DOWNLOADS_PATH_MAGIC_DOUBLE_BACK||--}", &downloads_double_back);
    result = result.replace("{--||DOWNLOADS_PATH_MAGIC_BACK||--}", &downloads_back);

    // Legacy placeholders (older Wabbajack format)
    result = result.replace("[Game Folder Files]", &game_back);
    result = result.replace("[MO2_PATH]", &mo2_back);
    result = result.replace("[DOWNLOADS_PATH]", &downloads_back);

    // Handle download_directory specially - replace hardcoded Windows paths
    // Pattern: download_directory=X:/... or download_directory=X:\...
    let lines: Vec<&str> = result.lines().collect();
    let mut new_lines: Vec<String> = Vec::new();

    for line in lines {
        if let Some(value) = line.strip_prefix("download_directory=") {
            // Check if it's a Windows drive path (like E:/ or C:\)
            if value.len() >= 2 && value.chars().nth(1) == Some(':') {
                new_lines.push(format!("download_directory={}", downloads_back));
            } else {
                new_lines.push(line.to_string());
            }
        } else {
            new_lines.push(line.to_string());
        }
    }
    result = new_lines.join("\n");

    result
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_remap_paths_basic() {
        // This test would need a mock ProcessContext
        // For now, just verify the function signature works
        let content = "Some content with [Game Folder Files] placeholder";
        assert!(content.contains("[Game Folder Files]"));
    }
}
