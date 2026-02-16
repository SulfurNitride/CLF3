//! TransformedTexture directive handler
//!
//! Extracts a DDS texture from an archive, resizes/recompresses it to the
//! specified dimensions and format, then writes to the output directory.

use crate::installer::processor::ProcessContext;
use crate::modlist::TransformedTextureDirective;
use crate::paths;
use crate::textures::{process_texture_with_fallback, OutputFormat};

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Write};

/// Check texture formats and return unsupported ones
/// Returns a map of format_name -> list of texture paths
pub fn find_unsupported_formats<'a, I>(directives: I) -> HashMap<String, Vec<String>>
where
    I: Iterator<Item = &'a (i64, TransformedTextureDirective)>,
{
    let mut unsupported: HashMap<String, Vec<String>> = HashMap::new();

    for (_, directive) in directives {
        if OutputFormat::from_str(&directive.image_state.format).is_none() {
            unsupported
                .entry(directive.image_state.format.clone())
                .or_default()
                .push(directive.to.clone());
        }
    }

    unsupported
}

/// Prompt user about unsupported formats and return whether to continue
pub fn prompt_unsupported_formats(unsupported: &HashMap<String, Vec<String>>) -> bool {
    if unsupported.is_empty() {
        return true;
    }

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║           UNSUPPORTED TEXTURE FORMATS DETECTED              ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("The following texture formats are not yet supported by clf3:\n");

    for (format, paths) in unsupported {
        println!("  Format: {} ({} textures)", format, paths.len());
        // Show first 3 examples
        for path in paths.iter().take(3) {
            println!("    - {}", path);
        }
        if paths.len() > 3 {
            println!("    ... and {} more", paths.len() - 3);
        }
        println!();
    }

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  Please report these formats to the clf3 developers!        ║");
    println!("║  https://github.com/your-repo/clf3/issues                   ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("If you continue, these textures will use BC1 fallback (may look wrong).\n");
    print!("Continue with fallback? [y/N]: ");
    io::stdout().flush().ok();

    let mut input = String::new();
    if io::stdin().read_line(&mut input).is_ok() {
        let input = input.trim().to_lowercase();
        input == "y" || input == "yes"
    } else {
        false
    }
}

/// Thread-safe flag for fallback mode
static FALLBACK_MODE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Enable fallback mode (use BC1 for unsupported formats)
pub fn enable_fallback_mode() {
    FALLBACK_MODE.store(true, std::sync::atomic::Ordering::SeqCst);
}

/// Check if fallback mode is enabled
pub fn is_fallback_mode() -> bool {
    FALLBACK_MODE.load(std::sync::atomic::Ordering::SeqCst)
}

/// Handle a TransformedTexture directive
///
/// Takes source texture data (already extracted), processes it according to
/// ImageState (width, height, format), and writes the result.
pub fn handle_transformed_texture(
    ctx: &ProcessContext,
    directive: &TransformedTextureDirective,
    source_data: &[u8],
) -> Result<()> {
    // Parse output format from ImageState
    let output_format = match OutputFormat::from_str(&directive.image_state.format) {
        Some(fmt) => fmt,
        None => {
            if is_fallback_mode() {
                // User chose to continue with fallback
                tracing::warn!(
                    "Using BC1 fallback for unsupported format '{}': {}",
                    directive.image_state.format,
                    directive.to
                );
                OutputFormat::BC1
            } else {
                // This shouldn't happen if pre-scan was done, but just in case
                anyhow::bail!(
                    "UNSUPPORTED_TEXTURE_FORMAT: '{}' - Please report this!\n\
                     Texture: {}\n\
                     Dimensions: {}x{}",
                    directive.image_state.format,
                    directive.to,
                    directive.image_state.width,
                    directive.image_state.height
                );
            }
        }
    };

    // Process the texture with fallback (copies unchanged on decode failure)
    let (processed, was_fallback) = process_texture_with_fallback(
        source_data,
        directive.image_state.width,
        directive.image_state.height,
        output_format,
    )
    .with_context(|| {
        format!(
            "Failed to process texture {}x{} {:?}",
            directive.image_state.width, directive.image_state.height, directive.image_state.format
        )
    })?;

    if was_fallback {
        tracing::warn!(
            "Texture copied unchanged (unsupported format): {}",
            directive.to
        );
    }

    // Verify size (warning only - texture compression is not perfectly deterministic)
    if !was_fallback && processed.data.len() as u64 != directive.size {
        tracing::debug!(
            "Texture size differs: expected {} bytes, got {} (format: {})",
            directive.size,
            processed.data.len(),
            directive.image_state.format
        );
    }

    // Write to output
    let output_path = ctx.resolve_output_path(&directive.to);
    paths::ensure_parent_dirs(&output_path)?;

    let mut file = File::create(&output_path)
        .with_context(|| format!("Failed to create output file: {}", output_path.display()))?;
    file.write_all(&processed.data)
        .with_context(|| format!("Failed to write output file: {}", output_path.display()))?;

    Ok(())
}
