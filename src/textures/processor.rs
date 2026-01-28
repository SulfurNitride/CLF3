//! Texture processing using image_dds (pure Rust) with DirectXTex fallback

use anyhow::{anyhow, Context, Result};
use directxtex::{ScratchImage, DDS_FLAGS, DXGI_FORMAT, TEX_FILTER_FLAGS};
use image::{DynamicImage, RgbaImage};
use image_dds::{ddsfile::Dds, ImageFormat, Mipmaps, Quality, SurfaceRgba8};
use rayon::prelude::*;
use std::io::Cursor;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{debug, info, warn};

/// Supported output compression formats
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    /// BC7 - High quality, best for diffuse/color textures
    BC7,
    /// BC5 - Two channel, ideal for normal maps
    BC5,
    /// BC4 - Single channel, good for grayscale (height, parallax)
    BC4,
    /// BC3 - DXT5, good for textures with alpha
    BC3,
    /// BC2 - DXT3, explicit 4-bit alpha (sharp alpha transitions)
    BC2,
    /// BC1 - DXT1, smallest size, no/1-bit alpha
    BC1,
    /// Uncompressed RGBA
    Rgba,
    /// Uncompressed BGRA (B8G8R8A8)
    Bgra,
}

impl OutputFormat {
    /// Convert to image_dds ImageFormat
    fn to_image_format(self) -> ImageFormat {
        match self {
            OutputFormat::BC7 => ImageFormat::BC7RgbaUnorm,
            OutputFormat::BC5 => ImageFormat::BC5RgUnorm,
            OutputFormat::BC4 => ImageFormat::BC4RUnorm,
            OutputFormat::BC3 => ImageFormat::BC3RgbaUnorm,
            OutputFormat::BC2 => ImageFormat::BC2RgbaUnorm,
            OutputFormat::BC1 => ImageFormat::BC1RgbaUnorm,
            OutputFormat::Rgba => ImageFormat::Rgba8Unorm,
            OutputFormat::Bgra => ImageFormat::Bgra8Unorm,
        }
    }

    /// Get format name for logging
    pub fn name(&self) -> &'static str {
        match self {
            OutputFormat::BC7 => "BC7",
            OutputFormat::BC5 => "BC5",
            OutputFormat::BC4 => "BC4",
            OutputFormat::BC3 => "BC3",
            OutputFormat::BC2 => "BC2",
            OutputFormat::BC1 => "BC1",
            OutputFormat::Rgba => "RGBA",
            OutputFormat::Bgra => "BGRA",
        }
    }

    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "BC7" | "BC7_UNORM" => Some(OutputFormat::BC7),
            "BC5" | "BC5_UNORM" => Some(OutputFormat::BC5),
            "BC4" | "BC4_UNORM" => Some(OutputFormat::BC4),
            "BC3" | "BC3_UNORM" | "DXT5" => Some(OutputFormat::BC3),
            "BC2" | "BC2_UNORM" | "DXT3" => Some(OutputFormat::BC2),
            "BC1" | "BC1_UNORM" | "DXT1" => Some(OutputFormat::BC1),
            "RGBA" | "R8G8B8A8" | "R8G8B8A8_UNORM" | "ARGB_8888" => Some(OutputFormat::Rgba),
            "BGRA" | "B8G8R8A8" | "B8G8R8A8_UNORM" => Some(OutputFormat::Bgra),
            _ => None,
        }
    }
}

/// Result of texture processing
#[derive(Debug)]
pub struct ProcessedTexture {
    /// DDS file data ready to write
    pub data: Vec<u8>,
    /// Final width
    pub width: u32,
    /// Final height
    pub height: u32,
    /// Output format used
    pub format: OutputFormat,
}

/// Basic texture info
#[derive(Debug, Clone)]
pub struct TextureInfo {
    pub width: u32,
    pub height: u32,
    pub format: String,
    pub mip_count: u32,
}

/// Job for batch texture processing
#[derive(Debug, Clone)]
pub struct TextureJob {
    /// Source data
    pub data: Vec<u8>,
    /// Target dimensions
    pub width: u32,
    pub height: u32,
    /// Output format
    pub format: OutputFormat,
    /// Optional identifier for tracking
    pub id: Option<String>,
}

/// Decode DDS to RGBA using image_dds, with DirectXTex fallback
fn decode_dds_to_rgba(input_data: &[u8]) -> Result<RgbaImage> {
    let cursor = Cursor::new(input_data);
    let dds = Dds::read(cursor).context("Failed to parse DDS")?;

    match image_dds::image_from_dds(&dds, 0) {
        Ok(rgba) => Ok(rgba),
        Err(e) => {
            // Fallback to DirectXTex for legacy formats
            let format_info = get_format_info(&dds);
            debug!(
                "image_dds failed for {}, trying DirectXTex: {}",
                format_info, e
            );

            decode_with_directxtex(input_data).map_err(|dtx_err| {
                anyhow!(
                    "Failed to decode DDS (format: {}). image_dds: {}, DirectXTex: {}",
                    format_info,
                    e,
                    dtx_err
                )
            })
        }
    }
}

/// Decode DDS using DirectXTex (handles legacy L8, RGB565, etc.)
fn decode_with_directxtex(input_data: &[u8]) -> Result<RgbaImage> {
    let flags = DDS_FLAGS::DDS_FLAGS_ALLOW_LARGE_FILES | DDS_FLAGS::DDS_FLAGS_EXPAND_LUMINANCE;

    let scratch =
        ScratchImage::load_dds(input_data, flags, None, None).context("DirectXTex: load failed")?;

    let metadata = scratch.metadata();
    let width = metadata.width as u32;
    let height = metadata.height as u32;

    // Convert to RGBA if needed
    let rgba_scratch = if metadata.format != DXGI_FORMAT::DXGI_FORMAT_R8G8B8A8_UNORM {
        info!("DirectXTex: converting from {:?} to RGBA", metadata.format);
        scratch
            .convert(
                DXGI_FORMAT::DXGI_FORMAT_R8G8B8A8_UNORM,
                TEX_FILTER_FLAGS::TEX_FILTER_DEFAULT,
                0.5,
            )
            .context("DirectXTex: convert failed")?
    } else {
        scratch
    };

    let images = rgba_scratch.images();
    if images.is_empty() {
        anyhow::bail!("DirectXTex: no images in scratch");
    }

    let image = &images[0];

    // Copy pixel data
    let pixel_data =
        unsafe { std::slice::from_raw_parts(image.pixels, image.slice_pitch) };

    RgbaImage::from_raw(width, height, pixel_data.to_vec())
        .context("DirectXTex: failed to create RgbaImage")
}

/// Process a DDS texture: decode, resize, re-encode
pub fn process_texture(
    input_data: &[u8],
    target_width: u32,
    target_height: u32,
    output_format: OutputFormat,
) -> Result<ProcessedTexture> {
    // Decode to RGBA
    let rgba = decode_dds_to_rgba(input_data)?;

    let current_w = rgba.width();
    let current_h = rgba.height();

    // Resize if needed
    let resized = if current_w != target_width || current_h != target_height {
        debug!(
            "Resizing {}x{} -> {}x{}",
            current_w, current_h, target_width, target_height
        );
        let dynamic = DynamicImage::ImageRgba8(rgba);
        dynamic
            .resize_exact(
                target_width,
                target_height,
                image::imageops::FilterType::Lanczos3,
            )
            .into_rgba8()
    } else {
        rgba
    };

    // Encode with mipmaps
    let surface = SurfaceRgba8::from_image(&resized);
    let encoded = surface
        .encode(
            output_format.to_image_format(),
            Quality::Normal,
            Mipmaps::GeneratedAutomatic,
        )
        .context("Failed to encode texture")?;

    // Convert to DDS bytes
    let output_dds = encoded.to_dds().context("Failed to create DDS")?;
    let mut output_data = Vec::new();
    output_dds
        .write(&mut output_data)
        .context("Failed to write DDS")?;

    Ok(ProcessedTexture {
        data: output_data,
        width: target_width,
        height: target_height,
        format: output_format,
    })
}

/// Process texture with fallback to copying unchanged on decode failure
pub fn process_texture_with_fallback(
    input_data: &[u8],
    target_width: u32,
    target_height: u32,
    output_format: OutputFormat,
) -> Result<(ProcessedTexture, bool)> {
    match process_texture(input_data, target_width, target_height, output_format) {
        Ok(result) => Ok((result, false)),
        Err(e) => {
            let err_str = e.to_string();
            if err_str.contains("Failed to decode") || err_str.contains("Failed to parse") {
                warn!("Unsupported DDS format, copying unchanged: {}", err_str);
                Ok((
                    ProcessedTexture {
                        data: input_data.to_vec(),
                        width: target_width,
                        height: target_height,
                        format: output_format,
                    },
                    true,
                ))
            } else {
                Err(e)
            }
        }
    }
}

/// Resize texture, keeping original format if possible
pub fn resize_texture(
    input_data: &[u8],
    target_width: u32,
    target_height: u32,
) -> Result<ProcessedTexture> {
    let cursor = Cursor::new(input_data);
    let dds = Dds::read(cursor).context("Failed to parse DDS")?;

    let output_format = detect_output_format(&dds).unwrap_or(OutputFormat::BC7);
    process_texture(input_data, target_width, target_height, output_format)
}

/// Process multiple textures in parallel
pub fn process_texture_batch(jobs: Vec<TextureJob>) -> Vec<(Option<String>, Result<ProcessedTexture>)> {
    let total = jobs.len();
    let completed = AtomicUsize::new(0);

    info!("Processing {} textures in parallel", total);

    let results: Vec<_> = jobs
        .into_par_iter()
        .map(|job| {
            let result = process_texture(&job.data, job.width, job.height, job.format);

            let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
            if done.is_multiple_of(100) || done == total {
                debug!("Texture progress: {}/{}", done, total);
            }

            (job.id, result)
        })
        .collect();

    let success = results.iter().filter(|(_, r)| r.is_ok()).count();
    info!(
        "Texture processing complete: {}/{} succeeded",
        success, total
    );

    results
}

/// Get texture info without full decode
pub fn get_texture_info(data: &[u8]) -> Result<TextureInfo> {
    let cursor = Cursor::new(data);
    let dds = Dds::read(cursor).context("Failed to parse DDS")?;

    let format = if let Ok(img_format) = image_dds::dds_image_format(&dds) {
        format!("{:?}", img_format)
    } else {
        "UNKNOWN".to_string()
    };

    Ok(TextureInfo {
        width: dds.header.width,
        height: dds.header.height,
        format,
        mip_count: dds.header.mip_map_count.unwrap_or(1),
    })
}

/// Detect appropriate output format from input DDS
fn detect_output_format(dds: &Dds) -> Option<OutputFormat> {
    if let Ok(format) = image_dds::dds_image_format(dds) {
        match format {
            ImageFormat::BC7RgbaUnorm | ImageFormat::BC7RgbaUnormSrgb => Some(OutputFormat::BC7),
            ImageFormat::BC5RgUnorm | ImageFormat::BC5RgSnorm => Some(OutputFormat::BC5),
            ImageFormat::BC4RUnorm | ImageFormat::BC4RSnorm => Some(OutputFormat::BC4),
            ImageFormat::BC3RgbaUnorm | ImageFormat::BC3RgbaUnormSrgb => Some(OutputFormat::BC3),
            ImageFormat::BC2RgbaUnorm | ImageFormat::BC2RgbaUnormSrgb => Some(OutputFormat::BC2),
            ImageFormat::BC1RgbaUnorm | ImageFormat::BC1RgbaUnormSrgb => Some(OutputFormat::BC1),
            ImageFormat::Rgba8Unorm | ImageFormat::Rgba8UnormSrgb => Some(OutputFormat::Rgba),
            ImageFormat::Bgra8Unorm | ImageFormat::Bgra8UnormSrgb => Some(OutputFormat::Bgra),
            _ => None,
        }
    } else {
        None
    }
}

/// Get format info string for error messages
fn get_format_info(dds: &Dds) -> String {
    if let Ok(format) = image_dds::dds_image_format(dds) {
        return format!("{:?}", format);
    }

    let header = &dds.header;
    let mut info = format!("{}x{}", header.width, header.height);

    let pf = &dds.header.spf;
    if let Some(ref fourcc) = pf.fourcc {
        let bytes = fourcc.0.to_le_bytes();
        let fourcc_str = std::str::from_utf8(&bytes).unwrap_or("????");
        info.push_str(&format!(", FOURCC={}", fourcc_str));
    } else {
        info.push_str(&format!(
            ", RGBBitCount={}",
            pf.rgb_bit_count.unwrap_or(0)
        ));
    }

    if dds.header10.is_some() {
        info.push_str(", DX10+");
    }

    info
}

/// Process a texture file from disk
pub fn process_texture_file(
    input_path: &Path,
    output_path: &Path,
    target_width: u32,
    target_height: u32,
    output_format: OutputFormat,
) -> Result<ProcessedTexture> {
    let input_data = std::fs::read(input_path)
        .with_context(|| format!("Failed to read: {:?}", input_path))?;

    let result = process_texture(&input_data, target_width, target_height, output_format)?;

    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(output_path, &result.data)
        .with_context(|| format!("Failed to write: {:?}", output_path))?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_output_format_from_str() {
        assert_eq!(OutputFormat::from_str("BC7"), Some(OutputFormat::BC7));
        assert_eq!(OutputFormat::from_str("bc7_unorm"), Some(OutputFormat::BC7));
        assert_eq!(OutputFormat::from_str("DXT5"), Some(OutputFormat::BC3));
        assert_eq!(OutputFormat::from_str("DXT1"), Some(OutputFormat::BC1));
        assert_eq!(OutputFormat::from_str("RGBA"), Some(OutputFormat::Rgba));
        assert_eq!(OutputFormat::from_str("unknown"), None);
    }

    #[test]
    fn test_format_names() {
        assert_eq!(OutputFormat::BC7.name(), "BC7");
        assert_eq!(OutputFormat::BC1.name(), "BC1");
        assert_eq!(OutputFormat::Rgba.name(), "RGBA");
    }
}
