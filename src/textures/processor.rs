//! Texture processing using GPU (wgpu) for BC7, CPU (image_dds) for other formats
//!
//! DirectXTex is only used for legacy format DECODING (L8, RGB565, etc.)
//! All encoding goes through GPU (BC7) or CPU (BC4, BC5, BC3, BC1).

use anyhow::{anyhow, Context, Result};
use directxtex::{ScratchImage, DDS_FLAGS, DXGI_FORMAT, TEX_FILTER_FLAGS};
use image::{DynamicImage, RgbaImage};
use image_dds::{ddsfile::Dds, ImageFormat, Mipmaps, Quality, SurfaceRgba8};
use rayon::prelude::*;
use std::io::Cursor;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{debug, info, warn};

use super::gpu_encoder::GpuEncoder;

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
            "BC7" | "BC7_UNORM" | "BC7_UNORM_SRGB" | "BC7_SRGB" => Some(OutputFormat::BC7),
            "BC5" | "BC5_UNORM" | "BC5_SNORM" | "BC5_TYPELESS" => Some(OutputFormat::BC5),
            "BC4" | "BC4_UNORM" | "BC4_SNORM" | "BC4_TYPELESS" => Some(OutputFormat::BC4),
            "BC3" | "BC3_UNORM" | "BC3_UNORM_SRGB" | "BC3_SRGB" | "DXT5" => Some(OutputFormat::BC3),
            "BC2" | "BC2_UNORM" | "BC2_UNORM_SRGB" | "BC2_SRGB" | "DXT3" => Some(OutputFormat::BC2),
            "BC1" | "BC1_UNORM" | "BC1_UNORM_SRGB" | "BC1_SRGB" | "DXT1" => Some(OutputFormat::BC1),
            "RGBA" | "R8G8B8A8" | "R8G8B8A8_UNORM" | "R8G8B8A8_UNORM_SRGB" | "ARGB_8888" => Some(OutputFormat::Rgba),
            "BGRA" | "B8G8R8A8" | "B8G8R8A8_UNORM" | "B8G8R8A8_UNORM_SRGB" => Some(OutputFormat::Bgra),
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

/// Global GPU encoder (lazy initialized)
static GPU_ENCODER: std::sync::OnceLock<Arc<Mutex<Option<GpuEncoder>>>> = std::sync::OnceLock::new();

/// Initialize the global GPU encoder
pub fn init_gpu() -> Result<()> {
    let encoder = GPU_ENCODER.get_or_init(|| Arc::new(Mutex::new(None)));
    let mut lock = encoder.lock().expect("GPU encoder lock poisoned");
    if lock.is_none() {
        match GpuEncoder::new() {
            Ok(e) => {
                info!("GPU encoder initialized: {} ({})", e.gpu_info.name, e.gpu_info.backend);
                *lock = Some(e);
            }
            Err(e) => {
                warn!("GPU encoder not available: {}. BC7 will use CPU fallback.", e);
            }
        }
    }
    Ok(())
}

/// Get the global GPU encoder
fn get_gpu_encoder() -> Option<Arc<Mutex<Option<GpuEncoder>>>> {
    GPU_ENCODER.get().cloned()
}

/// Calculate mipmap levels for a texture
fn calculate_mip_levels(width: u32, height: u32) -> u32 {
    let max_dim = width.max(height);
    (max_dim as f32).log2().floor() as u32 + 1
}

/// Generate all mipmap levels from base image
fn generate_mipmaps(base: &RgbaImage) -> Vec<(Vec<u8>, u32, u32)> {
    let mut mips = Vec::new();
    let mut width = base.width();
    let mut height = base.height();

    // Base level
    mips.push((base.as_raw().clone(), width, height));

    // Generate mip chain
    let mut current = DynamicImage::ImageRgba8(base.clone());
    while width > 1 || height > 1 {
        width = (width / 2).max(1);
        height = (height / 2).max(1);

        current = current.resize_exact(width, height, image::imageops::FilterType::Lanczos3);
        mips.push((current.to_rgba8().into_raw(), width, height));
    }

    mips
}

/// Create BC7 DDS file with mipmap data
fn create_bc7_dds_with_mips(mip_data: Vec<Vec<u8>>, base_width: u32, base_height: u32) -> Result<Vec<u8>> {
    use image_dds::ddsfile::{AlphaMode, D3D10ResourceDimension, Dds, DxgiFormat, NewDxgiParams};

    let mip_count = mip_data.len() as u32;

    // Create DDS with DX10 header for BC7
    let params = NewDxgiParams {
        width: base_width,
        height: base_height,
        depth: None,
        format: DxgiFormat::BC7_UNorm,
        mipmap_levels: Some(mip_count),
        array_layers: None,
        caps2: None,
        is_cubemap: false,
        resource_dimension: D3D10ResourceDimension::Texture2D,
        alpha_mode: AlphaMode::Straight,
    };

    let mut dds = Dds::new_dxgi(params).context("Failed to create BC7 DDS header")?;

    // Concatenate all mipmap data
    let total_size: usize = mip_data.iter().map(|m| m.len()).sum();
    let mut combined_data = Vec::with_capacity(total_size);
    for mip in mip_data {
        combined_data.extend(mip);
    }
    dds.data = combined_data;

    let mut output = Vec::new();
    dds.write(&mut output).context("Failed to write BC7 DDS")?;

    Ok(output)
}

/// Process a DDS texture: decode, resize, re-encode
/// Uses GPU for BC7 encoding, CPU for other formats
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

    // For BC7, try GPU encoding first
    if output_format == OutputFormat::BC7 {
        if let Some(encoder_arc) = get_gpu_encoder() {
            if let Ok(mut guard) = encoder_arc.lock() {
                if let Some(ref mut encoder) = *guard {
                    match process_texture_bc7_gpu(encoder, &resized) {
                        Ok(data) => {
                            return Ok(ProcessedTexture {
                                data,
                                width: target_width,
                                height: target_height,
                                format: output_format,
                            });
                        }
                        Err(e) => {
                            warn!("GPU BC7 failed, using CPU: {}", e);
                        }
                    }
                }
            }
        }
    }

    // CPU path for non-BC7 or GPU fallback
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

/// Process texture to BC7 using GPU encoder
fn process_texture_bc7_gpu(encoder: &mut GpuEncoder, rgba: &RgbaImage) -> Result<Vec<u8>> {
    let width = rgba.width();
    let height = rgba.height();

    debug!("GPU BC7 encoding {}x{} with mipmaps", width, height);

    // Generate all mipmap levels on CPU
    let mips = generate_mipmaps(rgba);

    // Encode all mips on GPU using batch processing
    let mut batch = encoder.create_batch();
    for (mip_data, w, h) in &mips {
        encoder.queue_bc7(&mut batch, mip_data, *w, *h)?;
    }

    let mip_results = encoder.flush_batch(batch)?;

    // Create DDS with all mip levels
    create_bc7_dds_with_mips(mip_results, width, height)
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

/// Process multiple textures with GPU acceleration for BC7
/// BC7 textures are batched for GPU, other formats use parallel CPU
pub fn process_texture_batch(jobs: Vec<TextureJob>) -> Vec<(Option<String>, Result<ProcessedTexture>)> {
    let total = jobs.len();
    let completed = AtomicUsize::new(0);

    info!("Processing {} textures (GPU for BC7, CPU for others)", total);

    // Initialize GPU if not already done
    let _ = init_gpu();

    // Separate BC7 jobs from others
    let (bc7_jobs, other_jobs): (Vec<_>, Vec<_>) = jobs.into_iter().partition(|j| j.format == OutputFormat::BC7);

    let mut results: Vec<(Option<String>, Result<ProcessedTexture>)> = Vec::with_capacity(total);

    // Process BC7 jobs on GPU in batches
    if !bc7_jobs.is_empty() {
        info!("GPU batch processing {} BC7 textures", bc7_jobs.len());

        if let Some(encoder_arc) = get_gpu_encoder() {
            if let Ok(mut guard) = encoder_arc.lock() {
                if let Some(ref mut encoder) = *guard {
                    // Process BC7 in batches of 32 to avoid GPU memory pressure
                    for chunk in bc7_jobs.chunks(32) {
                        let chunk_results = process_bc7_batch_gpu(encoder, chunk, &completed);
                        results.extend(chunk_results);
                    }
                } else {
                    // No GPU, fall back to CPU
                    let cpu_results = process_batch_cpu(bc7_jobs, &completed);
                    results.extend(cpu_results);
                }
            } else {
                // Lock failed, fall back to CPU
                let cpu_results = process_batch_cpu(bc7_jobs, &completed);
                results.extend(cpu_results);
            }
        } else {
            // No GPU encoder, fall back to CPU
            let cpu_results = process_batch_cpu(bc7_jobs, &completed);
            results.extend(cpu_results);
        }
    }

    // Process non-BC7 jobs on CPU in parallel
    if !other_jobs.is_empty() {
        info!("CPU parallel processing {} non-BC7 textures", other_jobs.len());
        let cpu_results = process_batch_cpu(other_jobs, &completed);
        results.extend(cpu_results);
    }

    let success = results.iter().filter(|(_, r)| r.is_ok()).count();
    info!(
        "Texture processing complete: {}/{} succeeded",
        success, total
    );

    results
}

/// Process BC7 textures using GPU batch encoding
fn process_bc7_batch_gpu(
    encoder: &mut GpuEncoder,
    jobs: &[TextureJob],
    completed: &AtomicUsize,
) -> Vec<(Option<String>, Result<ProcessedTexture>)> {
    let mut results = Vec::with_capacity(jobs.len());

    // Decode and resize all textures (CPU)
    let decoded: Vec<_> = jobs
        .iter()
        .map(|job| {
            let decode_result = decode_dds_to_rgba(&job.data).and_then(|rgba| {
                let w = rgba.width();
                let h = rgba.height();
                if w != job.width || h != job.height {
                    let dynamic = DynamicImage::ImageRgba8(rgba);
                    Ok(dynamic
                        .resize_exact(job.width, job.height, image::imageops::FilterType::Lanczos3)
                        .into_rgba8())
                } else {
                    Ok(rgba)
                }
            });
            (job.id.clone(), job.width, job.height, decode_result)
        })
        .collect();

    // Separate successful decodes from failures
    let (good, bad): (Vec<_>, Vec<_>) = decoded.into_iter().partition(|(_, _, _, r)| r.is_ok());

    // Add failed decodes to results
    for (id, w, h, result) in bad {
        completed.fetch_add(1, Ordering::Relaxed);
        results.push((id, result.map(|rgba| ProcessedTexture {
            data: rgba.into_raw(),
            width: w,
            height: h,
            format: OutputFormat::BC7,
        })));
    }

    if good.is_empty() {
        return results;
    }

    // For each successful decode, generate mipmaps and queue for GPU
    let mut batch = encoder.create_batch();
    let mut job_mip_counts: Vec<(Option<String>, u32, u32, usize)> = Vec::new(); // (id, w, h, mip_count)

    for (id, w, h, result) in good {
        let rgba = result.unwrap();
        let mips = generate_mipmaps(&rgba);
        let mip_count = mips.len();

        for (mip_data, mw, mh) in mips {
            if let Err(e) = encoder.queue_bc7(&mut batch, &mip_data, mw, mh) {
                warn!("Failed to queue BC7 mip: {}", e);
            }
        }

        job_mip_counts.push((id, w, h, mip_count));
    }

    // Flush batch and get all encoded mips
    match encoder.flush_batch(batch) {
        Ok(all_mips) => {
            // Reassemble each texture's mips into a DDS
            let mut mip_offset = 0;
            for (id, w, h, mip_count) in job_mip_counts {
                let mip_data: Vec<Vec<u8>> = all_mips[mip_offset..mip_offset + mip_count].to_vec();
                mip_offset += mip_count;

                let result = create_bc7_dds_with_mips(mip_data, w, h).map(|data| ProcessedTexture {
                    data,
                    width: w,
                    height: h,
                    format: OutputFormat::BC7,
                });

                completed.fetch_add(1, Ordering::Relaxed);
                results.push((id, result));
            }
        }
        Err(e) => {
            // GPU batch failed, fall back to CPU for all
            warn!("GPU batch failed: {}, falling back to CPU", e);
            for (id, _w, _h, _) in job_mip_counts {
                completed.fetch_add(1, Ordering::Relaxed);
                results.push((id, Err(anyhow!("GPU batch failed: {}", e))));
            }
        }
    }

    results
}

/// Process textures using CPU (rayon parallel)
fn process_batch_cpu(
    jobs: Vec<TextureJob>,
    completed: &AtomicUsize,
) -> Vec<(Option<String>, Result<ProcessedTexture>)> {
    jobs.into_par_iter()
        .map(|job| {
            let result = process_texture_cpu(&job.data, job.width, job.height, job.format);
            completed.fetch_add(1, Ordering::Relaxed);
            (job.id, result)
        })
        .collect()
}

/// Process texture using CPU only (no GPU attempt)
fn process_texture_cpu(
    input_data: &[u8],
    target_width: u32,
    target_height: u32,
    output_format: OutputFormat,
) -> Result<ProcessedTexture> {
    let rgba = decode_dds_to_rgba(input_data)?;

    let current_w = rgba.width();
    let current_h = rgba.height();

    let resized = if current_w != target_width || current_h != target_height {
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

    let surface = SurfaceRgba8::from_image(&resized);
    let encoded = surface
        .encode(
            output_format.to_image_format(),
            Quality::Normal,
            Mipmaps::GeneratedAutomatic,
        )
        .context("Failed to encode texture")?;

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

/// Estimate the size of a DDS file for memory-aware batching.
///
/// Returns estimated bytes for the output DDS at the given dimensions and format.
/// Useful for planning batch sizes to avoid OOM.
pub fn estimate_dds_size(width: u32, height: u32, format: OutputFormat) -> u64 {
    let pixels = width as u64 * height as u64;
    let base_size = match format {
        // BCn formats: 4x4 block compression
        OutputFormat::BC1 => pixels / 2,        // 0.5 bytes/pixel (8 bytes per 4x4 block)
        OutputFormat::BC2 | OutputFormat::BC3 => pixels,  // 1 byte/pixel (16 bytes per 4x4 block)
        OutputFormat::BC4 => pixels / 2,        // 0.5 bytes/pixel
        OutputFormat::BC5 => pixels,            // 1 byte/pixel
        OutputFormat::BC7 => pixels,            // 1 byte/pixel
        // Uncompressed
        OutputFormat::Rgba | OutputFormat::Bgra => pixels * 4,  // 4 bytes/pixel
    };
    // Add ~33% for mipmaps + 148 bytes for DDS header
    base_size * 4 / 3 + 148
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
